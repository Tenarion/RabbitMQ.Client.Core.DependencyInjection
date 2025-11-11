using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client.Core.DependencyInjection.Exceptions;
using RabbitMQ.Client.Core.DependencyInjection.InternalExtensions.Validation;
using RabbitMQ.Client.Core.DependencyInjection.Models;
using RabbitMQ.Client.Core.DependencyInjection.Services.Interfaces;
using RabbitMQ.Client.Events;

namespace RabbitMQ.Client.Core.DependencyInjection.Services
{
    /// <inheritdoc cref="IProducingService"/>
    public sealed class ProducingService : IProducingService, IAsyncDisposable
    {
        /// <inheritdoc/>
        public IConnection? Connection { get; private set; }

        /// <inheritdoc/>
        public IChannel? Channel { get; private set; }

        private readonly IReadOnlyCollection<RabbitMqExchange> _exchanges;
        private readonly Lock _lock = new();

        private const int QueueExpirationTime = 60000;

        public ProducingService(IEnumerable<RabbitMqExchange> exchanges)
        {
            _exchanges = exchanges.Where(x => x.IsProducing).ToList();
        }

        /// <inheritdoc/>
        public async ValueTask DisposeAsync()
        {
            if (Channel?.IsOpen == true)
            {
                await Channel.CloseAsync((int)HttpStatusCode.OK, "Channel closed");
            }

            if (Connection?.IsOpen == true)
            {
                await Connection.CloseAsync();
            }

            Channel?.Dispose();
            Connection?.Dispose();
        }

        /// <inheritdoc/>
        public void UseConnection(IConnection connection)
        {
            Connection = connection;
        }

        /// <inheritdoc/>
        public void UseChannel(IChannel channel)
        {
            Channel = channel;
        }

        /// <inheritdoc/>
        public async Task SendAsync<T>(T @object, string exchangeName, string routingKey) where T : class
        {
            EnsureProducingChannelIsNotNull();
            ValidateArguments(exchangeName, routingKey);
            var json = JsonSerializer.Serialize(@object);
            var bytes = Encoding.UTF8.GetBytes(json);
            var properties = CreateJsonProperties();
            await SendAsync(bytes, properties, exchangeName, routingKey);
        }

        /// <inheritdoc/>
        public async Task SendAsync<T>(T @object, string exchangeName, string routingKey, int millisecondsDelay)
            where T : class
        {
            EnsureProducingChannelIsNotNull();
            ValidateArguments(exchangeName, routingKey);
            var deadLetterExchange = GetDeadLetterExchange(exchangeName);
            var delayedQueueName =
                await DeclareDelayedQueue(exchangeName, deadLetterExchange, routingKey, millisecondsDelay);
            await SendAsync(@object, deadLetterExchange, delayedQueueName);
        }

        /// <inheritdoc/>
        public async Task SendJsonAsync(string json, string exchangeName, string routingKey)
        {
            EnsureProducingChannelIsNotNull();
            ValidateArguments(exchangeName, routingKey);
            var bytes = Encoding.UTF8.GetBytes(json);
            var properties = CreateJsonProperties();
            await SendAsync(bytes, properties, exchangeName, routingKey);
        }

        /// <inheritdoc/>
        public async Task SendJsonAsync(string json, string exchangeName, string routingKey, int millisecondsDelay)
        {
            EnsureProducingChannelIsNotNull();
            ValidateArguments(exchangeName, routingKey);
            var deadLetterExchange = GetDeadLetterExchange(exchangeName);
            var delayedQueueName =
                await DeclareDelayedQueue(exchangeName, deadLetterExchange, routingKey, millisecondsDelay);
            await SendJsonAsync(json, deadLetterExchange, delayedQueueName);
        }

        /// <inheritdoc/>
        public async Task SendStringAsync(string message, string exchangeName, string routingKey)
        {
            EnsureProducingChannelIsNotNull();
            ValidateArguments(exchangeName, routingKey);
            var bytes = Encoding.UTF8.GetBytes(message);
            await SendAsync(bytes, CreateProperties(), exchangeName, routingKey);
        }

        /// <inheritdoc/>
        public async Task SendStringAsync(string message, string exchangeName, string routingKey, int millisecondsDelay)
        {
            EnsureProducingChannelIsNotNull();
            ValidateArguments(exchangeName, routingKey);
            var deadLetterExchange = GetDeadLetterExchange(exchangeName);
            var delayedQueueName =
                await DeclareDelayedQueue(exchangeName, deadLetterExchange, routingKey, millisecondsDelay);
            await SendStringAsync(message, deadLetterExchange, delayedQueueName);
        }

        /// <inheritdoc/>
        public async Task SendAsync(ReadOnlyMemory<byte> bytes, BasicProperties properties, string exchangeName,
            string routingKey)
        {
            EnsureProducingChannelIsNotNull();
            ValidateArguments(exchangeName, routingKey);

            ValueTask publishTask;
            lock (_lock)
            {
                publishTask = Channel!.BasicPublishAsync(
                    exchange: exchangeName,
                    routingKey: routingKey,
                    basicProperties: properties,
                    body: bytes,
                    mandatory: false);
            }

            await publishTask.ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public async Task SendAsync(ReadOnlyMemory<byte> bytes, BasicProperties properties, string exchangeName,
            string routingKey,
            int millisecondsDelay)
        {
            EnsureProducingChannelIsNotNull();
            ValidateArguments(exchangeName, routingKey);
            var deadLetterExchange = GetDeadLetterExchange(exchangeName);
            var delayedQueueName =
                await DeclareDelayedQueue(exchangeName, deadLetterExchange, routingKey, millisecondsDelay);
            await SendAsync(bytes, properties, deadLetterExchange, delayedQueueName);
        }

        /// <inheritdoc/>
        public async Task<TResponse?> SendRpcAsync<TResponse>(ReadOnlyMemory<byte> bytes, BasicProperties properties,
            string exchangeName,
            string routingKey,
            TimeSpan timeout,
            Func<TaskCompletionSource<TResponse?>, object, BasicDeliverEventArgs, Task>? onResponseReceived = null,
            Action<TaskCompletionSource<TResponse?>>? onTimeout = null)
        {
            onResponseReceived ??= async (tcs, obj, ea) =>
            {
                tcs.TrySetResult(ea.GetPayload<TResponse?>());
                await Task.CompletedTask;
            };

            onTimeout ??= tcs => tcs.TrySetException(new TimeoutException("RPC response timed out"));

            var corrId = Guid.NewGuid().ToString();
            var q = await Channel!.QueueDeclareAsync(queue: string.Empty, durable: false, exclusive: true,
                autoDelete: true,
                arguments: null);
            var replyQueue = q.QueueName;

            var tcs = new TaskCompletionSource<TResponse?>(TaskCreationOptions.RunContinuationsAsynchronously);

            var consumer = new AsyncEventingBasicConsumer(Channel);
            consumer.ReceivedAsync += async (obj, ea) =>
            {
                var props = ea.BasicProperties;
                if (props == null)
                    return;

                if (props.CorrelationId != corrId)
                {
                    // not our ack
                    return;
                }

                await onResponseReceived(tcs, obj, ea);

                await Task.CompletedTask;
            };

            var consumerTag = await Channel.BasicConsumeAsync(queue: replyQueue, autoAck: true, consumer: consumer);

            var propsToSend = new BasicProperties(properties)
            {
                ReplyTo = replyQueue,
                CorrelationId = corrId,
            };

            await SendAsync(bytes, propsToSend, exchangeName, routingKey);

            // now wait for either an ack or a timeout;
            using var cts = new CancellationTokenSource(timeout);
            await using (cts.Token.Register(() => onTimeout(tcs)))
            {
                var response = await tcs.Task;
                await Channel.BasicCancelAsync(consumerTag);
                await Channel.QueueDeleteAsync(replyQueue);
                return response;
            }
        }

        /// <inheritdoc/>
        public async Task<TResponse?> SendRpcAsync<TRequest, TResponse>(TRequest request, string exchangeName,
            string routingKey,
            TimeSpan timeout,
            Func<TaskCompletionSource<TResponse?>, object, BasicDeliverEventArgs, Task>? onResponseReceived = null,
            Action<TaskCompletionSource<TResponse?>>? onTimeout = null)
        {
            var json = JsonSerializer.Serialize(request);
            var bytes = Encoding.UTF8.GetBytes(json);
            var properties = CreateJsonProperties();
            return await SendRpcAsync(bytes, properties, exchangeName, routingKey, timeout,
                onResponseReceived, onTimeout);
        }

        /// <inheritdoc/>
        public async Task<TResponse?> SendRpcAsync<TRequest, TResponse>(TRequest request, string exchangeName,
            string routingKey,
            int millisecondsDelay,
            TimeSpan timeout,
            Func<TaskCompletionSource<TResponse?>, object, BasicDeliverEventArgs, Task>? onResponseReceived = null,
            Action<TaskCompletionSource<TResponse?>>? onTimeout = null)
        {
            var json = JsonSerializer.Serialize(request);
            var bytes = Encoding.UTF8.GetBytes(json);
            var properties = CreateJsonProperties();
            EnsureProducingChannelIsNotNull();
            ValidateArguments(exchangeName, routingKey);
            var deadLetterExchange = GetDeadLetterExchange(exchangeName);
            var delayedQueueName =
                await DeclareDelayedQueue(exchangeName, deadLetterExchange, routingKey, millisecondsDelay);
            return await SendRpcAsync(bytes, properties, deadLetterExchange, delayedQueueName, timeout,
                onResponseReceived, onTimeout);
        }

        /// <inheritdoc/>
        public async Task<TResponse?> SendRpcAsync<TResponse>(string json, string exchangeName,
            string routingKey,
            TimeSpan timeout,
            Func<TaskCompletionSource<TResponse?>, object, BasicDeliverEventArgs, Task>? onResponseReceived = null,
            Action<TaskCompletionSource<TResponse?>>? onTimeout = null)
        {
            var bytes = Encoding.UTF8.GetBytes(json);
            var properties = CreateJsonProperties();
            return await SendRpcAsync(bytes, properties, exchangeName, routingKey, timeout,
                onResponseReceived, onTimeout);
        }

        /// <inheritdoc/>
        public async Task<bool> SendRpcResponseAsync<T>(
            T response,
            string? replyTo,
            string? correlationId) where T : class
        {
            if (string.IsNullOrEmpty(replyTo) || string.IsNullOrEmpty(correlationId))
                return false;

            try
            {
                var json = JsonSerializer.Serialize(response);
                var bytes = Encoding.UTF8.GetBytes(json);
                var properties = new BasicProperties
                {
                    CorrelationId = correlationId,
                    ContentType = "application/json"
                };

                await Channel!.BasicPublishAsync(
                    exchange: string.Empty,
                    routingKey: replyTo,
                    body: bytes,
                    basicProperties: properties,
                    mandatory: false);

                return true;
            }
            catch
            {
                return false;
            }
        }


        /// <inheritdoc/>
        public async Task<bool> SendRpcResponseAsync(
            ReadOnlyMemory<byte> bytes,
            string? replyTo,
            string? correlationId,
            BasicProperties? properties = null)
        {
            if (string.IsNullOrEmpty(replyTo) || string.IsNullOrEmpty(correlationId))
                return false;

            try
            {
                properties ??= new BasicProperties();
                properties.CorrelationId = correlationId;

                await Channel!.BasicPublishAsync(
                    exchange: string.Empty,
                    routingKey: replyTo,
                    body: bytes,
                    basicProperties: properties,
                    mandatory: false);

                return true;
            }
            catch
            {
                return false;
            }
        }


        /// <inheritdoc/>
        public async Task<bool> SendRpcResponseJsonAsync(
            string json,
            string? replyTo,
            string? correlationId)
        {
            if (string.IsNullOrEmpty(replyTo) || string.IsNullOrEmpty(correlationId))
                return false;

            try
            {
                var bytes = Encoding.UTF8.GetBytes(json);
                var properties = new BasicProperties
                {
                    CorrelationId = correlationId,
                    ContentType = "application/json"
                };

                await Channel!.BasicPublishAsync(
                    exchange: string.Empty,
                    routingKey: replyTo,
                    body: bytes,
                    basicProperties: properties,
                    mandatory: false);

                return true;
            }
            catch
            {
                return false;
            }
        }

        private static BasicProperties CreateProperties()
        {
            var properties = new BasicProperties
            {
                Persistent = true,
            };
            return properties;
        }

        private static BasicProperties CreateJsonProperties()
        {
            var properties = new BasicProperties
            {
                Persistent = true,
                ContentType = "application/json"
            };
            return properties;
        }

        private void EnsureProducingChannelIsNotNull()
        {
            if (Channel is null)
            {
                throw new ProducingChannelIsNullException(
                    $"Producing channel is null. Configure {nameof(IProducingService)} or full functional {nameof(IProducingService)} for producing messages");
            }
        }

        internal void ValidateArguments(string exchangeName, string routingKey)
        {
            if (string.IsNullOrEmpty(exchangeName))
            {
                throw new ArgumentException($"Argument {nameof(exchangeName)} is null or empty.", nameof(exchangeName));
            }

            if (string.IsNullOrEmpty(routingKey))
            {
                throw new ArgumentException($"Argument {nameof(routingKey)} is null or empty.", nameof(routingKey));
            }

            var deadLetterExchanges = _exchanges.Select(x => x.Options.DeadLetterExchange).Distinct();
            if (_exchanges.All(x => x.Name != exchangeName) && deadLetterExchanges.All(x => x != exchangeName))
            {
                throw new ArgumentException($"Exchange {nameof(exchangeName)} has not been declared yet.",
                    nameof(exchangeName));
            }
        }

        private string GetDeadLetterExchange(string exchangeName)
        {
            var exchange = _exchanges.FirstOrDefault(x => x.Name == exchangeName);
            if (string.IsNullOrEmpty(exchange?.Options.DeadLetterExchange))
            {
                throw new ArgumentException(
                    $"Exchange {nameof(exchangeName)} has not been configured with a dead letter exchange.",
                    nameof(exchangeName));
            }

            return exchange.Options.DeadLetterExchange;
        }

        private async Task<string> DeclareDelayedQueue(string exchange, string deadLetterExchange, string routingKey,
            int millisecondsDelay)
        {
            var delayedQueueName = $"{routingKey}.delayed.{millisecondsDelay}";
            var arguments = CreateArguments(exchange, routingKey, millisecondsDelay);

            Channel.EnsureIsNotNull();
            await Channel.QueueDeclareAsync(
                queue: delayedQueueName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: arguments);

            await Channel.QueueBindAsync(
                queue: delayedQueueName,
                exchange: deadLetterExchange,
                routingKey: delayedQueueName);
            return delayedQueueName;
        }

        private static Dictionary<string, object?> CreateArguments(string exchangeName, string routingKey,
            int millisecondsDelay) =>
            new()
            {
                { "x-dead-letter-exchange", exchangeName },
                { "x-dead-letter-routing-key", routingKey },
                { "x-message-ttl", millisecondsDelay },
                { "x-expires", millisecondsDelay + QueueExpirationTime }
            };
    }
}