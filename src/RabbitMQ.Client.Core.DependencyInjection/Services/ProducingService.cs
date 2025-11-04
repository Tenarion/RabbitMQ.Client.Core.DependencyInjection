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
        public async Task SendJson(string json, string exchangeName, string routingKey)
        {
            EnsureProducingChannelIsNotNull();
            ValidateArguments(exchangeName, routingKey);
            var bytes = Encoding.UTF8.GetBytes(json);
            var properties = CreateJsonProperties();
            await SendAsync(bytes, properties, exchangeName, routingKey);
        }

        /// <inheritdoc/>
        public async Task SendJson(string json, string exchangeName, string routingKey, int millisecondsDelay)
        {
            EnsureProducingChannelIsNotNull();
            ValidateArguments(exchangeName, routingKey);
            var deadLetterExchange = GetDeadLetterExchange(exchangeName);
            var delayedQueueName =
                await DeclareDelayedQueue(exchangeName, deadLetterExchange, routingKey, millisecondsDelay);
            await SendJson(json, deadLetterExchange, delayedQueueName);
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

        // /// <inheritdoc/>
        // public async Task SendAsync<T>(T @object, string exchangeName, string routingKey) where T : class =>
        //     await Task.Run(() => Send(@object, exchangeName, routingKey)).ConfigureAwait(false);
        //
        // /// <inheritdoc/>
        // public async Task SendAsync<T>(T @object, string exchangeName, string routingKey, int millisecondsDelay) where T : class =>
        //     await Task.Run(() => Send(@object, exchangeName, routingKey, millisecondsDelay)).ConfigureAwait(false);
        //
        // /// <inheritdoc/>
        // public async Task SendJsonAsync(string json, string exchangeName, string routingKey) =>
        //     await Task.Run(() => SendJson(json, exchangeName, routingKey)).ConfigureAwait(false);
        //
        // /// <inheritdoc/>
        // public async Task SendJsonAsync(string json, string exchangeName, string routingKey, int millisecondsDelay) =>
        //     await Task.Run(() => SendJson(json, exchangeName, routingKey, millisecondsDelay)).ConfigureAwait(false);
        //
        // /// <inheritdoc/>
        // public async Task SendStringAsync(string message, string exchangeName, string routingKey) =>
        //     await Task.Run(() => SendString(message, exchangeName, routingKey)).ConfigureAwait(false);
        //
        // /// <inheritdoc/>
        // public async Task SendStringAsync(string message, string exchangeName, string routingKey, int millisecondsDelay) =>
        //     await Task.Run(() => SendString(message, exchangeName, routingKey, millisecondsDelay)).ConfigureAwait(false);
        //
        // /// <inheritdoc/>
        // public async Task SendAsync(ReadOnlyMemory<byte> bytes, BasicProperties properties, string exchangeName,
        //     string routingKey) =>
        //     await Task.Run(() => Send(bytes, properties, exchangeName, routingKey)).ConfigureAwait(false);
        //
        // /// <inheritdoc/>
        // public async Task SendAsync(ReadOnlyMemory<byte> bytes, BasicProperties properties, string exchangeName,
        //     string routingKey, int millisecondsDelay) =>
        //     await Task.Run(() => Send(bytes, properties, exchangeName, routingKey, millisecondsDelay)).ConfigureAwait(false);

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