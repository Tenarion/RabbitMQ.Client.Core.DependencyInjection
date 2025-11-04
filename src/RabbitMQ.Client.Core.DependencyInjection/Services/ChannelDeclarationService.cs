using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using RabbitMQ.Client.Core.DependencyInjection.Configuration;
using RabbitMQ.Client.Core.DependencyInjection.InternalExtensions.Validation;
using RabbitMQ.Client.Core.DependencyInjection.Models;
using RabbitMQ.Client.Core.DependencyInjection.Services.Interfaces;
using RabbitMQ.Client.Events;

namespace RabbitMQ.Client.Core.DependencyInjection.Services
{
    /// <inheritdoc/>
    public class ChannelDeclarationService : IChannelDeclarationService
    {
        private readonly RabbitMqConnectionOptions _connectionOptions;
        private readonly IProducingService _producingService;
        private readonly IConsumingService _consumingService;
        private readonly IRabbitMqConnectionFactory _rabbitMqConnectionFactory;
        private readonly IEnumerable<RabbitMqExchange> _exchanges;
        private readonly ILoggingService _loggingService;
        
        public ChannelDeclarationService(
            IProducingService producingService,
            IConsumingService consumingService,
            IRabbitMqConnectionFactory rabbitMqConnectionFactory,
            IOptions<RabbitMqConnectionOptions> connectionOptions,
            IEnumerable<RabbitMqExchange> exchanges,
            ILoggingService loggingService)
        {
            _producingService = producingService;
            _consumingService = consumingService;
            _rabbitMqConnectionFactory = rabbitMqConnectionFactory;
            _connectionOptions = connectionOptions.Value;
            _exchanges = exchanges;
            _loggingService = loggingService;
        }

        /// <inheritdoc/>
        public async Task SetConnectionInfrastructureForRabbitMqServices()
        {
            if (_connectionOptions.ProducerOptions != null)
            {
                var connection = (await CreateConnection(_connectionOptions.ProducerOptions)).EnsureIsNotNull();
                var channel = await CreateChannel(connection);
                await StartClient(channel);
                _producingService.UseConnection(connection);
                _producingService.UseChannel(channel);
            }

            if (_connectionOptions.ConsumerOptions != null)
            {
                var connection = (await CreateConnection(_connectionOptions.ConsumerOptions)).EnsureIsNotNull();
                var channel = await CreateChannel(connection);
                await StartClient(channel);
                var consumer = _rabbitMqConnectionFactory.CreateConsumer(channel);
                _consumingService.UseConnection(connection);
                _consumingService.UseChannel(channel);
                _consumingService.UseConsumer(consumer);
            }
        }

        private async Task<IConnection?> CreateConnection(RabbitMqServiceOptions options) => await _rabbitMqConnectionFactory.CreateRabbitMqConnection(options);

        private async Task<IChannel> CreateChannel(IConnection connection)
        {
            connection.CallbackExceptionAsync += HandleConnectionCallbackException!;
            connection.ConnectionRecoveryErrorAsync += HandleConnectionRecoveryError!;

            var channel = await connection.CreateChannelAsync();
            channel.CallbackExceptionAsync += HandleChannelCallbackException!;
            channel.BasicAcksAsync += HandleChannelBasicRecoverOk!;
            return channel;
        }

        private async Task StartClient(IChannel channel)
        {
            var deadLetterExchanges = _exchanges
                .Select(x => x.Options)
                .Where(x => !string.IsNullOrEmpty(x.DeadLetterExchange))
                .Select(x => new DeadLetterExchange(x.DeadLetterExchange, x.DeadLetterExchangeType))
                .Distinct(new DeadLetterExchangeEqualityComparer())
                .ToList();

            await StartChannel(channel, _exchanges, deadLetterExchanges);
        }

        private static async Task StartChannel(IChannel channel, IEnumerable<RabbitMqExchange> exchanges, IEnumerable<DeadLetterExchange> deadLetterExchanges)
        {
            foreach (var exchange in deadLetterExchanges)
            {
                await StartDeadLetterExchange(channel, exchange);
            }

            foreach (var exchange in exchanges)
            {
                await StartExchange(channel, exchange);
            }
        }

        private static async Task StartDeadLetterExchange(IChannel channel, DeadLetterExchange exchange)
        {
            await channel.ExchangeDeclareAsync(
                exchange: exchange.Name,
                type: exchange.Type,
                durable: true,
                autoDelete: false,
                arguments: null);
        }

        private static async Task StartExchange(IChannel channel, RabbitMqExchange exchange)
        {
            await channel.ExchangeDeclareAsync(
                exchange: exchange.Name,
                type: exchange.Options.Type,
                durable: exchange.Options.Durable,
                autoDelete: exchange.Options.AutoDelete,
                arguments: exchange.Options.Arguments);

            foreach (var queue in exchange.Options.Queues)
            {
                await StartQueue(channel, queue, exchange.Name);
            }
        }

        private static async Task StartQueue(IChannel channel, RabbitMqQueueOptions queue, string exchangeName)
        {
            await channel.QueueDeclareAsync(
                queue: queue.Name,
                durable: queue.Durable,
                exclusive: queue.Exclusive,
                autoDelete: queue.AutoDelete,
                arguments: queue.Arguments);

            if (queue.RoutingKeys.Count > 0)
            {
                foreach (var route in queue.RoutingKeys)
                {
                    await channel.QueueBindAsync(
                        queue: queue.Name,
                        exchange: exchangeName,
                        routingKey: route);
                }
            }
            else
            {
                // If there are not any routing keys then make a bind with a queue name.
                await channel.QueueBindAsync(
                    queue: queue.Name,
                    exchange: exchangeName,
                    routingKey: queue.Name);
            }
        }

        private Task HandleConnectionCallbackException(object sender, CallbackExceptionEventArgs? @event)
        {
            if (@event?.Exception is null)
            {
                return Task.CompletedTask;
            }

            _loggingService.LogError(@event.Exception, @event.Exception.Message);
            throw @event.Exception;
        }

        private Task HandleConnectionRecoveryError(object sender, ConnectionRecoveryErrorEventArgs? @event)
        {
            if (@event?.Exception is null)
            {
                return Task.CompletedTask;
            }

            _loggingService.LogError(@event.Exception, @event.Exception.Message);
            throw @event.Exception;
        }

        private Task HandleChannelBasicRecoverOk(object sender, BasicAckEventArgs? @event)
        {
            if (@event is null)
            {
                return Task.CompletedTask;
            }

            _loggingService.LogInformation("Connection has been reestablished");
            return Task.CompletedTask;
        }

        private Task HandleChannelCallbackException(object sender, CallbackExceptionEventArgs? @event)
        {
            if (@event?.Exception is null)
            {
                return Task.CompletedTask;
            }

            _loggingService.LogError(@event.Exception, @event.Exception.Message);
            throw @event.Exception;
        }
    }
}