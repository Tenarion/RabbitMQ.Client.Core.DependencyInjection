using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using RabbitMQ.Client.Core.DependencyInjection.InternalExtensions.Validation;
using RabbitMQ.Client.Core.DependencyInjection.Models;
using RabbitMQ.Client.Core.DependencyInjection.Services.Interfaces;
using RabbitMQ.Client.Events;

namespace RabbitMQ.Client.Core.DependencyInjection.Services
{
    /// <inheritdoc cref="IConsumingService"/>
    public class ConsumingService : IConsumingService, IAsyncDisposable
    {
        /// <inheritdoc/>
        public IConnection? Connection { get; private set; }

        /// <inheritdoc/>
        public IChannel? Channel { get; private set; }

        /// <inheritdoc/>
        public AsyncEventingBasicConsumer? Consumer { get; private set; }

        private bool _consumingStarted;

        private readonly IMessageHandlingPipelineExecutingService _messageHandlingPipelineExecutingService;
        private readonly IEnumerable<RabbitMqExchange> _exchanges;

        private IEnumerable<string> _consumerTags = new List<string>();

        public ConsumingService(
            IMessageHandlingPipelineExecutingService messageHandlingPipelineExecutingService,
            IEnumerable<RabbitMqExchange> exchanges)
        {
            _messageHandlingPipelineExecutingService = messageHandlingPipelineExecutingService;
            _exchanges = exchanges;
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

            if (Channel != null) await Channel.DisposeAsync();

            if (Connection != null) await Connection.DisposeAsync();
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
        public void UseConsumer(AsyncEventingBasicConsumer consumer)
        {
            Consumer = consumer;
        }

        /// <inheritdoc/>
        public async Task StartConsuming()
        {
            Channel.EnsureIsNotNull();
            Consumer.EnsureIsNotNull();

            if (_consumingStarted)
            {
                return;
            }

            Consumer.ReceivedAsync += ConsumerOnReceived;
            _consumingStarted = true;

            var consumptionExchanges = _exchanges.Where(x => x.IsConsuming);
            var tags = await Task.WhenAll(consumptionExchanges.SelectMany(exchange =>
                exchange.Options.Queues.Select(queue =>
                    Channel.BasicConsumeAsync(queue: queue.Name, autoAck: false, consumer: Consumer))));
            _consumerTags = tags.Distinct().ToList();
        }

        /// <inheritdoc/>
        public async Task StopConsuming()
        {
            Channel.EnsureIsNotNull();
            Consumer.EnsureIsNotNull();

            if (!_consumingStarted)
            {
                return;
            }

            Consumer.ReceivedAsync -= ConsumerOnReceived;
            _consumingStarted = false;
            foreach (var tag in _consumerTags)
            {
                await Channel.BasicCancelAsync(tag);
            }
        }

        private async Task AckAction(object sender, BasicDeliverEventArgs eventArgs) =>
            await Channel.EnsureIsNotNull().BasicAckAsync(eventArgs.DeliveryTag, false);

        private async Task ConsumerOnReceived(object sender, BasicDeliverEventArgs eventArgs)
        {
            var exchangeOptions = _exchanges.FirstOrDefault(x => string.Equals(x.Name, eventArgs.Exchange))
                .EnsureIsNotNull().Options;
            var context = new MessageHandlingContext(eventArgs, AckAction, exchangeOptions.DisableAutoAck);
            await _messageHandlingPipelineExecutingService.Execute(context);
        }
    }
}