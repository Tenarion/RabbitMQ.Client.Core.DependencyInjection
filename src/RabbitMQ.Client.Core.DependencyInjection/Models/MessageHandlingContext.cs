using System;
using System.Threading.Tasks;
using RabbitMQ.Client.Core.DependencyInjection.Exceptions;
using RabbitMQ.Client.Events;

namespace RabbitMQ.Client.Core.DependencyInjection.Models
{
    public class MessageHandlingContext
    {
        private readonly AsyncEventHandler<BasicDeliverEventArgs> _ackAction;
        private readonly AsyncEventHandler<BasicDeliverEventArgs> _nackAction;
        private bool _alreadyAcknowledged;

        public MessageHandlingContext(BasicDeliverEventArgs message, AsyncEventHandler<BasicDeliverEventArgs> ackAction, AsyncEventHandler<BasicDeliverEventArgs> nackAction, bool disableAutoAck)
        {
            Message = message;
            _ackAction = ackAction;
            _nackAction = nackAction;
            AutoAckEnabled = !disableAutoAck;
        }

        public BasicDeliverEventArgs Message { get; }

        public bool AutoAckEnabled { get; }

        public async Task AcknowledgeMessage()
        {
            if (_alreadyAcknowledged)
            {
                throw new MessageHasAlreadyBeenAcknowledgedException();
            }

            await _ackAction(this, Message);
            _alreadyAcknowledged = true;
        }

        public async Task RejectMessage(bool requeue)
        {
            if (_alreadyAcknowledged)
            {
                throw new MessageHasAlreadyBeenAcknowledgedException();
            }

            await _nackAction(requeue, Message);
            _alreadyAcknowledged = true;
        }
    }
}