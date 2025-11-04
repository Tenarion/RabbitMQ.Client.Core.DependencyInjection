using System;

namespace RabbitMQ.Client.Core.DependencyInjection.Exceptions;

public class BatchMessageHandlerProcessingException : Exception
{
    public BatchMessageHandlerProcessingException(string message, Exception exception) : base(message, exception)
    {
    }
}