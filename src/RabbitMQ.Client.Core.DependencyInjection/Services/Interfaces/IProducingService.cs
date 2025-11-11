using System;
using System.Threading.Tasks;
using RabbitMQ.Client.Events;

namespace RabbitMQ.Client.Core.DependencyInjection.Services.Interfaces
{
    /// <summary>
    /// Custom RabbitMQ producing service interface.
    /// </summary>
    public interface IProducingService : IRabbitMqService
    {
        /// <summary>
        /// RabbitMQ producing connection.
        /// </summary>
        IConnection? Connection { get; }

        /// <summary>
        /// RabbitMQ producing channel.
        /// </summary>
        IChannel? Channel { get; }

        /// <summary>
        /// Send a message.
        /// </summary>
        /// <typeparam name="T">Model class.</typeparam>
        /// <param name="object">Object message.</param>
        /// <param name="exchangeName">Exchange name.</param>
        /// <param name="routingKey">Routing key.</param>
        Task SendAsync<T>(T @object, string exchangeName, string routingKey) where T : class;

        /// <summary>
        /// Send a delayed message.
        /// </summary>
        /// <typeparam name="T">Model class.</typeparam>
        /// <param name="object">Object message.</param>
        /// <param name="exchangeName">Exchange name.</param>
        /// <param name="routingKey">Routing key.</param>
        /// <param name="millisecondsDelay">Delay time in milliseconds.</param>
        Task SendAsync<T>(T @object, string exchangeName, string routingKey, int millisecondsDelay) where T : class;

        /// <summary>
        /// Send a message.
        /// </summary>
        /// <param name="json">Json message.</param>
        /// <param name="exchangeName">Exchange name.</param>
        /// <param name="routingKey">Routing key.</param>
        Task SendJsonAsync(string json, string exchangeName, string routingKey);

        /// <summary>
        /// Send a delayed message.
        /// </summary>
        /// <param name="json"></param>
        /// <param name="exchangeName"></param>
        /// <param name="routingKey"></param>
        /// <param name="millisecondsDelay">Delay time in milliseconds.</param>
        Task SendJsonAsync(string json, string exchangeName, string routingKey, int millisecondsDelay);

        /// <summary>
        /// Send a message.
        /// </summary>
        /// <param name="message">Message.</param>
        /// <param name="exchangeName">Exchange name.</param>
        /// <param name="routingKey">Routing key.</param>
        Task SendStringAsync(string message, string exchangeName, string routingKey);

        /// <summary>
        /// Send a delayed message.
        /// </summary>
        /// <param name="message">Message.</param>
        /// <param name="exchangeName">Exchange name.</param>
        /// <param name="routingKey">Routing key.</param>
        /// <param name="millisecondsDelay">Delay time in milliseconds.</param>
        Task SendStringAsync(string message, string exchangeName, string routingKey, int millisecondsDelay);

        /// <summary>
        /// Send a message.
        /// </summary>
        /// <param name="bytes">Byte array message.</param>
        /// <param name="properties">Message properties.</param>
        /// <param name="exchangeName">Exchange name.</param>
        /// <param name="routingKey">Routing key.</param>
        Task SendAsync(ReadOnlyMemory<byte> bytes, BasicProperties properties, string exchangeName, string routingKey);

        /// <summary>
        /// Send a delayed message.
        /// </summary>
        /// <param name="bytes">Byte array message.</param>
        /// <param name="properties">Message properties.</param>
        /// <param name="exchangeName">Exchange name.</param>
        /// <param name="routingKey">Routing key.</param>
        /// <param name="millisecondsDelay">Delay time in milliseconds.</param>
        Task SendAsync(ReadOnlyMemory<byte> bytes, BasicProperties properties, string exchangeName, string routingKey,
            int millisecondsDelay);

        /// <summary>
        /// Send RPC message and wait for response.
        /// </summary>
        /// <param name="bytes">Byte array message.</param>
        /// <param name="properties">Message properties.</param>
        /// <param name="exchangeName">Exchange name.</param>
        /// <param name="routingKey">Routing key.</param>
        /// <param name="timeout">Timeout duration.</param>
        /// <param name="onResponseReceived">When a response is received.</param>
        /// <param name="onTimeout">When a timeout occurs.</param>
        /// <typeparam name="TResponse">Response model class.</typeparam>
        /// <returns>Response message.</returns>
        public Task<TResponse?> SendRpcAsync<TResponse>(ReadOnlyMemory<byte> bytes, BasicProperties properties,
            string exchangeName,
            string routingKey,
            TimeSpan timeout,
            Func<TaskCompletionSource<TResponse?>, object, BasicDeliverEventArgs, Task>? onResponseReceived = null,
            Action<TaskCompletionSource<TResponse?>>? onTimeout = null);

        /// <summary>
        /// Send RPC message and wait for response.
        /// </summary>
        /// <param name="request">Request model class.</param>
        /// <param name="exchangeName">Exchange name.</param>
        /// <param name="routingKey">Routing key.</param>
        /// <param name="timeout">Timeout duration.</param>
        /// <param name="onResponseReceived">When a response is received.</param>
        /// <param name="onTimeout">When a timeout occurs.</param>
        /// <typeparam name="TRequest">Request model class.</typeparam>
        /// <typeparam name="TResponse">Response model class.</typeparam>
        /// <returns>Response message.</returns>
        public Task<TResponse?> SendRpcAsync<TRequest, TResponse>(TRequest request, string exchangeName,
            string routingKey,
            TimeSpan timeout,
            Func<TaskCompletionSource<TResponse?>, object, BasicDeliverEventArgs, Task>? onResponseReceived = null,
            Action<TaskCompletionSource<TResponse?>>? onTimeout = null);

        /// <summary>
        /// Send RPC message with delay and wait for response.
        /// </summary>
        /// <param name="request">Request model class.</param>
        /// <param name="exchangeName">Exchange name.</param>
        /// <param name="routingKey">Routing key.</param>
        /// <param name="millisecondsDelay">Delay time in milliseconds.</param>
        /// <param name="timeout">Timeout duration.</param>
        /// <param name="onResponseReceived">When a response is received.</param>
        /// <param name="onTimeout">When a timeout occurs.</param>
        /// <typeparam name="TRequest">Request model class.</typeparam>
        /// <typeparam name="TResponse">Response model class.</typeparam>
        /// <returns>Response message.</returns>
        public Task<TResponse?> SendRpcAsync<TRequest, TResponse>(TRequest request, string exchangeName,
            string routingKey,
            int millisecondsDelay,
            TimeSpan timeout,
            Func<TaskCompletionSource<TResponse?>, object, BasicDeliverEventArgs, Task>? onResponseReceived = null,
            Action<TaskCompletionSource<TResponse?>>? onTimeout = null);

        /// <summary>
        /// Send RPC message and wait for response.
        /// </summary>
        /// <param name="json">Json message.</param>
        /// <param name="exchangeName">Exchange name.</param>
        /// <param name="routingKey">Routing key.</param>
        /// <param name="timeout">Timeout duration.</param>
        /// <param name="onResponseReceived">When a response is received.</param>
        /// <param name="onTimeout">When a timeout occurs.</param>
        /// <typeparam name="TResponse">Response model class.</typeparam>
        /// <returns>Response message.</returns>
        public Task<TResponse?> SendRpcAsync<TResponse>(string json, string exchangeName,
            string routingKey,
            TimeSpan timeout,
            Func<TaskCompletionSource<TResponse?>, object, BasicDeliverEventArgs, Task>? onResponseReceived = null,
            Action<TaskCompletionSource<TResponse?>>? onTimeout = null);

        /// <summary>
        /// Send an RPC response back to the caller.
        /// </summary>
        /// <typeparam name="T">Response object type.</typeparam>
        /// <param name="producingService">The producing service.</param>
        /// <param name="response">The response object to send.</param>
        /// <param name="replyTo">The reply-to queue name from the original request.</param>
        /// <param name="correlationId">The correlation ID from the original request.</param>
        /// <returns>True if the response was sent successfully, false otherwise.</returns>
        public Task<bool> SendRpcResponseAsync<T>(
            T response,
            string? replyTo,
            string? correlationId) where T : class;

        /// <summary>
        /// Send an RPC response back to the caller with custom properties.
        /// </summary>
        /// <param name="bytes">The response bytes to send.</param>
        /// <param name="replyTo">The reply-to queue name from the original request.</param>
        /// <param name="correlationId">The correlation ID from the original request.</param>
        /// <param name="properties">Custom properties (correlation ID will be set automatically).</param>
        /// <returns>True if the response was sent successfully, false otherwise.</returns>
        public Task<bool> SendRpcResponseAsync(
            ReadOnlyMemory<byte> bytes,
            string? replyTo,
            string? correlationId,
            BasicProperties? properties = null);

        /// <summary>
        /// Send an RPC response as a JSON string back to the caller.
        /// </summary>
        /// <param name="json">The JSON string to send.</param>
        /// <param name="replyTo">The reply-to queue name from the original request.</param>
        /// <param name="correlationId">The correlation ID from the original request.</param>
        /// <returns>True if the response was sent successfully, false otherwise.</returns>
        public Task<bool> SendRpcResponseJsonAsync(
            string json,
            string? replyTo,
            string? correlationId);
    }
}