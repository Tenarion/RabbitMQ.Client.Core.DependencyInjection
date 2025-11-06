using System;
using System.Threading.Tasks;

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
        Task SendAsync(ReadOnlyMemory<byte> bytes, BasicProperties properties, string exchangeName, string routingKey, int millisecondsDelay);
    }
}