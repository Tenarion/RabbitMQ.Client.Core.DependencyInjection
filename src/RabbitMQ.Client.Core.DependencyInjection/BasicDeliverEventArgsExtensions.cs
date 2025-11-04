using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using RabbitMQ.Client.Events;

namespace RabbitMQ.Client.Core.DependencyInjection
{
    /// <summary>
    /// BasicDeliverEventArgsExtensions extension that help to work with messages,
    /// </summary>
    public static class BasicDeliverEventArgsExtensions
    {
        /// <summary>
        /// Get message from BasicDeliverEventArgs body.
        /// </summary>
        /// <param name="eventArgs">Message event args.</param>
        /// <returns>Message as a string.</returns>
        public static string GetMessage(this BasicDeliverEventArgs eventArgs)
        {
            eventArgs.EnsureIsNotNull();
            return Encoding.UTF8.GetString(eventArgs.Body.ToArray());
        }

        /// <summary>
        /// Get message payload.
        /// </summary>
        /// <param name="eventArgs">Message event args.</param>
        /// <typeparam name="T">Type of a message body.</typeparam>
        /// <returns>Object of type <see cref="T"/>.</returns>
        public static T GetPayload<T>(this BasicDeliverEventArgs eventArgs)
        {
            eventArgs.EnsureIsNotNull();
            var messageString = Encoding.UTF8.GetString(eventArgs.Body.ToArray());
            return (T)JsonSerializer.Deserialize<T>(messageString)!;
        }
        
        /// <summary>
        /// Get message payload.
        /// </summary>
        /// <param name="eventArgs">Message event args.</param>
        /// <param name="settings">Serializer settings <see cref="JsonSerializerOptions"/>.</param>
        /// <typeparam name="T">Type of a message body.</typeparam>
        /// <returns>Object of type <see cref="T"/>.</returns>
        public static T? GetPayload<T>(this BasicDeliverEventArgs eventArgs, JsonSerializerOptions settings)
        {
            eventArgs.EnsureIsNotNull();
            var messageString = Encoding.UTF8.GetString(eventArgs.Body.ToArray());
            return JsonSerializer.Deserialize<T>(messageString, settings);
        }
        
        /// <summary>
        /// Get message payload.
        /// </summary>
        /// <param name="eventArgs">Message event args.</param>
        /// <param name="converters">A collection of json converters <see cref="JsonConverter"/>.</param>
        /// <typeparam name="T">Type of a message body.</typeparam>
        /// <returns>Object of type <see cref="T"/>.</returns>
        public static T? GetPayload<T>(this BasicDeliverEventArgs eventArgs, IEnumerable<JsonConverter> converters)
        {
            // Ensure eventArgs is not null (this method must exist in your codebase)
            eventArgs.EnsureIsNotNull();

            // Convert the byte array to string
            var messageString = Encoding.UTF8.GetString(eventArgs.Body.ToArray());

            // Create JsonSerializerOptions and add converters
            var options = new JsonSerializerOptions();
            foreach(var converter in converters)
            {
                options.Converters.Add(converter);
            }

            // Deserialize the message string to the specified type
            return JsonSerializer.Deserialize<T>(messageString, options);
        }
        
        /// <summary>
        /// Get message payload as an anonymous object.
        /// </summary>
        /// <param name="eventArgs">Message event args.</param>
        /// <param name="anonymousTypeObject">An anonymous object base.</param>
        /// <typeparam name="T">Type of an anonymous object.</typeparam>
        /// <returns>Anonymous object.</returns>
        public static T GetAnonymousPayload<T>(this BasicDeliverEventArgs eventArgs, T anonymousTypeObject)
        {
            eventArgs.EnsureIsNotNull();
            var messageString = Encoding.UTF8.GetString(eventArgs.Body.ToArray());
            return (T)JsonSerializer.Deserialize(messageString, anonymousTypeObject!.GetType())!;
        }
        
        /// <summary>
        /// Get message payload as an anonymous object.
        /// </summary>
        /// <param name="eventArgs">Message event args.</param>
        /// <param name="anonymousTypeObject">An anonymous object base.</param>
        /// <param name="settings">Serializer settings <see cref="JsonSerializerOptions"/>.</param>
        /// <typeparam name="T">Type of an anonymous object.</typeparam>
        /// <returns>Anonymous object.</returns>
        public static T GetAnonymousPayload<T>(this BasicDeliverEventArgs eventArgs, T anonymousTypeObject, JsonSerializerOptions settings)
        {
            eventArgs.EnsureIsNotNull();
            var messageString = Encoding.UTF8.GetString(eventArgs.Body.ToArray());
            return (T)JsonSerializer.Deserialize(messageString, anonymousTypeObject!.GetType(), settings)!;
        }

        private static BasicDeliverEventArgs EnsureIsNotNull(this BasicDeliverEventArgs eventArgs)
        {
            if (eventArgs is null)
            {
                throw new ArgumentNullException(nameof(eventArgs), "BasicDeliverEventArgs have to be not null to parse a message");
            }

            return eventArgs;
        }
    }
}