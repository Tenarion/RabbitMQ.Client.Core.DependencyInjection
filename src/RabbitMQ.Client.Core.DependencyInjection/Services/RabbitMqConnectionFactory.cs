using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client.Core.DependencyInjection.Configuration;
using RabbitMQ.Client.Core.DependencyInjection.Exceptions;
using RabbitMQ.Client.Core.DependencyInjection.Services.Interfaces;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace RabbitMQ.Client.Core.DependencyInjection.Services
{
    /// <inheritdoc/>
    public class RabbitMqConnectionFactory : IRabbitMqConnectionFactory
    {
        /// <inheritdoc/>
        public async Task<IConnection?> CreateRabbitMqConnection(RabbitMqServiceOptions? options)
        {
            if (options is null)
            {
                return null;
            }

            var factory = new ConnectionFactory
            {
                Port = options.Port,
                UserName = options.UserName,
                Password = options.Password,
                VirtualHost = options.VirtualHost,
                AutomaticRecoveryEnabled = options.AutomaticRecoveryEnabled,
                TopologyRecoveryEnabled = options.TopologyRecoveryEnabled,
                RequestedConnectionTimeout = options.RequestedConnectionTimeout,
                RequestedHeartbeat = options.RequestedHeartbeat,
                // DispatchConsumersAsync = true
            };

            if (options.TcpEndpoints.Any())
            {
                return await CreateConnectionWithTcpEndpoints(options, factory);
            }

            if (string.IsNullOrEmpty(options.ClientProvidedName))
                return await CreateConnection(options, factory);

            return await CreateNamedConnection(options, factory);
        }

        /// <inheritdoc/>
        public AsyncEventingBasicConsumer CreateConsumer(IChannel channel) => new(channel);

        private static Task<IConnection?> CreateConnectionWithTcpEndpoints(RabbitMqServiceOptions options,
            ConnectionFactory factory)
        {
            var clientEndpoints = new List<AmqpTcpEndpoint>();
            foreach (var endpoint in options.TcpEndpoints)
            {
                var sslOption = endpoint.SslOption;
                if (sslOption != null)
                {
                    var convertedOption =
                        new SslOption(sslOption.ServerName, sslOption.CertificatePath, sslOption.Enabled);
                    if (!string.IsNullOrEmpty(sslOption.CertificatePassphrase))
                    {
                        convertedOption.CertPassphrase = sslOption.CertificatePassphrase;
                    }

                    if (sslOption.AcceptablePolicyErrors != null)
                    {
                        convertedOption.AcceptablePolicyErrors = sslOption.AcceptablePolicyErrors.Value;
                    }

                    clientEndpoints.Add(new AmqpTcpEndpoint(endpoint.HostName, endpoint.Port, convertedOption));
                }
                else
                {
                    clientEndpoints.Add(new AmqpTcpEndpoint(endpoint.HostName, endpoint.Port));
                }
            }

            return TryToCreateConnection(() => factory.CreateConnectionAsync(clientEndpoints),
                options.InitialConnectionRetries, options.InitialConnectionRetryTimeoutMilliseconds);
        }

        private static Task<IConnection?> CreateNamedConnection(RabbitMqServiceOptions options,
            ConnectionFactory factory)
        {
            if (options.HostNames.Any())
            {
                return TryToCreateConnection(
                    () => factory.CreateConnectionAsync(options.HostNames.ToList(), options.ClientProvidedName),
                    options.InitialConnectionRetries, options.InitialConnectionRetryTimeoutMilliseconds);
            }

            factory.HostName = options.HostName;
            return TryToCreateConnection(() => factory.CreateConnectionAsync(options.ClientProvidedName),
                options.InitialConnectionRetries, options.InitialConnectionRetryTimeoutMilliseconds);
        }

        private static Task<IConnection?> CreateConnection(RabbitMqServiceOptions options, ConnectionFactory factory)
        {
            if (options.HostNames.Any())
            {
                return TryToCreateConnection(() => factory.CreateConnectionAsync(options.HostNames.ToList()),
                    options.InitialConnectionRetries, options.InitialConnectionRetryTimeoutMilliseconds);
            }

            factory.HostName = options.HostName;
            return TryToCreateConnection(() => factory.CreateConnectionAsync(), options.InitialConnectionRetries,
                options.InitialConnectionRetryTimeoutMilliseconds);
        }

        private static async Task<IConnection?> TryToCreateConnection(Func<Task<IConnection>> connectionFunction,
            int numberOfRetries, int timeoutMilliseconds)
        {
            ValidateArguments(numberOfRetries, timeoutMilliseconds);

            var attempts = 0;
            BrokerUnreachableException? latestException = null;
            while (attempts < numberOfRetries)
            {
                try
                {
                    if (attempts > 0)
                    {
                        Thread.Sleep(timeoutMilliseconds);
                    }

                    return await connectionFunction();
                }
                catch (BrokerUnreachableException exception)
                {
                    attempts++;
                    latestException = exception;
                }
            }

            throw new InitialConnectionException(
                $"Could not establish an initial connection in {numberOfRetries} retries", latestException)
            {
                NumberOfRetries = attempts
            };
        }

        private static void ValidateArguments(int numberOfRetries, int timeoutMilliseconds)
        {
            if (numberOfRetries < 1)
            {
                throw new ArgumentException("Number of retries should be a positive number.", nameof(numberOfRetries));
            }

            if (timeoutMilliseconds < 1)
            {
                throw new ArgumentException("Initial reconnection timeout should be a positive number.",
                    nameof(timeoutMilliseconds));
            }
        }
    }
}