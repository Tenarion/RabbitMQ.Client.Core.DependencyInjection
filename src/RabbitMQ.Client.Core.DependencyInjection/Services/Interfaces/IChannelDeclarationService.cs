using System.Threading.Tasks;

namespace RabbitMQ.Client.Core.DependencyInjection.Services.Interfaces
{
    /// <summary>
    /// Service that is responsible for declaring queues and exchanges.
    /// </summary>
    public interface IChannelDeclarationService
    {
        /// <summary>
        /// Create connection and declare everything for both consuming and producing services.
        /// </summary>
        Task SetConnectionInfrastructureForRabbitMqServices();

        /// <summary>
        /// A task that resolves when the setup is complete.
        /// </summary>
        /// <returns></returns>
        Task WaitForSetupCompletionAsync();
    }
}