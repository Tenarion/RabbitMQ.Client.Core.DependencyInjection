using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client.Core.DependencyInjection.Services.Interfaces;

namespace RabbitMQ.Client.Core.DependencyInjection.Services
{
    /// <summary>
    /// Hosted service that is responsible for creating connections and channels for both producing and consuming services.
    /// It does its thing by using IChannelDeclarationService <see cref="IChannelDeclarationService"/>.
    /// </summary>
    public class ChannelDeclarationHostedService : IHostedService
    {
        private readonly IChannelDeclarationService _channelDeclarationService;
        private readonly TaskCompletionSource<bool> _setupCompleteSignal;

        public ChannelDeclarationHostedService(IChannelDeclarationService channelDeclarationService)
        {
            _channelDeclarationService = channelDeclarationService;
            _setupCompleteSignal = new TaskCompletionSource<bool>();
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            await _channelDeclarationService.SetConnectionInfrastructureForRabbitMqServices();
            _setupCompleteSignal.SetResult(true);
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public Task WaitForSetupCompletionAsync()
        {
            return _setupCompleteSignal.Task;
        }
    }
}