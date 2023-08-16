using Api.ReferenceLib.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Simple.RabbitMQ;

namespace Api.ReferenceLib.Services
{
    public class ScopePublisher : IScopePublisher
    {
        private readonly IServiceProvider _serviceProvider;
        public ScopePublisher(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;
        }
        public void Publish(string message, string routingKey, IDictionary<string, object>? headers)
        {
            using (var scope = _serviceProvider.CreateScope())
            {
                var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();

                try
                {
                    publisher.Publish(message, routingKey, headers);
                }
                finally
                {
                    publisher.Dispose();
                }
            }
        }
    }
}