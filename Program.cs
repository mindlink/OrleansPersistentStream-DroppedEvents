namespace OrleansPersistentStream_DroppedEvents
{
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.Hosting;
    using Streaming;

    internal class Program
    {
        static async Task Main(string[] args)
        {
            var siloHostBuilder = new HostBuilder().UseOrleans(siloBuilder =>
            {
                siloBuilder
                    .UseLocalhostClustering()
                    .AddMemoryGrainStorage("PubSubStore")
                    .AddTestStreams("TestStream");
            });

            var host = siloHostBuilder.Build();

            await host.StartAsync();

            var grainFactory = (IGrainFactory)host.Services.GetRequiredService(typeof(IGrainFactory));

            foreach (var _ in Enumerable.Range(0, 20))
            {
                var grainId = Guid.NewGuid();

                await grainFactory.GetGrain<IProducerGrain>(grainId).EmitEventsAsync();

                await Task.Delay(200);
            }

            Console.ReadLine();
        }
    }
}
