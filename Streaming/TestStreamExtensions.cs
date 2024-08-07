namespace OrleansPersistentStream_DroppedEvents.Streaming
{
    using Microsoft.Extensions.DependencyInjection;
    using Orleans.Configuration;
    using Orleans.Providers;

    /// <summary>
    /// <see cref="ISiloBuilder"/> extension methods for configuring in-memory streams. 
    /// </summary>
    public static class TestStreamExtensions
    {

        /// <summary>
        /// Configure silo to use memory streams, using the default message serializer
        /// (<see cref="DefaultMemoryMessageBodySerializer"/>).
        /// </summary>
        /// using the default built-in serializer
        /// <param name="builder">The builder.</param>
        /// <param name="name">The stream provider name.</param>
        /// <param name="configure">The configuration delegate.</param>
        /// <returns>The silo builder.</returns>
        public static ISiloBuilder AddTestStreams(this ISiloBuilder builder, string name, Action<ISiloTestStreamConfigurator> configure = null)
        {
            return AddTestStreams<DefaultMemoryMessageBodySerializer>(builder, name, configure);
        }

        /// <summary>
        /// Configure silo to use memory streams.
        /// </summary>
        /// <typeparam name="TSerializer">The message serializer type, which must implement <see cref="IMemoryMessageBodySerializer"/>.</typeparam>
        /// <param name="builder">The builder.</param>
        /// <param name="name">The stream provider name.</param>
        /// <param name="configure">The configuration delegate.</param>
        /// <returns>The silo builder.</returns>
        public static ISiloBuilder AddTestStreams<TSerializer>(this ISiloBuilder builder, string name,
            Action<ISiloTestStreamConfigurator> configure = null)
             where TSerializer : class, IMemoryMessageBodySerializer
        {
            //the constructor wire up DI with all default components of the streams , so need to be called regardless of configureStream null or not
            var memoryStreamConfiguretor = new SiloTestStreamConfigurator<TSerializer>(name,
                configureDelegate => builder.ConfigureServices(configureDelegate)
            );
            configure?.Invoke(memoryStreamConfiguretor);
            return builder;
        }
    }

    /// <summary>
    /// Configuration builder for memory streams.
    /// </summary>
    public interface ITestStreamConfigurator : INamedServiceConfigurator { }

    /// <summary>
    /// Configuration extensions for memory streams.
    /// </summary>
    public static class TestStreamConfiguratorExtensions
    {
        /// <summary>
        /// Configures partitioning for memory streams.
        /// </summary>
        /// <param name="configurator">The configuration builder.</param>
        /// <param name="numOfQueues">The number of queues.</param>
        public static void ConfigurePartitioning(this ITestStreamConfigurator configurator, int numOfQueues = HashRingStreamQueueMapperOptions.DEFAULT_NUM_QUEUES)
        {
            configurator.Configure<HashRingStreamQueueMapperOptions>(ob => ob.Configure(options => options.TotalQueueCount = numOfQueues));
        }
    }

    /// <summary>
    /// Silo-specific configuration builder for memory streams.
    /// </summary>
    public interface ISiloTestStreamConfigurator : ITestStreamConfigurator, ISiloRecoverableStreamConfigurator { }

    /// <summary>
    /// Configures memory streams.
    /// </summary>
    /// <typeparam name="TSerializer">The message body serializer type, which must implement <see cref="IMemoryMessageBodySerializer"/>.</typeparam>
    public class SiloTestStreamConfigurator<TSerializer> : SiloRecoverableStreamConfigurator, ISiloTestStreamConfigurator
          where TSerializer : class, IMemoryMessageBodySerializer
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="SiloTestStreamConfigurator{TSerializer}"/> class.
        /// </summary>
        /// <param name="name">The stream provider name.</param>
        /// <param name="configureServicesDelegate">The services configuration delegate.</param>
        public SiloTestStreamConfigurator(
            string name, Action<Action<IServiceCollection>> configureServicesDelegate)
            : base(name, configureServicesDelegate, TestStreamAdapterFactory<TSerializer>.Create)
        {
            this.ConfigureDelegate(services => services.ConfigureNamedOptionForLogging<HashRingStreamQueueMapperOptions>(name));
        }
    }
}
