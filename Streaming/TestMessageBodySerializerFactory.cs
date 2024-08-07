namespace OrleansPersistentStream_DroppedEvents.Streaming
{
    using Microsoft.Extensions.DependencyInjection;
    using Orleans.Providers;

    internal static class TestMessageBodySerializerFactory<TSerializer>
        where TSerializer : class, IMemoryMessageBodySerializer
    {
        private static readonly Lazy<ObjectFactory> ObjectFactory = new Lazy<ObjectFactory>(
            () => ActivatorUtilities.CreateFactory(
                typeof(TSerializer),
                Type.EmptyTypes));

        public static TSerializer GetOrCreateSerializer(IServiceProvider serviceProvider)
        {
            return serviceProvider.GetService<TSerializer>() ??
                   (TSerializer)ObjectFactory.Value(serviceProvider, null);
        }
    }
}
