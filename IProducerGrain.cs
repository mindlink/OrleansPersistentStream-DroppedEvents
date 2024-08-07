namespace OrleansPersistentStream_DroppedEvents
{
    internal interface IProducerGrain : IGrainWithGuidKey
    {
        Task EmitEventsAsync();
    }
}
