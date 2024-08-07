namespace OrleansPersistentStream_DroppedEvents
{
    using Orleans.Streams;

    internal interface IConsumerGrain : IGrainWithGuidKey, IAsyncObserver<int>
    {
    }
}
