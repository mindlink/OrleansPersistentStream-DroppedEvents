namespace OrleansPersistentStream_DroppedEvents.Streaming
{
    using System.Collections.Generic;
    using System.Threading.Tasks;

    /// <summary>
    /// Interface for In-memory stream queue grain.
    /// </summary>
    public interface ITestStreamQueueGrain : IGrainWithGuidKey
    {
        /// <summary>
        /// Enqueues an event.
        /// </summary>
        /// <param name="data">The data.</param>
        /// <returns>A <see cref="Task"/> representing the operation.</returns>
        Task Enqueue(TestMessageData data);

        /// <summary>
        /// Dequeues up to <paramref name="maxCount"/> events.
        /// </summary>
        /// <param name="maxCount">
        /// The maximum number of events to dequeue.
        /// </param>
        /// <returns>A <see cref="Task"/> representing the operation.</returns>
        Task<List<TestMessageData>> Dequeue(int maxCount);
    }
}
