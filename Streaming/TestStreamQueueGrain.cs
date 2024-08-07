namespace OrleansPersistentStream_DroppedEvents.Streaming
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Orleans.Runtime;

    /// <summary>
    /// Memory stream queue grain. This grain works as a storage queue of event data. Enqueue and Dequeue operations are supported.
    /// the max event count sets the max storage limit to the queue.
    /// </summary>
    public class TestStreamQueueGrain : Grain, ITestStreamQueueGrain, IGrainMigrationParticipant
    {
        private Queue<TestMessageData> _eventQueue = new Queue<TestMessageData>();
        private long sequenceNumber = DateTime.UtcNow.Ticks;

        /// <summary>
        /// The maximum event count. 
        /// </summary>
        private const int MaxEventCount = 16384;

        /// <summary>
        /// Enqueues an event data. If the current total count reaches the max limit. throws an exception.
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        public Task Enqueue(TestMessageData data)
        {
            if (this._eventQueue.Count >= MaxEventCount)
            {
                throw new InvalidOperationException($"Can not enqueue since the count has reached its maximum of {MaxEventCount}");
            }
            data.SequenceNumber = this.sequenceNumber++;
            this._eventQueue.Enqueue(data);
            return Task.CompletedTask;
        }

        /// <summary>
        /// Dequeues up to a max amount of maxCount event data from the queue.
        /// </summary>
        /// <param name="maxCount"></param>
        /// <returns></returns>
        public Task<List<TestMessageData>> Dequeue(int maxCount)
        {
            List<TestMessageData> list = new List<TestMessageData>();

            for (int i = 0; i < maxCount && this._eventQueue.Count > 0; ++i)
            {
                list.Add(this._eventQueue.Dequeue());
            }

            return Task.FromResult(list);
        }

        void IGrainMigrationParticipant.OnDehydrate(IDehydrationContext dehydrationContext)
        {
            dehydrationContext.TryAddValue("queue", this._eventQueue);
        }

        void IGrainMigrationParticipant.OnRehydrate(IRehydrationContext rehydrationContext)
        {
            if (rehydrationContext.TryGetValue("queue", out Queue<TestMessageData> value))
            {
                this._eventQueue = value;
            }
        }
    }
}
