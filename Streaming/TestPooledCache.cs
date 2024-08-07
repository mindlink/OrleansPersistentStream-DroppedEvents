namespace OrleansPersistentStream_DroppedEvents.Streaming
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using Microsoft.Extensions.Logging;
    using Orleans.Providers;
    using Orleans.Providers.Streams.Common;
    using Orleans.Runtime;
    using Orleans.Streams;

    /// <summary>
    /// Pooled cache for memory stream provider
    /// </summary>
    public class TestPooledCache<TSerializer> : IQueueCache, ICacheDataAdapter
        where TSerializer : class, IMemoryMessageBodySerializer
    {
        private readonly IObjectPool<FixedSizeBuffer> bufferPool;
        private readonly TSerializer serializer;
        private readonly IEvictionStrategy evictionStrategy;
        private readonly PooledQueueCache cache;

        private FixedSizeBuffer currentBuffer;

        /// <summary>
        /// Pooled cache for memory stream provider.
        /// </summary>
        /// <param name="bufferPool">The buffer pool.</param>
        /// <param name="purgePredicate">The purge predicate.</param>
        /// <param name="logger">The logger.</param>
        /// <param name="serializer">The serializer.</param>
        /// <param name="cacheMonitor">The cache monitor.</param>
        /// <param name="monitorWriteInterval">The monitor write interval.</param>
        public TestPooledCache(
            IObjectPool<FixedSizeBuffer> bufferPool,
            TimePurgePredicate purgePredicate,
            ILogger logger,
            TSerializer serializer,
            ICacheMonitor cacheMonitor,
            TimeSpan? monitorWriteInterval,
            TimeSpan? purgeMetadataInterval)
        {
            this.bufferPool = bufferPool;
            this.serializer = serializer;
            this.cache = new PooledQueueCache(this, logger, cacheMonitor, monitorWriteInterval, purgeMetadataInterval);
            this.evictionStrategy = new ChronologicalEvictionStrategy(logger, purgePredicate, cacheMonitor, monitorWriteInterval) { PurgeObservable = this.cache };
        }

        private CachedMessage QueueMessageToCachedMessage(TestMessageData queueMessage, DateTime dequeueTimeUtc)
        {
            StreamPosition streamPosition = this.GetStreamPosition(queueMessage);
            return new CachedMessage()
            {
                StreamId = streamPosition.StreamId,
                SequenceNumber = queueMessage.SequenceNumber,
                EnqueueTimeUtc = queueMessage.EnqueueTimeUtc,
                DequeueTimeUtc = dequeueTimeUtc,
                Segment = this.SerializeMessageIntoPooledSegment(queueMessage)
            };
        }

        // Placed object message payload into a segment from a buffer pool.  When this get's too big, older blocks will be purged
        private ArraySegment<byte> SerializeMessageIntoPooledSegment(TestMessageData queueMessage)
        {
            // serialize payload
            int size = SegmentBuilder.CalculateAppendSize(queueMessage.Payload);

            // get segment from current block
            ArraySegment<byte> segment;
            if (this.currentBuffer == null || !this.currentBuffer.TryGetSegment(size, out segment))
            {
                // no block or block full, get new block and try again
                this.currentBuffer = this.bufferPool.Allocate();
                //call EvictionStrategy's OnBlockAllocated method
                this.evictionStrategy.OnBlockAllocated(this.currentBuffer);
                // if this fails with clean block, then requested size is too big
                if (!this.currentBuffer.TryGetSegment(size, out segment))
                {
                    string errmsg = string.Format(CultureInfo.InvariantCulture,
                        "Message size is too big. MessageSize: {0}", size);
                    throw new ArgumentOutOfRangeException(nameof(queueMessage), errmsg);
                }
            }
            // encode namespace, offset, partitionkey, properties and payload into segment
            int writeOffset = 0;
            SegmentBuilder.Append(segment, ref writeOffset, queueMessage.Payload);
            return segment;
        }

        private StreamPosition GetStreamPosition(TestMessageData queueMessage)
        {
            return new StreamPosition(queueMessage.StreamId,
                new EventSequenceTokenV2(queueMessage.SequenceNumber));
        }

        private class Cursor : IQueueCacheCursor
        {
            private readonly PooledQueueCache cache;
            private readonly object cursor;
            private IBatchContainer current;

            public Cursor(PooledQueueCache cache, StreamId streamId,
                StreamSequenceToken token)
            {
                this.cache = cache;
                this.cursor = cache.GetCursor(streamId, token);
            }

            public void Dispose()
            {
            }

            public IBatchContainer GetCurrent(out Exception exception)
            {
                exception = null;
                return this.current;
            }

            public bool MoveNext()
            {
                IBatchContainer next;
                if (!this.cache.TryGetNextMessage(this.cursor, out next))
                {
                    return false;
                }

                this.current = next;
                return true;
            }

            public void Refresh(StreamSequenceToken token)
            {
            }

            public void RecordDeliveryFailure()
            {
            }
        }

        /// <inheritdoc/>
        public int GetMaxAddCount()
        {
            return 100;
        }

        /// <inheritdoc/>
        public void AddToCache(IList<IBatchContainer> messages)
        {
            DateTime utcNow = DateTime.UtcNow;
            List<CachedMessage> memoryMessages = messages
                .Cast<TestBatchContainer<TSerializer>>()
                .Select(container => container.MessageData)
                .Select(batch => this.QueueMessageToCachedMessage(batch, utcNow))
                .ToList();
            this.cache.Add(memoryMessages, DateTime.UtcNow);
        }

        /// <inheritdoc/>
        public bool TryPurgeFromCache(out IList<IBatchContainer> purgedItems)
        {
            purgedItems = null;
            this.evictionStrategy.PerformPurge(DateTime.UtcNow);
            return false;
        }

        /// <inheritdoc/>
        public IQueueCacheCursor GetCacheCursor(StreamId streamId, StreamSequenceToken token)
        {
            return new Cursor(this.cache, streamId, token);
        }

        /// <inheritdoc/>
        public bool IsUnderPressure()
        {
            return false;
        }

        /// <inheritdoc/>
        public IBatchContainer GetBatchContainer(ref CachedMessage cachedMessage)
        {
            //Deserialize payload
            int readOffset = 0;
            ArraySegment<byte> payload = SegmentBuilder.ReadNextBytes(cachedMessage.Segment, ref readOffset);
            TestMessageData message = TestMessageData.Create(cachedMessage.StreamId, new ArraySegment<byte>(payload.ToArray()));
            message.SequenceNumber = cachedMessage.SequenceNumber;
            return new TestBatchContainer<TSerializer>(message, this.serializer);
        }

        /// <inheritdoc/>
        public StreamSequenceToken GetSequenceToken(ref CachedMessage cachedMessage)
        {
            return new EventSequenceToken(cachedMessage.SequenceNumber);
        }
    }
}
