namespace OrleansPersistentStream_DroppedEvents.Streaming
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;
    using Orleans.Providers;
    using Orleans.Providers.Streams.Common;
    using Orleans.Streams;

    internal class TestAdapterReceiver<TSerializer> : IQueueAdapterReceiver
    where TSerializer : class, IMemoryMessageBodySerializer
    {
        private readonly ITestStreamQueueGrain queueGrain;
        private readonly List<Task> awaitingTasks;
        private readonly ILogger logger;
        private readonly TSerializer serializer;
        private readonly IQueueAdapterReceiverMonitor receiverMonitor;

        public TestAdapterReceiver(ITestStreamQueueGrain queueGrain, ILogger logger, TSerializer serializer, IQueueAdapterReceiverMonitor receiverMonitor)
        {
            this.queueGrain = queueGrain;
            this.logger = logger;
            this.serializer = serializer;
            this.awaitingTasks = new List<Task>();
            this.receiverMonitor = receiverMonitor;
        }

        public Task Initialize(TimeSpan timeout)
        {
            this.receiverMonitor?.TrackInitialization(true, TimeSpan.MinValue, null);
            return Task.CompletedTask;
        }

        public async Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
        {
            var watch = Stopwatch.StartNew();
            List<IBatchContainer> batches;
            Task<List<TestMessageData>> task = null;
            try
            {
                task = this.queueGrain.Dequeue(maxCount);
                this.awaitingTasks.Add(task);
                var eventData = await task;
                batches = eventData.Select(data => new TestBatchContainer<TSerializer>(data, this.serializer)).ToList<IBatchContainer>();
                watch.Stop();
                this.receiverMonitor?.TrackRead(true, watch.Elapsed, null);
                if (eventData.Count > 0)
                {
                    var oldestMessage = eventData[0];
                    var newestMessage = eventData[eventData.Count - 1];
                    this.receiverMonitor?.TrackMessagesReceived(eventData.Count, oldestMessage.EnqueueTimeUtc, newestMessage.EnqueueTimeUtc);
                }
            }
            catch (Exception exc)
            {
                watch.Stop();
                this.receiverMonitor?.TrackRead(true, watch.Elapsed, exc);
                throw;
            }
            finally
            {
                this.awaitingTasks.Remove(task);
            }
            return batches;
        }

        public Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
        {
            return Task.CompletedTask;
        }

        public async Task Shutdown(TimeSpan timeout)
        {
            var watch = Stopwatch.StartNew();
            try
            {
                if (this.awaitingTasks.Count != 0)
                {
                    await Task.WhenAll(this.awaitingTasks);
                }
                watch.Stop();
                this.receiverMonitor?.TrackShutdown(true, watch.Elapsed, null);
            }
            catch (Exception ex)
            {
                watch.Stop();
                this.receiverMonitor?.TrackShutdown(false, watch.Elapsed, ex);
            }
        }
    }
}
