namespace OrleansPersistentStream_DroppedEvents
{
    using Orleans.Runtime;
    using Orleans.Streams;

    [ImplicitStreamSubscription("ns")]
    internal class ConsumerGrain : Grain, IConsumerGrain
    {
        private int lastSeenEvent = 0;

        public override async Task OnActivateAsync(CancellationToken cancellationToken)
        {
            await this.GetStreamProvider("TestStream")
                .GetStream<int>(StreamId.Create("ns", this.GetPrimaryKey()))
                .SubscribeAsync(this);

            await base.OnActivateAsync(cancellationToken);
        }

        public Task OnNextAsync(int item, StreamSequenceToken? token = null)
        {
            if (item != this.lastSeenEvent + 1)
            {
                Console.WriteLine(
                    $"Skipped events detected. Last observed event is {this.lastSeenEvent} but received event is {item}.");
            }

            this.lastSeenEvent = item;

            Console.WriteLine($"Updated last observed event to: {item}.");

            return Task.CompletedTask;
        }

        public Task OnCompletedAsync()
        {
            return Task.CompletedTask;
        }

        public Task OnErrorAsync(Exception ex)
        {
            Console.WriteLine("Got error");

            return Task.CompletedTask;
        }
    }
}
