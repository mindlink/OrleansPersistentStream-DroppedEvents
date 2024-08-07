namespace OrleansPersistentStream_DroppedEvents
{
    using Orleans.Runtime;
    using Orleans.Streams;

    [ImplicitStreamSubscription("ns")]
    internal class SlowConsumerGrain : Grain, IConsumerGrain
    {
        public override async Task OnActivateAsync(CancellationToken cancellationToken)
        {
            await this.GetStreamProvider("TestStream")
                .GetStream<int>(StreamId.Create("ns", this.GetPrimaryKey()))
                .SubscribeAsync(this);

            var rnd = new Random();

            await Task.Delay(rnd.Next(50, 300));

            await base.OnActivateAsync(cancellationToken);
        }

        public Task OnNextAsync(int item, StreamSequenceToken? token = null)
        {
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
