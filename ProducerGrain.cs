namespace OrleansPersistentStream_DroppedEvents
{
    using Orleans.Runtime;

    internal class ProducerGrain : Grain, IProducerGrain
    {
        public async Task EmitEventsAsync()
        {
            var stream = this.GetStreamProvider("TestStream")
                .GetStream<int>(StreamId.Create("ns", this.GetPrimaryKey()));

            Console.WriteLine("Emitting events...");

            var rnd = new Random();

            foreach (var i in Enumerable.Range(1, 10))
            {
                await stream.OnNextAsync(i);

                await Task.Delay(rnd.Next(0, 100));
            }
        }
    }
}
