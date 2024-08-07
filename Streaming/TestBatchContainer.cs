namespace OrleansPersistentStream_DroppedEvents.Streaming
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Orleans.Providers;
    using Orleans.Providers.Streams.Common;
    using Orleans.Runtime;
    using Orleans.Serialization;
    using Orleans.Streams;

    [Serializable]
    [GenerateSerializer]
    [SerializationCallbacks(typeof(OnDeserializedCallbacks))]
    internal sealed class TestBatchContainer<TSerializer> : IBatchContainer, IOnDeserialized
    where TSerializer : class, IMemoryMessageBodySerializer
    {
        [NonSerialized]
        private TSerializer serializer;
        [Id(0)]
        private readonly EventSequenceToken realToken;

        public StreamId StreamId => this.MessageData.StreamId;
        public StreamSequenceToken SequenceToken => this.realToken;

        [Id(1)]
        public TestMessageData MessageData { get; set; }
        public long SequenceNumber => this.realToken.SequenceNumber;

        // Payload is local cache of deserialized payloadBytes.  Should never be serialized as part of batch container.  During batch container serialization raw payloadBytes will always be used.
        [NonSerialized] private MemoryMessageBody payload;

        private MemoryMessageBody Payload()
        {
            return this.payload ?? (this.payload = this.serializer.Deserialize(this.MessageData.Payload));
        }

        public TestBatchContainer(TestMessageData messageData, TSerializer serializer)
        {
            this.serializer = serializer;
            this.MessageData = messageData;
            this.realToken = new EventSequenceToken(messageData.SequenceNumber);
        }

        public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>()
        {
            return this.Payload().Events.Cast<T>().Select((e, i) => Tuple.Create<T, StreamSequenceToken>(e, this.realToken.CreateSequenceTokenForEvent(i)));
        }

        public bool ImportRequestContext()
        {
            var context = this.Payload().RequestContext;
            if (context != null)
            {
                RequestContextExtensions.Import(context);
                return true;
            }
            return false;
        }

        void IOnDeserialized.OnDeserialized(DeserializationContext context)
        {
            this.serializer = TestMessageBodySerializerFactory<TSerializer>.GetOrCreateSerializer(context.ServiceProvider);
        }
    }
}
