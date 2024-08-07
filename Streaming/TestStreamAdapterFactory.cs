namespace OrleansPersistentStream_DroppedEvents.Streaming
{
    using System.Buffers.Binary;
    using System.Collections.Concurrent;
    using System.IO.Hashing;
    using System.Runtime.InteropServices;
    using System.Text;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.Logging;
    using Orleans.Configuration;
    using Orleans.Providers;
    using Orleans.Providers.Streams.Common;
    using Orleans.Runtime;
    using Orleans.Streams;

    /// <summary>
    /// Adapter factory for in test stream provider - this has directly ripped off the implementation of MemoryStreamAdapterFactory and plugged in a SimpleQueueCache instead of a MemoryPooledCache.
    /// </summary>
    public class TestStreamAdapterFactory<TSerializer> : IQueueAdapterFactory, IQueueAdapter, IQueueAdapterCache
        where TSerializer : class, IMemoryMessageBodySerializer
    {
        private readonly StreamCacheEvictionOptions cacheOptions;
        private readonly StreamStatisticOptions statisticOptions;
        private readonly HashRingStreamQueueMapperOptions queueMapperOptions;
        private readonly IGrainFactory grainFactory;
        private readonly ILoggerFactory loggerFactory;
        private readonly ILogger logger;
        private readonly ulong nameHash;
        private readonly TSerializer serializer;
        private readonly ulong _nameHash;
        private readonly IStreamFailureHandler streamFailureHandler = new NoOpStreamDeliveryFailureHandler();

        private IStreamQueueMapper streamQueueMapper;
        private ConcurrentDictionary<QueueId, ITestStreamQueueGrain> queueGrains;
        private IObjectPool<FixedSizeBuffer> bufferPool;
        private BlockPoolMonitorDimensions blockPoolMonitorDimensions;
        private TimePurgePredicate purgePredicate;

        /// <inheritdoc />
        public string Name { get; }

        /// <inheritdoc />
        public bool IsRewindable => true;

        /// <inheritdoc />
        public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;

        protected Func<CacheMonitorDimensions, ICacheMonitor> CacheMonitorFactory;
        protected Func<BlockPoolMonitorDimensions, IBlockPoolMonitor> BlockPoolMonitorFactory;
        protected Func<ReceiverMonitorDimensions, IQueueAdapterReceiverMonitor>? ReceiverMonitorFactory;

        public TestStreamAdapterFactory(
            string providerName,
            StreamCacheEvictionOptions cacheOptions,
            StreamStatisticOptions statisticOptions,
            HashRingStreamQueueMapperOptions queueMapperOptions,
            IServiceProvider serviceProvider,
            IGrainFactory grainFactory,
            ILoggerFactory loggerFactory)
        {
            this.Name = providerName;
            this.cacheOptions = cacheOptions ?? throw new ArgumentNullException(nameof(cacheOptions));
            this.statisticOptions = statisticOptions ?? throw new ArgumentException(nameof(statisticOptions));
            this.queueMapperOptions = queueMapperOptions ?? throw new ArgumentNullException(nameof(queueMapperOptions));
            this.grainFactory = grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));
            this.loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
            this.logger = loggerFactory.CreateLogger<ILogger<MemoryAdapterFactory<TSerializer>>>();
            this.serializer = TestMessageBodySerializerFactory<TSerializer>.GetOrCreateSerializer(serviceProvider);

            var nameBytes = BitConverter.IsLittleEndian ? MemoryMarshal.AsBytes(this.Name.AsSpan()) : Encoding.Unicode.GetBytes(this.Name);
            XxHash64.Hash(nameBytes, MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref this._nameHash, 1)));
        }

        /// <summary>
        /// Initializes this instance.
        /// </summary>
        public void Init()
        {
            this.queueGrains = new ConcurrentDictionary<QueueId, ITestStreamQueueGrain>();
            if (this.CacheMonitorFactory == null)
                this.CacheMonitorFactory = (dimensions) => new DefaultCacheMonitor(dimensions);
            if (this.BlockPoolMonitorFactory == null)
                this.BlockPoolMonitorFactory = (dimensions) => new DefaultBlockPoolMonitor(dimensions);
            if (this.ReceiverMonitorFactory == null)
                this.ReceiverMonitorFactory = (dimensions) => new DefaultQueueAdapterReceiverMonitor(dimensions);
            this.purgePredicate = new TimePurgePredicate(this.cacheOptions.DataMinTimeInCache, this.cacheOptions.DataMaxAgeInCache);
            this.streamQueueMapper = new HashRingBasedStreamQueueMapper(this.queueMapperOptions, this.Name);
        }

        private void CreateBufferPoolIfNotCreatedYet()
        {
            if (this.bufferPool == null)
            {
                // 1 meg block size pool
                this.blockPoolMonitorDimensions = new BlockPoolMonitorDimensions($"BlockPool-{Guid.NewGuid()}");
                var oneMb = 1 << 20;
                var objectPoolMonitor = new ObjectPoolMonitorBridge(this.BlockPoolMonitorFactory(this.blockPoolMonitorDimensions), oneMb);
                this.bufferPool = new ObjectPool<FixedSizeBuffer>(() => new FixedSizeBuffer(oneMb), objectPoolMonitor, this.statisticOptions.StatisticMonitorWriteInterval);
            }
        }

        /// <inheritdoc />
        public Task<IQueueAdapter> CreateAdapter()
        {
            return Task.FromResult<IQueueAdapter>(this);
        }

        /// <inheritdoc />
        public IQueueAdapterCache GetQueueAdapterCache()
        {
            return this;
        }

        /// <inheritdoc />
        public IStreamQueueMapper GetStreamQueueMapper()
        {
            return this.streamQueueMapper;
        }

        /// <inheritdoc />
        public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
        {
            var dimensions = new ReceiverMonitorDimensions(queueId.ToString());
            var receiverLogger = this.loggerFactory.CreateLogger($"{typeof(TestAdapterReceiver<TSerializer>).FullName}.{this.Name}.{queueId}");
            var receiverMonitor = this.ReceiverMonitorFactory!.Invoke(dimensions);
            IQueueAdapterReceiver receiver = new TestAdapterReceiver<TSerializer>(this.GetQueueGrain(queueId), receiverLogger, this.serializer, receiverMonitor);
            return receiver;
        }

        /// <inheritdoc />
        public async Task QueueMessageBatchAsync<T>(StreamId streamId, IEnumerable<T> events, StreamSequenceToken token, Dictionary<string, object> requestContext)
        {
            var queueId = this.streamQueueMapper.GetQueueForStream(streamId);
            ArraySegment<byte> bodyBytes = this.serializer.Serialize(new MemoryMessageBody(events.Cast<object>(), requestContext));
            var messageData = TestMessageData.Create(streamId, bodyBytes);
            ITestStreamQueueGrain queueGrain = this.GetQueueGrain(queueId);
            await queueGrain.Enqueue(messageData);
        }

        /// <inheritdoc />
        public IQueueCache CreateQueueCache(QueueId queueId)
        {
            /*CreateBufferPoolIfNotCreatedYet();
            var logger = this.loggerFactory.CreateLogger($"{typeof(MemoryPooledCache<TSerializer>).FullName}.{this.Name}.{queueId}");
            var monitor = this.CacheMonitorFactory(new CacheMonitorDimensions(queueId.ToString(), this.blockPoolMonitorDimensions.BlockPoolId));
            return new TestPooledCache<TSerializer>(bufferPool, purgePredicate, logger, this.serializer, monitor, this.statisticOptions.StatisticMonitorWriteInterval, this.cacheOptions.MetadataMinTimeInCache);*/

            return new SimpleQueueCache(100, this.logger);
        }

        /// <inheritdoc />
        public Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId)
        {
            return Task.FromResult(this.streamFailureHandler);
        }

        /// <summary>
        /// Generate a deterministic Guid from a queue Id. 
        /// </summary>
        private Guid GenerateDeterministicGuid(QueueId queueId)
        {
            Span<byte> bytes = stackalloc byte[16];
            MemoryMarshal.Write(bytes, in this.nameHash);
            BinaryPrimitives.WriteUInt32LittleEndian(bytes[8..], queueId.GetUniformHashCode());
            BinaryPrimitives.WriteUInt32LittleEndian(bytes[12..], queueId.GetNumericId());
            return new(bytes);
        }

        /// <summary>
        /// Get a MemoryStreamQueueGrain instance by queue Id. 
        /// </summary>
        private ITestStreamQueueGrain GetQueueGrain(QueueId queueId)
        {
            return this.queueGrains.GetOrAdd(queueId, (id, arg) => arg.grainFactory.GetGrain<ITestStreamQueueGrain>(arg.instance.GenerateDeterministicGuid(id)), (instance: this, this.grainFactory));
        }

        /// <summary>
        /// Creates a new <see cref="MemoryAdapterFactory{TSerializer}"/> instance.
        /// </summary>
        /// <param name="services">The services.</param>
        /// <param name="name">The provider name.</param>
        /// <returns>A mew <see cref="MemoryAdapterFactory{TSerializer}"/> instance.</returns>
        public static TestStreamAdapterFactory<TSerializer> Create(IServiceProvider services, string name)
        {
            var cachePurgeOptions = services.GetOptionsByName<StreamCacheEvictionOptions>(name);
            var statisticOptions = services.GetOptionsByName<StreamStatisticOptions>(name);
            var queueMapperOptions = services.GetOptionsByName<HashRingStreamQueueMapperOptions>(name);
            var factory = ActivatorUtilities.CreateInstance<TestStreamAdapterFactory<TSerializer>>(services, name, cachePurgeOptions, statisticOptions, queueMapperOptions);
            factory.Init();
            return factory;
        }
    }
}
