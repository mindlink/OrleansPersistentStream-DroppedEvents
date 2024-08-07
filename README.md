# Overview
There appears to be a race when registering an implicit stream producer when it has multiple consumers that negotiate stream setup at different speeds. A Consumer can process an entire batch of events and become `Inactive` before the other Consumers finish handling the initial stream handshake. Subsequent batches that occur in the interim are effectively dropped for the faster Consumer. This issue is masked when using cache implementations that support rewind and time-based eviction like MemoryPooledCache, but is expressed dangerously when using the SimpleQueueCache which is designed to purge cache items as soon as they have been handled by all Consumers.

## The Bug
When PersistentStreamPullingAgent starts registering itself as the producer of an Implicit stream for the first time it statically resolves the Consumers from the mappings in the ImplicitStreamPubSub.

It starts stream cursors for each of these consumers and waits for all of them to be finished (perform initial stream handshake) before setting a state flag on the StreamConsumerCollection.StreamRegistered.

The bug occurs when both these things happen:
1. There are multiple Consumers and the time it takes them to initialize and perform the stream handshake differs.
2. Additional batches of messages are pulled off the queue while this happens (i.e. while the producer is still registering).

This is the sequence:
1. First batch pulled off the cache gets processed
2. No stream data exists in the pubSubCache so the agent registers itself as the producer of the stream and starts the initial cursors

    ```
    else
    {
        RegisterStream(streamId, startToken, now).Ignore(); // if this is a new stream register as producer of stream in pub sub system
    }
    ```

3. First Consumer (ConsumerFast) performs the initial handshake and processes the first batch to completion, making the cursor `Inactive` again. (e.g. `StreamSequenceToken=1`, `Events=1,2,3`)
4. Second Consumer (ConsumerSlow) is still activating and doing the handshake
5. Second batch (e.g. `StreamSequenceToken=2`, `Events=4,5,6`) is pulled off the queue but because stream registration has started but not completed and the ConsumerFast's cursor is `Inactive`, ReadFromQueue effectively does nothing.

    ```
    else
    {
        if(this.logger.IsEnabled(LogLevel.Debug))
            this.logger.LogDebug(
                $"Pulled new messages in stream {streamId} from the queue, but pulling agent haven't succeeded in" +
                                                $"RegisterStream yet, will start deliver on this stream after RegisterStream succeeded");
    }
    ```

6. ConsumerSlow finishes handshake, the stream is registered, StreamConsumerCollection.StreamRegistered is set to True.
7. Third batch (`StreamSequenceToken=3`, `Events=7,8`) is pulled off the queue, inactive cursors are woken up since the stream is now registered.
    ```
    if (streamData.StreamRegistered)
    {
        StartInactiveCursors(streamData,
            startToken); // if this is an existing stream, start any inactive cursors
    }
   ```

8. When activating ConsumerFast cursor it refreshes the `startToken` first with the latest batch token (e.g. `StreamSequenceToken=3` from the _third_ batch).

    ```
    consumerData.Cursor?.Refresh(startToken);
    if (consumerData.State == StreamConsumerDataState.Inactive)
    {
        // wake up inactive consumers
        RunConsumerCursor(consumerData).Ignore();
    }
    ```
9. The implementation for a `SimpleQueueCacheCursor` will set the token internally because it had been unset after completely processing the initial batch (`StreamSequenceToken=1`).

    ```
    // SimpleQueueCacheCursor.cs
    if (!IsSet)
    {
        cache.RefreshCursor(this, sequenceToken);
    }
    ```

10. The cursor is then run from event 7. Events 4-6 have been skipped.

## Repro
The code sample rips off the implementation of the MemoryStream stack, plugging in a SimpleQueueCache in the `TestStreamAdapterFactory` rather than the `MemoryPooledCache`. It then uses three grains to re-create the race condition described above:

1. `ProducerGrain`
2. `ConsumerGrain`
3. `SlowConsumerGrain`

The `ConsumerGrain` and `SlowConsumerGrain` have an Implicit subscription on `ProducerGrain`.

`ProducerGrain` is told to emit events 1-10 in order, `ConsumerGrain` writes out to the console as it sees each event and warns of any breaks in the event sequence.

`SlowConsumerGrain` introduces a delay when activating to prolong the stream registration in the `PersistentStreamPullingAgent.cs`.

> **_NOTE:_** The race is obviously highly dependent on timings so an element of randomness has been added to the emit-events loop in the ProducerGrain and the SlowConsumerGrain delayed activation to try and capture the race on other systems as well. The repro is run 20 times to account for environment/random-delay variability, but on my machine these conditions produce dropped events in about 80% of the runs.

### Expected Output (x20):
```
Emitting events...
Updated last observed event to: 1.
Updated last observed event to: 2.
Updated last observed event to: 3.
Updated last observed event to: 4.
Updated last observed event to: 5.
Updated last observed event to: 6.
Updated last observed event to: 7.
Updated last observed event to: 8.
Updated last observed event to: 9.
Updated last observed event to: 10.
```

### Actual Output:
```
Emitting events...
Updated last observed event to: 1.
Updated last observed event to: 2.
Updated last observed event to: 3.
Skipped events detected. Last observed event is 3 but received event is 7.
Updated last observed event to: 7.
Updated last observed event to: 8.
Updated last observed event to: 9.
Updated last observed event to: 10.
```

## Solution
Initially I thought we could change the `SimpleQueueCache` to fix the problem by changing the way it unsets the stream token after becoming inactive, but after poking around the cache implementation more I don't think that's actually the problem here.

Seems to me it's the `PersistentStreamPullingAgent` at fault because it's letting batches slip through the cracks and is also breaking the fundamental principal around independent consumer cursor progression. Just because the stream isn't fully registered doesn't mean there aren't _some_ cursors ready to process more batches.

As a potential solution I was thinking you could add a third state to `StreamConsumerDataState` called `Registering` or something and initialize the `StreamConsumerData` in that state rather that `Inactive`. Then, when a batch is being pulled off the queue, instead of doing nothing if the stream is still registering, try to start the inactive cursors anyway. Those that are `Registering` won't be disturbed while they're in the handshake flow, but those that have already completed the handshake and become `Inactive` will be allowed to process more batches while stream registration is still in progress meaning we won't skip batches on fast Consumers:

```
if (pubSubCache.TryGetValue(streamId, out streamData))
{
    streamData.RefreshActivity(now);

    StartInactiveCursors(streamData, startToken); // if this is an existing stream, start any inactive cursors

}
else
{
    RegisterStream(streamId, startToken, now).Ignore(); // if this is a new stream register as producer of stream in pub sub system
}
```

Just a thought though, haven't followed all the details through or tried it out - interested to get opinions/alternatives!