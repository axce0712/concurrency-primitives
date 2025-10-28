using System.Threading.Channels;

namespace ConcurrencyPrimitives;

public sealed class AsyncBoundedTopic<T>(int capacity)
{
    // Readers
    private readonly List<AsyncBoundedTopicReader<T>> _readers = [];
    
    // Writers waiting for space
    private TaskCompletionSource? _tcs; 

    // Async lock for thread-safety
    internal AsyncLock Lock { get; } = new();
    
    internal bool Completed { get; private set; }
    
    internal CircularBuffer<T> Buffer { get; } = new(capacity);
    
    public async ValueTask WriteAsync(T item, CancellationToken cancellationToken = default)
    {
        while (true)
        {
            Task? waitTask;
            using (await Lock.LockAsync(cancellationToken))
            {
                if (Completed)
                {
                    throw new InvalidOperationException("The topic is already completed.");
                }
                
                if (Buffer.TryEnqueueTail(item))
                {
                    foreach (var reader in _readers)
                    {
                        reader.SignalNewItem();
                    }

                    return;
                }

                _tcs ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                waitTask = _tcs.Task;
            }

            await waitTask.WaitAsync(cancellationToken);
        }
    }

    public async ValueTask<AsyncBoundedTopicReader<T>> CreateReader()
    {
        using (await Lock.LockAsync())
        {
            if (Completed)
            {
                throw new InvalidOperationException("The topic is already completed.");
            }
            
            var reader = new AsyncBoundedTopicReader<T>(this, Buffer.TailSequence);
            _readers.Add(reader);
            return reader;
        }
    }

    public async ValueTask Complete()
    {
        if (Completed)
        {
            return;
        }

        using (await Lock.LockAsync())
        {
            Completed = true;
            Interlocked.Exchange(ref _tcs, null)
                ?.TrySetException(new InvalidOperationException("The topic is already completed."));
        }
    }

    internal void TryAdvanceHead()
    {
        var minRead = _readers.Count == 0 ? Buffer.TailSequence : _readers.Min(x => x.ReadIndex);
        if (!Buffer.TryAdvanceHeadTo(minRead))
        {
            return;
        }
        
        _tcs?.TrySetResult();
        _tcs = null;
    }
}

public sealed class AsyncBoundedTopicReader<T>
{
    private TaskCompletionSource? _tcs;
    private readonly AsyncBoundedTopic<T> _topic;

    internal AsyncBoundedTopicReader(AsyncBoundedTopic<T> topic, int startIndex)
    {
        _topic = topic;
        ReadIndex = startIndex;
    }

    internal int ReadIndex { get; private set; }

    public ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken = default)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return ValueTask.FromCanceled<bool>(cancellationToken);
        }

        if (ReadIndex < _topic.Buffer.TailSequence)
        {
            return ValueTask.FromResult(true);
        }

        return _topic.Completed ? ValueTask.FromResult(false) : WaitToReadAsyncCore(cancellationToken);

        async ValueTask<bool> WaitToReadAsyncCore(CancellationToken ct)
        {
            while (true)
            {
                TaskCompletionSource? tcs;
                using (await _topic.Lock.LockAsync(ct))
                {
                    // Item available
                    if (ReadIndex < _topic.Buffer.TailSequence)
                    {
                        return true;
                    }

                    if (_topic.Completed)
                    {
                        return false;
                    }

                    // Wait for new item
                    _tcs ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    tcs = _tcs;
                }

                // Wait outside the lock
                await tcs.Task.WaitAsync(ct);
                _tcs = null;
            }
        }
    }

    public ValueTask<T> GetCurrentAsync(CancellationToken cancellationToken = default)
    {
        if (ReadIndex >= _topic.Buffer.TailSequence)
        {
            throw new InvalidOperationException("No item available. Call WaitToReadAsync first.");
        }

        var lockTask = _topic.Lock.LockAsync(cancellationToken);
        if (lockTask.IsCompletedSuccessfully)
        {
            using (lockTask.Result)
            {
                var item = _topic.Buffer[ReadIndex];
                ReadIndex++;
                _topic.TryAdvanceHead();
                return ValueTask.FromResult(item);
            }
        }

        return GetCurrentAsyncCore(cancellationToken);

        async ValueTask<T> GetCurrentAsyncCore(CancellationToken ct)
        {
            using (await _topic.Lock.LockAsync(ct))
            {
                var item = _topic.Buffer[ReadIndex];
                ReadIndex++;
                _topic.TryAdvanceHead();
                return item;
            }
        }
    }

    public async ValueTask<T> ReadAsync(CancellationToken cancellationToken = default)
    {
        if (await WaitToReadAsync(cancellationToken).ConfigureAwait(false))
        {
            return await GetCurrentAsync(cancellationToken);
        }

        throw new ChannelClosedException();
    }

    internal void SignalNewItem()
    {
        Interlocked.Exchange(ref _tcs, null)?.TrySetResult();
    }
}
