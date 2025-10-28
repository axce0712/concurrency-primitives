using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace ConcurrencyPrimitives;

public sealed class BoundedTopic<T>(int capacity)
{
    private readonly List<BoundedTopicReader<T>> _readers = [];
    private readonly Queue<TaskCompletionSource> _writers = new();

    internal object SyncObj { get; } = new();

    internal CircularBuffer<T> Buffer { get; } = new(capacity);

    internal bool Completed { get; private set; }

    public ValueTask<bool> WaitToWriteAsync(CancellationToken cancellationToken = default)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return ValueTask.FromCanceled<bool>(cancellationToken);
        }

        TaskCompletionSource? tcs;
        lock (SyncObj)
        {
            if (Completed)
            {
                return ValueTask.FromResult(false);
            }

            if (Buffer.RemainingSize > 0)
            {
                return ValueTask.FromResult(true);
            }

            tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _writers.Enqueue(tcs);
        }

        return WaitAsync(tcs, cancellationToken);
        
        static async ValueTask<bool> WaitAsync(TaskCompletionSource tcs, CancellationToken ct)
        {
            await using var registration = ct.Register(() => tcs.TrySetCanceled(ct));
            await tcs.Task.ConfigureAwait(false);
            return true;
        }
    }

    public bool TryWrite(T item)
    {
        lock (SyncObj)
        {
            if (Completed)
            {
                return false;
            }

            if (!Buffer.TryEnqueueTail(item))
            {
                return false;
            }
            
            foreach (var reader in _readers)
            {
                reader.SignalNewItem();
            }

            if (_writers.TryDequeue(out var writer))
            {
                writer.TrySetResult();
            }
            
            return true;
        }
    }

    public ValueTask WriteAsync(T item, CancellationToken cancellationToken = default)
    {
        try
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return ValueTask.FromCanceled(cancellationToken);
            }

            if (TryWrite(item))
            {
                return ValueTask.CompletedTask;
            }

            return WriteAsyncCore(item, this, cancellationToken);
        }
        catch (Exception e)
        {
            return new ValueTask(Task.FromException(e));
        }

        static async ValueTask WriteAsyncCore(T item, BoundedTopic<T> boundedTopic, CancellationToken ct)
        {
            while (await boundedTopic.WaitToWriteAsync(ct).ConfigureAwait(false))
            {
                if (boundedTopic.TryWrite(item))
                {
                    return;
                }
            }

            throw new ChannelClosedException();
        }
    }

    public BoundedTopicReader<T> CreateReader()
    {
        lock (SyncObj)
        {
            if (Completed)
            {
                throw new ChannelClosedException();
            }
            
            var reader = new BoundedTopicReader<T>(this, Buffer.TailSequence);
            _readers.Add(reader);
            return reader;
        }
    }

    public void Complete()
    {
        lock (SyncObj)
        {
            Completed = true;
            foreach (var reader in _readers)
            {
                reader.SignalNewItem();
            }

            while (_writers.TryDequeue(out var writer))
            {
                writer.TrySetException(new ChannelClosedException());
            }
        }
    }

    internal void TryAdvanceHead()
    {
        var minIndex = _readers.Count == 0 ? Buffer.TailSequence : _readers.Min(x => x.ReadIndex);
        if (!Buffer.TryAdvanceHeadTo(minIndex))
        {
            return;
        }
        
        if (Buffer.RemainingSize > 0 && _writers.TryDequeue(out var writer))
        {
            writer.TrySetResult();
        }
    }
}

public sealed class BoundedTopicReader<T>
{
    private readonly BoundedTopic<T> _topic;
    private TaskCompletionSource? _tcs;

    internal BoundedTopicReader(BoundedTopic<T> topic, int startIndex)
    {
        _topic = topic;
        ReadIndex = startIndex;
    }

    public int Count
    {
        get
        {
            lock (_topic.SyncObj)
            {
                return _topic.Buffer.TailSequence - ReadIndex;
            }
        }
    }

    internal int ReadIndex { get; private set; }

    public ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken = default)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return ValueTask.FromCanceled<bool>(cancellationToken);
        }

        lock (_topic.SyncObj)
        {
            if (ReadIndex < _topic.Buffer.TailSequence)
            {
                return new ValueTask<bool>(true);
            }

            if (_topic.Completed)
            {
                return new ValueTask<bool>(false);
            }

            _tcs ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            return WaitAsync(_tcs, cancellationToken);

            static async ValueTask<bool> WaitAsync(TaskCompletionSource tcs, CancellationToken ct)
            {
                await using var registration = ct.Register(() => tcs.TrySetCanceled(ct));
                await tcs.Task.ConfigureAwait(false);
                return true;
            }
        }
    }

    public bool TryRead([MaybeNullWhen(false)] out T item)
    {
        if (!TryPeek(out item))
        {
            return false;
        }

        lock (_topic.SyncObj)
        {
            ReadIndex++;
            _topic.TryAdvanceHead();
        }
            
        return true;

    }

    public bool TryPeek([MaybeNullWhen(false)] out T item)
    {
        item = default;
        if (ReadIndex >= _topic.Buffer.TailSequence)
        {
            return false;
        }

        item = _topic.Buffer[ReadIndex];
        return true;
    }

    public ValueTask<T> ReadAsync(CancellationToken cancellationToken = default)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return ValueTask.FromCanceled<T>(cancellationToken);
        }

        if (TryRead(out var item))
        {
            return ValueTask.FromResult(item);
        }

        return ReadAsyncCore(this, cancellationToken);

        static async ValueTask<T> ReadAsyncCore(BoundedTopicReader<T> reader, CancellationToken ct)
        {
            while (true)
            {
                if (!await reader.WaitToReadAsync(ct).ConfigureAwait(false))
                {
                    throw new ChannelClosedException();
                }

                if (reader.TryRead(out var item))
                {
                    return item;
                }
            }
        }
    }
    
    public async IAsyncEnumerable<T> ReadAllAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        while (await WaitToReadAsync(cancellationToken).ConfigureAwait(false))
        {
            while (TryRead(out var item))
            {
                yield return item;
            }
        }
    }

    internal void SignalNewItem()
    {
        Interlocked.Exchange(ref _tcs, null)?.TrySetResult();
    }
}