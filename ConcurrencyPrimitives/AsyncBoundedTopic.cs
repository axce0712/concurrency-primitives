using System.Threading.Channels;

namespace ConcurrencyPrimitives;

public sealed class AsyncBoundedTopic<T>
{
    private readonly int _capacity;

    // Circular buffer
    private T[] _buffer;
    private int _head; // physical index of first valid item
    private int _tail; // physical index of next free slot
    private int _size;

    // Readers
    private readonly List<AsyncBoundedTopicReader<T>> _readers = [];

    // Async lock for thread-safety
    internal AsyncLock Lock { get; } = new();

    // Writers waiting for space
    private TaskCompletionSource? _tcs;

    internal bool Completed { get; private set; }

    // logical sequence of first valid item
    internal int HeadSequence { get; private set; }

    // logical sequence of next item to write
    internal int TailSequence { get; private set; }

    public AsyncBoundedTopic(int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity);
        _capacity = capacity;
        _buffer = new T[Math.Min(4, capacity)];
    }

    public async ValueTask WriteAsync(T item, CancellationToken cancellationToken = default)
    {
        while (true)
        {
            if (Completed)
            {
                throw new InvalidOperationException("The topic is already completed.");
            }

            Task? waitTask;
            using (await Lock.LockAsync(cancellationToken))
            {
                // Check if there is total capacity left
                if (_size < _capacity)
                {
                    // Grow  buffer if needed
                    if (_size == _buffer.Length)
                    {
                        var newLength = Math.Min(_buffer.Length * 2, _capacity);
                        var newBuffer = new T[newLength];
                        if (_head < _tail)
                        {
                            // contiguous block
                            Array.Copy(_buffer, _head, newBuffer, 0, _size);
                        }
                        else
                        {
                            var rightCount = _buffer.Length - _head;
                            Array.Copy(_buffer, _head, newBuffer, 0, rightCount);
                            Array.Copy(_buffer, 0, newBuffer, rightCount, _tail);
                        }

                        _buffer = newBuffer;
                        _head = 0;
                        _tail = _size;
                    }

                    _buffer[_tail] = item;
                    _tail = (_tail + 1) % _buffer.Length;
                    _size++;
                    TailSequence++;
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
        if (Completed)
        {
            throw new InvalidOperationException("The topic is already completed.");
        }

        using (await Lock.LockAsync())
        {
            var reader = new AsyncBoundedTopicReader<T>(this, TailSequence);
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
        var minRead = _readers.Select(x => x.ReadIndex)
            .DefaultIfEmpty(TailSequence)
            .Min();

        var itemsToRemove = minRead - HeadSequence;
        if (itemsToRemove == 0)
        {
            return;
        }

        _head = (_head + itemsToRemove) % _buffer.Length;
        _size -= itemsToRemove;
        HeadSequence = minRead;
        _tcs?.TrySetResult();
        _tcs = null;
    }

    public T GetItem(int position) => _buffer[position % _buffer.Length];
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

        if (ReadIndex < _topic.TailSequence)
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
                    if (ReadIndex < _topic.TailSequence)
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
        if (ReadIndex >= _topic.TailSequence)
        {
            throw new InvalidOperationException("No item available. Call WaitToReadAsync first.");
        }

        var lockTask = _topic.Lock.LockAsync(cancellationToken);
        if (lockTask.IsCompletedSuccessfully)
        {
            using (lockTask.Result)
            {
                var item = _topic.GetItem(ReadIndex);
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
                var item = _topic.GetItem(ReadIndex);
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
