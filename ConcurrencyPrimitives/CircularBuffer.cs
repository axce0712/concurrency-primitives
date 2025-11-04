using System.Collections;

namespace ConcurrencyPrimitives;

public sealed class CircularBuffer<T> : IEnumerable<T>
{
    private readonly int _capacity;
    private T[] _buffer;
    private int _head;
    private int _tail;
    private int _size;

    public CircularBuffer(int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity);
        _capacity = capacity;
        _buffer = new T[Math.Min(4, capacity)];
    }

    public int HeadSequence { get; private set; }

    public int TailSequence { get; private set; }

    public int RemainingSize => _capacity - _size;

    public T this[int sequence] => _buffer[sequence % _buffer.Length];
    
    public bool TryEnqueueTail(T item)
    {
        if (_size == _capacity)
        {
            return false;
        }

        if (_size == _buffer.Length)
        {
            var newLength = Math.Min(_buffer.Length * 2, _capacity);
            var newBuffer = new T[newLength];
            if (_head < _tail)
            {
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
        return true;
    }

    public bool TryAdvanceHeadTo(int sequence)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(sequence);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(sequence, TailSequence);
        var itemsToRemove = sequence - HeadSequence;
        if (itemsToRemove <= 0)
        {
            return false;
        }

        _head = (_head + itemsToRemove) % _buffer.Length;
        _size -= itemsToRemove;
        HeadSequence = sequence;
        return true;
    }

    public IEnumerator<T> GetEnumerator()
    {
        for (var i = HeadSequence; i < TailSequence; i++)
        {
            yield return this[i];
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}