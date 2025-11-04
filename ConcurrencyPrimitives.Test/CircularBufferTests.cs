namespace ConcurrencyPrimitives.Test;

public sealed class CircularBufferTests
{
    [Fact]
    public void TryEnqueueTail_ShouldFillBuffer()
    {
        var sut = new CircularBuffer<int>(2);

        var added = sut.TryEnqueueTail(1);
        
        Assert.True(added);
        Assert.Equal([1], sut);
        Assert.Equal(0, sut.HeadSequence);
        Assert.Equal(1, sut.TailSequence);
        Assert.Equal(1, sut.RemainingSize);
    }

    [Fact]
    public void TryEnqueueHead_ShouldFailWhenBufferIsFull()
    {
        var sut = new CircularBuffer<int>(2);
        sut.TryEnqueueTail(1);
        sut.TryEnqueueTail(2);
        
        var added = sut.TryEnqueueTail(3);
        
        Assert.False(added);
        Assert.Equal([1, 2], sut);
        Assert.Equal(0, sut.HeadSequence);
    }

    [Fact]
    public void TryAdvanceHeadTo_ShouldReleaseBuffer()
    {
        var sut = new CircularBuffer<int>(2);
        sut.TryEnqueueTail(1);
        sut.TryEnqueueTail(2);

        var advanced = sut.TryAdvanceHeadTo(1);

        Assert.True(advanced);
        Assert.Equal([2], sut);
        Assert.Equal(1, sut.HeadSequence);
        Assert.Equal(2, sut.TailSequence);
    }

    [Fact]
    public void TryEnqueueTail_ShouldFillBufferWhenSpaceIsAvailableAgain()
    {
        var sut = new CircularBuffer<int>(2);
        sut.TryEnqueueTail(1);
        sut.TryEnqueueTail(2);
        sut.TryAdvanceHeadTo(1);

        var added = sut.TryEnqueueTail(3);
        
        Assert.True(added);
        Assert.Equal([2, 3], sut);
        Assert.Equal(0, sut.RemainingSize);
    }
    
    [Fact]
    public void TryAdvanceToHead_ShouldFailWhenIndexIsOutOfRange()
    {
        var sut = new CircularBuffer<int>(2);
        sut.TryEnqueueTail(1);
        sut.TryEnqueueTail(2);

        void When() => sut.TryAdvanceHeadTo(3);

        Assert.Throws<ArgumentOutOfRangeException>(When);
    }
}