using System.Threading.Channels;

namespace ConcurrencyPrimitives.Test;

public sealed class BoundedTopicTests
{
    [Fact]
    public async Task WriteAsync_ShouldGrow()
    {
        var sut = new BoundedTopic<int>(4);
        var reader = sut.CreateReader();
        await sut.WriteAsync(1);
        await sut.WriteAsync(2);
        await sut.WriteAsync(3);
        await sut.WriteAsync(4);

        var writeTask = sut.WriteAsync(5).AsTask();
        Assert.True(reader.TryRead(out _));

        await writeTask;
        Assert.True(writeTask.IsCompletedSuccessfully);
    }

    [Fact]
    public async Task ReadAsync_ShouldHappenConcurrently()
    {
        var sut = new BoundedTopic<int>(4);
        var reader1 = sut.CreateReader();
        var reader2 = sut.CreateReader();

        var read1Task = Task.Run(async () => await reader1.ReadAsync());
        var read2Task = Task.Run(async () => await reader2.ReadAsync());
        await sut.WriteAsync(1);
        var actual = await Task.WhenAll(read1Task, read2Task);

        Assert.True(read1Task.IsCompletedSuccessfully);
        Assert.True(read2Task.IsCompletedSuccessfully);
        Assert.Equal(1, actual[0]);
        Assert.Equal(1, actual[1]);
    }
    
    [Fact]
    public async Task Complete_ShouldUnblockWriters()
    {
        var sut = new BoundedTopic<int>(2);
        await sut.WriteAsync(1);
        await sut.WriteAsync(2);
        var writeTask = sut.WriteAsync(3).AsTask();
        
        sut.Complete();
        
        await Assert.ThrowsAsync<ChannelClosedException>(async () => await writeTask);
    }
    
    [Fact]
    public async Task Complete_ShouldUnblockReaders()
    {
        var sut = new BoundedTopic<int>(2);
        var reader = sut.CreateReader();
        var readTask = reader.ReadAsync().AsTask();
        
        sut.Complete();
        
        await Assert.ThrowsAsync<ChannelClosedException>(async () => await readTask);
    }
}
