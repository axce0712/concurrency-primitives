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

        var read1Task = reader1.ReadAsync().AsTask();
        var read2Task = reader2.ReadAsync().AsTask();
        await sut.WriteAsync(1);
        var actual = await Task.WhenAll(read1Task, read2Task);

        Assert.True(read1Task.IsCompletedSuccessfully);
        Assert.True(read2Task.IsCompletedSuccessfully);
        Assert.Equal(1, actual[0]);
        Assert.Equal(1, actual[1]);
    }
}
