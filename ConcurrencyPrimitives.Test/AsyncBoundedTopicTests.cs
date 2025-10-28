using System.Threading.Channels;

namespace ConcurrencyPrimitives.Test;

public sealed class AsyncBoundedTopicTests
{
    [Fact]
    public async Task WriteAsync_ShouldGrow()
    {
        var sut = new AsyncBoundedTopic<int>(4);
        var reader = await sut.CreateReader();
        await sut.WriteAsync(1);
        await sut.WriteAsync(2);
        await sut.WriteAsync(3);
        await sut.WriteAsync(4);

        var writeTask = sut.WriteAsync(5).AsTask();
        await reader.GetCurrentAsync();

        await writeTask.WaitAsync(TimeSpan.FromMilliseconds(1));
        Assert.True(writeTask.IsCompletedSuccessfully);
    }

    [Fact]
    public async Task ReadAsync_ShouldReadTheLatestItemWhenSubscribed()
    {
        var sut = new AsyncBoundedTopic<int>(4);
        await sut.WriteAsync(1);
        var reader = await sut.CreateReader();
        await sut.WriteAsync(2);
        
        var actual = await reader.ReadAsync();

        Assert.Equal(2, actual);
    }

    [Fact]
    public async Task ReadAsync_ShouldWaitTillItemIsAvailable()
    {
        var sut = new AsyncBoundedTopic<int>(4);
        var reader = await sut.CreateReader();
        var actualTask = reader.ReadAsync().AsTask();
        
        await sut.WriteAsync(1);

        var actual = await actualTask.WaitAsync(TimeSpan.FromMilliseconds(1));
        Assert.Equal(1, actual);
    }
}