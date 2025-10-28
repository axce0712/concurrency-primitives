namespace ConcurrencyPrimitives;

public sealed class AsyncLock
{
    private readonly SemaphoreSlim _semaphore = new(1, 1);
    private readonly Task<IDisposable> _releaser;

    public AsyncLock()
    {
        _releaser = Task.FromResult<IDisposable>(new Releaser(this));
    }

    public Task<IDisposable> LockAsync(CancellationToken cancellationToken = default)
    {
        var wait = _semaphore.WaitAsync(cancellationToken);
        
        // Fast path: lock already available and not cancelled
        if (wait.IsCompletedSuccessfully)
        {
            return _releaser;
        }
        
        // Slow path: await asynchronously, respecting cancellation
        return wait.ContinueWith(
            (_, state) => (IDisposable)state!,
            _releaser.Result,
            cancellationToken,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }

    private sealed class Releaser(AsyncLock toRelease) : IDisposable
    {
        public void Dispose()
        {
            toRelease._semaphore.Release();
        }
    }
}