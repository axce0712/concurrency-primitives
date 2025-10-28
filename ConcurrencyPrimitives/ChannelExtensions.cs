using System.Threading.Channels;

namespace ConcurrencyPrimitives;

public static class ChannelExtensions
{
    extension<T>(Channel<T> source)
    {
        public async Task Broadcast(IEnumerable<Channel<T>> channels, CancellationToken cancellationToken = default)
        {
            var materialized = channels as Channel<T>[] ?? channels.ToArray();
            if (materialized.Length == 0)
            {
                return;
            }
            
            await foreach (var item in source.Reader.ReadAllAsync(cancellationToken))
            {
                foreach (var channel in materialized)
                {
                    await channel.Writer.WriteAsync(item, cancellationToken);
                }
            }
        }
    }
}