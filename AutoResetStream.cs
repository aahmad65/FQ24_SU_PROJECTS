namespace CPSC3200;

public class AutoResetStream<TKey> : IAutoStream<TKey>, IDisposable where TKey : IEquatable<TKey>
{
    private Timer _resetTimer;
    private readonly MsgStream<TKey> _stream;
    
    public AutoResetStream(uint streamSize, uint operationLimit, TKey partitionKey, string? filePath, uint resetIntervalMs)
    {
        if (filePath is null)
        {
            _stream = new MsgStream<TKey>(streamSize, operationLimit, partitionKey);
        }
        else
        {
            _stream = new DurableStream<TKey>(streamSize, operationLimit, partitionKey, filePath);
        }
        _resetTimer = new Timer(_ => Reset(), null, resetIntervalMs * 1000, resetIntervalMs * 1000);
    }

    public void Reset()
    {
        _stream.Reset();
    }

    public void Append(string msg)
    {
        _stream.Append(msg);
    }

    public string[] GetMessages(uint start, uint end)
    {
        return _stream.GetMessages(start, end);
    }

    public MsgStream<TKey> DeepCopy()
    {
        return _stream.DeepCopy();
    }

    public uint GetMsgCount()
    {
        return _stream.GetMsgCount();
    }

    public uint GetMsgLimit()
    {
        return _stream.GetMsgLimit();
    }

    public TKey GetPartitionKey()
    {
        return _stream.GetPartitionKey();
    }

    public void Dispose()
    {
        _resetTimer?.Change(Timeout.Infinite, Timeout.Infinite);
        _resetTimer?.Dispose();
        if (_stream is IDisposable disposableStream)
        {
            disposableStream.Dispose();
        }
    }
}