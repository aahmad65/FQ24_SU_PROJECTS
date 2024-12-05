/* Aidan Ahmad, 10-21-24, Revised: 10-26-24 */

namespace CPSC3200;


public class PartitionStream<TPKey> : IDisposable where TPKey : IEquatable<TPKey>
{
    private IAutoStream<TPKey>[] _streams;
    private uint _partitionLength;
    private bool _disposed;

    public PartitionStream(IAutoStream<TPKey>[] msgStreams, uint numStreams)
    {
        _partitionLength = numStreams;
        _streams = msgStreams;
    }

    public PartitionStream(uint numStreams)
    {
        _streams = new IAutoStream<TPKey>[numStreams];
        _partitionLength = numStreams;
    }

    public PartitionStream<TPKey> DeepCopy()
    {
        var copy = new PartitionStream<TPKey>(_partitionLength);
        for (int i = 0; i < _partitionLength; i++)
        {
            copy._streams[i] = _streams[i].DeepCopy();
        }
        return copy;
    }

    public void AddStream(uint streamSize, uint operationLimit, TPKey partitionKey, string? filePath, uint? resetIntervalMs)
    {
        IAutoStream<TPKey>[] newStreams = new IAutoStream<TPKey>[_partitionLength + 1];
        
        for (int i = 0; i < _streams.Length; i++)
        {
            newStreams[i] = _streams[i];
        }
        
        if (resetIntervalMs != null)
        {
            newStreams[_partitionLength] = new AutoResetStream<TPKey>(streamSize, operationLimit, partitionKey, filePath, resetIntervalMs.Value);
        }
        else
        {
            if (filePath == null)
            {
                newStreams[_partitionLength] = new MsgStream<TPKey>(streamSize, operationLimit, partitionKey);
            }
            else
            {
                newStreams[_partitionLength] = new DurableStream<TPKey>(streamSize, operationLimit, partitionKey, filePath);
            }
        }

        _partitionLength++;
        _streams = newStreams;
    }

    public string[] ReadMsgs(TPKey inputKey, uint msgStart, uint msgEnd)
    {
        foreach (IAutoStream<TPKey> msgStream in _streams)
        {
            if (msgStream.GetPartitionKey().Equals(inputKey))
            {
                return msgStream.GetMessages(msgStart, msgEnd);
            }
        }
        throw new KeyNotFoundException();
    }

    public void AppendMsgs(TPKey inputKey, string msg)
    {
        foreach (var msgStream in _streams)
        {
            if (msgStream.GetPartitionKey().Equals(inputKey))
            {
                msgStream.Append(msg);
                return;
            }
        }
        throw new KeyNotFoundException();
    }

    public void Reset()
    {
        foreach (var msgStream in _streams)
        {
            msgStream.Reset();
        }
    }

    public void ResetOne(TPKey inputKey)
    {
        foreach (var msgStream in _streams)
        {
            if (msgStream.GetPartitionKey().Equals(inputKey))
            {
                msgStream.Reset();
                return;
            }
        }
        throw new KeyNotFoundException();
    }

    public uint MessageCountCheck<TKey>(TKey inputKey) where TKey : IEquatable<TKey>
    {
        for (int i = 0; i < _partitionLength; i++)
        {
            if (_streams[i].GetPartitionKey().Equals(inputKey))
            {
                return _streams[i].GetMsgCount();
            }
        }
        throw new KeyNotFoundException();
    }

    public uint MessageLimitCheck<TKey>(TKey inputKey) where TKey : IEquatable<TKey>
    {
        for (int i = 0; i < _partitionLength; i++)
        {
            if (_streams[i].GetPartitionKey().Equals(inputKey))
            {
                return _streams[i].GetMsgLimit();
            }
        }
        throw new KeyNotFoundException();
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (!_disposed)
        {
            if (disposing)
            {
                foreach (var msgStream in _streams)
                {
                    if (msgStream is IDisposable disposableStream)
                    {
                        disposableStream.Dispose();
                    }
                }
            }

            _disposed = true;
        }
    }

}