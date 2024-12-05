// Aidan Ahmad, 9-30-24, Revised: 10-26-24

namespace CPSC3200;

//Invariants: Operation count will never surpass operation limit. GetMessages will never go out of range. A message
//will never be longer than 256 characters. MsgStream object array size will never be equal to or larger than 100.
public class MsgStream<TKey> : IAutoStream<TKey> where TKey : IEquatable<TKey>
{
    protected string[] _messages;
    protected uint _messageCount;
    protected uint _operationLimit;
    protected uint _operationCount;
    protected uint _streamLength;
    protected TKey _partitionKey;
    
    //Preconditions: Operation limit is not smaller than size of the MsgStream object array
    public MsgStream(uint streamSize, uint operationLimit, TKey partitionKey)
    {
        _messages = new string[streamSize];
        _messageCount = 0;
        _operationCount = 0;
        _operationLimit = operationLimit;
        _streamLength = streamSize;
        _partitionKey = partitionKey;
    }

    public virtual MsgStream<TKey> DeepCopy()
    {
        MsgStream<TKey> copy = new MsgStream<TKey>(_streamLength, _operationLimit, GenerateNewPartitionKey(_partitionKey));
        foreach (string msg in _messages)
        {
            if (!string.IsNullOrEmpty(msg))
            {
                copy.Append(msg);
            }
        }
        return copy;
    }
    //Postconditions: Copy is made with partition key + 1 to assure individual partition keys.
    public string[] GetMessages(uint start, uint end)
    {
        OperationLimitCheck();
        if (start >= _messageCount || end >= _messageCount || start > end)
        {
            throw new ArgumentOutOfRangeException("Invalid start or end indices.");
        }
        
        uint length = end - start + 1;
        string[] messageReturn = new string[length];
        
        for (uint i = 0; i < length; i++)
        {
            messageReturn[i] = _messages[start + i];
        }
        _operationCount++;
        return messageReturn;
    }
    
    //Preconditions: String is not empty
    public virtual void Append(string msg)
    {
        if (msg.Length > 256)
        {
            throw new Exception("Message is longer than 256 characters.");
        }
        
        OperationLimitCheck();
        _messages[_messageCount] = msg;
        _messageCount++;
        _operationCount++;            
    }
    public virtual void Reset()
    {
        EmptyMsgStream();
        _operationCount = 0;
        _messageCount = 0;
    }

    public uint GetMsgCount() => _messageCount;

    public uint GetMsgLimit() => _streamLength;

    public TKey GetPartitionKey() => _partitionKey;
    
    protected void EmptyMsgStream()
    {
        for (int i = 0; i < _messageCount; i++)
        {
            _messages[i] = string.Empty;
        }
    }

    protected void OperationLimitCheck()
    {
        if (_operationCount >= _operationLimit)
        {
            throw new Exception("Operation limit reached, please reset the object or make a new object.");
        }
    }
    
    protected TKey GenerateNewPartitionKey(TKey currentKey)
    {
        if (currentKey is int intKey)
        {
            return (TKey)(object)(intKey + 1);
        }
        if (currentKey is uint uintKey)
        {
            return (TKey)(object)(uintKey + 1);
        }
        if (currentKey is long longKey)
        {
            return (TKey)(object)(longKey + 1);
        }
        if (currentKey is ulong ulongKey)
        {
            return (TKey)(object)(ulongKey + 1);
        }
        if (currentKey is double doubleKey)
        {
            return (TKey)(object)(doubleKey + 1);
        }
        if (currentKey is float floatKey)
        {
            return (TKey)(object)(floatKey + 1);
        }
        if (currentKey is string stringKey)
        {
            return (TKey)(object)(stringKey + "_copy");
        }
        if (currentKey is char charKey)
        {
            return (TKey)(object)((char)(charKey + 1));
        }
        throw new NotSupportedException($"Partition key type {typeof(TKey).Name} is not supported for key generation.");
    }
    
    //Implementation invariant: Limits are not dependent on the system this is running on. They are just
    //rough placeholder estimates to show limitation.
}