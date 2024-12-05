namespace CPSC3200;

public interface IAutoStream<TKey> where TKey : IEquatable<TKey>
{
    void Reset();
    void Append(string msg);
    string[] GetMessages(uint start, uint end);
    MsgStream<TKey> DeepCopy();
    uint GetMsgCount();
    uint GetMsgLimit();
    TKey GetPartitionKey();
}