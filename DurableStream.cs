// Aidan Ahmad, 10-21-24, Revised: 10-26-24

namespace CPSC3200;

public class DurableStream<TKey> : MsgStream<TKey>, IDisposable where TKey : IEquatable<TKey>
    {
        private readonly string _filePath;
        private readonly string _backupFilePath;
        private FileStream _fileStream;
        private BufferedStream _bufferedStream;
        private StreamWriter _writer;
        private bool _disposed;

        public DurableStream(uint streamSize, uint operationLimit, TKey partitionKey, string filePath) : base(streamSize, operationLimit, partitionKey)
        {
            _filePath = filePath;
            _backupFilePath = GetBackupFilePath(filePath);

            _fileStream = new FileStream(_filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite);
            _bufferedStream = new BufferedStream(_fileStream);
            _writer = new StreamWriter(_bufferedStream) { AutoFlush = false };

            _messageCount = 0;
            _operationCount = 0;
            _messages = new string[_streamLength];

            SyncFromFile();
            CreateBackup();
        }

        public override DurableStream<TKey> DeepCopy()
        {
            string copyFilePath = GetCopyFilePath(_filePath);

            _fileStream.Flush();
            _bufferedStream.Flush();

            try
            {
                File.Copy(_filePath, copyFilePath, overwrite: true);

                TKey newPartitionKey = GenerateNewPartitionKey(_partitionKey); // Replace with a valid key generation method
                var copy = new DurableStream<TKey>(_streamLength, _operationLimit, newPartitionKey, copyFilePath);

                for (int i = 0; i < _messageCount; i++)
                {
                    copy._messages[i] = _messages[i];
                }
                copy._messageCount = _messageCount;

                copy.SyncFromFile();
                return copy;
            }
            finally
            {
                _fileStream.Seek(0, SeekOrigin.Begin);
            }
        }
    //Postconditions: Copy is made with partition key + 1 to assure individual partition keys.

    private void SyncFromFile()
    {
        
        _fileStream.Seek(0, SeekOrigin.Begin);

        StreamReader reader = new StreamReader(_fileStream, leaveOpen: true);
        try
        {
            string? line;
            _messageCount = 0;
            
            while ((line = reader.ReadLine()) != null && _messageCount < _messages.Length)
            {
                _messages[_messageCount++] = line;
            }
        }
        finally
        {
            reader.Close();
        }
    }
    
    private string GetBackupFilePath(string filePath)
    {
        string directory = Path.GetDirectoryName(filePath) ?? string.Empty;
        string fileNameWithoutExtension = Path.GetFileNameWithoutExtension(filePath);
        string extension = Path.GetExtension(filePath);
        
        return Path.Combine(directory, $"{fileNameWithoutExtension}_backup{extension}");
    }
    
    private string GetCopyFilePath(string filePath)
    {
        string directory = Path.GetDirectoryName(filePath) ?? string.Empty;
        string fileNameWithoutExtension = Path.GetFileNameWithoutExtension(filePath);
        string extension = Path.GetExtension(filePath);
        
        return Path.Combine(directory, $"{fileNameWithoutExtension}_copy{extension}");
    }
    
    private void CreateBackup()
    {
        File.Copy(_filePath, _backupFilePath, overwrite: true);
    }
    public override void Append(string msg)
    {
        base.Append(msg);
        
        _writer.WriteLine(msg);

        if (_messageCount % 5 == 0)
        {
            _writer.Flush();
        }
    }

    public override void Reset()
    {
        EmptyMsgStream();
        if (File.Exists(_backupFilePath))
        {
            _fileStream.SetLength(0);
            
            FileStream backupStream = new FileStream(_backupFilePath, FileMode.Open, FileAccess.Read);

            try
            {
                backupStream.Seek(0, SeekOrigin.Begin);
                backupStream.CopyTo(_fileStream);
                
                _fileStream.Seek(0, SeekOrigin.Begin);
                
                SyncFromFile();
            }
            finally
            {
                backupStream.Close();
            }
        }
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
                _writer?.Flush();
                _writer?.Close();
                _bufferedStream?.Close();
                _fileStream?.Close();
            }
            _disposed = true;
        }
    }
}