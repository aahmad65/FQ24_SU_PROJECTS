using CPSC3200;

namespace P5;

public class PartitionStreamFactory
{
    public PartitionStream<string> FromParameters(PartitionStreamParameters parameters)
    {
        IAutoStream<string>[] partitionStreams = new IAutoStream<string>[parameters.StreamParameters.Length];
        uint count = 0;

        foreach (StreamParametersWithKey paramWithKey in parameters.StreamParameters)
        {
            StreamParameters streamParams = paramWithKey.Parameters;
            string key = paramWithKey.Key.ToString();
            IAutoStream<string> stream;

            if (streamParams.ResetPeriodSeconds.HasValue)
            {
                stream = new AutoResetStream<string>(
                    streamSize: streamParams.MaxCapacity,
                    operationLimit: streamParams.MaxOperations,
                    partitionKey: key,
                    filePath: streamParams.Filename,
                    resetIntervalMs: streamParams.ResetPeriodSeconds.Value
                );
            }
            else if (!string.IsNullOrEmpty(streamParams.Filename))
            {
                stream = new DurableStream<string>(
                    streamSize: streamParams.MaxCapacity,
                    operationLimit: streamParams.MaxOperations,
                    partitionKey: key,
                    filePath: streamParams.Filename
                );
            }
            else
            {
                stream = new MsgStream<string>(
                    streamSize: streamParams.MaxCapacity,
                    operationLimit: streamParams.MaxOperations,
                    partitionKey: key
                );
            }

            partitionStreams[count] = stream;
            count++;
        }

        return new PartitionStream<string>(partitionStreams, count + 1);
    }
}