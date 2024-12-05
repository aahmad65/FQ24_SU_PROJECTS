using System.Text.Json;

namespace P5;

public record StreamParameters
{
    public uint MaxCapacity { get; init; }
    public uint MaxOperations { get; init; }
    public string? Filename { get; init; }
    
    public uint? ResetPeriodSeconds { get; init; }
}

public record StreamParametersWithKey
{
    public object Key { get; init; }
    public StreamParameters Parameters { get; init; }
}

public record PartitionStreamParameters
{
    public StreamParametersWithKey[] StreamParameters { get; init; } = [];

    public static PartitionStreamParameters FromFile(string filename)
    {
        string json = File.ReadAllText(filename);
        JsonSerializerOptions options = new()
        {
            PropertyNameCaseInsensitive = true,
            AllowTrailingCommas = true,
        };

        return JsonSerializer.Deserialize<PartitionStreamParameters>(json, options)!;
    }
}