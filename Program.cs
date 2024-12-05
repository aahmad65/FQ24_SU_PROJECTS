using System.Text.Json;
using CPSC3200;
using P5.Commands;

namespace P5;

class Program
{
    static void Main(string[] args)
    {
        PartitionStreamParameters parameters = PartitionStreamParameters.FromFile(args[0]);

        using PartitionStream<string> partitionStream = new PartitionStreamFactory().FromParameters(parameters);
        
        Console.CancelKeyPress += (_, e) =>
        {
            partitionStream.Dispose();
        };
        
        AppDomain.CurrentDomain.ProcessExit += (_, e) =>
        {
            partitionStream.Dispose();
        };

        CommandRouter commandRouter = new CommandRouter()
            .WithCommand(new AddMessageCommandFactory())
            .WithCommand(new ReadRangeCommandFactory())
            .WithCommand(new ResetCommandFactory());

        while (true)
        {
            ICommand? command = commandRouter.TryParseCommand(Console.ReadLine()!);
            if (command == null)
            {
                Console.WriteLine("Invalid command");
            }
            else
            {
                try
                {
                    command.Apply(partitionStream);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                }
            }
        }
    }
}