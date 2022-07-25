using Azure.Core.Diagnostics;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using System.Diagnostics;
using System.Diagnostics.Tracing;

namespace EventHubsListener
{
    internal class Program
    {
        private const string connectionString = "Endpoint=sb://enginepoc.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=DhR9qwGiWajQERdblhPuJnMb0M1bg8Yk7mYcdV2f8cU=";
        private const string eventHubName = "myeventhub";

        static async Task Main(string[] args)
        {
            var producer = new EventHubProducerClient(connectionString, eventHubName);

            using AzureEventSourceListener customListener = new((args, message) =>
            {
                if (args.EventSource.Name.StartsWith("Azure-Identity") && args.Level == EventLevel.Verbose)
                {
                    Trace.WriteLine(message);
                }
                else if (args.EventSource.Name.StartsWith("Azure-Messaging-EventHubs"))
                {
                    switch (args.EventId)
                    {
                        case 3:   // Publish Start
                        case 4:   // Publish Complete
                        case 5:   // Publish Error
                            Console.WriteLine(message);
                            break;
                    }
                }
            }, EventLevel.LogAlways);

            try
            {
                var events = new[]
                {
                    new EventData("EventOne"),
                    new EventData("EventTwo")
                };

                await producer.SendAsync(events);
            }
            finally
            {
                await producer.CloseAsync();
            }

            Console.WriteLine("Hello, World!");
        }
    }
}