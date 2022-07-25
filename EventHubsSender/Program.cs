using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using System.Text;

namespace EventHubsSender
{
    internal class Program
    {
        // connection string to the Event Hubs namespace
        private const string connectionString = "Endpoint=sb://enginepoc.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=DhR9qwGiWajQERdblhPuJnMb0M1bg8Yk7mYcdV2f8cU=";
        // name of the event hub
        private const string eventHubName = "myeventhub";
        // number of events to be sent to the event hub
        private const int numOfEvents = 50;
        // number of batches to be sent to the event hub
        private const int numOfBatches = 10;
        // The Event Hubs client types are safe to cache and use as a singleton for the lifetime
        // of the application, which is best practice when events are being published or read regularly.
        static EventHubProducerClient producerClient;

        static async Task Main(string[] args)
        {
            //await EventsSingleBatch();
            await EventsMultipleBatches();
        }

        static async Task EventsMultipleBatches()
        {
            // Create a producer client that you can use to send events to an event hub
            producerClient = new EventHubProducerClient(connectionString, eventHubName);

            /// Assume this is the process that will create an event for each claim in the batch ordered by Family Sequence 
            /// possible event data can be: BatchID & ApClaimID or XML request
            for (int batchCount = 1; batchCount <= numOfBatches; batchCount++)
            {
                // Create a batch of events 
                using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();

                for (int i = 1; i <= numOfEvents; i++)
                {
                    if (!eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes($"Event {i}"))))
                    {
                        // if it is too large for the batch
                        throw new Exception($"Event {i} is too large for the batch and cannot be sent.");
                    }
                }

                try
                {
                    // Use the producer client to send the batch of events to the event hub
                    await producerClient.SendAsync(eventBatch);
                    Console.WriteLine($"Batch #{batchCount} of {numOfEvents} events has been published.");
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                }
            }

            await producerClient.DisposeAsync();
        }

        static async Task EventsSingleBatch()
        {
            // Create a producer client that you can use to send events to an event hub
            producerClient = new EventHubProducerClient(connectionString, eventHubName);

            /// Assume this is the process that will create an event for each claim in the batch ordered by Family Sequence 
            /// possible event data can be: BatchID & ApClaimID or XML request
            using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();

            for (int i = 1; i <= numOfEvents; i++)
            {
                if (!eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes($"Event {i}"))))
                {
                    // if it is too large for the batch
                    throw new Exception($"Event {i} is too large for the batch and cannot be sent.");
                }
            }

            try
            {
                // Use the producer client to send the batch of events to the event hub
                await producerClient.SendAsync(eventBatch);
                Console.WriteLine($"A Batch of {numOfEvents} events has been published.");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
            finally
            {
                await producerClient.DisposeAsync();
            }
        }
    }
}