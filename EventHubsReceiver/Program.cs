using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Primitives;
using Azure.Messaging.EventHubs.Processor;
using Azure.Storage.Blobs;
using EventHubsReceiver.Extensions;
using System.Text;

namespace EventHubsReceiver
{
    internal class Program
    {

        private const string ehubNamespaceConnectionString = "Endpoint=sb://enginepoc.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=DhR9qwGiWajQERdblhPuJnMb0M1bg8Yk7mYcdV2f8cU=";
        private const string eventHubName = "myeventhub";
        private const string blobStorageConnectionString = "DefaultEndpointsProtocol=https;AccountName=engineeventstorage;AccountKey=xOoFEONOq2p51DGYRGmbrI3GfqDU5V5PiikVQR8SjiUn3r74r9TC9iQHk7qjL0LwCV9J4IhoA3Y2+AStdwkGxA==;EndpointSuffix=core.windows.net";
        private const string blobContainerName = "eventcontainer";
        // Read from the default consumer group: $Default
        private const string consumerGroup = EventHubConsumerClient.DefaultConsumerGroupName;

        static BlobContainerClient storageClient;

        // The Event Hubs client types are safe to cache and use as a singleton for the lifetime
        // of the application, which is best practice when events are being published or read regularly.        
        static EventProcessorClient processor;


        static async Task Main()
        {
            await ReceiveSingleBatches();
        }

        static async Task ReceiveSingleBatches()
        {
            // Create a blob container client that the event processor will use 
            storageClient = new BlobContainerClient(blobStorageConnectionString, blobContainerName);

            // Create an event processor client to process events in the event hub
            processor = new EventProcessorClient(storageClient, consumerGroup, ehubNamespaceConnectionString, eventHubName);

            // Register handlers for processing events and handling errors
            processor.ProcessEventAsync += ProcessEventHandler;
            processor.ProcessErrorAsync += ProcessErrorHandler;

            // Start the processing
            await processor.StartProcessingAsync();

            // Wait for 30 seconds for the events to be processed
            await Task.Delay(TimeSpan.FromSeconds(30));

            // Stop the processing
            await processor.StopProcessingAsync();
        }

        static async Task ProcessEventHandler(ProcessEventArgs eventArgs)
        {
            // Write the body of the event to the console window
            Console.WriteLine("\tReceived event: {0}", Encoding.UTF8.GetString(eventArgs.Data.Body.ToArray()));

            // Update checkpoint in the blob storage so that the app receives only new events the next time it's run
            await eventArgs.UpdateCheckpointAsync(eventArgs.CancellationToken);
        }

        static Task ProcessErrorHandler(ProcessErrorEventArgs eventArgs)
        {
            // Write details about the error to the console window
            Console.WriteLine($"\tPartition '{eventArgs.PartitionId}': an unhandled exception was encountered. This was not expected to happen.");
            Console.WriteLine(eventArgs.Exception.Message);
            return Task.CompletedTask;
        }

        /// <summary>
        /// Receive Multiple batches
        /// </summary>

        private const string storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=engineeventstorage;AccountKey=xOoFEONOq2p51DGYRGmbrI3GfqDU5V5PiikVQR8SjiUn3r74r9TC9iQHk7qjL0LwCV9J4IhoA3Y2+AStdwkGxA==;EndpointSuffix=core.windows.net";
        //private const string blobContainerName = "eventcontainer";
        private const string eventHubsConnectionString = "Endpoint=sb://enginepoc.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=DhR9qwGiWajQERdblhPuJnMb0M1bg8Yk7mYcdV2f8cU=";

        //private const string eventHubName = "myeventhub";
        //private const string consumerGroup = EventHubConsumerClient.DefaultConsumerGroupName;

        static async Task ReceiveMultipleBatches()
        {
            var storageClient = new BlobContainerClient(storageConnectionString, blobContainerName);

            var checkpointStore = new BlobCheckpointStore(storageClient);
            var maximumBatchSize = 50;

            var processor = new SimpleBatchProcessor(checkpointStore, maximumBatchSize, consumerGroup, eventHubsConnectionString, eventHubName);

            using var cancellationSource = new CancellationTokenSource();
            cancellationSource.CancelAfter(TimeSpan.FromSeconds(30));

            // There are no event handlers to set for the processor.  All logic
            // normally associated with the processor event handlers is
            // implemented directly via method override in the custom processor.

            try
            {
                await processor.StartProcessingAsync(cancellationSource.Token);
                await Task.Delay(Timeout.Infinite, cancellationSource.Token);
            }
            catch (TaskCanceledException)
            {
                // This is expected if the cancellation token is
                // signaled.
            }
            finally
            {
                // Stopping may take up to the length of time defined
                // as the TryTimeout configured for the processor;
                // By default, this is 60 seconds.

                await processor.StopProcessingAsync();
            }
        }

        
    }
}