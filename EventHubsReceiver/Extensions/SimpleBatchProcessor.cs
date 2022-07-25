using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Primitives;
using System.Text;

namespace EventHubsReceiver.Extensions
{
    public class SimpleBatchProcessor : PluggableCheckpointStoreEventProcessor<EventProcessorPartition>
    {
        public SimpleBatchProcessor(CheckpointStore checkpointStore,
                                int eventBatchMaximumCount,
                                string consumerGroup,
                                string connectionString,
                                string eventHubName,
                                EventProcessorOptions clientOptions = default)
        : base(
            checkpointStore,
            eventBatchMaximumCount,
            consumerGroup,
            connectionString,
            eventHubName,
            clientOptions)
        {
        }

        protected override Task OnProcessingErrorAsync(Exception exception, EventProcessorPartition partition, string operationDescription, CancellationToken cancellationToken)
        {
            // Like the event handler, it is very important that you guard
            // against exceptions in this override; the processor does not
            // have enough understanding of your code to determine the correct
            // action to take.  Any exceptions from this method go uncaught by
            // the processor and will NOT be handled.  Unhandled exceptions will
            // not impact the processor operation but will go unobserved, hiding
            // potential application problems.

            try
            {
                Console.WriteLine($"\tPartition '{partition.PartitionId}': an unhandled exception was encountered. This was not expected to happen.");
                Console.WriteLine(exception.Message);

                //await Application.HandleErrorAsync(
                //    exception,
                //    partition.PartitionId,
                //    operationDescription,
                //    cancellationToken);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                //Application.LogErrorHandlingFailure(ex);
            }

            return Task.CompletedTask;
        }

        protected override async Task OnProcessingEventBatchAsync(IEnumerable<EventData> events, EventProcessorPartition partition, CancellationToken cancellationToken)
        {
            // Like the event handler, it is very important that you guard
            // against exceptions in this override; the processor does not
            // have enough understanding of your code to determine the correct
            // action to take.  Any exceptions from this method go uncaught by
            // the processor and will NOT be handled.  The partition processing
            // task will fault and be restarted from the last recorded checkpoint.

            try
            {
                // The implementation of how events are processed is not relevant in
                // this sample; for illustration, responsibility for managing the processing
                // of events is being delegated to the application.

                foreach (var item in events)
                {
                    Console.WriteLine("\tReceived event: {0} partition: {1}", Encoding.UTF8.GetString(item.EventBody.ToArray()), partition.PartitionId);
                }

                //await Application.DispatchEventsForProcessing(
                //        events,
                //        partition.PartitionId,
                //        cancellationToken);

                // Create a checkpoint based on the last event in the batch.

                var lastEvent = events.Last();

                await UpdateCheckpointAsync(
                    partition.PartitionId,
                    lastEvent.Offset,
                    lastEvent.SequenceNumber,
                    cancellationToken);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                //Application.HandleProcessingException(events, partition.PartitionId, ex);
            }

            // Calling the base would only invoke the process event handler and provide no
            // value; we will not call it here.
        }
    }
}
