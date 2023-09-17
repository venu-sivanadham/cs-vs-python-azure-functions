using Azure.Storage.Queues.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Common.Model;
using Azure.Storage.Blobs.Specialized;
using System.Text;
using common.Clients;
using System.Diagnostics;

namespace MessageProcessor
{
    public class MessageProcessor
    {
        const string OS_WEB_ENDPOINT = "https://veshivancsfunc01b9e1.z13.web.core.windows.net/metrics.html";
        private readonly ILogger<MessageProcessor> _logger;

        public MessageProcessor(ILogger<MessageProcessor> logger)
        {
            _logger = logger;
        }

        [Function(nameof(MessageProcessor))]
        public async Task RunAsync(
            [QueueTrigger("checks", Connection = "AzureWebJobsStorage")] QueueMessage message,
            FunctionContext context)
        {
            var status = new TriggerEventDetails
            {
                StartTime = DateTime.UtcNow,
                TriggerType = "MessageProcessor",
                Status = "Succeeded"
            };

            try
            {
                var jobMsg = JobMessageContent.ToJobMessageContent(message.MessageText);
                var invocationid = jobMsg.InvocationId;
                status.PickupTime = DateTime.UtcNow - jobMsg.InsertTimeUtc;
                status.TriggerData = jobMsg;

                await ProcessMessageAsync(jobMsg, context);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing message");
                status.Status = "Failed";
            }
            finally
            {
                status.EndTime = DateTime.UtcNow;
                status.Duration = status.EndTime - status.StartTime;

                _logger.LogInformation($"{status.TriggerType} execution details: {status.ToString()}");
            }
        }

        private async Task ProcessMessageAsync(JobMessageContent jobMsg, FunctionContext context)
        {
            // Do work here.
            var sw = Stopwatch.StartNew();
            var osClient = new OSProviderClient(OS_WEB_ENDPOINT);
            var osInfo = await osClient.GetOSInfoAsync();
            sw.Stop();

            _logger.LogInformation($"Received the OS Info of size: {osInfo.Length}," +
                $" call duration: {sw.ElapsedMilliseconds}ms.");

            await UpdateStatus(jobMsg, context);
        }

        private async Task UpdateStatus(JobMessageContent jobMsg, FunctionContext context)
        {
            var constring = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
            var hostId = Environment.GetEnvironmentVariable("WEBSITE_INSTANCE_ID");

            var blobClient = new AppendBlobClient(constring, "checks", jobMsg.JobName);
            if (await blobClient.ExistsAsync())
            {
                var blockContent = $"{hostId}:{context.InvocationId};";
                _logger.LogInformation($"Blob {blobClient.Name} exists, appending the block with {blockContent}.");

                await blobClient.AppendBlockAsync(new MemoryStream(Encoding.UTF8.GetBytes(blockContent)));
            }
            else
            {
                _logger.LogInformation($"Blob {blobClient.Name} does not exist");
            }
        }
    }
}
