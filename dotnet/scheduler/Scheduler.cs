using System;
using System.ComponentModel.DataAnnotations;
using System.Diagnostics;
using System.Text;
using Azure.Storage.Blobs;
using Azure.Storage.Queues;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Extensions;
using Microsoft.Extensions.Logging;

namespace scheduler
{
    public class Scheduler
    {
        private readonly ILogger _logger;

        public Scheduler(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<Scheduler>();
        }

        [Function("Scheduler")]
        public async Task RunAsync([TimerTrigger("*/10 * * * * *")] MyInfo myTimer,
            FunctionContext context,
            CancellationToken token)
        {
            int i = 0;
            int queueMsgCount = 0;
            string invocationId = string.Empty;

            try
            {
                _logger.LogInformation($"C# Timer trigger function executed at: {DateTime.UtcNow}");

                if (!int.TryParse(Environment.GetEnvironmentVariable("MessageCount"), out int msgCount))
                {
                    msgCount = 32;
                }

                invocationId = context.InvocationId.ToString();

                var constring = Environment.GetEnvironmentVariable("AzureWebJobsStorage");

                var blobClient = new BlobContainerClient(constring, "checks");
                await blobClient.CreateIfNotExistsAsync(cancellationToken: token);

                await blobClient.UploadBlobAsync(
                    $"{context.InvocationId}",
                    new MemoryStream(Encoding.UTF8.GetBytes($"{context.InvocationId}")),
                    cancellationToken: token);

                var queueClient = new QueueClient(constring, "checks");
                await queueClient.CreateIfNotExistsAsync(cancellationToken: token);
                for (; i < msgCount; i++)
                {
                    var bytes = Encoding.UTF8.GetBytes($"{i}:{context.InvocationId}");
                    await queueClient.SendMessageAsync(
                        Convert.ToBase64String(bytes),
                        cancellationToken: token,
                        timeToLive: TimeSpan.FromMinutes(30));
                }

                queueMsgCount = (await queueClient.GetPropertiesAsync(cancellationToken: token))
                    .Value.ApproximateMessagesCount;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error sending message to queue");
            }
            finally
            {
                _logger.LogInformation($"Added message count: {i}, Queue length: {queueMsgCount}, duration: , invocation: {invocationId}");
            }
        }
    }

    public class MyInfo
    {
        public MyScheduleStatus ScheduleStatus { get; set; }

        public bool IsPastDue { get; set; }
    }

    public class MyScheduleStatus
    {
        public DateTime Last { get; set; }

        public DateTime Next { get; set; }

        public DateTime LastUpdated { get; set; }
    }
}
