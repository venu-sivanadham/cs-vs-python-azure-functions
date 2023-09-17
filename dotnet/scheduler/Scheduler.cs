using System.Text;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Queues;
using Common.Constants;
using Common.Model;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

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
        public async Task RunAsync([TimerTrigger("0,20,40 * * * * *")] TriggerInfo myTimer,
            FunctionContext context,
            CancellationToken token)
        {
            var status = new TriggerEventDetails
            {
                StartTime = DateTime.UtcNow,
                TriggerType = "Scheduler",
                Status = "Succeeded"
            };

            int iMsg = 0;

            try
            {
                _logger.LogInformation($"C# Timer trigger function executed at: {status.StartTime}");

                var constring = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
                if (!int.TryParse(Environment.GetEnvironmentVariable("MessageCount"), out int msgCount))
                {
                    msgCount = 32;
                }

                var jobName = await SetupAppendBlob(constring, status, token);

                var queueClient = new QueueClient(constring, "checks");
                await queueClient.CreateIfNotExistsAsync(cancellationToken: token);
                for (; iMsg < msgCount; iMsg++)
                {
                    var jobMessageContent = new JobMessageContent
                    {
                        InsertTimeUtc = DateTime.UtcNow,
                        JobName = jobName,
                        InvocationId = context.InvocationId.ToString(),
                        JobId = iMsg.ToString()
                    };

                    var bytes = Encoding.UTF8.GetBytes(jobMessageContent.ToString());
                    await queueClient.SendMessageAsync(
                        Convert.ToBase64String(bytes),
                        cancellationToken: token,
                        timeToLive: TimeSpan.FromMinutes(30));
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error sending message to queue");
                status.Status = "Failed";
            }
            finally
            {
                status.EndTime = DateTime.UtcNow;
                status.Duration = status.EndTime - status.StartTime;
                status.TriggerData = iMsg;

                _logger.LogInformation($"{status.TriggerType} execution details: {status}");
            }
        }

        private async Task<string> SetupAppendBlob(
            string connectionString,
            TriggerEventDetails status,
            CancellationToken token)
        {
            var appendBlobName = string.Format(
                MessageConstants.ApendBlobFromatter,
                status.StartTime.Second / 20);

            var blobContainerClient = new BlobContainerClient(connectionString, "checks");
            await blobContainerClient.CreateIfNotExistsAsync(cancellationToken: token);

            var blobClient = blobContainerClient.GetAppendBlobClient(appendBlobName);
            if( await blobClient.ExistsAsync(cancellationToken: token))
            {
                var blobPropertiesResponse = await blobClient.GetPropertiesAsync(cancellationToken: token);

                var contentResonse = await blobClient.DownloadContentAsync(cancellationToken: token);
                ProcessAppendBlob(appendBlobName, contentResonse.Value, blobPropertiesResponse.Value);

                await blobClient.DeleteAsync(cancellationToken: token);
            }
            else
            {
                _logger.LogInformation($"Blob {appendBlobName} does not exist.");
            }

            var metadata = new Dictionary<string, string>
            {
                { "TriggerData", status.StartTime.ToString() }
            };  

            await blobClient.CreateAsync(
                new AppendBlobCreateOptions()
                { Metadata = metadata },
                cancellationToken: token);

            return appendBlobName;
        }

        private void ProcessAppendBlob(string blobName, BlobDownloadResult result, BlobProperties properties)
        {
            var content = result.Content.ToString();
            var blobStats = new StatsFromAppendBlob()
            {
                AppendBlobName = blobName,
                BlockCount = result.Details.BlobCommittedBlockCount,
            };

            try
            {
                blobStats.LastSchedulerStartTime = DateTimeOffset.Parse(result.Details.Metadata["TriggerData"]);
                blobStats.BlobLastModifiedTime = result.Details.LastModified;
                blobStats.BlobCreationTime = properties.CreatedOn;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting metadata");
            }

            HashSet<string> hostIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            HashSet<string> invocationIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            content.Split(';',StringSplitOptions.RemoveEmptyEntries)
                .Select(s => s.Split(':', StringSplitOptions.RemoveEmptyEntries))
                .Where(s => s.Length == 2)
                .Select(s => new KeyValuePair<string, string>(s[0], s[1]))
                .ToList()
                .ForEach( p =>
                {
                    hostIds.Add(p.Key);
                    invocationIds.Add(p.Value);
                });

            blobStats.ProcessedMessageCount = invocationIds.Count;
            blobStats.HostCount = hostIds.Count;

            _logger.LogInformation($"{blobStats}");
        }
    }

    public class TriggerInfo
    {
        public ScheduleStatus ScheduleStatus { get; set; }

        public bool IsPastDue { get; set; }
    }

    public class ScheduleStatus
    {
        public DateTime Last { get; set; }

        public DateTime Next { get; set; }

        public DateTime LastUpdated { get; set; }
    }

    public class StatsFromAppendBlob
    {
        [JsonProperty("appendBlobName")]
        public string? AppendBlobName { get; set; }

        [JsonProperty("blockCount")]
        public int BlockCount { get; set; }

        [JsonProperty("processedMessageCount")]
        public int ProcessedMessageCount { get; set; }

        [JsonProperty("hostCount")]
        public int HostCount { get; set; }

        [JsonProperty("lastSchedulerStartTime")]
        public DateTimeOffset LastSchedulerStartTime { get; set; }

        [JsonProperty("blobCreationTime")]
        public DateTimeOffset BlobCreationTime { get; set; }

        [JsonProperty("blobLastModifiedTime")]
        public DateTimeOffset BlobLastModifiedTime { get; set; }

        public override string ToString()
        {
            return JsonConvert.SerializeObject(this);
        }
    }
}
