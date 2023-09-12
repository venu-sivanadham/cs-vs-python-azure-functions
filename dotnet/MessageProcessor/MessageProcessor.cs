using System;
using Azure.Storage.Queues.Models;
using Azure.Storage.Blobs;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace MessageProcessor
{
    public class MessageProcessor
    {
        private readonly ILogger<MessageProcessor> _logger;

        public MessageProcessor(ILogger<MessageProcessor> logger)
        {
            _logger = logger;
        }

        [Function(nameof(MessageProcessor))]
        public async Task RunAsync([QueueTrigger("checks", Connection = "AzureWebJobsStorage")] QueueMessage message)
        {
            var sw = Stopwatch.StartNew();
            string msg = string.Empty;

            try
            {
                msg = message.MessageText;

                _logger.LogInformation($"C# Queue trigger: {msg}");

                var constring = Environment.GetEnvironmentVariable("AzureWebJobsStorage");

                int.TryParse(msg.Split(':').ToList().First(), out int iteration);
                var invocationid = msg.Split(':').ToList().Last();

                var blobClient = new BlobClient(constring, "checks", invocationid);
                if(await blobClient.ExistsAsync())
                {
                    _logger.LogInformation($"Blob {blobClient.Name} exists");

                    if (iteration == 31)
                    {
                        await blobClient.DeleteAsync();
                        _logger.LogInformation($"Deleted the blob {blobClient.Name}.");
                    }
                    else
                    {
                        var content = (await blobClient.DownloadContentAsync()).Value.Content.ToString();
                        _logger.LogInformation($"Blob {blobClient.Name} content: {content}");
                    }
                }
                else
                {
                    _logger.LogInformation($"Blob {blobClient.Name} does not exist");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing message");
            }
            finally
            {
                sw.Stop();
                _logger.LogInformation($"Message processing time: {sw.ElapsedMilliseconds}ms, Content: {msg}");
            }
        }
    }
}
