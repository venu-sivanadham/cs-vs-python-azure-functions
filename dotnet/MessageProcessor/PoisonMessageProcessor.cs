using System;
using Azure.Storage.Queues.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace MessageProcessor
{
    public class PoisonMessageProcessor
    {
        private readonly ILogger<PoisonMessageProcessor> _logger;

        public PoisonMessageProcessor(ILogger<PoisonMessageProcessor> logger)
        {
            _logger = logger;
        }

        [Function(nameof(PoisonMessageProcessor))]
        public void Run([QueueTrigger("checks-poison")] QueueMessage message)
        {
            _logger.LogInformation($"C# Poison Queue trigger function processed: {message.MessageText}");
        }
    }
}
