using System;
using Newtonsoft.Json;

namespace Common.Model
{
    public class JobMessageContent
    {
        [JsonProperty("insertTimeUtc")]
        public DateTime InsertTimeUtc { get; set; }

        [JsonProperty("jobId", DefaultValueHandling = DefaultValueHandling.Ignore)]
        public string JobId { get; set; }

        [JsonProperty("jobName", DefaultValueHandling = DefaultValueHandling.Ignore)]
        public string JobName { get; set; }

        [JsonProperty("invocationId", DefaultValueHandling = DefaultValueHandling.Ignore)]
        public string InvocationId { get; set; }

        public static JobMessageContent ToJobMessageContent(string jsonString)
        {
            return JsonConvert.DeserializeObject<JobMessageContent>(jsonString);
        }

        public override string ToString()
        {
            return JsonConvert.SerializeObject(this);
        }
    }
}
