using Newtonsoft.Json;
using System;

namespace Common.Model
{
    public class TriggerEventDetails
    {
        [JsonProperty("startTime")]
        public DateTime StartTime { get; set; }

        [JsonProperty("endTime")]
        public DateTime EndTime { get; set; }

        [JsonProperty("duration")]
        public TimeSpan Duration { get; set; }

        [JsonProperty("triggerType")]
        public string TriggerType { get; set; }

        [JsonProperty("status")]
        public string Status { get; set; }

        [JsonProperty("triggerData")]
        public object TriggerData { get; set; }

        [JsonProperty("pickupTime")]
        public TimeSpan PickupTime { get; set; }

        public override string ToString()
        {
            return JsonConvert.SerializeObject(this);
        }
    }
}
