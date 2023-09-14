import logging
import azure.functions as func
from azure.storage.blob import BlobServiceClient, AppendBlobServiceClient
from azure.storage.queue import QueueServiceClient
import os
from datetime import datetime, timedelta

class Scheduler:
    def __init__(self):
        self._logger = logging.getLogger('azure')

    def run(self, my_timer: func.TimerRequest) -> None:
        status = {
            "StartTime": datetime.utcnow(),
            "TriggerType": "Scheduler",
            "Status": "Succeeded"
        }
        iMsg = 0

        try:
            self._logger.info(f"Python Timer trigger function executed at: {status['StartTime']}")

            constring = os.environ.get("AzureWebJobsStorage")
            msg_count = int(os.environ.get("MessageCount", 32))

            job_name = self.setup_append_blob(constring, status)

            queue_client = QueueServiceClient.from_connection_string(constring)
            queue_service = queue_client.get_service_properties()
            if not queue_service['queues']['analytics']['enabled']:
                queue_client.get_queue_client('analytics').create_queue()

            for iMsg in range(msg_count):
                job_message_content = {
                    "InsertTimeUtc": datetime.utcnow(),
                    "JobName": job_name,
                    "InvocationId": str(func.Context.default().invocation_id),
                    "JobId": str(iMsg)
                }

                queue_client.get_queue_client('analytics').send_message(job_message_content, time_to_live=timedelta(minutes=30))

        except Exception as ex:
            self._logger.error(f"Error sending message to queue: {ex}")
            status["Status"] = "Failed"
        finally:
            status["EndTime"] = datetime.utcnow()
            status["Duration"] = status["EndTime"] - status["StartTime"]
            status["TriggerData"] = iMsg

            self._logger.info(f"{status['TriggerType']} execution details: {status}")

    def setup_append_blob(self, connection_string, status):
        append_blob_name = f"ApendBlob_{status['StartTime'].second // 10}"

        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client("checks")
        container_props = container_client.get_container_properties()
        if not container_props["etag"]:
            container_client.create_container()

        append_blob_client = container_client.get_append_blob_client(append_blob_name)
        if append_blob_client.exists():
            blob_properties = append_blob_client.get_blob_properties()
            blob_content = append_blob_client.download_blob().readall()
            self.process_append_blob(append_blob_name, blob_content, blob_properties)

            append_blob_client.delete_blob()

        metadata = {
            "TriggerData": status["StartTime"].isoformat()
        }
        append_blob_client.upload_blob("", overwrite=True, metadata=metadata)

        return append_blob_name

    def process_append_blob(self, blob_name, blob_content, blob_properties):
        content = blob_content.decode('utf-8')
        blob_stats = {
            "AppendBlobName": blob_name,
            "BlockCount": blob_properties["blob_committed_block_count"],
        }

        try:
            blob_stats["LastSchedulerStartTime"] = datetime.fromisoformat(blob_properties["metadata"]["TriggerData"])
            blob_stats["BlobLastModifiedTime"] = blob_properties["last_modified"]
            blob_stats["BlobCreationTime"] = blob_properties["created_on"]
        except Exception as ex:
            self._logger.error(f"Error getting metadata: {ex}")

        host_ids = set()
        invocation_ids = set()

        for entry in content.split(';'):
            key, value = entry.split(':')
            host_ids.add(key)
            invocation_ids.add(value)

        blob_stats["ProcessedMessageCount"] = len(invocation_ids)
        blob_stats["HostCount"] = len(host_ids)

        self._logger.info(blob_stats)

# Entry point for Azure Function
def main(my_timer: func.TimerRequest) -> None:
    scheduler = Scheduler()
    scheduler.run(my_timer)
