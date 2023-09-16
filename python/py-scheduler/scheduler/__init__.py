import logging
import os
import azure.functions as func
import json

from base64 import b64encode
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from azure.storage.queue import QueueClient
from azure.core.exceptions import ResourceExistsError

# Setup the append blob
def setup_append_blob(connection_string, append_blob_name) -> None:
    
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client("checks")

    try:
        container_client.create_container()
    except ResourceExistsError:
        logging.info('Container exist.')

    append_blob_client = container_client.get_blob_client(append_blob_name)
    if append_blob_client.exists():
        blob_properties = append_blob_client.get_blob_properties()
        
        blob_stats = {
            "AppendBlobName": append_blob_name,
            "BlockCount": blob_properties.append_blob_committed_block_count,
        }

        try:
            blob_stats["LastSchedulerStartTime"] = blob_properties.metadata["TriggerData"]
            blob_stats["BlobLastModifiedTime"] = f"{blob_properties.last_modified}"
            blob_stats["BlobCreationTime"] = f"{blob_properties.creation_time}"
            
            blob_content = append_blob_client.download_blob().content_as_text()
            host_ids = set()
            invocation_ids = set()

            if (blob_content is not None) and len(blob_content) > 0:
                for entry in blob_content.split(';'):
                    if entry.find(':') > 0:
                        key, value = entry.split(':')
                        host_ids.add(key)
                        invocation_ids.add(value)

            blob_stats["ProcessedMessageCount"] = len(invocation_ids)
            blob_stats["HostCount"] = len(host_ids)

            logging.info(json.dumps(blob_stats))

            append_blob_client.delete_blob()
        except Exception as ex:
            logging.exception(f"Exception while processing append blob: {ex}")

    metadata = {
        "TriggerData": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S%z")
    }
    append_blob_client.create_append_blob(metadata=metadata)

def main(mytimer: func.TimerRequest, context: func.Context) -> None:
    start_time = datetime.utcnow()
    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', start_time)
    
    status = {
        "StartTime": start_time.strftime("%Y-%m-%d %H:%M:%S%z"),
        "TriggerType": "Scheduler",
        "Status": "Succeeded"
    }
    iMsg = 0

    try:
        # Create a queue client using connection string
        connection_string = os.environ["AzureWebJobsStorage"]

        # Setup the append blob
        blob_name = f"ApendBlob_{int(start_time.second / 10)}"
        setup_append_blob(connection_string, blob_name)

        queue_client = QueueClient.from_connection_string(connection_string, "checks")
                
        try:
            # Create the queue
            queue_client.create_queue()
        except ResourceExistsError:
            logging.info('Queue exist.')

        # Send messages to the queue
        max_messages = int(os.environ["MessageCount"])
        for i in range(max_messages):
            msgObj = {
                "InsertTimeUtc": datetime.utcnow(),
                "JobName": blob_name,
                "InvocationId": context.invocation_id,
                "JobId": i
            }
            message = b64encode(json.dumps(msgObj, sort_keys=True, default=str).encode('utf-8')).decode('ascii')
            queue_client.send_message(message)
            iMsg += 1
            
    except Exception as ex:
        logging.exception(f"Error sending message to queue: {ex}")
        status["Status"] = "Failed"
    finally:
        end_time = datetime.utcnow()
        status["EndTime"] = end_time.strftime("%Y-%m-%d %H:%M:%S%z")
        status["DurationInSec"] = f"{(end_time - start_time).total_seconds()}"
        status["ScheduledMessages"] = f"{iMsg}"
        logging.info(json.dumps(status))