import logging
import os
import azure.functions as func
import json

from base64 import b64encode
from datetime import datetime
from azure.storage.blob.aio import BlobServiceClient
from azure.storage.queue.aio import QueueClient
from azure.core.exceptions import ResourceExistsError

# Setup the append blob
async def setup_append_blob(connection_string: str, append_blob_name: str, start_time: datetime) -> None:
    
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client("checks")

    try:
        await container_client.create_container()
    except ResourceExistsError:
        logging.info('Container exist.')

    append_blob_client = container_client.get_blob_client(append_blob_name)
    if (await append_blob_client.exists()):
        blob_properties = await append_blob_client.get_blob_properties()
        
        blob_stats = {
            "AppendBlobName": append_blob_name,
            "BlockCount": blob_properties.append_blob_committed_block_count,
        }

        try:
            blob_stats["LastSchedulerStartTime"] = blob_properties.metadata["TriggerData"]
            blob_stats["BlobLastModifiedTime"] = f"{blob_properties.last_modified}"
            blob_stats["BlobCreationTime"] = f"{blob_properties.creation_time}"
            
            blob_stream = await append_blob_client.download_blob()
            blob_content = await blob_stream.content_as_text()
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

            logging.critical(json.dumps(blob_stats))

            await append_blob_client.delete_blob()
        except Exception as ex:
            logging.exception(f"Exception while processing append blob: {ex}")

    metadata = {
        "TriggerData": start_time.strftime("%Y-%m-%d %H:%M:%S%z")
    }
    await append_blob_client.create_append_blob(metadata=metadata)

async def main(mytimer: func.TimerRequest, context: func.Context) -> None:
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
        blob_name = f"ApendBlob_{int(start_time.second / 20)}"
        await setup_append_blob(connection_string, blob_name, start_time)

        queue_client = QueueClient.from_connection_string(connection_string, "checks")
                
        try:
            # Create the queue
            await queue_client.create_queue()
        except ResourceExistsError:
            logging.info('Queue exist.')

        # Send messages to the queue
        max_messages = int(os.environ["MessageCount"])
        # task_list = []
        for i in range(max_messages):
            msgObj = {
                "InsertTimeUtc": datetime.utcnow(),
                "JobName": blob_name,
                "InvocationId": context.invocation_id,
                "JobId": i
            }
            message = b64encode(json.dumps(msgObj, sort_keys=True, default=str).encode('utf-8')).decode('ascii')
            await queue_client.send_message(message)
            iMsg += 1
        # Wait for all the tasks to complete
        # await asyncio.gather(*task_list)
            
    except Exception as ex:
        logging.exception(f"Error sending message to queue: {ex}")
        status["Status"] = "Failed"
    finally:
        end_time = datetime.utcnow()
        status["EndTime"] = end_time.strftime("%Y-%m-%d %H:%M:%S%z")
        status["DurationInSec"] = f"{(end_time - start_time).total_seconds()}"
        status["ScheduledMessages"] = f"{iMsg}"
        logging.critical(json.dumps(status))