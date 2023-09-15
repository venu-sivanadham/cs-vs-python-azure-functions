import logging
import os
import json

import azure.functions as func
from datetime import datetime
from azure.storage.blob import BlobServiceClient

def main(msg: func.QueueMessage) -> None:

    start_time = datetime.utcnow()        
    status = {
        "StartTime": start_time,
        "TriggerType": "MessageProcessor",
        "Status": "Succeeded"
    }

    try:
        msg_content = msg.get_body().decode('utf-8')
        logging.info('Python queue trigger function processed a queue item: %s', msg_content)
        msg = json.loads(msg_content)

        # Create a queue client using connection string
        connection_string = os.environ["AzureWebJobsStorage"]
        host_id = os.environ["WEBSITE_INSTANCE_ID"]

        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client("checks")

        blob_client = container_client.get_blob_client(f"{host_id}.txt")

    except Exception as ex:
        logging.exception(f'Exception: {ex}')
        status["Status"] = "Failed"
    finally:
        status["EndTime"] = datetime.utcnow()
        status["Duration"] = status["EndTime"] - status["StartTime"]
        logging.info(status)
