import logging
import os
import json
import aiohttp

import azure.functions as func
from datetime import datetime
from azure.storage.blob.aio import BlobServiceClient

async def main(msg: func.QueueMessage, context: func.Context) -> None:

    start_time = datetime.utcnow()        
    status = {
        "StartTime": start_time.strftime("%Y-%m-%d %H:%M:%S%z"),
        "TriggerType": "MessageProcessor",
        "Status": "Succeeded"
    }

    try:
        msg_content = msg.get_body().decode('utf-8')
        logging.info('Python queue trigger function processed a queue item: %s', msg_content)
        msg = json.loads(msg_content)
        status["TriggerData"] = msg_content
        blob_name = msg["JobName"]

        # Create a queue client using connection string
        connection_string = os.environ["AzureWebJobsStorage"]
        host_id = os.environ["WEBSITE_INSTANCE_ID"]

        # Do work here
        request_start_time = datetime.utcnow()
        os_provider_url = "https://veshivanpyasyncfunca636.z13.web.core.windows.net/metrics.html"
        async with aiohttp.ClientSession() as session:
            async with session.get(os_provider_url) as response:
                os_web_response = await response.text()
        
        request_duration = datetime.utcnow() - request_start_time
        logging.info(f"Received the OS Info of size: {len(os_web_response)}, call duration: {request_duration.microseconds}ms")

        # Update status to upend blob
        blob_service_client = BlobServiceClient.from_connection_string(conn_str=connection_string)
        blob_client = blob_service_client.get_blob_client(container="checks", blob=blob_name)
        if (await blob_client.exists()):
            logging.info(f"Blob {blob_name} exists")
            await blob_client.append_block(f"{host_id}:{context.invocation_id};")
        else:
            logging.info(f"Blob {blob_name} does not exist")

    except Exception as ex:
        logging.exception(f'Exception: {ex}')
        status["Status"] = "Failed"
    finally:
        end_time = datetime.utcnow()
        status["EndTime"] = end_time.strftime("%Y-%m-%d %H:%M:%S%z")
        status["DurationInSec"] = f"{(end_time - start_time).total_seconds()}"
        logging.info(json.dumps(status))
