import datetime
import logging
import os
from base64 import b64encode

import azure.functions as func
from azure.storage.queue import QueueClient, QueueMessage
from azure.core.exceptions import ResourceExistsError


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    
    max_messages = 0

    try:
        # Create a queue client using connection string
        connection_string = os.environ["AzureWebJobsStorage"]    
        queue_client = QueueClient.from_connection_string(connection_string, "checks")

        try:
            # Create the queue
            queue_client.create_queue()
        except ResourceExistsError:
            logging.info('Queue exist.')

        # Send messages to the queue
        max_messages = int(os.environ["MessageCount"])
        for i in range(max_messages):
            message = b64encode('Message {0}'.format(i).encode('utf-8')).decode('ascii')
            queue_client.send_message(message)
            
    except Exception as ex:
        logging.error('Exception: %s', ex)
    
    logging.info('Added {0} messages to the queue.'.format(max_messages))

