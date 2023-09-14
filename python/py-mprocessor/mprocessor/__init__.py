import logging

import azure.functions as func


def main(msg: func.QueueMessage) -> None:
    try:
        logging.info('Python queue trigger function processed a queue item: %s', msg.get_body().decode('utf-8'))
    except Exception as ex:
        logging.error('Exception: %s', ex)
