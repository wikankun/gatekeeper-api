import logging
import json
import os
from dotenv import load_dotenv
from kafka import KafkaConsumer
from google.api_core.exceptions import BadRequest
from .database import Database

load_dotenv()

DATASET_ID = os.environ.get("DATASET_ID")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC")
BOOTSTRAP_SERVER = os.environ.get("BOOTSTRAP_SERVER")

logging.basicConfig(format='%(asctime)s - %(message)s', datefmt='%d-%m-%y %H:%M:%S')
consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=BOOTSTRAP_SERVER)
db = Database(DATASET_ID)

logging.warning('Consumer Starting')
for msg in consumer:
    # deserialize json
    payload = json.loads(msg.value)

    # looping through activities
    for activity in payload['activities']:
        try:
            db.process(activity)
            logging.warning(activity)
        except BadRequest as e:
            db.insert_error_log(activity, e.code)
            logging.error(e)
        except BaseException as e:
            db.insert_error_log(activity, e)
            logging.error(e)
