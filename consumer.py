import os
import json
from kafka import KafkaConsumer
from database import Database

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "keyfile.json"
DATASET_ID = 'week4'

consumer = KafkaConsumer('gatekeeper', bootstrap_servers='localhost:9092')
db = Database(DATASET_ID)


for msg in consumer:
    # deserialize Json
    payload = json.loads(msg.value)

    # looping through activities
    for activity in payload['activities']:
        # 1. insert operation
        if activity['operation'] == 'insert':
            # db.insert(activity)
            print(activity)

        # 2. delete operation
        elif activity['operation'] == 'delete':
            # db.delete(activity)
            print(activity)

        # should be filtered by gatekeeper API
        else:
            pass
