import json
from kafka import KafkaConsumer
from .database import Database

consumer = KafkaConsumer('gatekeeper', bootstrap_servers='localhost:9092')
db = Database()


for msg in consumer:
    # deserialize Json
    payload = json.loads(msg.value)

    # looping through activities
    for activity in payload['activities']:
        # 1. insert operation
        if activity['operation'] == 'insert':
            print(activity)

        # 2. delete operation
        elif activity['operation'] == 'delete':
            print(activity)

        # should be filtered by gatekeeper API
        else:
            pass
