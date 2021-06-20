import json
from kafka import KafkaConsumer
from .database import Database

consumer = KafkaConsumer('gatekeeper', bootstrap_servers='localhost:9092')
db = Database('week4')

print('Consumer Starting')
for msg in consumer:
    # deserialize json
    payload = json.loads(msg.value)

    # looping through activities
    for activity in payload['activities']:
        try:
            # 1. insert operation
            if activity['operation'] == 'insert':
                db.insert(activity)
                print(activity)
            # 2. delete operation
            elif activity['operation'] == 'delete':
                db.delete(activity)
                print(activity)
        except Exception as e:
            db.insert_error_log(activity, e)
            print(e)
