import json
from fastapi import FastAPI, Response, status
from fastapi.encoders import jsonable_encoder
from kafka import KafkaProducer
from .models import Payload

producer = KafkaProducer(bootstrap_servers='localhost:9092')

app = FastAPI()


@app.get("/")
def base(response: Response):
    response.status_code = status.HTTP_200_OK
    return {"message": "ok"}


@app.post("/api/activities")
def read_item(payload: Payload, response: Response):
    request = jsonable_encoder(payload)
    for activity in request['activities']:
        print(activity)
        if activity['operation'] not in ['insert', 'delete']:
            response.status_code = status.HTTP_406_NOT_ACCEPTABLE
            return {'error': 'activity operation not allowed'}

    producer.send('gatekeeper',
                  json.dumps(request).encode('utf-8'))

    return {"message": "ok", "payload": request}
