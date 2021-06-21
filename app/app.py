import json
import os
from dotenv import load_dotenv
from fastapi import FastAPI, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from kafka import KafkaProducer
from .models import Payload
from .database import Database

load_dotenv()

DATASET_ID = os.environ.get("DATASET_ID")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC")
BOOTSTRAP_SERVER = os.environ.get("BOOTSTRAP_SERVER")

producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)
db = Database(DATASET_ID)

app = FastAPI()


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc):
    """Override validation exception handler to insert error log"""
    error_msg = exc.errors()
    db.insert_error_log(request, error_msg)
    return JSONResponse(
        status_code=status.HTTP_406_NOT_ACCEPTABLE,
        content={'error': error_msg},
    )


@app.get("/")
def base():
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"message": "ok"},
    )


@app.post("/api/activities")
def read_item(payload: Payload):
    """Activities API endpoint"""
    request = jsonable_encoder(payload)
    for activity in request['activities']:
        if activity['operation'] not in ['insert', 'delete']:
            error_msg = 'activity operation not allowed'
            db.insert_error_log(payload, error_msg)
            return JSONResponse(
                status_code=status.HTTP_406_NOT_ACCEPTABLE,
                content={'error': error_msg},
            )

    producer.send(KAFKA_TOPIC,
                  json.dumps(request).encode('utf-8'))

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={'message': 'ok', 'payload': request},
    )
