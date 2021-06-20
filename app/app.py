import json
from fastapi import FastAPI, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from kafka import KafkaProducer
from .models import Payload
from .database import Database

producer = KafkaProducer(bootstrap_servers='localhost:9092')
db = Database('week4')

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

    producer.send('gatekeeper',
                  json.dumps(request).encode('utf-8'))

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={'message': 'ok', 'payload': request},
    )
