# Gatekeeper API

Setup kafka
```
docker-compose up -d
```

Create kafka topic
```
docker-compose exec broker kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic gatekeeper
```

Create virtual environment and activate it
```
pip install virtualenv
virtualenv env
. env/bin/activate
```

Install required packages
```
pip install -r requirements.txt
```

Run FastAPI server
```
uvicorn app.app:app --reload
```

Run Consumer.py
```
python consumer.py
```

Run load testing with locust
```
locust
```

Run unit testing
```
pytest
```
