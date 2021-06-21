# Gatekeeper API

[![FastAPI Test](https://github.com/wikankun/gatekeeper-api/actions/workflows/fastapi.yml/badge.svg)](https://github.com/wikankun/gatekeeper-api/actions/workflows/fastapi.yml)
[![GitHub tag](https://img.shields.io/github/tag/wikankun/gatekeeper-api.svg)](https://GitHub.com/wikankun/gatekeeper-api/tags/)
[![made-with-python](https://img.shields.io/badge/Made%20with-Python-1f425f.svg)](https://www.python.org/)



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

Run `consumer.py`
```
python -m app.consumer
```

Load test with locust
```
locust
```

Unit test
```
pytest
```
