# Gatekeeper API

Setup kafka
```
docker-compose up -d
```

Create kafka topic
```
docker-compose exec broker kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic gatekeeper
```
