# Kafka Streams Stateful DEMO App

## Before you start

### Prerequisites

- Java 17
- Maven
- Docker Compose
- JR

### Spin-up the environment

```bash
docker compose up -d
```

### Create source topics

```
docker exec jr bash -c 'jr emitter run shoestore'
```

## Run the application

```bash
mvn package
java -cp target/myapp-1.0-SNAPSHOT.jar \
  -javaagent:monitoring/jmx_prometheus_javaagent-1.1.0.jar=9191:monitoring/kafka_streams.yml \
  com.github.rampi.App
```
