## Prerequisites

- [Docker](https://www.docker.com/) installed and running
- Docker Compose (included with Docker Desktop)

---

## Start Kafka Broker

Run the following command to start Kafka using the provided `docker-compose.yml` file:

```sh
docker-compose up
```

---

## Install Kafka CLI Tools

To interact with Kafka from the command line, install the Kafka tools:

```sh
brew install kafka
```

---

## List Kafka Topics

Use the following command to list all topics in the Kafka cluster:

```sh
kafka-topics --list --bootstrap-server localhost:9092
```

---

## Create a Kafka Topic

Create a topic named `orders` with 4 partitions and a replication factor of 1:

```sh
kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --replication-factor 1 \
  --partitions 4
```

---

## Verify Topic Creation

List topics again to verify the new topic was created:

```sh
kafka-topics --list --bootstrap-server localhost:9092
```