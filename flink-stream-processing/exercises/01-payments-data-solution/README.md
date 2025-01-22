

# 0. Stop all Docker containers

Before you start, you would need to stop Docker containers related to this bootcamp
running on your machine.


# 1. Start Kafka

First, start Kafka using Docker Compose:

```sh
docker-compose up
```


## 2. Create a Python Virtual Environment

Run the following commands to create a virtual environment and install dependencies:

```bash
python -m venv venv
source ./venv/bin/activate
pip install -r requirements.txt
```


## 3. Download the Flink Kafka Connector

We need to download the Kafka connector for Flink. Run the following command to download it from Maven:

```bash
wget https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar
```

The downloaded `.jar` file should be placed in the current directory with the `flink-app.py` file


## 4. Configure the `flink` Command in the Virtual Environment

Ensure the `flink` CLI is available in the virtual environment.

```bash
export PATH=$PATH:"$(./venv/bin/find_flink_home.py)/bin"
```

Run `flink` without arguments to check if it was installed correctly

```bash
flink --version
```

NOTE: You would need to set the `PATH` variable every time to activate a virtual environment.


## 5. Implement the Flink Application

Implement the Flink application in the `flink-app.py` follow the instructions in the `TODO` comments.


## 6. Start the Producer

In the virtual environment run the producer script to generate the payments data:

```bash
python payments-producer.py
```

This will produce random payments data to the `payments` Kafka topic.


## 7. (Optional) Verify Producer Output

To double-check that everything is working as expected, run Kafka CLI tools to check that the producer is writing data to the `payments` topic:

```bash
kafka-console-consumer --topic payments \
  --bootstrap-server localhost:9092 \
  --from-beginning
```

Make sure that you see payments data and that it is written every second.


## 8. Run the Flink Application

Run the Flink application to process the data. Make sure the Kafka connector JAR is included in the classpath.

```bash
flink run \
  --python flink-app.py \
  --target local \
  --jarfile flink-sql-connector-kafka-3.4.0-1.20.jar
```


## 9. Verify Flink Output

Run the Flink application to process the payments data:

```bash
kafka-console-consumer --topic filtered-payments \
  --bootstrap-server localhost:9092 \
  --from-beginning
```
