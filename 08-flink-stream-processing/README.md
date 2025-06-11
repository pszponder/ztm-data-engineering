## Prerequisites

- Python 3.11 (Try to use this version if you have errors)
- Java (required for running Flink)

---

## Create and Activate a Virtual Environment

```sh
python3.11 -m venv venv
source ./venv/bin/activate
```

---

## Install Required Python Packages

Install the Confluent Kafka client and Apache Flink with:

```sh
pip install confluent-kafka
pip install apache-flink
```

---

## Configure `flink` CLI Command

Determine the Flink installation path and update the `PATH` environment variable:

```sh
FLINK_HOME=$(./venv/bin/find_flink_home.py)
export PATH=$PATH:$FLINK_HOME/bin
```

Verify the installation:

```sh
flink --version
```

---

## Download the Kafka Connector

Search for **`flink-sql-connector-kafka maven`** in your browser and download the latest available JAR file. Place it in the project directory.

---

## Sample Flink Application

Create a Python file named `01-flink-hello-world.py` with the following content:

```python
from pyflink.datastream import StreamExecutionEnvironment

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    data_stream = env.from_collection([1, 2, 3, 4, 5])

    mapped_stream = data_stream.map(lambda x: x * 2)

    mapped_stream.print()

    env.execute("Flink Hello World")

if __name__ == "__main__":
    main()
```

---

## Run the Application

Run the Flink application locally with the Kafka connector:

```sh
flink run \
  --python 01-flink-hello-world.py \
  --target local \
  --jarfile flink-sql-connector-kafka-3.4.0-1.20.jar
```

---

Flink is now ready and running locally. You can build on this setup in future demos using real-time Kafka streams.