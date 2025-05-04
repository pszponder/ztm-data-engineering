import json
from dataclasses import dataclass

from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSink, KafkaSource, KafkaRecordSerializationSchema
from pyflink.datastream.execution_mode import RuntimeExecutionMode

# TODO: In this exercise you will need to
#
# * Read data written by the "payments-producer.py"
# * Filter payments with amount greater than 500
# * Output new records with only two fields: "payment_id" and "amount"
# * Write output to another Kafka topic

# TODO: Implement any functions and types that you need


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)

    # TODO: Create a Kafka source
    # TODO: Create a payments stream

    # TODO: Implement stream processing logic

    # TODO: Write resulting data to Kafka

    env.execute("Payments stream processing")


if __name__ == "__main__":
    main()