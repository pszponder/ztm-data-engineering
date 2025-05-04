import json
from dataclasses import dataclass

from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.datastream.execution_mode import RuntimeExecutionMode
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common import Time
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream import StreamExecutionEnvironment, ProcessWindowFunction


@dataclass
class Payment:
    payment_id: str
    user_id: str
    merchant_id: str
    amount: float
    payment_time: str


def parse_payment(json_str: str) -> Payment:
    data = json.loads(json_str)
    return Payment(
        payment_id=data.get("payment_id", "unknown"),
        user_id=data.get("user_id", "unknown"),
        merchant_id=data.get("merchant_id", "unknown"),
        amount=float(data.get("amount", 0.0)),
        payment_time=data.get("payment_time", "unknown")
    )


class PaymentsAnomaliesDetector(ProcessWindowFunction):

    def open(self, runtime_context):
        # TODO: Define state for the total number of payments from a merchant
        self.total_count = None
        # TODO: Define state for the sum of all payment amounts from a merchant
        self.total_amount = None

    def process(self,
                key,
                context,
                elements):
        current_total_count = self.total_count.value() or 0
        current_total_amount = self.total_amount.value() or 0

        window_total = 0
        window_count = 0

        for input in elements:
            # TODO: Compute window_total and window_count using elements in the window

        if current_total_count > 0:
            # TODO: Compute average payment amount using values from the local state
            current_average = None
            # TODO: Compute average payment amount for the current window
            window_average = None

            if window_average > 1.5 * current_average:
                # TODO: Emit a record about a detected anomaly

        # TODO: Update local state using data from the current window


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_topics("payments") \
        .set_group_id("flink-consumer-group") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    payments_stream = env.from_source(
        kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="kafka_source"
    ).map(parse_payment)


    anomalies_stream = payments_stream 
    # TODO: Add stream processing steps for anomaly detection.
    # For each merchant, use PaymentsAnomaliesDetector on
    # 10 seconds tumbling windows

    anomalies_stream.print("DetectedAnomalies")

    env.execute("Payment anomalies detection")


if __name__ == "__main__":
    main()