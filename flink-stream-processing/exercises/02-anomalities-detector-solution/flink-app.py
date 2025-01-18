import json
from dataclasses import dataclass

from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSink, KafkaSource, KafkaRecordSerializationSchema
from pyflink.datastream.execution_mode import RuntimeExecutionMode
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common import Time
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.datastream.functions import RichProcessWindowFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream import TimeCharacteristic


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


class PaymentsAnomaliesDetector(RichProcessWindowFunction):

    def open(self, runtime_context):
        descriptor = ValueStateDescriptor("avg_state", Types.DOUBLE())
        self.avg_state = runtime_context.get_state(descriptor)

    def process(self, key, context, elements, out):
        # Retrieve historical average from state
        historical_avg = self.avg_state.value() or 0.0

        # Calculate the sum and average of payments in the current window
        current_sum = sum(payment.amount for payment in elements)
        current_count = len(elements)
        current_avg = current_sum / current_count if current_count > 0 else 0.0

        # Detect surge: if current sum exceeds 1.5 times the historical average
        if historical_avg > 0 and current_sum > 1.5 * historical_avg:
            alert = (f"Surge detected for merchant {key}: current_sum = {current_sum}, "
                     f"historical_avg = {historical_avg}")
            out.collect(alert)

        # Update historical average state (simple moving average)
        new_avg = (historical_avg + current_avg) / 2 if historical_avg else current_avg
        self.avg_state.update(new_avg)


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
    ).map(parse_payment, output_type=Types.PICKLED_BYTE_ARRAY())

    surge_stream = payments_stream \
        .key_by(lambda payment: payment.merchant_id) \
        .window(TumblingProcessingTimeWindows.of(Time.minutes(1))) \
        .process(PaymentsAnomaliesDetector(), output_type=Types.STRING())

    # Print surge alerts to console
    surge_stream.print()

    env.execute("Payment anomalies detection")


if __name__ == "__main__":
    main()