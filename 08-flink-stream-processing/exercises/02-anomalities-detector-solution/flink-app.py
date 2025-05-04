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
        self.total_count = runtime_context.get_state(
            ValueStateDescriptor("total_count", Types.LONG())
        )
        self.total_amount = runtime_context.get_state(
            ValueStateDescriptor("total_amount", Types.DOUBLE())
        )

    def process(self,
                key,
                context,
                elements):
        current_total_count = self.total_count.value() or 0
        current_total_amount = self.total_amount.value() or 0

        window_total = 0
        window_count = 0

        for input in elements:
            window_count += 1
            window_total += input.amount

        if current_total_count > 0:
            current_average = current_total_amount / current_total_count
            window_average = window_total / window_count

            if window_average > 1.5 * current_average:
                yield json.dumps({
                    "merchant_id": key,
                    "running_average": current_average,
                    "window_average": window_average,
                })

        new_total_count = current_total_count + window_count
        new_total_amount = current_total_amount + window_total

        self.total_count.update(new_total_count)
        self.total_amount.update(new_total_amount)


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

    anomalies_stream = payments_stream \
        .key_by(lambda payment: payment.merchant_id) \
        .window(TumblingProcessingTimeWindows.of(Time.seconds(10))) \
        .process(PaymentsAnomaliesDetector(), output_type=Types.STRING())

    anomalies_stream.print("DetectedAnomalies")

    env.execute("Payment anomalies detection")


if __name__ == "__main__":
    main()