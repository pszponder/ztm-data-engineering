
class Order:
    def __init__(
        self,
        order_id: int,
        customer_id: int,
        total_price: float,
        customer_country: str,
        merchant_country: str,
        order_datetime: str,
    ):
        self.order_id = order_id
        self.customer_id = customer_id
        self.total_price = total_price
        self.customer_country = customer_country
        self.merchant_country = merchant_country
        self.order_datetime = order_datetime
    

    @staticmethod
    def from_dict(obj: dict) -> "Order":
        return Order(
            order_id=obj["order_id"],
            customer_id=obj["customer_id"],
            total_price=obj["total_price"],
            customer_country=obj["customer_country"],
            merchant_country=obj["merchant_country"],
            order_datetime=obj["order_datetime"],
        )

    def to_json(self) -> dict:
        return {
            "order_id": self.order_id,
            "customer_id": self.customer_id,
            "total_price": self.total_price,
            "customer_country": self.customer_country,
            "merchant_country": self.merchant_country,
            "order_datetime": self.order_datetime,
        }

    def __str__(self) -> str:
        return (f"Order("
                f"order_id={self.order_id}, "
                f"customer_id={self.customer_id}, "
                f"total_price={self.total_price}, "
                f"customer_country='{self.customer_country}', "
                f"merchant_country='{self.merchant_country}', "
                f"order_datetime='{self.order_datetime}')")


ORDER_SCHEMA = {
    "type": "record",
    "name": "Order",
    "fields": [
        {"name": "order_id", "type": "int"},
        {"name": "customer_id", "type": "int"},
        {"name": "total_price", "type": "float"},
        {"name": "customer_country", "type": "string"},
        {"name": "merchant_country", "type": "string"},
        {"name": "order_datetime", "type": "string"},
    ],
}