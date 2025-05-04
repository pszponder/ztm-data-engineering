
class Order:
    def __init__(
        self,
        order_id,
        customer_id,
        total_price,
        customer_country,
        merchant_country,
        order_datetime,
    ):
        self.order_id = order_id
        self.customer_id = customer_id
        self.total_price = total_price
        self.customer_country = customer_country
        self.merchant_country = merchant_country
        self.order_datetime = order_datetime
    

    @staticmethod
    def from_dict(obj):
        return Order(
            order_id=obj["order_id"],
            customer_id=obj["customer_id"],
            total_price=obj["total_price"],
            customer_country=obj["customer_country"],
            merchant_country=obj["merchant_country"],
            order_datetime=obj["order_datetime"],
        )

    def to_dict(self):
        return {
            "order_id": self.order_id,
            "customer_id": self.customer_id,
            "total_price": self.total_price,
            "customer_country": self.customer_country,
            "merchant_country": self.merchant_country,
            "order_datetime": self.order_datetime,
        }

    def __str__(self):
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
        {"name": "customer_id", "type": "string"},
        {"name": "total_price", "type": "float"},
        {"name": "customer_country", "type": "string"},
        {"name": "merchant_country", "type": "string"},
        {"name": "order_datetime", "type": "string"},
    ],
}