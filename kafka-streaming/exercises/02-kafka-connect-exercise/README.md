
# 0. Stop all Docker containers 

Before you start, you would need to stop Docker containers related to this bootcamp 
running on your machine.

# 1. Start Kafka

First, start Kafka, Kafka Connect, and Postrges using Docker Compose:

```sh
docker-compose up
```


# 2. Create a virtual environment and install dependencies

Run the following commands to create a virtual environment and install dependencies:

```sh
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

# 3. Create a Debezium connector

Once you have Kafka Connect running, you need to create a connector to read a stream of updates from Postgres. Run this command to create it:

```sh
curl -X POST -H "Content-Type: application/json" -d @config_debezium.json  http://localhost:8083/connectors
```

# 4. Connect to a database

You will need to execute several SQL operations. To connect to a database, use the following arguments:

* *URL* - `127.0.0.1:5432`
* *Username* - `user`
* **Password** - `password`
* **Database** - `onlineshop`

# 5. Create "orders" table

First, you need to create the `orders` table using the following SQL statement:

```sql
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    customer_name VARCHAR(100),
    customer_email VARCHAR(255),
    product_id VARCHAR(50),
    total_amount NUMERIC(10, 2),
    order_date TIMESTAMPTZ,
    status VARCHAR(50),
    payment_method VARCHAR(50)
);
```


# 6. Alter table

By default, WAL records produced by Postgres will only contain data in a table after the update. To include a snapshot of data before the update, we need to run the following SQL command:

```sql
ALTER TABLE orders REPLICA IDENTITY FULL;
```

After this command every `UPDATE` or `DELETE` operation on the `orders` table, Postgres will log the entire row’s data before and after the update in the Write-Ahead Log.

# 7. Create a new order 

Once you have a table, you can create the `orders` table using this SQL statement.

```sql
INSERT INTO orders (
    customer_id,
    customer_name,
    customer_email,
    product_id,
    total_amount,
    order_date,
    status,
    payment_method
)
VALUES (
    'CUST-1234',
    'John Smith',
    'john.smith@example.com',
    'PROD-XYZ789',
    59.95,
    '2024-12-09T10:45:00Z',
    'processed',
    'paypal'
)
RETURNING id;
```

This should return the `id` of the newly created record that you can use to perform an update operation.

# 8. Update an order status

Now, we can update the created record. You can do it using this command:

```sql
UPDATE orders
SET status = 'refunded'
WHERE id = 1
```

Since it changes the `status` value from `processed` to `refunded` it should.

# 9. Check if Kafka Connect writes records to Kafka

Run the following command to test if Kafka Connect writes records to Kafka:

```sh
kafka-console-consumer --bootstrap-server localhost:9092 --topic postgres-.public.orders --from-beginning
```

You should see two records: one for the `INSERT` operation and another one for the `UPDATE` operation.

# 10. Implement and run your consumer and see if it works

You should now implement and run your Python consumer.
It should print a single message for the executed update operation.

# 11. (Optional) Create more test records

If you need more test records, you can repeat steps **7** and **8** again for a new record, but you would need to change the `id` comparison value in the `UPDATE` statement.