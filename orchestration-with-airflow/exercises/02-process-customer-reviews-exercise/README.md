This is a README for the second exercise in this section.

# 0. Follow the instruction in the README.md file in the "exercises" folder

Before following steps in this README, follow the steps in the `README.md` file in the `exercises` folder to set up your local Airflow.

# 1. Install Airflow provider

Since your DAG will read data from Postgres and use Spark, you need to first install Postgres and Spark providers:

```sh
pip install apache-airflow-providers-postgres
pip install apache-airflow-providers-apache-spark
```

You need to run this command in the virtual environment you've created for exercises in this section.

# 2. Start the Postgres database

Start the Postgres instance from which your DAG will ingest data using the Docker Compose command:

```sh
docker-compose up
```

# 3. Create a table in the Postgres database

Having a database running we can create a table from which Airflow will ingest data.

To connect to a database use the following parameters:

* *Host* - `localhost`
* *Database* - `rental_site`
* *Login* - `user`
* *Password* - `password`
* *Port* - `5432`

Then, execute this statement to create a database for this exercise:

```sql
CREATE TABLE customer_reviews (
    review_id SERIAL PRIMARY KEY,
    listing_id INT NOT NULL,
    review_score INT NOT NULL,
    review_comment TEXT,
    review_date TIMESTAMP NOT NULL DEFAULT NOW()
);
```

# 4. Create a Postgres connection in the Airflow UI

Create a Postgres connection, so your DAG could use a Postgres hook. 

To do it run the following command from your virtual environment:

```sh
airflow connections add 'postgres_rental_site' \
    --conn-type 'postgres' \
    --conn-host 'localhost' \
    --conn-login 'user' \
    --conn-password 'password' \
    --conn-port '5432' \
    --conn-schema 'rental_site'
```

# 5. Create a Spark connection in the Airflow UI

Create a Spark connection, so your DAG could run Spark applications.

To do it run the following command from your virtual environment:

```sh
airflow connections add 'spark_rental_site' \
    --conn-type 'spark' \
    --conn-host 'local' \
    --conn-extra '{"deploy_mode": "client"}'
```


# 6. Copy the DAG and the Spark code

Copy the following files to the `dags` folder you've created while setting up Airflow locally:

* `customer_reviews_dag.py` - Airflow DAGs implementing customer reviews processing
* `spark_etl_reviews.py` - Spark job for processing customer reviews

# 7. Restart the scheduler

To restart a scheduler process open the terminal with the running scheduler process, and stop it using the `Ctrl+C`.

After this, start it again using the following command:

```sh
airflow scheduler
```

# 8. Implement the TODOs in the code

Now implement the TODO comments in the starter code.


# 9. Start the DAG

Once the DAG is implemented you can start it by clicking on the toggle in the Airflow UI for the DAG you've implemented.

# 10. Add some test reviews to test the created pipeline

Now you can test your pipeline. Add these reviews to the `customer_reviews` table:

```sql
INSERT INTO customer_reviews (listing_id, review_score, review_comment, review_date)
VALUES
    (101, 5, 'Excellent stay, highly recommend!', NOW()),
    (101, 5, 'Great location!', NOW()),
    (102, 4, 'Good location but a bit noisy.', NOW()),
    (102, 3, 'Poor room service.', NOW()),
    (103, 3, 'Could have been worse.', NOW());
```

At the next run your pipeline will read these reviews and compute an average score per listing ID.
