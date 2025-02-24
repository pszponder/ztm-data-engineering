This is a README for the first exercise in this section.

# 0. Follow the instruction in the README.md file in the "exercises" folder

Before following steps in this README, follow the steps in the `README.md` file in the `exercises` folder to set up your local Airflow.

# 1. Install Airflow provider

Since your DAG will read data from Postgres and use Spark, you need to first install Postgres and Spark providers:

```sh
pip install apache-airflow-providers-postgres
pip install apache-airflow-providers-apache-spark
```

You need to run this command in the virtual environment you've created for exercises in this section.

# 3. Restart the webserver

In order to be able to create Postgres connections enabled by the Postgres provider you need to restart the webserver.

To restart a scheduler process open the terminal with the running webserver process, and stop it using the `Ctrl+C`.

After this, start it again using the following command:

```sh
airflow webserver --port 8080
```

# 4. Start the Postgres database

Start the Postgres instance from which your DAG will ingest data using the Docker Compose command:

```sh
docker-compose up
```

# 5. Create a table in the Postgres database

Having a database running we can create a table from which Airflow will ingest data.

To connect to a database use the following parameters:

* *Host* - `localhost`
* *Database* - `rental_site`
* *Login* - `user`
* *Password* - `password`
* *Port* - `5432`

Then, execute this statement to create a database for this exercise:

```sql
CREATE TABLE guest_reviews (
    review_id SERIAL PRIMARY KEY,
    listing_id INT NOT NULL,
    review_score INT NOT NULL,
    review_comment TEXT,
    review_date TIMESTAMP NOT NULL DEFAULT NOW()
);
```

# 6. Create a Postgres connection in the Airflow UI

Create a Postgres connection in the Airflow UI, so your DAG could use a Postgres hook. In the Airflow UI go to `Admin` -> `Connections`. Then click on the `+` button to create a new connection.

Enter the following configuration parameters:

* `Connection Id` to `postgres_rental_site`
* `Connection Type` to `Postgres`
* `Host` to `localhost`
* `Database` to `rental_site`
* `Login` to `user`
* `Password` to `password`
* `Port` to `5432`

Click on the `Save` button to create a new connection.

# 7. Create a Spark connection in the Airflow UI

Create a Postgres connection in the Airflow UI, so your DAG could use a Postgres hook. In the Airflow UI go to `Admin` -> `Connections`. Then click on the `+` button to create a new connection.

Enter the following configuration parameters:

* `Connection Id` to `spark_rental_site`
* `Connection Type` to `Spark`



# 8. Copy the DAG

Copy the starter code for the DAG to the `dags` folder you've created while setting up Airflow locally

# 9. Restart the scheduler

To restart a scheduler process open the terminal with the running scheduler process, and stop it using the `Ctrl+C`.

After this, start it again using the following command:

```sh
airflow scheduler
```

# 10. Implement the TODOs in the code

Now implement the TODO comments in the starter code.


# 11. Start the DAG

Once the DAG is implemented you can start it by clicking on the toggle in the Airflow UI for the DAG you've implemented.