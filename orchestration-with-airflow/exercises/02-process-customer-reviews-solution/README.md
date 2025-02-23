


# 0. Follow the instruction in the README.md file in the "exercises" folder


# 1. Install Postgres provider

```sh
pip install apache-airflow-providers-postgres
```


# 2. Start the Postgres database


```sh
docker-compose up
```

# 3. Create a table

```sql
CREATE TABLE guest_reviews (
    review_id SERIAL PRIMARY KEY,
    listing_id INT NOT NULL,
    review_score INT NOT NULL,
    review_comment TEXT,
    review_date TIMESTAMP NOT NULL DEFAULT NOW()
);
```

# 4. Create a Postgres connection in Airflow UI


* `Connection Type` to `Postgres`
* `Host` to `localhost`
* `Database` to `rental_site`
* `Login` to `user`
* `Password` to `password`
* `Port` to `5432`


# 5. Copy the DAG



# 6. Restart the scheduler


