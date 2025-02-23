
```sh
python3.12 -m venv venv
```


```sh
source venv/bin/activate
```


```sh
pip install 'apache-airflow==2.10.4' \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.12.txt"
```


```sh
airflow db migrate
```



```sh
airflow info
```

```
vim <airflow-home-path>/airflow.cfg
```

* `dags_folder` to ...
* `load_examples` to `False`


```sh
airflow webserver --port 8080
```


```sh
airflow users create \
  --username admin \
  --firstname <your-name> \
  --lastname <your-surname> \
  --role Admin \
  --email admin@example.com \
  --password admin
```

```
airflow scheduler
```
