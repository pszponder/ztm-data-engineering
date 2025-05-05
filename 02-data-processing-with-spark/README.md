
# Install Spark

To install Spark locally on MacOS run the following command:

```sh
brew install apache-spark
```

To check that it was installed correctly you can run the following command:

```sh
pyspark --version
```

# Create a virtual environment

```sh
python -m venv venv
```

To activate the virtual environment run the following command:

```sh
source venv/bin/activate
```

# Install Jupyter Lab

Run the following command inside the virtual environment to install Jupyter Lab and configure Spark to use it:

```sh
pip install jupyter
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='lab'
```

# Start Jupyter Notebooks with local PySpark

Now you can start Jupyter Notebooks in the virtual environment:

```sh
pyspark
```

# Spark "Hello World"

You can try to run the following Spark code:


```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("First Spark application")
         .getOrCreate())

data = [
    {"userId": 1, "paymentAmount": 100.0, "date": "2025-01-01"},
    {"userId": 2, "paymentAmount": 150.5, "date": "2025-01-02"},
    {"userId": 3, "paymentAmount": 200.75, "date": "2025-01-03"},
    {"userId": 2, "paymentAmount":  50.25, "date": "2025-01-04"},
    {"userId": 1, "paymentAmount":  80.0,  "date": "2025-01-05"},
]

df = spark.createDataFrame(data)
df.count()
```
