# Install Spark

To install Spark locally on macOS, run the following command:

```sh
brew install apache-spark
```

To check that it was installed correctly, you can run:

```sh
pyspark --version
```

---

# Create a Virtual Environment

Create a virtual environment in a directory named `venv`:

```sh
python -m venv venv
```

Activate the virtual environment:

```sh
source venv/bin/activate
```

---

# Install Jupyter Lab

Run the following command inside the virtual environment to install Jupyter Lab:

```sh
pip install jupyter
```

Then configure PySpark to use Jupyter Lab as the driver:

```sh
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='lab'
```

---

# Start Jupyter Notebooks with Local PySpark

Now you can start Jupyter Lab with PySpark:

```sh
pyspark
```

This will open a Jupyter Lab interface in your browser where you can interact with Spark using notebooks.

---

# Spark "Hello World"

You can try to run the following Spark code to verify everything is working:

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

You should see the number of rows in the DataFrame as the output.

---

# Clean Up

When you're done working, follow these steps to shut everything down:

1. **Stop the Spark session (in a notebook):**

    ```python
    spark.stop()
    ```

2. **Stop Jupyter Lab (in terminal):**

    Press `Ctrl+C` in the terminal where `pyspark` was running.

3. **Deactivate the virtual environment:**

    ```sh
    deactivate
    ```

At this point, you're back to your global Python environment. You're now ready to continue developing Spark applications!

---