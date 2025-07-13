# Pyspark

## Setup

### Create a Virtual Environment

```sh
uv venv
```

### Install Spark

```sh
uv add pyspark
```

To check that it was installed correctly, you can run:

```sh
uv run pyspark --version
```

### Install Jupyter Lab

Run the following command to install Jupyter Lab:

```sh
uv add jupyter
```

Then configure PySpark to use Jupyter Lab as the driver:

```sh
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='lab'
```

## Start Jupyter Notebooks with Local PySpark

Now you can start Jupyter Lab with PySpark:

```sh
uv run pyspark
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

## What is Apache Spark?

**Apache Spark** is a distributed data processing engine

### When not to use Spark

- Only need to process a small amount of data
- Processing data in real-time (use Spark Streaming for this instead)

### Spark Architecture

- **Spark Cluster**
    - A collection of distributed nodes that run Spark applications
        - **Worker Nodes** — machines that run executors
        - Nodes may also host **Driver** or **Cluster Manager**
    - Contains a *Cluster Manager* (e.g. Standalone, YARN, K8s)
    - Accepts 1 or more *Spark Applications* to run

- **Spark Cluster Manager**
    - Allocates resources across Spark applications
    - Types: Spark Standalone, YARN, Kubernetes
    - In Spark Standalone:
        - A special node runs a **Master Process** (sometimes called a *Master Node*)
        - The Master daemon manages the **Worker Processes**

- **Spark Application**
    - Reads one or more datasets, processes them, and writes one or more output datasets
    - Submitted to a Spark Cluster
    - When submitted:
        - A **Driver process** is started (on a client machine or cluster node)
        - The *Driver* requests **Executors** from the *Cluster Manager*
        - The *Driver* orchestrates task execution on the *Executors*
    - Each application has its own Driver and its own set of *Executors*

- **Spark Driver (Process)**
    - Runs user code and creates a DAG of transformations
    - Requests *Executors* from the *Cluster Manager*
    - Delegates tasks and coordinates results
    - Lives for the duration of a Spark *application*

- **Spark Worker Node**
    - A physical machine in the Spark cluster
    - Runs the **Worker Process**
    - Hosts **Executor processes** for one or more applications

- **Spark Worker Process**
    - A daemon process running on a Worker Node
    - Registers with the Cluster Manager
    - Manages the launching and monitoring of **Executors** on its node

- **Spark Executor**
    - A JVM process launched by the Worker Process
    - Runs on a Worker Node
    - Performs tasks assigned by the Driver
    - Caches data, performs shuffles, and returns results
    - Is dedicated to one Spark Application only

```txt
                    +----------------------------+
                    |      Cluster Manager       |
                    |  (Standalone / YARN / K8s) |
                    +-------------+--------------+
                                  |
                Registers workers & allocates resources
                                  |
                                  v
               +--------------------------------------+
               |          Spark Driver Process        |
               |  (Runs main app code, builds DAG)    |
               +------------------+-------------------+
                                  |
        ------------------------------------------------------
        |                         |                          |
        v                         v                          v

+-------------------+   +-------------------+    +-------------------+
| Worker Node 1 |  | Worker Node 2 |  | Worker Node 3 |
| ------------- ||-------------------|    |-------------------|
| Worker Process    |   | Worker Process    |    | Worker Process    |
| (Daemon) |  | (Daemon) |  | (Daemon) |
| -------- ||-------------------|    |-------------------|
| +---------------+ |   | +---------------+ |    | +---------------+ |
| | Executor 1    | |   | | Executor 2    | |    | | Executor 3    | |
| | (App-specific)| |   | | (App-specific)| |    | | (App-specific)| |
| +---------------+ |   | +---------------+ |    | +---------------+ |
+-------------------+   +-------------------+    +-------------------+
```

### Spark Execution

Two modes of execution
- **Cluster mode**: Multiple machines / prod environment
- **Local mode**: Single machine for testing / development