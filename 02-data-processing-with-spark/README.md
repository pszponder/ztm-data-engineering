# Pyspark

## Setup

### Create a Virtual Environment

```sh
uv venv
```

### Install Spark and Jupyter

```sh
uv add pyspark jupyter
```

To check that it was installed correctly, you can run:

```sh
uv run pyspark --version
```

## Using VSCode w/ Spark + Jupyter Notebook

When running a notebook in VSCode, make sure you have the .venv selected as the kernel

## Spark "Hello World"

You can try to run the following Spark code to verify everything is working:

```python
from pyspark.sql import SparkSession

# Initialize Spark Session (kind of like a client)
spark = (
    SparkSession.builder
    .master("local[*]")  # Use all cores locally
    .appName("My Local Spark App")
    .getOrCreate()
)

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

## Clean Up

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

## Reading / Writing Data w/ Spark

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

################################################
############ Create Spark Session ##############
################################################

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Fellowship of the Spark") \
    .getOrCreate()

################################################
################# DEFINE SCHEMA ################
################################################

schema = StructType([
    StructField("name", StringType(), nullable=False),
    StructField("race", StringType(), nullable=False),
    StructField("age", IntegerType(), nullable=False),
    StructField("weapon", StringType(), nullable=True),
])

################################################
################ READING DATA ##################
################################################

# Read from CSV
csv_df = spark.read.csv(
    'test.csv',
    header=True,
    schema=schema,
    quote='"',
    mode='PERMISSIVE'
)

# Read from multiple CSV files
csv_multiple_df = spark.read.csv('test/*.csv', header=True)

# Read from JSON
json_df = spark.read.json('test.json')

# Read from Parquet
parquet_df = spark.read.parquet('test.parquet')

################################################
################# WRITING DATA #################
################################################

# Write CSV with header, overwrite if exists
csv_df.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("output/fellowship_csv")

"""
mode options:
error      - Default: fail if exists
overwrite  - Overwrite existing files
append     - Add rows to existing dataset
ignore     - Do nothing if output exists

'overwrite' is useful in testing or pipelines
where old output can be replaced.

'append' is common in ETL jobs or streaming jobs
that keep adding to logs/tables.

'ignore' is handy in idempotent jobs
where re-processing should not duplicate results.

'error' is the safest default in production pipelines
to avoid accidental data loss.
"""

# Write JSON
csv_df.write \
    .mode("overwrite") \
    .json("output/fellowship_json")

# Write Parquet
csv_df.write \
    .mode("overwrite") \
    .parquet("output/fellowship_parquet")

# Optional: write a single CSV file (coalesce to 1 partition)
csv_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("output/fellowship_csv_single_file")

################################################
################### DONE #######################
################################################

# Stop SparkSession when done
spark.stop()
```

## Spark DataFrames

A DataFrame...
- Represents tabular data
- Is immutable (all operations on a DataFrame return a new DataFrame)

### How to create a DataFrame

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create Spark session
spark = SparkSession.builder.appName("SampleDF").getOrCreate()

# Define sample data as list of tuples
data = [
    ("Frodo", "Hobbit", 50),
    ("Aragorn", "Human", 87),
    ("Legolas", "Elf", 2931)
]

# Define schema
schema = StructType([
    StructField("name", StringType(), True),
    StructField("race", StringType(), True),
    StructField("age", IntegerType(), True)
])

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Show the DataFrame
df.show()
```

### Select Columns from a DataFrame

```python
# Select columns by passing in the column names as strings
# Returns a new DataFrame w/ specified columns
df_sample = df.select(
    'col_a',
    'col_b',
    'col_c',
    # ...
    'col_N',
)

# Can also use the fields of the df
df_sample2 = df.select(
    df.col_a,
    df.col_b,
    df.col_c,
    # ...
    df.col_N,
)
```

### Add a new column to a DataFrame

```python
# Add a new column to the dataset of the existing df
df_new = df.withColumn('new_column', df.col_a * 100)
```

### Remove a column from a DataFrame

```python
df_dropped_col = df.dropColumn('col_name_to_drop')
```

### Filter / Where with DataFrames

```python
df_filtered = df.filter(
    df.col_a == 50
)

# Can also pass in the condition as a string
df_filtered2 = df.filter(
    "col_a == 50 AND col_b > 10"
)

# where is an alias to the filter method
df_filtered3 = df.where(
    df.col_a == 50
)
```

### Conditional Operations

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Step 1: Create Spark session
spark = SparkSession.builder.appName("NoLitExample").getOrCreate()

# Step 2: Define schema
schema = StructType([
    StructField("name", StringType(), nullable=False),
    StructField("race", StringType(), nullable=False),
    StructField("age", IntegerType(), nullable=False)
])

# Step 3: Sample data
data = [
    ("Frodo", "Hobbit", 50),
    ("Gandalf", "Maia", 2019),
    ("Legolas", "Elf", 2931),
    ("Gimli", "Dwarf", 139),
    ("Aragorn", "Human", 87),
    ("Sauron", "Maia", 10000)
]

# Step 4: Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Step 5: Transformations using conditional operations
df_transformed = df.select(
    df.name,
    df.race,
    df.age,

    # Column 1: is_immortal -> true if race is Elf or Maia
    # Using .alias provides this new column with a name
    F.when(df.race.isin("Elf", "Maia"), True)
     .otherwise(False)
     .alias("is_immortal"),

    # Column 2: age_group -> classify based on age thresholds
    F.when(df.age > 1000, "Ancient")
     .when(df.age > 100, "Old")
     .otherwise("Young")
     .alias("age_group")
)

# Step 6: Show results
df_transformed.show()
```

### Aggregating Data

Aggregation refers to performing the following operations:
- Counting
- Summing
- Averaging
- Min/Max
- Grouping data by one or more columns
- etc.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum, min, max, mean

# Start Spark session
spark = SparkSession.builder.appName("Aggregation Example").getOrCreate()

# Sample Data
data = [
    ("Aragorn", "Human", 87),
    ("Legolas", "Elf", 2931),
    ("Gimli", "Dwarf", 140),
    ("Frodo", "Hobbit", 50),
    ("Samwise", "Hobbit", 38),
    ("Gandalf", "Maia", 2019)
]

# Create DataFrame
df = spark.createDataFrame(data, ["name", "race", "age"])
df.show()

# Count all rows
df.count()

# Summary statistics
df.describe().show()

# Aggregate all numeric columns
df.select(
    count("*").alias("total_count"),
    avg("age").alias("average_age"),
    sum("age").alias("sum_age"),
    min("age").alias("min_age"),
    max("age").alias("max_age")
).show()

# Group by race and compute count and average age
df.groupBy("race") \
  .agg(
      count("*").alias("count"),
      avg("age").alias("avg_age"),
      max("age").alias("max_age")
  ).show()

# Filtering after aggregation
df.groupBy("race") \
  .agg(avg("age").alias("avg_age")) \
  .filter(col("avg_age") > 100) \
  .show()

# Multiple column GroupBy
df.groupBy("race", "name") \
  .agg(sum("age").alias("total_age")) \
  .show()
```

### Joining Data