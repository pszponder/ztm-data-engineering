# Data Lake

## What is a Data Warehouse?

A **data warehouse** is a centralized system designed for **storing, integrating, and analyzing large volumes of data** from different sources to support business intelligence (BI), reporting, and decision-making.

### 🔑 Key Characteristics of a Data Warehouse

- **Structured Data**
    - Data Warehouses primarily store structured data
    - Modern systems are evolving to also handle semi-structured data
- **Schema-on-write**
    - Schema is enforced **before** data is stored
- **Centralized**
    - Consolidates data from multiple sources (e.g., databases, APIs, logs) into one system.
- **Subject-Oriented**
    - Organized around key business areas like sales, finance, or customer behavior.
- **Time-Variant**
    - Maintains historical data for trend analysis (e.g., monthly sales over years).
- **Non-Volatile**
    - Once data is entered, it isn’t frequently changed or deleted — it’s used for analysis.
- **Optimized for Read**
    - Tuned for complex queries and reporting rather than fast updates/inserts.

### 🏗️ Data Warehouse vs Database

| Aspect         | Database                    | Data Warehouse                      |
| -------------- | --------------------------- | ----------------------------------- |
| Purpose        | Transactional (OLTP)        | Analytical (OLAP)                   |
| Operations     | INSERT, UPDATE, DELETE      | SELECT, JOIN, AGGREGATE             |
| Data Freshness | Real-time or near real-time | Often batch-loaded (daily, hourly)  |
| Design         | Normalized (3NF)            | Denormalized/star/snowflake schemas |

### 🛠️ Components of a Data Warehouse

1. **ETL/ELT Pipeline**

   * **Extract** data from sources
   * **Transform** it into a common format
   * **Load** it into the warehouse
     (e.g., using tools like Apache Airflow, dbt, Talend)

2. **Data Storage**

   * Cloud or on-prem systems optimized for analytics
   * Examples: Amazon Redshift, Google BigQuery, Snowflake, Azure Synapse

3. **Query Engine / BI Layer**

   * Allows analysts to run SQL queries, create dashboards, and generate reports
   * Examples: Looker, Tableau, Power BI

### 📊 Example Use Case

> A retail company wants to analyze customer purchases across hundreds of stores.
> They use ETL to pull data from point-of-sale systems, e-commerce platforms, and marketing tools into a data warehouse.
> Analysts then use SQL to answer questions like:

* "Which products are selling best by region?"
* "How have sales changed year-over-year?"

## What is a Data Lake?

A **data lake** is a centralized storage system designed to hold **vast amounts of raw data**-structured, semi-structured, and unstructured—in its native format until it is needed for processing or analysis.

### Key Characteristics of a Data Lake

- **Stores all data types**
    - Structured (tables), semi-structured (JSON, XML), unstructured (images, logs, videos)
- **Schema-on-read**
    - Data is stored as-is; schema is applied only when the data is read or queried
- **Distributed File System (DFS)**
    - Spread out data across multiple machines (nodes), allowing for *scalable, fault-tolerant* storage and parallel access
    - A separate *metadata* service keeps track of the data on the system and what servers (nodes) store which files.
- **Scalable and cost-effective**
    - Built on low-cost, scalable storage (e.g., Amazon S3, Azure Data Lake Storage)
- **Supports diverse use cases**
    - BI, machine learning, real-time analytics, data exploration
- **Batch or streaming ingest**
    - Can handle high-throughput data ingestion in real time or batches
- **ELT**
    - An *ELT* process is typically used when working with a *Data Lake*
    - Data is extracted from a source and loaded into the *Data Lake*
    - After the data is loaded into the *Data Lake*, it is processed and transformed (ex. using *Apache Spark* or *Apache Flink*)

### 🆚 Data Lake vs Data Warehouse

| Aspect              | Data Lake                             | Data Warehouse                           |
| ------------------- | ------------------------------------- | ---------------------------------------- |
| **Data Types**      | All types (structured + unstructured) | Mostly structured (some semi-structured) |
| **Storage Format**  | Raw files (e.g., JSON, CSV, Parquet)  | Optimized tables in SQL format           |
| **Schema Handling** | Schema-on-read                        | Schema-on-write                          |
| **Performance**     | Slower (for raw data)                 | Faster (for analytics)                   |
| **Cost**            | Lower storage cost                    | Higher compute cost                      |
| **Use Cases**       | Data science, ML, IoT                 | BI, dashboards, reporting                |

### 📁 Example: What Goes Into a Data Lake

| Data Type       | Example                                        |
| --------------- | ---------------------------------------------- |
| Structured      | CSV exports from a CRM system                  |
| Semi-Structured | JSON logs from a web server                    |
| Unstructured    | Video files from surveillance cameras          |
| Binary          | Parquet or Avro for efficient big data storage |

All of these can be dumped into a data lake **without transforming them upfront**.

### 🧠 Why Use a Data Lake?

* You want to **store everything** and decide later how to use it
* You need to support **machine learning and big data analytics**
* You want to **separate storage from compute** (especially with cloud-native architectures)

### 🛠️ Common Technologies

| Category               | Examples                                                 |
| ---------------------- | -------------------------------------------------------- |
| **Storage**            | Amazon S3, Azure Data Lake Storage, Google Cloud Storage |
| **Processing**         | Apache Spark, Presto, Databricks, Flink                  |
| **Querying**           | Amazon Athena, BigQuery, Trino                           |
| **Metadata & Catalog** | Apache Hive, AWS Glue, Apache Atlas                      |

## Data Warehouse vs Data Lake

| Feature        | Data Warehouse                    | Data Lake                                         |
| -------------- | --------------------------------- | ------------------------------------------------- |
| **Data Types** | Structured, some semi-structured  | Structured, semi-structured, unstructured         |
| **Schema**     | Schema-on-write                   | Schema-on-read                                    |
| **Use Case**   | Business intelligence, dashboards | Machine learning, raw storage, advanced analytics |

### Schema-on-Write vs Schema-on-Read

| Feature             | Schema-on-Write                              | Schema-on-Read                                     |
| ------------------- | -------------------------------------------- | -------------------------------------------------- |
| **Definition**      | Schema is enforced **before** data is stored | Schema is applied **when** data is read or queried |
| **Used By**         | Data Warehouses                              | Data Lakes                                         |
| **Data Structure**  | Must match schema to be stored               | Can store anything, apply structure later          |
| **Example Format**  | SQL tables, relational DBs                   | JSON, CSV, Parquet, log files                      |
| **Validation Time** | At write time (strict)                       | At read time (flexible)                            |
| **Performance**     | Faster queries, highly optimized             | Slower queries (may require parsing, validation)   |
| **Flexibility**     | Less flexible \u2014 strict types, format    | Very flexible \u2014 store raw data of any kind    |
