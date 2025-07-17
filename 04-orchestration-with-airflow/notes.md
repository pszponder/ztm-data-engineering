# Airflow

## Airflow's Architecture

### DAG Directory

- Central location (filesystem/directory) containing all DAG files (Python scripts).
- **Scheduler, Executor, and Workers must all have access** to this directory

### Scheduler

- **Monitors all DAGs and their tasks**
- Evaluates task dependencies
- Schedules and triggers tasks at the appropriate time
- Manages retries and task rescheduling upon failure
- Periodically parses DAGs to discover changes
- Delegates task execution to the configured *Executor*

### Web Server

- Provides the Airflow **user interface (UI)** accessible via a browser
- Lets users **inspect, trigger, and monitor DAGs and task runs**
- Enables log viewing, manual task run triggers, task state changes, and administration

### Metadata Database

- **Single source of truth for Airflow’s state and history**
- Stores:
    - DAG definitions and task run history
    - User accounts, roles, and permissions
    - Connection credentials and parameters
    - Serialized DAGs and XCom data (cross-task communication)
- Supported backends: **Postgres (recommended), MySQL, SQLite** (mostly for development).

### Executor

- **Determines how and where tasks are executed (locally, distributed, containers, etc.).**
- Integrates tightly with the Scheduler
- Offloads running of tasks to Workers if configured for distributed execution
- The executor type is set in `airflow.cfg`.
- **Common Executor types:**

    | Executor Type | Description                                                                             |
    | ------------- | --------------------------------------------------------------------------------------- |
    | Sequential    | Executes one task at a time, single-threaded (for testing/debug)                        |
    | Local         | Executes multiple tasks in parallel on the local machine                                |
    | Celery        | Uses Celery and a message broker (e.g., Redis, RabbitMQ) to distribute tasks to workers |
    | Kubernetes    | Creates a new Kubernetes pod for each task for maximum isolation and scalability        |
    | Dask          | Uses Dask distributed computing for executing tasks                                     |

### Additional/Optional Components

- **Workers:** Execute tasks as instructed by the Executor (especially with Celery or Kubernetes Executors).
- **DAG Processor:** Responsible for parsing DAG files and serializing them into the database for efficiency and scalability. May run as a separate process.
- **Triggerer:** Executes asynchronous and deferred tasks using an asyncio event loop (necessary for deferrable operators like `AsyncSensor`).
- **Plugins:** Extend Airflow’s core functionality (custom operators, hooks, etc.)

## Resources / References
- [Airflow Docs - Architecture Overview](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/overview.html)