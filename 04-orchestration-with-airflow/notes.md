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

## Defining DAGs & Tasks

```python
from datetime import datetime
from airflow.decorators import dag, task

# 1. Define the DAG using the @dag decorator.
@dag(
    schedule_interval='@daily',  # How often to run (cron, timedelta, or preset like '@daily')
    start_date=datetime(2024, 1, 1),  # First run date
    catchup=False,  # Don't run for past dates
    tags=['example'],
)
def my_taskflow_dag():
    # 2. Define tasks using @task decorator
    @task
    def extract():
        # Simulate extracting data
        return {"value": 42}

    @task
    def transform(data):
        # Simulate transforming data
        return data["value"] * 2

    @task
    def load(result):
        # Simulate loading data
        print(f"Final result: {result}")

    # 3. Set up dependencies using Python function calls
    data = extract()
    result = transform(data)
    load(result)

# 4. Instantiate the DAG
my_dag = my_taskflow_dag()
```

## Error Handling

### Retrying / Rerunning Tasks

You can use `retries`, `retry_delay` and `execution_timeout` properties in the task decorator to dictate how a task will behave if it fails.

Note if the `execution_timeout` is met, the task will fail.

```python
from airflow.decorators import dag, task
from datetime import datetime, timedelta

@dag(
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
)
def retry_timeout_example():
    @task(
        retries=3,  # Number of times to retry on failure
        retry_delay=timedelta(minutes=10),  # Wait time between retries
        execution_timeout=timedelta(minutes=5),  # Max time for task to run
    )
    def unstable_task():
        # Simulate flaky behavior
        import random
        if random.random() < 0.7:
            raise Exception("Random failure!")
        return "Success!"

    unstable_task()

dag = retry_timeout_example()
```

If you want to set global defaults (which apply to all tasks in the DAG), use `default_args`

```python
default_args = {
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=3),
}

@dag(
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
)
def global_default_args_example():
    @task
    def t1():
        print("This task inherits defaults.")

    t1()

dag = global_default_args_example()
```

### Running callbacks after task failure

You can specify a custom callback function to trigger when a task fails using the `on_failure_callback` property in the tas decorator.

This is useful for sending notifications, logging, or taking remedial action.

```python
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

# 1. Define your failure callback function (must accept context argument)
def notify_on_failure(context):
    # This context dict contains information about the task instance, DAG, etc.
    task_id = context["task"].task_id
    logging.error(f"TASK FAILED: {task_id}")
    # For example, you could send an alert email, Slack message, etc.

@dag(
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    tags=['example'],
)
def dag_with_failure_callback():
    @task(
        retries=1,
        retry_delay=timedelta(seconds=5),
        on_failure_callback=notify_on_failure,  # <-- set your callback here!
    )
    def always_fail():
        raise Exception("This task fails on purpose.")

    always_fail()

dag = dag_with_failure_callback()
```

You can also use the `default_args` to apply the callback to all tasks in the dag.

```python
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

# 1. Define your failure callback function (must accept context argument)
def notify_on_failure(context):
    # This context dict contains information about the task instance, DAG, etc.
    task_id = context["task"].task_id
    logging.error(f"TASK FAILED: {task_id}")
    # For example, you could send an alert email, Slack message, etc.

# 2. Use default_args to define on_failure_callback
default_args = {
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'on_failure_callback': notify_on_failure,  # <--- GLOBAL failure callback
}

@dag(
    default_args=default_args,        # Pass default_args with the callback
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    tags=['example'],
)
def dag_with_failure_callback():
    @task
    def succeed():
        print("All good!")

    @task
    def fail():
        raise Exception("This one fails")

    succeed()
    fail()

dag = dag_with_failure_callback()
```

**NOTE** You can also use the `on_failure_callback` property in the `@dag` decorator instead of `default_args`

```python
@dag(
    on_failure_callback=notify_on_failure,  # Airflow 2.6+
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
)
def global_failure_callback_example():
    # tasks here...
    pass
```

## Resources / References
- [Airflow Docs - Architecture Overview](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/overview.html)