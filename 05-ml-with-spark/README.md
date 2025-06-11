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

## Set Up Virtual Environment

Create and activate a virtual environment:

```sh
python3 -m venv .
source bin/activate
```

---

## Install Required Packages

Install Jupyter and NumPy (used for numerical operations in machine learning):

```sh
pip install jupyter numpy
```

---

## Configure PySpark to Use Jupyter Notebooks

Set environment variables so PySpark launches in Jupyter Lab:

```sh
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='lab'
```

---

## Launch PySpark with Jupyter

Start the interactive Spark environment in Jupyter Lab:

```sh
pyspark
```

---