from pyspark.sql import SparkSession

def main():
    # Create a Spark session
    spark = SparkSession.builder \
        .appName("SimpleSparkScript") \
        .master("local[*]") \
        .getOrCreate()

    # Parallelize a list of numbers from 1 to 100
    numbers = spark.sparkContext.parallelize(range(1, 101))

    # Calculate the sum using a reduce operation
    total = numbers.reduce(lambda a, b: a + b)
    print("Sum of numbers from 1 to 100 is:", total)

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()