import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--customer_reviews", required=True, help="Input CSV file path")
    parser.add_argument("--output_path", required=True, help="Output CSV file path")
    args = parser.parse_args()

    spark = SparkSession \
        .builder \
        .appName("CustomerReviews") \
        .getOrCreate()

    # TODO: Read input data
    customer_reviews = None

    customer_reviews = customer_reviews \
        .withColumn("review_score", customer_reviews["review_score"].cast("float"))

    # TODO: Calculate an average review score per listing ID

    # TODO: Write the result to an output path

if __name__ == "__main__":
    main()