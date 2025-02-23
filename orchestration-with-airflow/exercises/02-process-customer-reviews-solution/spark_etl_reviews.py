import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_file", required=True, help="Input CSV file path")
    parser.add_argument("--output_file", required=True, help="Output CSV file path")
    args = parser.parse_args()

    spark = SparkSession \
        .builder \
        .appName("GuestReviewsETL") \
        .getOrCreate()

    df = spark.read.option("header", True).csv(args.input_file)

    df = df.withColumn("review_score", df["review_score"].cast("float"))
    result = df.groupBy("listing_id").agg(avg("review_score").alias("avg_review_score"))

    result.write.option("header", True).mode("overwrite").csv(args.output_file)

    spark.stop()

if __name__ == "__main__":
    main()