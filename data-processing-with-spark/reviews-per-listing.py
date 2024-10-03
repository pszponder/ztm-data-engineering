from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Most popular listings") \
    .getOrCreate()

listings = spark.read.csv("data/listings.csv.gz",
    header=True,
    inferSchema=True,
    sep=",",
    quote='"',
    escape='"',
    multiLine=True,
    mode="PERMISSIVE"
)

reviews = spark.read.csv("data/reviews.csv.gz",
    header=True,
    inferSchema=True,
    sep=",",
    quote='"',
    escape='"',
    multiLine=True,
    mode="PERMISSIVE"
)

listings_reviews = listings.join(
    reviews, listings.id == reviews.listing_id, how='inner'
)

reviews_per_listing = listings_reviews \
  .groupBy(listings.id, listings.name) \
  .agg(
    F.count(reviews.id).alias('num_reviews')
  ) \
  .orderBy('num_reviews', ascending=False) \

reviews_per_listing \
  .write \
  .csv('data/output')