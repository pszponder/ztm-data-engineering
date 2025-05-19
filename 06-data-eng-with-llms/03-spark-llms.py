%pip install transformers
%pip install outlines


# -----------------------

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HotelSentiment") \
    .master("local[*]") \
    .getOrCreate()

spark


# ------------------------

reviews = [
    (1, "This is absolutely delightful!"),
    (2, "This was the worst hotel I've ever seen"),
    (3, "Great location but the rooms were dirty."),
    (4, "Staff were friendly and helpful."),
    (5, "Mediocre breakfast, but I'd stay again."),
]
df = spark.createDataFrame(
    reviews,
    ["review_id", "review"]
)
df.show()

# ------------------------

import outlines
import json
import torch
from functools import lru_cache

model_name = "mistralai/Mistral-7B-Instruct-v0.3"

schema = json.dumps({
    "type": "object",
    "properties": {
        "sentiment": {
            "type": "string",
            "enum": ["positive", "negative"]
        }
    },
    "required": ["sentiment"]
})

def classify(classifier, review):
    prompt = (
        "Classify the following customer review as positive or negative.\n\n"
        f"Review:\n{review}\n"
    )
    output_json = classifier(prompt, max_tokens=40)
    return output_json['sentiment']

from pyspark.sql.functions import pandas_udf

@pandas_udf("string")
def sentiment_udf(review_col):

    @lru_cache(maxsize=1)
    def get_classifier():
        login(token="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")
        generator = outlines.models.transformers(
            model_name,
            device="cuda",
            model_kwargs={"torch_dtype": torch.float16},
        )
        return outlines.generate.json(generator, schema)
    
    classifier = get_classifier()

    return review_col.apply(
        lambda review: classify(classifier, review)
    )


result_df = df.withColumn("sentiment", sentiment_udf("review"))
result_df.show(truncate=False)