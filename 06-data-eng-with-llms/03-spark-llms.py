%pip install transformers
%pip install outlines

# -----------------------

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ReviewsClassifier") \
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

def classify(generate_json, review):
    prompt = (
        "Classify the following customer review as positive or negative.\n\n"
        f"Review:\n{review}\n"
    )
    output_json = generate_json(prompt, max_tokens=40)
    return output_json['sentiment']

# ------------------------------------------

from pyspark.sql.functions import udf
from functools import cache
from huggingface_hub import login

@udf("string")
def sentiment_udf(review):

    @cache
    def get_generate_json():
        login(token="hf_...")
        generator = outlines.models.transformers(
            model_name,
            device="cuda",
            model_kwargs={"torch_dtype": torch.float16},
        )
        return outlines.generate.json(generator, schema)
    
    generate_json = get_generate_json()

    return classify(generate_json, review)


result_df = df.withColumn("sentiment", sentiment_udf("review"))
result_df.show(truncate=False)