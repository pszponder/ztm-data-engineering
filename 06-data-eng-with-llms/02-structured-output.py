%pip install outlines

from huggingface_hub import login

login(token="hf_...")

import torch
import json
import outlines

model_name = "mistralai/Mistral-7B-Instruct-v0.3"

generator = outlines.models.transformers(
    model_name,
    device="cuda",
    model_kwargs={
        "torch_dtype": torch.float16,
    }
)

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

generate_json = outlines.generate.json(generator, schema)

def classify_review(review):
    prompt = (
        "Classify the following customer review as positive or negative.\n\n"
        f"Review:\n{review}\n"
    )

    output_json = generate_json(prompt, max_tokens=40)

    return output_json


print(classify_review("This is absolutely delightful!"))

print(classify_review("This was the worst hotel I've ever seen"))