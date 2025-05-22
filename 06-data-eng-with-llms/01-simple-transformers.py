%pip install transformers

from huggingface_hub import login

login(token="hf_...")

import torch
from transformers import pipeline

model_name = "mistralai/Mistral-7B-Instruct-v0.3"

generator = pipeline(
    "text-generation",
    model=model_name,
    device_map="cuda", 
    torch_dtype=torch.float16,
    max_new_tokens=20,
    return_full_text=False,
)

import textwrap

def classify_review(review):
    messages = [
        {
            "role": "user",
            "content": textwrap.dedent(f"""
                You are a sentiment classifier.
                Is the following customer review positive or negative?
                Respond with exactly one of the two words: positive, negative.

                Review:
                ```
                {review}
                ```
            """)
        }
    ]
    
    print('----------------------------')
    print(f"{messages[0]['content']}")
    print('----------------------------')

    output = generator(messages)
    generated_text = output[0]["generated_text"]
    return generated_text.strip().lower() 

print(classify_review("This is absolutely delightful!"))

print(classify_review("This was the worst hotel I've ever seen"))