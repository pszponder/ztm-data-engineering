%pip install transformers

from huggingface_hub import login

# Log in to Hugging Face
login(token="hf_...")

import os
import torch
from transformers import AutoModelForCausalLM, AutoTokenizer

model_name = "mistralai/Mistral-7B-Instruct-v0.3"

tokenizer = AutoTokenizer.from_pretrained(model_name)
tokenizer.pad_token = tokenizer.eos_token

model = AutoModelForCausalLM.from_pretrained(
    model_name,
    torch_dtype=torch.float16,
    device_map="auto",
).to("cuda")

model.config.pad_token_id = tokenizer.pad_token_id
model.generation_config.pad_token_id = tokenizer.pad_token_id




def classify_review(review):
    prompt = f"""
    Is the following customer review positive or negative. Write words "postive" or "negative":

    Review to classify:

    ```
    {review}
    ```
    """

    inputs = tokenizer(prompt, return_tensors="pt").to(model.device)

    out_ids = model.generate(
        **inputs,
        max_new_tokens=20
    )

    new_tokens = out_ids[0, inputs["input_ids"].shape[-1]:]

    return tokenizer.decode(new_tokens, skip_special_tokens=True).strip()


print(classify_review("This is absolutely delightful!"))

print(classify_review("This was the worst hotel I've ever seen"))