%pip install transformers

from huggingface_hub import login

# Log in to Hugging Face
login(token="hf_...")

import torch
from transformers import AutoModelForCausalLM, AutoTokenizer

model_name = "mistralai/Mistral-7B-Instruct-v0.3"

tokenizer = AutoTokenizer.from_pretrained(model_name)
tokenizer.pad_token = tokenizer.eos_token 
tokenizer.pad_token_id = tokenizer.eos_token_id 

model = AutoModelForCausalLM.from_pretrained(
    model_name,
    torch_dtype=torch.float16,
    device_map="auto",
).to("cuda")


import textwrap

def classify_review(review):
    prompt = f"""
    Is the following customer review positive or negative? Write one word, "positive" or "negative".

    Review to classify:

    ```
    {review}
    ```
    """

    print(textwrap.dedent(
    f"""
    Sending a prompt:

    -------------------------------------
    {prompt}
    -------------------------------------
    """))

    inputs = tokenizer(textwrap.dedent(prompt), return_tensors="pt").to(model.device)

    output = model.generate(
        **inputs,
        max_new_tokens=20,
        pad_token_id=tokenizer.pad_token_id
    )

    output_tokens = output[0]
    new_tokens = output_tokens[inputs["input_ids"].shape[-1]:]
    output_str = tokenizer.decode(new_tokens, skip_special_tokens=True)

    return output_str


print(classify_review("This is absolutely delightful!"))

print(classify_review("This was the worst hotel I've ever seen"))