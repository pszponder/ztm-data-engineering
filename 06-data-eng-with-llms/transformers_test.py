import os
import torch
from transformers import AutoModelForCausalLM, AutoTokenizer

# ——————————————————————————————————————————————————————
# (Optional) Limit PyTorch threads for more consistent perf
# os.environ["OMP_NUM_THREADS"] = "8"
# os.environ["MKL_NUM_THREADS"] = "8"
# ——————————————————————————————————————————————————————

# model_name = "mosaicml/mpt-30b"
model_name = "Intel/neural-chat-7b-v3-1"
# model_name = "mistralai/Mixtral-8x7B-Instruct-v0.1"

# 1. Load the tokenizer
tokenizer = AutoTokenizer.from_pretrained(model_name)

# 2. Load the model in FP16 on CPU
model = AutoModelForCausalLM.from_pretrained(
    model_name,
    torch_dtype=torch.float16,    # halve memory footprint
    device_map="cpu",             # force everything onto the CPU
    low_cpu_mem_usage=True,        # reduce peak memory during load
)

review = 'This is absolutely delightful!'

prompt = f"""
Is the following customer review positive or negative. Provide output in the JSON format with the following fields:

* sentiment: "positive" or "negative"

Review to classify:

```
{review}
```
"""
inputs = tokenizer(prompt, return_tensors="pt")

print("Generating output")
out = model.generate(**inputs, max_new_tokens=300)
print(tokenizer.decode(out[0], skip_special_tokens=True))