# Databricks Workspace Setup

This README describes how to create and configure a Databricks workspace on AWS, set up compute, and prepare the environment for running notebooks with GPU acceleration and Hugging Face models.

---

## Prerequisites

- An AWS account with administrator access
- A Hugging Face account with an access token

---

## Step 1: Create a Databricks Workspace

1. Go to [https://databricks.com](https://databricks.com) and sign in
2. Go to **Manage Account** → **Workspaces**.
3. Click **Create Workspace**.
4. You’ll be redirected to an AWS login page. Sign in using your AWS root or IAM credentials.
5. AWS will ask you to create a **CloudFormation stack**. A stack is a group of AWS resources (e.g., EC2, S3) that Databricks uses to manage compute.
6. Acknowledge the required permissions and click **Create stack**.
7. Wait for the stack creation to complete. The workspace will then appear in your Databricks account.

---

## Step 2: Create a Compute Cluster

1. Go to your Databricks workspace.
2. Navigate to **Compute** → **Create Compute**.
3. Choose a **GPU-accelerated instance**.
4. Set the number of Spark workers:
   - **Min workers:** 0
   - **Max workers:** 1
   (This ensures a worker is only created when Spark is actually used.)
5. Create the compute cluster.

---

## Step 3: Create a Notebook

1. In the workspace, go to **Workspace** → **Create** → **Notebook**.
2. Name it something like: `llm-classification`.
3. Select the compute cluster you just created as the execution environment.

---

## Step 4: Configure Hugging Face Token

To download models from Hugging Face, you need an access token.

1. Go to [https://huggingface.co](https://huggingface.co).
2. Click on your **User icon** → **Access Tokens**.
3. Create a new token and copy it.

In your Databricks notebook, run the following:

```python
%pip install transformers

from huggingface_hub import login

login(token="hf_...")
```

> ⚠️ Do **not** hardcode your token in production environments. Use Databricks Secrets instead. This example uses a token directly for simplicity in a demo setting.

---

You're now ready to run notebooks using Spark and Hugging Face models in Databricks.