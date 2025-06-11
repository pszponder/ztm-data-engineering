# AWS Setup for Data Lake

This README describes how to configure your AWS account and credentials to enable programmatic access via the AWS CLI. This setup will be used for working with services like Amazon S3 in the data lake section.

---

## Prerequisites

- An [AWS account](https://aws.amazon.com/) (sign-up requires a valid payment method)
- [AWS CLI installed](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html)

---

## Step 1: Sign In to AWS

1. Go to the [AWS Management Console](https://aws.amazon.com/console/).
2. Log in as the **root user** (only for initial setup).
3. Navigate to the **IAM (Identity and Access Management)** service using the search bar.

---

## Step 2: Create an IAM User

1. In the IAM dashboard, go to **Users** → **Create user**.
2. Enter a username, e.g., `rental-website-admin`.
3. Under **Permissions**, select **Attach existing policies directly** and choose **AdministratorAccess** for full access.
   - Alternatively, you can choose more restricted permissions (e.g., S3-only or read-only).
4. Continue through the wizard and create the user.

---

## Step 3: Generate Programmatic Credentials

1. After creating the user, go to the **Security credentials** tab.
2. Under **Access keys**, click **Create access key**.
3. Choose **Command Line Interface (CLI)** as the use case.
4. Click **Create access key**.
5. Copy both the **Access key ID** and **Secret access key**.

> ⚠️ The secret key is shown only once. Store it securely.

---

## Step 4: Set Up Local AWS Credentials

Create the AWS credentials file:

```sh
vim ~/.aws/credentials
```

Paste the following into the file, replacing the placeholders with your actual keys:

```ini
[default]
aws_access_key_id = YOUR_ACCESS_KEY_ID
aws_secret_access_key = YOUR_SECRET_ACCESS_KEY
```

Save and close the file.

---

## Step 5: Test Your Configuration

Run the following command to verify that your credentials are working:

```sh
aws s3 ls
```

You should see a list of accessible S3 buckets (or an empty list if you have none yet).

---

You're now ready to use the AWS CLI to interact with AWS services.