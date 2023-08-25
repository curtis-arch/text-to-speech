# text-to-speech (POC)

This repository contains code for two AWS Lambda functions which integrate both
murf.ai and play.ht and make them available from AWS API Gateway.

## Prerequisites

Make sure your OS has Python installed. As the next step, install [pyenv](https://github.com/pyenv/pyenv#installation) and
[poetry](https://python-poetry.org/docs/#installation)

Run `pyenv versions` to list all Python versions on your machine, which `pyenv` knows about. Now install the latest
Python 3.10 version since this is the runtime used for this project.

```bash
pyenv install 3.10.12
```

While inside the root of this project run:

```bash
pyenv local 3.10.12
```

Finally, install the [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) and set up a profile
using the AWS credentials mapping to the AWS Account you wish to use for this project.

## Overview

The project uses [AWS Chalice](https://aws.github.io/chalice/) as the framework. Chalice will take care of deploying and removing the
AWS resources for the project (Lambda, API Gateway, IAM). Note that the S3 bucket must be created on the side.

Lets have a look at the config file, which can be found under `.chalice/config.json`.

```json
{
  "version": "2.0",
  "app_name": "...",
  "api_gateway_endpoint_type": "REGIONAL",
  "lambda_memory_size": 2048,
  "lambda_timeout": 300,
  "environment_variables": {
    "APP_NAME": "..."
  },
  "stages": {
    "dev": {
      "api_gateway_stage": "dev",
      "autogen_policy": false,
      "iam_policy_file": "app-policy.json",
      "environment_variables": {
        "STAGE": "dev",
        "FEATURE_TOGGLES": "0",
        "INPUT_BUCKET_NAME": "",
        "WEBHOOK_URL": ""
      }
    }
  }
}
```

The following lines are interesting:

| Config Value       | Description                                                                            |
|--------------------|----------------------------------------------------------------------------------------|
| lambda_memory_size | Sets the memory in MB for the Lambda function.                                         |
| lambda_timeout     | Sets a timeout in seconds after which the Lambda function will stop running.           |
| iam_policy_file    | Denotes a file in the same directory with an IAM policy to be used for the function.   |
| FEATURE_TOGGLES    | A way to turn features on and off.                                                     |
| INPUT_BUCKET_NAME  | The name of the bucket, which will later contain the input .txt and .json files.       |
| WEBHOOK_URL        | A URL that we will use to notify when speech has been synthesized or an error occured. |




## Deployments

Make sure you are standing in the root of this project in your terminal window. Start by installing the Python dependencies:

```bash
poetry install
```

Next, make sure we are using Python 3.10 and not a newer version that AWS Lambda might not support as a runtime yet.

```bash
poetry env list 
```

In case that the active environment user a newer version than Python 3.10 (see suffix in environment name), you need to create another
environment for Python 3.10 and install dependencies again.

```bash
poetry env use python3.10
poetry install
```

When deploying to AWS Lambda, we need a `requirements.txt` file which will contain the dependencies for the project. Lets use `poetry`
to generate that file.

```bash
poetry export -f requirements.txt --output requirements.txt --without-hashes
```

Now that we have the `requirements.txt` file, lets make a deployment. Replace PROFILE_NAME with the name of the profile from your AWS CLI setup earlier.

```bash
poetry run chalice deploy --profile PROFILE_NAME --stage dev
```