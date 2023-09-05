# text-to-speech (POC)

This repository contains code for multiple AWS Lambda functions which integrate:
- murf.ai and play.ht to convert txt files into speech
- cloud convert to separate the audio and video track from a video file


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


### Configuration

Let's have a look at the config file, which can be found under `.chalice/config.json`.

```json
{
  "version": "2.0",
  "app_name": "...",
  "api_gateway_endpoint_type": "REGIONAL",
  "environment_variables": {
    "APP_NAME": "..."
  },
  "stages": {
    "test": {
      "api_gateway_stage": "test",
      "autogen_policy": false,
      "iam_policy_file": "app-policy.json",
      "lambda_functions": {
        "on_conversion_job_message": {
          "lambda_memory_size": 512,
          "lambda_timeout": 15
        },
        "on_text_input_file": {
          "lambda_memory_size": 1024,
          "lambda_timeout": 300
        }
      },
      "environment_variables": {
        "STAGE": "test",
        "FEATURE_TOGGLES": "0",
        "SECRETS_MANAGER_KEY_NAME": "",
        "INPUT_BUCKET_NAME": "",
        "SERVICE_BASE_URL": "",
        "CLOUD_CONVERT_API_URL": "",
        "STATUS_POLLER_QUEUE_URL": "",
        "DOWNLOADER_QUEUE_URL": "",        
        "WEBHOOK_URL": ""
      }
    }
  }
}
```

The following lines are interesting:

| Config Value             | Description                                                                                                                 |
|--------------------------|-----------------------------------------------------------------------------------------------------------------------------|
| lambda_memory_size       | Sets the memory in MB for each Lambda function.                                                                             |
| lambda_timeout           | Sets a timeout in seconds after which each Lambda function will stop running.                                               |
| iam_policy_file          | Denotes a file in the same directory with an IAM policy to be used for the function.                                        |
| FEATURE_TOGGLES          | A way to turn features on and off.                                                                                          |
| SECRETS_MANAGER_KEY_NAME | Contains the name of a secret in AWS Secrets Manager which will contain secrets.                                            |
| INPUT_BUCKET_NAME        | The name of the bucket, which will later contain the input .txt and .json files.                                            |
| SERVICE_BASE_URL         | The base URL under which the service can be accessed via API Gateway. This is needed to register webhooks in Cloud Convert. |
| CLOUD_CONVERT_API_URL    | The base URL for the Cloud Convert API.                                                                                     |
| STATUS_POLLER_QUEUE_URL  | The URL to an SQS queue which will be used for polling the status of a conversion job.                                      |
| DOWNLOADER_QUEUE_URL     | The URL to an SQS queue which will be used for downloading files.                                                           |
| WEBHOOK_URL              | A URL that we will use to notify when speech has been synthesized or an error occurred.                                     |

### Generating Speech from Text

The project comes with an integration for multiple text-to-speech engines. To generate a speech file:
- upload the conversion configuration to S3 with a .json extension (step 1)
- upload a second file in the same S3 location but with the .txt extension (step 2)

The conversion configuration allows for the following parameters:

```json
{
  "api_key": "foo",
  "murf_config": {
    "voice_id": "en-UK-hazel",
    "style": "Conversational",
    "rate": 0,
    "pitch": 0,
    "sample_rate": 24000,
    "format": "MP3",
    "channel_type": "STEREO"
  }
}
```

A valid `api_key` for the desired engine must always be specified. Some engines might require additional attributes, e.g. `user_id`. 
Possible values for the murf.ai configuration are documented [here](https://murf.ai/api/docs/api-reference/generate-with-key).

Another engine that is supported is play.ht. Here is an example:

```json
{
  "api_key": "foo",
  "user_id": "bar",
  "play_config": {
    "voice": "en-UK-hazel",
    "global_speed": 110,
    "trim_silence": true,
    "narration_style": "angry"
  }
}
```

### Splitting audio and video tracks

Another AWS Lambda function can be used to split the audio and video track from a video file uploaded into S3.
The function will trigger when a .mp4, .wmv, .mov or .mkv file is uploaded. 

It will fetch the Cloud Convert api key from AWS Secrets Manager, make sure we have webhooks configured in
Cloud Convert and finally create a new job, which will split the audio and video tracks.

When the job finishes - or fails - we will receive a notification from Cloud Convert upon which we will 
download the output files and store them in the `/output` folder in S3. Finally, we will send a notification
on the configured webhook.


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
poetry run chalice deploy --profile PROFILE_NAME --stage test
```

## Basic Testing

The folder `tests/samples` will always contain some dummy test files, that can be sent to S3 for basic testing.
**Important**: Make sure to set a proper `api_key` in the JSON file first.

As always, send the `.json` file first:

```bash
curl -X PUT --upload-file tests/samples/play/play1.json "https://quotient-text2speech-test-input.s3.amazonaws.com/inputz/play1.json"
```

**Important**: Send all files into the `inputz` subfolder!

Now you can send the `.txt` file.

```bash
curl -X PUT --upload-file tests/samples/play/play1.txt "https://quotient-text2speech-test-input.s3.amazonaws.com/inputz/play1.txt"
```
