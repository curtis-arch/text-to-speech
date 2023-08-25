import json
import logging
import os
from typing import Dict, Any, Union

import boto3
import requests
from botocore.client import BaseClient
from botocore.exceptions import BotoCoreError
from chalice import Chalice
from chalice.app import S3Event
from requests import HTTPError

from chalicelib.entities.engine_config import ConversionConfig, MurfAIConfig
from chalicelib.entities.murf_ai import SynthesizeSpeechResponse

logger = logging.getLogger()
logger.setLevel(logging.INFO)

app = Chalice(app_name=os.environ.get("APP_NAME"))

STAGE = os.environ.get("STAGE", "dev")

_S3_CLIENT = None

logger.info(f"Running in stage: {STAGE}")


class ReportableError(Exception):
    def __init__(self: "ReportableError", message: str, context: Dict[str, Union[str, Dict[str, Any]]]) -> None:
        self.message = message
        self.context = context
        super().__init__(self.message)


@app.on_s3_event(bucket=os.environ.get("INPUT_BUCKET_NAME"), events=["s3:ObjectCreated:*"], suffix=".txt")
def on_text_input_file(event: S3Event) -> Dict[str, str]:
    logger.info(f"Received event: {event.to_dict()}")

    try:
        config = _conversion_config(event)
        logger.info(f"Found the following conversion config: {config.to_dict()}")

        text_content = _read_file_content(bucket_name=event.bucket, object_key=event.key)

        if config.murf_config is not None:
            synthesize_speech_response = _invoke_murfai(text_content=text_content, config=config.murf_config, api_key=config.token)
            _report_success(url=os.environ["WEBHOOK_URL"], response=synthesize_speech_response)
        else:
            pass
    except ReportableError as e:
        _report_error(url=os.environ["WEBHOOK_URL"], error=e)

    return {"result": "success"}


def _invoke_murfai(text_content: str, config: MurfAIConfig, api_key: str) -> SynthesizeSpeechResponse:
    url = "https://api.murf.ai/v1/speech/generate-with-key"
    headers = {
        "Content-Type": "application/json",
        "api-key": api_key,
        "Accept": "application/json",
    }

    data = {**config.to_dict(), "text": text_content}

    logger.info(f"About to POST to {url}. data: {data}")
    try:
        response = requests.post(url, data=data, headers=headers)
        response.raise_for_status()
    except HTTPError as err:
        logger.exception("Failed to generate speech using murf.ai.")
        if err.response is not None:
            if err.response.text:
                error_response = json.loads(err.response.text)
                raise ReportableError(
                    message="Something went wrong generating speech using murf.ai.",
                    context={**data, "error_message": error_response.get("errorMessage"), "error_status": error_response.get("errorCode")}
                )
            else:
                raise ReportableError(
                    message="Something went wrong generating speech using murf.ai.",
                    context={**data, "error_status": err.response.status_code}
                )
        else:
            raise ReportableError(
                message="Something went wrong generating speech using murf.ai.",
                context={"data": data}
            )
    else:
        logger.info(f"Response: {response.text}")
        try:
            return SynthesizeSpeechResponse.from_dict(response.json())
        except Exception:
            logger.exception("Response does not map to a SynthesizeSpeechResponse.")
            raise ReportableError(
                message="Unable to parse synthesize speech response from murf.ai.",
                context={"response": response.text}
            )


def _report_success(url: str, response: SynthesizeSpeechResponse) -> None:
    headers = {
        "Content-Type": "application/json"
    }

    data = response.to_json()
    logger.info(f"About to POST to {url}. data: {data}")
    try:
        response = requests.post(url, json=data, headers=headers)
        response.raise_for_status()
    except HTTPError:
        logger.exception("Failed to notify webhook about generated speech file.")
    else:
        logger.info(f"Response: {response.text}")


def _report_error(url: str, error: ReportableError) -> None:
    headers = {
        "Content-Type": "application/json"
    }

    data = json.dumps({
        "message": error.message,
        "context": error.context
    })
    logger.info(f"About to POST to {url}. data: {data}")
    try:
        response = requests.post(url, json=data, headers=headers)
        response.raise_for_status()
    except HTTPError:
        logger.exception("Failed to notify webhook about processing errors.")
    else:
        logger.info(f"Response: {response.text}")


def _conversion_config(event: S3Event) -> ConversionConfig:
    config_object_key = os.path.splitext(event.key)[0] + ".json"
    config_content = _read_file_content(bucket_name=event.bucket, object_key=config_object_key)
    try:
        return ConversionConfig.from_json(config_content)
    except Exception:
        logger.exception("Supplied file content does not map to a ConversionConfig.")
        raise ReportableError(
            message="Unable to parse config.",
            context={"bucket_name": event.bucket, "key": config_object_key, "content": config_content}
        )


def _read_file_content(bucket_name: str, object_key: str) -> str:
    try:
        s3_client = get_s3_client()

        logger.info(f"About to fetch object: {object_key} from bucket {bucket_name}")
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        return response["Body"].read().decode("utf-8")
    except UnicodeDecodeError:
        raise ReportableError(
            message="Unable to decode using UTF-8 charset.", context={"bucket": bucket_name, "key": object_key})
    except BotoCoreError:
        logger.exception("Unable to fetch from S3.")
        raise ReportableError(
            message="Something went wrong fetching file from S3.", context={"bucket": bucket_name, "key": object_key})


def get_s3_client() -> BaseClient:
    global _S3_CLIENT
    if _S3_CLIENT is None:
        _S3_CLIENT = boto3.client("s3")
    return _S3_CLIENT
