import logging
import os
from typing import Dict

import boto3
from botocore.client import BaseClient
from chalice import Chalice
from chalice.app import S3Event

from chalicelib.entities.engine_config import EngineConfig

logger = logging.getLogger()
logger.setLevel(logging.INFO)

app = Chalice(app_name=os.environ.get("APP_NAME"))

REGION = os.environ.get("REGION")
STAGE = os.environ.get("STAGE", "dev")

_S3_CLIENT = None

logger.info(f"Using region: {REGION}, stage: {STAGE}")


@app.on_s3_event(bucket=os.environ.get("INPUT_BUCKET_NAME"), events=["s3:ObjectCreated:*"], suffix=".txt")
def on_text_input_file(event: S3Event) -> Dict[str, str]:
    logger.info(f"Received event: {event.to_dict()}")

    engine_config = _engine_input_config(event)
    logger.info(f"Found the following engine config: {engine_config.to_dict()}")

    text_content = _read_file_content(bucket_name=event.bucket, object_key=event.key)

    if engine_config.engine == "murf.ai":
        pass
    else:
        pass

    return {"result": "success"}


def _engine_input_config(event: S3Event) -> EngineConfig:
    config_object_key = os.path.splitext(event.key)[0] + ".json"
    config_content = _read_file_content(bucket_name=event.bucket, object_key=config_object_key)
    return EngineConfig.from_json(config_content)


def _read_file_content(bucket_name: str, object_key: str) -> str:
    s3_client = get_s3_client()

    logger.info(f"About to fetch object: {object_key} from bucket {bucket_name}")
    response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    return response["Body"].read().decode("utf-8")


def _invoke_murf_ai(text: str, config: EngineConfig) -> None:
    pass


def get_s3_client() -> BaseClient:
    global _S3_CLIENT
    if _S3_CLIENT is None:
        _S3_CLIENT = boto3.client("s3")
    return _S3_CLIENT
