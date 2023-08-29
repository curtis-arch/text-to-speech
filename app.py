import json
import logging
import os
from json import JSONDecodeError
from typing import Dict, Any, Union

import boto3
import requests
from botocore.client import BaseClient
from chalice import Chalice
from chalice.app import S3Event, SQSEvent
from requests import HTTPError

from chalicelib.entities.engine_config import ConversionConfig, MurfAIConfig, PlayHTConfig
from chalicelib.entities.messages import ConversionJob, ConversionJobConfig
from chalicelib.entities.murf_ai import SynthesizeSpeechResponse
from chalicelib.entities.play_ht import ConversionJobCreatedResponse, ConversionJobStatusResponse

logger = logging.getLogger()
logger.setLevel(logging.INFO)

app = Chalice(app_name=os.environ.get("APP_NAME"))

STAGE = os.environ.get("STAGE", "test")
QUEUE_NAME = os.environ.get("STATUS_POLLER_QUEUE_URL").split("/")[-1]

_S3_CLIENT = None
_SQS_CLIENT = None

logger.info(f"Running in stage: {STAGE}")


class ReportableError(Exception):
    def __init__(self: "ReportableError", message: str, context: Dict[str, Union[str, Dict[str, Any]]]) -> None:
        self.message = message
        self.context = context
        super().__init__(self.message)


class JobNotFinishedError(Exception):
    def __init__(self: "JobNotFinishedError", message: str) -> None:
        self.message = message
        super().__init__(self.message)


@app.on_s3_event(bucket=os.environ.get("INPUT_BUCKET_NAME"), events=["s3:ObjectCreated:*"], suffix=".txt")
def on_text_input_file(event: S3Event) -> Dict[str, str]:
    logger.info(f"Received event: {event.to_dict()}")

    bucket = event.bucket
    config_object_key = os.path.splitext(event.key)[0] + ".json"

    try:
        config = _conversion_config(bucket=bucket, config_object_key=config_object_key)
        logger.info(f"Found the following conversion config: {config.to_dict()}")

        text_content = _read_file_content(bucket_name=event.bucket, object_key=event.key)

        if config.murf_config is not None:
            synthesize_speech_response = _invoke_murfai(
                text_content=text_content,
                config=config.murf_config,
                api_key=config.api_key
            )

            _report_success(url=os.environ["WEBHOOK_URL"], response=synthesize_speech_response)

            return {"result": "success"}

        elif config.play_config is not None:
            job_created_response = _invoke_playht_create(
                text_content=text_content, config=config.play_config, api_key=config.api_key, user_id=config.user_id
            )

            _notify_status_poller(
                queue_url=os.environ["STATUS_POLLER_QUEUE_URL"],
                payload=ConversionJob(
                    job_id=job_created_response.transcription_id,
                    config=ConversionJobConfig(
                        bucket=event.bucket, config_object_key=config_object_key
                    )
                )
            )

            return {"result": "created"}

        else:
            error = ReportableError(
                message="No engine configured.",
                context={
                    "bucket": event.bucket,
                    "key": event.key,
                    "config": config.to_dict()
                }
            )
            _report_error(url=os.environ["WEBHOOK_URL"], error=error)
    except ReportableError as e:
        _report_error(url=os.environ["WEBHOOK_URL"], error=e)

    return {"result": "failure"}


@app.on_sqs_message(queue=QUEUE_NAME, batch_size=1)
def on_conversion_job_message(event: SQSEvent) -> None:
    logger.info(f"Received event: {event.to_dict()}")
    for record in event:
        logger.info(f"Converting record ({record.body}) into ConversionJob.")
        try:
            job = ConversionJob.from_json(record.body)
        except Exception:
            logger.exception("Incoming SQS message payload does not map to a ConversionJob.")
            raise ReportableError(
                message="Unable to parse SQS message.",
                context={"body": record.body}
            )
        else:
            try:
                config = _conversion_config(bucket=job.config.bucket, config_object_key=job.config.config_object_key)
                logger.info(f"Found the following conversion config: {config.to_dict()}")

                job_status_response = _invoke_playht_status(
                    transcription_id=job.job_id, api_key=config.api_key, user_id=config.user_id
                )

                if job_status_response.converted:
                    if job_status_response.error:
                        logger.info(f"Conversion job {job.job_id} has finished with errors.")
                        error = ReportableError(
                            message=f"Conversion job {job.job_id} failed",
                            context={
                                "error_message": job_status_response.error_message
                            }
                        )
                        _report_error(url=os.environ["WEBHOOK_URL"], error=error)

                    else:
                        logger.info(f"Conversion job {job.job_id} has finished successfully.")
                        response = SynthesizeSpeechResponse(
                            audio_file=job_status_response.audio_url,
                            audio_length_seconds=job_status_response.audio_duration_seconds
                        )
                        _report_success(url=os.environ["WEBHOOK_URL"], response=response)
                else:
                    logger.info(f"Conversion job {job.job_id} has not finished.")
                    raise JobNotFinishedError(message=f"Conversion job {job.job_id} not done.")

            except ReportableError as e:
                _report_error(url=os.environ["WEBHOOK_URL"], error=e)


def _invoke_playht_status(transcription_id: str, api_key: str, user_id: str) -> ConversionJobStatusResponse:
    url = f"https://play.ht/api/v1/articleStatus?transcriptionId={transcription_id}"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-USER-ID": user_id,
        "AUTHORIZATION": api_key,
    }

    logger.info(f"About to GET from {url}")
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except HTTPError:
        logger.exception(f"Failed to get status for transcriptionId {transcription_id} from play.ht")
        raise ReportableError(
            message="Unable to get status of conversion job from play.ht",
            context={"transcription_id": transcription_id}
        )
    else:
        logger.info(f"Response: {response.text}")
        try:
            return ConversionJobStatusResponse.from_dict(response.json())
        except Exception:
            logger.exception("Response does not map to a ConversionJobStatusResponse.")
            raise ReportableError(
                message="Unable to parse conversion job status response from play.ht",
                context={"response": response.text}
            )


def _invoke_playht_create(text_content: str, config: PlayHTConfig, api_key: str, user_id: str) -> ConversionJobCreatedResponse:
    url = "https://play.ht/api/v1/convert"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-USER-ID": user_id,
        "AUTHORIZATION": api_key,
    }

    data = {**config.to_dict(), "content": [text_content]}

    logger.info(f"About to POST to {url}. data: {data}")
    try:
        response = requests.post(url, json=data, headers=headers)
        response.raise_for_status()
    except HTTPError as err:
        logger.exception("Failed to generate speech using play.ht")
        if err.response.status_code == 403:
            raise ReportableError(
                message="The provided play.ht api key's plan does not have access to the requested resource.",
                context={**data, "error_message": err.response.text, "error_status": "403"}
            )
        elif err.response.status_code == 400:
            error_response = json.loads(err.response.text)
            raise ReportableError(
                message="Error while creating conversion job using play.ht",
                context={**data, "error_message": error_response.get("error"), "error_status": "400"}
            )
        else:
            raise ReportableError(
                message="Unknown error using play.ht",
                context={**data, "error_message": "Unspecified", "error_status": "400"}
            )
    else:
        logger.info(f"Response: {response.text}")
        try:
            return ConversionJobCreatedResponse.from_dict(response.json())
        except Exception:
            logger.exception("Response does not map to a ConversionJobCreatedResponse.")
            raise ReportableError(
                message="Unable to parse conversion job created response from play.ht",
                context={"response": response.text}
            )


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
        response = requests.post(url, json=data, headers=headers)
        response.raise_for_status()
    except HTTPError as err:
        logger.exception("Failed to generate speech using murf.ai")
        if err.response is not None:
            logger.info(err.response.text)
            try:
                error_response = json.loads(err.response.text)
                raise ReportableError(
                    message="Something went wrong generating speech using murf.ai.",
                    context={**data, "error_message": error_response.get("errorMessage"), "error_status": error_response.get("errorCode")}
                )
            except JSONDecodeError:
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


def _notify_status_poller(queue_url: str, payload: ConversionJob) -> None:
    sqs = get_sqs_client()
    logger.info(f"About to send message to SQS at {queue_url}")
    try:
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=payload.to_json()
        )
    except Exception:
        logger.exception(f"Unable to notify SQS about conversion job. (url: {queue_url}, message: {payload})")
        raise ReportableError(
            message="Unable to notify SQS about new conversion job.",
            context={"message": payload.to_dict()}
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


def _conversion_config(bucket: str, config_object_key: str) -> ConversionConfig:
    config_content = _read_file_content(bucket_name=bucket, object_key=config_object_key)
    try:
        return ConversionConfig.from_json(config_content)
    except Exception:
        logger.exception("Supplied file content does not map to a ConversionConfig.")
        raise ReportableError(
            message="Unable to parse config.",
            context={"bucket_name": bucket, "key": config_object_key, "content": config_content}
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
    except Exception:
        logger.exception("Unable to fetch from S3.")
        raise ReportableError(
            message="Something went wrong fetching file from S3.", context={"bucket": bucket_name, "key": object_key})


def get_s3_client() -> BaseClient:
    global _S3_CLIENT
    if _S3_CLIENT is None:
        _S3_CLIENT = boto3.client("s3")
    return _S3_CLIENT


def get_sqs_client() -> BaseClient:
    global _SQS_CLIENT
    if _SQS_CLIENT is None:
        _SQS_CLIENT = boto3.client("sqs")
    return _SQS_CLIENT
