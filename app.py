import json
import logging
import os
from json import JSONDecodeError
from typing import Dict, Any, Union
from urllib.parse import urlparse

import boto3
import requests
from botocore.client import BaseClient
from botocore.exceptions import ClientError
from chalice import Chalice
from chalice.app import S3Event, SQSEvent, Response, BadRequestError
from requests import HTTPError

from chalicelib.entities.cloud_convert import JobStatusEvent
from chalicelib.entities.engine_config import ConversionConfig, MurfAIConfig, PlayHTConfig
from chalicelib.entities.messages import ConversionJob, ConversionJobConfig, DownloadTask
from chalicelib.entities.murf_ai import SynthesizeSpeechResponse
from chalicelib.entities.play_ht import ConversionJobCreatedResponse, ConversionJobStatusResponse
from chalicelib.entities.secrets import Secrets
from chalicelib.utils.request_templates import cloud_convert_create_webhook, cloud_convert_create_job
from chalicelib.utils.responder import Responder

logger = logging.getLogger()
logger.setLevel(logging.INFO)

app = Chalice(app_name=os.environ.get("APP_NAME"))

STAGE = os.environ.get("STAGE", "test")
STATUS_QUEUE_NAME = os.environ.get("STATUS_POLLER_QUEUE_URL").split("/")[-1]
DOWNLOADER_QUEUE_NAME = os.environ.get("DOWNLOADER_QUEUE_URL").split("/")[-1]
CLOUD_CONVERT_WEBHOOK_PATH = "/v1/webhooks/cloud_convert"

_SSM_CLIENT = None
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


@app.on_s3_event(bucket=os.environ.get("INPUT_BUCKET_NAME"), events=["s3:ObjectCreated:*"], suffix=".mp4")
def on_video_input_file(event: S3Event) -> Dict[str, str]:
    """
    AWS Lambda function which gets triggered by the S3 event that a .mp4 file has been put into a bucket. The function
    will call a third party API to initiate a task that separates the audio track from the video file. The task will
    run asynchronously and notify another Lambda function when it completes.
    :param event: the S3 event
    :return: Dict
    """
    logger.info(f"Received event: {event.to_dict()}")

    try:
        secrets = _read_secrets()
        service_url = os.environ.get("SERVICE_BASE_URL") or "http://localhost"
        _invoke_cloud_convert_ensure_webhook(service_url=service_url, secrets=secrets)

        job_id = _invoke_cloud_convert_create_job(source_bucket=event.bucket, source_video_file=event.key, secrets=secrets)
        logger.info(f"Created cloud convert job {job_id}")

        return {"result": "success"}
    except ReportableError as e:
        _report_error(url=os.environ["WEBHOOK_URL"], error=e)

    return {"result": "failure"}


@app.route(CLOUD_CONVERT_WEBHOOK_PATH, methods=["POST"])
def cloud_convert_job_finished() -> Response:
    request = app.current_request
    logger.info(f"POST {CLOUD_CONVERT_WEBHOOK_PATH}. Body: {request.raw_body}, Headers: {request.headers}")

    try:
        try:
            job_status = JobStatusEvent.from_dict(request.json_body)
        except (BadRequestError, LookupError, AttributeError, JSONDecodeError) as e:
            logger.warning(f"Request isn't valid JSON denoting a JobStatusEvent: {request.raw_body}")
            raise ReportableError(message="Unable to parse cloud convert response.", context={"body": request.raw_body})

        result_files = []
        for task in [job_task for job_task in job_status.job.tasks if job_task.operation == "export/url"]:
            if task.result and task.result.files:
                for file in task.result.files:
                    result_files.append(file)

        if job_status.event == "job.failed":
            raise ReportableError(
                message="Cloud convert job failed with an error.", context={"job_id": job_status.job.id}
            )

        else:
            logger.info(f"Creating DownloadTasks for {len(result_files)} job result file(s).")
            for result_file in result_files:
                file_name = result_file.filename
                if file_name.endswith(".mp4"):
                    logger.info("Inserting -noaudio into .mp4 file.")
                    name, ext = os.path.splitext(file_name)
                    file_name = f"{name}-noaudio{ext}"

                download_task = DownloadTask(
                    destination_bucket=os.environ["INPUT_BUCKET_NAME"],
                    destination_key=f"converted/{file_name}",
                    job_result_file=result_file
                )
                _notify_downloader(queue_url=os.environ["DOWNLOADER_QUEUE_URL"], task=download_task)
    except ReportableError as e:
        _report_error(url=os.environ["WEBHOOK_URL"], error=e)

    # always respond success upstream
    return Responder.respond(payload={})


@app.on_s3_event(bucket=os.environ.get("INPUT_BUCKET_NAME"), events=["s3:ObjectCreated:*"], suffix=".txt")
def on_text_input_file(event: S3Event) -> Dict[str, str]:
    """
    AWS Lambda function which gets triggered by an S3 event when a .txt file is put into a bucket. The function will
    look up an additional config file in the same location and start the flow for generating a speech file.
    :param event: the S3 event
    :return: Dict
    """
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

            logger.info(f"Extracting filename from speech file url: {synthesize_speech_response.audio_file}")
            destination_filename = os.path.basename(urlparse(synthesize_speech_response.audio_file).path)
            download_task = DownloadTask(
                destination_bucket=event.bucket,
                destination_key=destination_filename,
                speech_synthesized_response=synthesize_speech_response
            )
            _notify_downloader(queue_url=os.environ["DOWNLOADER_QUEUE_URL"], task=download_task)

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


@app.on_sqs_message(queue=STATUS_QUEUE_NAME, batch_size=1)
def on_conversion_job_message(event: SQSEvent) -> None:
    """
    AWS Lambda function which is triggered by an SQS message denoting a speech conversion job. The function will check
    if the job has been finished in which case it will send a message to the downloader. Otherwise, it will end with an
    exception, forcing the same message to be redelivered and processed a few seconds later.
    :param event: a SQSEvent
    :return: None
    """
    logger.info(f"Received event: {event.to_dict()}")
    try:
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

                        logger.info(f"Extracting filename from speech file url: {response.audio_file}")
                        destination_filename = os.path.basename(urlparse(response.audio_file).path)
                        download_task = DownloadTask(
                            destination_bucket=job.config.bucket,
                            destination_key=destination_filename,
                            speech_synthesized_response=response
                        )
                        _notify_downloader(queue_url=os.environ["DOWNLOADER_QUEUE_URL"], task=download_task)
                else:
                    logger.info(f"Conversion job {job.job_id} has not finished.")
                    raise JobNotFinishedError(message=f"Conversion job {job.job_id} not done.")

    except ReportableError as e:
        _report_error(url=os.environ["WEBHOOK_URL"], error=e)


@app.on_sqs_message(queue=DOWNLOADER_QUEUE_NAME, batch_size=1)
def on_download_message(event: SQSEvent) -> None:
    """
    AWS Lambda function which is triggered by an SQS message denoting a download task. The function will attempt to
    download the file based on the instructions in the specified event. Finally, it will send a notification on the
    configured webhook.
    :param event: a SQSEvent
    :return: None
    """
    logger.info(f"Received event: {event.to_dict()}")
    try:
        for record in event:
            logger.info(f"Converting record ({record.body}) into DownloadTask.")
            try:
                task = DownloadTask.from_json(record.body)
            except Exception:
                logger.exception("Incoming SQS message payload does not map to a DownloadTask.")
                raise ReportableError(
                    message="Unable to parse SQS message.",
                    context={"body": record.body}
                )
            else:
                s3_client = get_s3_client()
                _download_file(s3_client=s3_client, task=task)

                _report_download_success(
                    url=os.environ["WEBHOOK_URL"],
                    download_bucket=task.destination_bucket,
                    download_key=task.destination_key
                )
    except ReportableError as e:
        _report_error(url=os.environ["WEBHOOK_URL"], error=e)


def _download_file(s3_client: BaseClient, task: DownloadTask) -> None:
    """
    Downloads the file from the URL wrapped in the given DownloadTask straight into S3. Utilizes the streaming and
    multipart uploads for proper handling of very large files.
    :param s3_client: a client to be used with S3
    :param task: a DownloadTask
    :return: None
    """
    bucket_name = task.destination_bucket
    object_key = task.destination_key

    if task.speech_synthesized_response:
        file_url = task.speech_synthesized_response.audio_file
    elif task.job_result_file:
        file_url = task.job_result_file.url
    else:
        logger.warning("DownloadTask is empty.")
        raise ReportableError(
            message="DownloadTask must wrap at least one downloadable file.",
            context=task.to_dict()
        )

    logger.info(f"About to store download in S3. Bucket: {bucket_name}, Key: {object_key}, Url: {file_url}")

    buffer_size = 1024 * 1024  # 1 MB
    buffer = bytearray(buffer_size)

    try:
        logger.info("Initialising new multipart upload")
        upload_id = s3_client.create_multipart_upload(Bucket=bucket_name, Key=object_key)["UploadId"]
    except Exception:
        logger.exception("Unable to initialise multipart upload.")
        raise ReportableError(
            message="Unable to initialise multipart upload for converted speech file.",
            context=task.to_dict()
        )

    logger.info(f"Using upload_id: {upload_id} and buffer size: {buffer_size}")
    parts = list()

    try:
        logger.info(f"About to GET {file_url}")
        with requests.get(file_url, stream=True) as response:
            response.raise_for_status()
            logger.info("Starting to stream response ..")

            for count, chunk in enumerate(response.iter_content(chunk_size=buffer_size)):
                if chunk:
                    buffer[:len(chunk)] = chunk

                    part_number = len(parts) + 1
                    logger.info(f"Uploading part {part_number}")

                    upload_response = s3_client.upload_part(
                        Bucket=bucket_name,
                        Key=object_key,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=bytes(buffer[:len(chunk)])
                    )

                    parts.append({
                        "PartNumber": part_number,
                        "ETag": upload_response["ETag"]
                    })
    except HTTPError:
        logger.exception(f"Failed fetching file to download under {file_url}")
        raise ReportableError(
            message="Unable to fetch file for downloading.",
            context=task.to_dict()
        )
    except Exception:
        logger.exception("Unable to finish multipart upload. Aborting.")
        s3_client.abort_multipart_upload(
            Bucket=bucket_name, Key=object_key, UploadId=upload_id
        )
        raise ReportableError(
            message="Unable to finish multipart S3 upload and store converted speech file.",
            context=task.to_dict()
        )
    else:
        try:
            logger.info(f"Completing multipart upload after {len(parts)} part(s) ..")
            s3_client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=object_key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts}
            )
        except Exception:
            logger.exception("Unable to complete multipart upload.")
            raise ReportableError(
                message="Unable to complete multipart upload for converted speech file.",
                context=task.to_dict()
            )


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


def _invoke_cloud_convert_ensure_webhook(service_url: str, secrets: Secrets) -> None:
    list_webhooks_url = f"{os.environ['CLOUD_CONVERT_API_URL']}/v2/users/me/webhooks"
    headers = {
        "AUTHORIZATION": f"Bearer {secrets.cloud_conversion_api_key}",
    }

    logger.info(f"About to GET to {list_webhooks_url}")
    try:
        response = requests.get(url=list_webhooks_url, headers=headers)
        response.raise_for_status()
    except HTTPError:
        logger.exception("Failed to list cloud convert webhooks.")
        raise ReportableError(
            message="Unable to list webhooks in cloud convert",
            context={"url": list_webhooks_url}
        )
    else:
        logger.info(f"Response: {response.text}")
        try:
            webhooks = response.json().get("data", [])
            logger.info(f"Looking for existing cloud convert webhook in: {webhooks}")

            def cloud_convert_webhook(webhook_def: Dict[str, Any]) -> bool:
                return webhook_def.get("url", "").endswith(CLOUD_CONVERT_WEBHOOK_PATH)

            existing_webhook = next(filter(cloud_convert_webhook, webhooks), None)
            if existing_webhook:
                logger.info("Cloud convert webhook exists.")
                return None
        except Exception:
            logger.exception("Something failed checking if cloud convert webhook exists.")
            raise ReportableError(
                message="Unable to parse conversion job created response from play.ht",
                context={"response": response.text}
            )

        logger.info("Cloud convert webhook not found. Creating.")

        headers = {
            "AUTHORIZATION": f"Bearer {secrets.cloud_conversion_api_key}",
            "Content-Type": "application/json",
        }

        data = cloud_convert_create_webhook(url=service_url + CLOUD_CONVERT_WEBHOOK_PATH)

        create_webhook_url = f"{os.environ['CLOUD_CONVERT_API_URL']}/v2/webhooks"
        logger.info(f"About to POST to {create_webhook_url}, data: {data}")

        try:
            response = requests.post(url=create_webhook_url, json=data, headers=headers)
            response.raise_for_status()
        except HTTPError:
            logger.exception("Failed to create cloud convert webhook.")
            raise ReportableError(
                message="Failed to create cloud convert webhook.",
                context={"data": data}
            )


def _invoke_cloud_convert_create_job(source_bucket: str, source_video_file: str, secrets: Secrets) -> str:
    jobs_url = f"{os.environ['CLOUD_CONVERT_API_URL']}/v2/jobs"
    headers = {
        "AUTHORIZATION": f"Bearer {secrets.cloud_conversion_api_key}",
        "Content-Type": "application/json",
    }

    data = cloud_convert_create_job(url=f"https://{source_bucket}.s3.amazonaws.com/{source_video_file}")

    logger.info(f"About to POST to {jobs_url}")
    try:
        response = requests.post(url=jobs_url, json=data, headers=headers)
        response.raise_for_status()
    except HTTPError:
        logger.exception("Failed to create cloud convert job.")
        raise ReportableError(
            message="Failed to start converting input using cloud convert.",
            context={"data": data}
        )
    else:
        logger.info(f"Response: {response.text}")
        response_data = response.json().get("data", {})
        return response_data.get("id", "undefined")


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


def _notify_downloader(queue_url: str, task: DownloadTask) -> None:
    sqs = get_sqs_client()
    logger.info(f"About to send message to SQS at {queue_url}")
    try:
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=task.to_json()
        )
    except Exception:
        logger.exception(f"Unable to notify SQS about download task. (url: {queue_url}, message: {task})")
        raise ReportableError(
            message="Unable to notify SQS about download task.",
            context={"message": task.to_dict()}
        )


def _report_download_success(url: str, download_bucket: str, download_key: str) -> None:
    headers = {
        "Content-Type": "application/json"
    }

    data = json.dumps({"bucket": download_bucket, "key": download_key})
    logger.info(f"About to POST to {url}. data: {data}")
    try:
        response = requests.post(url, json=data, headers=headers)
        response.raise_for_status()
    except HTTPError:
        logger.exception("Failed to notify webhook about downloaded speech file.")
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


def _read_secrets() -> Secrets:
    client = get_ssm_client()

    key_name = f'/aws/reference/secretsmanager/{os.environ.get("SECRETS_MANAGER_KEY_NAME", "undefined")}'
    logger.info(f"Getting Secrets from SSM parameter named '{key_name}'")

    try:
        parameter = client.get_parameter(Name=key_name, WithDecryption=True)
        secrets_json = parameter.get("Parameter", {}).get("Value")
    except ClientError as e:
        logger.exception(
            "Unable to get SSM parameter: %s. %s"
            % (key_name, e.response.get("Error", {}).get("Message"))
        )
        raise ReportableError(
            message="Something went wrong fetching SSM config.", context={"parameter": key_name})

    try:
        return Secrets.from_json(secrets_json)
    except Exception:
        logger.exception("Supplied json string does not map to a Secrets.")
        raise ReportableError(
            message="Unable to parse secrets config.",
            context={"parameter": key_name}
        )


def get_ssm_client() -> BaseClient:
    global _SSM_CLIENT
    if _SSM_CLIENT is None:
        _SSM_CLIENT = boto3.client("ssm")
    return _SSM_CLIENT


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
