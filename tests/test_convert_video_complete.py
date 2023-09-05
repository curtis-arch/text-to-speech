import json
import os
from dataclasses import dataclass
from typing import Optional, Dict, List

from botocore.stub import Stubber, ANY
from chalice.test import Client, HTTPResponse
from dataclasses_json import dataclass_json
from pytest import fixture
from requests_mock import Mocker

from chalicelib.entities.cloud_convert import JobResultFile
from chalicelib.entities.messages import DownloadTask

bucket_name = "another_test_bucket"
webhook_url = "https://www.blackhole.com"
status_queue_url = "https://sqs.us-east-1.amazonaws.com/statusQueue"
download_queue_url = "https://sqs.us-east-1.amazonaws.com/downloadQueue"


@dataclass_json
@dataclass
class AwsStubs:
    sqs: Stubber


@fixture
def test_client() -> Client:
    os.environ["STATUS_POLLER_QUEUE_URL"] = status_queue_url
    os.environ["DOWNLOADER_QUEUE_URL"] = download_queue_url

    import app
    with Client(app.app, stage_name="unit_tests") as client:
        yield client


@fixture
def aws_stubs() -> AwsStubs:
    import app
    sqs_stub = Stubber(app.get_sqs_client())
    with sqs_stub:
        yield AwsStubs(sqs=sqs_stub)


def test_job_finished_successfully(monkeypatch, requests_mock: Mocker, test_client: Client, aws_stubs: AwsStubs):
    monkeypatch.setenv("INPUT_BUCKET_NAME", bucket_name)
    monkeypatch.setenv("WEBHOOK_URL", webhook_url)
    monkeypatch.setenv("STATUS_POLLER_QUEUE_URL", status_queue_url)
    monkeypatch.setenv("SECRETS_MANAGER_KEY_NAME", "key")
    monkeypatch.setenv("CLOUD_CONVERT_API_URL", "https://api.sandbox.cloudconvert.com")

    expected_download_tasks = [
        DownloadTask(destination_bucket=bucket_name, destination_key=f"converted/10minutes-noaudio.mp4",
                     job_result_file=JobResultFile(filename="10minutes.mp4", size=45668057, url="https://storage.cloudconvert.com/tasks-sandbox/bdfb9e8e-ab36-4f0f-af79-46967915bc56/10minutes.mp4?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=cloudconvert-sandbox%2F20230905%2Ffra%2Fs3%2Faws4_request&X-Amz-Date=20230905T063147Z&X-Amz-Expires=86400&X-Amz-Signature=16771b3e8a8f5e6052f14d6f074fa0c661b29326231109119ee3b07859a80351&X-Amz-SignedHeaders=host&response-content-disposition=attachment%3B%20filename%3D%2210minutes.mp4%22&response-content-type=video%2Fmp4&x-id=GetObject")),
        DownloadTask(destination_bucket=bucket_name, destination_key=f"converted/10minutes.mp3",
                     job_result_file=JobResultFile(filename="10minutes.mp3", size=14773666, url="https://storage.cloudconvert.com/tasks-sandbox/6765ddab-e589-48e1-b18a-7722c277d913/10minutes.mp3?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=cloudconvert-sandbox%2F20230905%2Ffra%2Fs3%2Faws4_request&X-Amz-Date=20230905T063018Z&X-Amz-Expires=86400&X-Amz-Signature=01b79f6e634c4c9b98a7d66022e330f9cea8c60646ed51306c38dba656b6330a&X-Amz-SignedHeaders=host&response-content-disposition=attachment%3B%20filename%3D%2210minutes.mp3%22&response-content-type=audio%2Fmpeg&x-id=GetObject"))
    ]

    _setup_stubs(aws_stubs=aws_stubs, download_tasks=expected_download_tasks)
    _setup_mock_success(mocker=requests_mock)

    response = _send_post(test_client=test_client, path="/v1/webhooks/cloud_convert", json_body=request_job_finished)

    assert response.status_code == 200

    aws_stubs.sqs.assert_no_pending_responses()


def test_job_finished_sqs_error(monkeypatch, requests_mock: Mocker, test_client: Client, aws_stubs: AwsStubs):
    monkeypatch.setenv("INPUT_BUCKET_NAME", bucket_name)
    monkeypatch.setenv("WEBHOOK_URL", webhook_url)
    monkeypatch.setenv("STATUS_POLLER_QUEUE_URL", status_queue_url)
    monkeypatch.setenv("SECRETS_MANAGER_KEY_NAME", "key")
    monkeypatch.setenv("CLOUD_CONVERT_API_URL", "https://api.sandbox.cloudconvert.com")

    _setup_stubs_sqs_error(aws_stubs=aws_stubs)
    _setup_mock_success(mocker=requests_mock)

    response = _send_post(test_client=test_client, path="/v1/webhooks/cloud_convert", json_body=request_job_finished)

    assert response.status_code == 200

    aws_stubs.sqs.assert_no_pending_responses()

    assert len(requests_mock.request_history) == 1
    webhook_request = requests_mock.request_history[0]
    assert webhook_request.json() == json.dumps({
        "message": "Unable to notify SQS about download task.",
        "context": {
            "message": {
                "destination_bucket": "another_test_bucket",
                "destination_key": "converted/10minutes-noaudio.mp4",
                "job_result_file": {
                    "filename": "10minutes.mp4",
                    "size": 45668057,
                    "url": "https://storage.cloudconvert.com/tasks-sandbox/bdfb9e8e-ab36-4f0f-af79-46967915bc56/10minutes.mp4?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=cloudconvert-sandbox%2F20230905%2Ffra%2Fs3%2Faws4_request&X-Amz-Date=20230905T063147Z&X-Amz-Expires=86400&X-Amz-Signature=16771b3e8a8f5e6052f14d6f074fa0c661b29326231109119ee3b07859a80351&X-Amz-SignedHeaders=host&response-content-disposition=attachment%3B%20filename%3D%2210minutes.mp4%22&response-content-type=video%2Fmp4&x-id=GetObject"
                }
            }
        }
    })


def test_job_failed(monkeypatch, requests_mock: Mocker, test_client: Client):
    monkeypatch.setenv("INPUT_BUCKET_NAME", bucket_name)
    monkeypatch.setenv("WEBHOOK_URL", webhook_url)
    monkeypatch.setenv("STATUS_POLLER_QUEUE_URL", status_queue_url)
    monkeypatch.setenv("SECRETS_MANAGER_KEY_NAME", "key")
    monkeypatch.setenv("CLOUD_CONVERT_API_URL", "https://api.sandbox.cloudconvert.com")

    _setup_mock_success(mocker=requests_mock)

    response = _send_post(test_client=test_client, path="/v1/webhooks/cloud_convert", json_body=request_job_failed)

    assert response.status_code == 200

    assert len(requests_mock.request_history) == 1
    webhook_request = requests_mock.request_history[0]
    assert webhook_request.json() == json.dumps({
        "message": "Cloud convert job failed with an error.",
        "context": {
            "job_id": "c9d46d98-c3a6-44d5-b03a-247e1c7feae1"
        }
    })


def _setup_mock_success(mocker: Mocker) -> None:
    mocker.post(webhook_url, json={})


def _setup_stubs(aws_stubs: AwsStubs, download_tasks: Optional[List[DownloadTask]] = None) -> None:
    if download_tasks:
        for task in download_tasks:
            aws_stubs.sqs.add_response(
                method='send_message',
                expected_params={
                    'QueueUrl': download_queue_url,
                    'MessageBody': task.to_json()
                },
                service_response={
                    'MessageId': 'test-message-id'
                }
            )


def _setup_stubs_sqs_error(aws_stubs: AwsStubs) -> None:
    aws_stubs.sqs.add_client_error(
        method='send_message',
        service_error_code='InvalidMessageContents',
        service_message='The message contains characters outside the allowed set.',
        http_status_code=400,
        expected_params={
            'QueueUrl': download_queue_url,
            'MessageBody': ANY
        },
    )


def _send_post(
        test_client: Client,
        path: str,
        json_body: str,
        headers: Optional[Dict[str, str]] = None,
) -> HTTPResponse:
    if not headers:
        headers = {"Content-Type": "application/json"}
    return test_client.http.post(path=path, headers=headers, body=json_body)


request_job_finished = '''{
    "event": "job.finished",
    "job": {
        "id": "a69254c0-4b0f-4253-88f2-ea3c2074ab75",
        "tag": null,
        "status": "finished",
        "created_at": "2023-09-05T06:30:03+00:00",
        "started_at": "2023-09-05T06:30:03+00:00",
        "ended_at": "2023-09-05T06:31:47+00:00",
        "tasks": [
            {
                "id": "bdfb9e8e-ab36-4f0f-af79-46967915bc56",
                "name": "export_video_artifact",
                "job_id": "a69254c0-4b0f-4253-88f2-ea3c2074ab75",
                "status": "finished",
                "credits": 0,
                "code": null,
                "message": null,
                "percent": 100,
                "operation": "export\\/url",
                "result": {
                    "files": [
                        {
                            "filename": "10minutes.mp4",
                            "size": 45668057,
                            "url": "https:\\/\\/storage.cloudconvert.com\\/tasks-sandbox\\/bdfb9e8e-ab36-4f0f-af79-46967915bc56\\/10minutes.mp4?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=cloudconvert-sandbox%2F20230905%2Ffra%2Fs3%2Faws4_request&X-Amz-Date=20230905T063147Z&X-Amz-Expires=86400&X-Amz-Signature=16771b3e8a8f5e6052f14d6f074fa0c661b29326231109119ee3b07859a80351&X-Amz-SignedHeaders=host&response-content-disposition=attachment%3B%20filename%3D%2210minutes.mp4%22&response-content-type=video%2Fmp4&x-id=GetObject"
                        }
                    ]
                },
                "created_at": "2023-09-05T06:30:03+00:00",
                "started_at": "2023-09-05T06:31:44+00:00",
                "ended_at": "2023-09-05T06:31:47+00:00",
                "retry_of_task_id": null,
                "copy_of_task_id": null,
                "user_id": 64955942,
                "priority": -10,
                "host_name": "laurence",
                "storage": "ceph-fra-sandbox",
                "region": "eu-central",
                "depends_on_task_ids": [
                    "85aaa0a0-d597-45d2-ab09-2c8aa71cfd6c"
                ],
                "links": {
                    "self": "https:\\/\\/api.sandbox.cloudconvert.com\\/v2\\/tasks\\/bdfb9e8e-ab36-4f0f-af79-46967915bc56"
                }
            },
            {
                "id": "6765ddab-e589-48e1-b18a-7722c277d913",
                "name": "export_audio_artifact",
                "job_id": "a69254c0-4b0f-4253-88f2-ea3c2074ab75",
                "status": "finished",
                "credits": 0,
                "code": null,
                "message": null,
                "percent": 100,
                "operation": "export\\/url",
                "result": {
                    "files": [
                        {
                            "filename": "10minutes.mp3",
                            "size": 14773666,
                            "url": "https:\\/\\/storage.cloudconvert.com\\/tasks-sandbox\\/6765ddab-e589-48e1-b18a-7722c277d913\\/10minutes.mp3?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=cloudconvert-sandbox%2F20230905%2Ffra%2Fs3%2Faws4_request&X-Amz-Date=20230905T063018Z&X-Amz-Expires=86400&X-Amz-Signature=01b79f6e634c4c9b98a7d66022e330f9cea8c60646ed51306c38dba656b6330a&X-Amz-SignedHeaders=host&response-content-disposition=attachment%3B%20filename%3D%2210minutes.mp3%22&response-content-type=audio%2Fmpeg&x-id=GetObject"
                        }
                    ]
                },
                "created_at": "2023-09-05T06:30:03+00:00",
                "started_at": "2023-09-05T06:30:17+00:00",
                "ended_at": "2023-09-05T06:30:18+00:00",
                "retry_of_task_id": null,
                "copy_of_task_id": null,
                "user_id": 64955942,
                "priority": -10,
                "host_name": "laurence",
                "storage": "ceph-fra-sandbox",
                "region": "eu-central",
                "depends_on_task_ids": [
                    "18dd09c0-4157-492d-8921-a3ebc265f4d8"
                ],
                "links": {
                    "self": "https:\\/\\/api.sandbox.cloudconvert.com\\/v2\\/tasks\\/6765ddab-e589-48e1-b18a-7722c277d913"
                }
            },
            {
                "id": "85aaa0a0-d597-45d2-ab09-2c8aa71cfd6c",
                "name": "split_video",
                "job_id": "a69254c0-4b0f-4253-88f2-ea3c2074ab75",
                "status": "finished",
                "credits": 1,
                "code": null,
                "message": null,
                "percent": 100,
                "operation": "convert",
                "engine": "ffmpeg",
                "engine_version": "5.1.2",
                "result": {
                    "files": [
                        {
                            "filename": "10minutes.mp4",
                            "size": 45668057
                        }
                    ]
                },
                "created_at": "2023-09-05T06:30:03+00:00",
                "started_at": "2023-09-05T06:30:09+00:00",
                "ended_at": "2023-09-05T06:31:44+00:00",
                "retry_of_task_id": null,
                "copy_of_task_id": null,
                "user_id": 64955942,
                "priority": -10,
                "host_name": "laurence",
                "storage": null,
                "region": "eu-central",
                "depends_on_task_ids": [
                    "9aab39a1-7e55-4598-a603-16dd652e74a4"
                ],
                "links": {
                    "self": "https:\\/\\/api.sandbox.cloudconvert.com\\/v2\\/tasks\\/85aaa0a0-d597-45d2-ab09-2c8aa71cfd6c"
                }
            },
            {
                "id": "18dd09c0-4157-492d-8921-a3ebc265f4d8",
                "name": "split_audio",
                "job_id": "a69254c0-4b0f-4253-88f2-ea3c2074ab75",
                "status": "finished",
                "credits": 1,
                "code": null,
                "message": null,
                "percent": 100,
                "operation": "convert",
                "engine": "ffmpeg",
                "engine_version": "5.1.2",
                "result": {
                    "files": [
                        {
                            "filename": "10minutes.mp3",
                            "size": 14773666
                        }
                    ]
                },
                "created_at": "2023-09-05T06:30:03+00:00",
                "started_at": "2023-09-05T06:30:09+00:00",
                "ended_at": "2023-09-05T06:30:17+00:00",
                "retry_of_task_id": null,
                "copy_of_task_id": null,
                "user_id": 64955942,
                "priority": -10,
                "host_name": "laurence",
                "storage": null,
                "region": "eu-central",
                "depends_on_task_ids": [
                    "9aab39a1-7e55-4598-a603-16dd652e74a4"
                ],
                "links": {
                    "self": "https:\\/\\/api.sandbox.cloudconvert.com\\/v2\\/tasks\\/18dd09c0-4157-492d-8921-a3ebc265f4d8"
                }
            },
            {
                "id": "9aab39a1-7e55-4598-a603-16dd652e74a4",
                "name": "import_source_from_url",
                "job_id": "a69254c0-4b0f-4253-88f2-ea3c2074ab75",
                "status": "finished",
                "credits": 0,
                "code": null,
                "message": null,
                "percent": 100,
                "operation": "import\\/url",
                "result": {
                    "files": [
                        {
                            "filename": "10minutes.mp4",
                            "md5": "b61d5952dbe6d3e14c25470f1d0f7695"
                        }
                    ]
                },
                "created_at": "2023-09-05T06:30:03+00:00",
                "started_at": "2023-09-05T06:30:03+00:00",
                "ended_at": "2023-09-05T06:30:09+00:00",
                "retry_of_task_id": null,
                "copy_of_task_id": null,
                "user_id": 64955942,
                "priority": -10,
                "host_name": "laurence",
                "storage": null,
                "region": "eu-central",
                "depends_on_task_ids": [],
                "links": {
                    "self": "https:\\/\\/api.sandbox.cloudconvert.com\\/v2\\/tasks\\/9aab39a1-7e55-4598-a603-16dd652e74a4"
                }
            }
        ],
        "links": {
            "self": "https:\\/\\/api.sandbox.cloudconvert.com\\/v2\\/jobs\\/a69254c0-4b0f-4253-88f2-ea3c2074ab75"
        }
    }
}'''

request_job_failed = '''{
  "event": "job.failed",
  "job": {
    "id": "c9d46d98-c3a6-44d5-b03a-247e1c7feae1",
    "tag": null,
    "status": "error",
    "created_at": "2023-09-05T14:54:39+00:00",
    "started_at": "2023-09-05T14:54:39+00:00",
    "ended_at": "2023-09-05T14:54:42+00:00",
    "tasks": [
      {
        "id": "2a1d3c8e-bdd2-4416-901b-43f1d3cfe490",
        "name": "export_artifacts",
        "job_id": "c9d46d98-c3a6-44d5-b03a-247e1c7feae1",
        "status": "error",
        "credits": 0,
        "code": "INPUT_TASK_FAILED",
        "message": "Input task has failed",
        "percent": 100,
        "operation": "export\\/url",
        "result": null,
        "created_at": "2023-09-05T14:54:39+00:00",
        "started_at": "2023-09-05T14:54:42+00:00",
        "ended_at": "2023-09-05T14:54:42+00:00",
        "retry_of_task_id": null,
        "copy_of_task_id": null,
        "user_id": 64955942,
        "priority": -10,
        "host_name": null,
        "storage": null,
        "region": "eu-central",
        "depends_on_task_ids": [
          "a5c9ccfc-9214-485e-807e-b504cf8f152b"
        ],
        "links": {
          "self": "https:\\/\\/api.sandbox.cloudconvert.com\\/v2\\/tasks\\/2a1d3c8e-bdd2-4416-901b-43f1d3cfe490"
        }
      },
      {
        "id": "a5c9ccfc-9214-485e-807e-b504cf8f152b",
        "name": "extract_audio",
        "job_id": "c9d46d98-c3a6-44d5-b03a-247e1c7feae1",
        "status": "error",
        "credits": 0,
        "code": "INPUT_TASK_FAILED",
        "message": "Input task has failed",
        "percent": 100,
        "operation": "convert",
        "engine": null,
        "engine_version": null,
        "result": null,
        "created_at": "2023-09-05T14:54:39+00:00",
        "started_at": "2023-09-05T14:54:42+00:00",
        "ended_at": "2023-09-05T14:54:42+00:00",
        "retry_of_task_id": null,
        "copy_of_task_id": null,
        "user_id": 64955942,
        "priority": -10,
        "host_name": null,
        "storage": null,
        "region": "eu-central",
        "depends_on_task_ids": [
          "4bb8e75e-8b8b-4cea-a252-8661c1e93f72"
        ],
        "links": {
          "self": "https:\\/\\/api.sandbox.cloudconvert.com\\/v2\\/tasks\\/a5c9ccfc-9214-485e-807e-b504cf8f152b"
        }
      },
      {
        "id": "4bb8e75e-8b8b-4cea-a252-8661c1e93f72",
        "name": "import_source_from_url",
        "job_id": "c9d46d98-c3a6-44d5-b03a-247e1c7feae1",
        "status": "error",
        "credits": 0,
        "code": "SANDBOX_FILE_NOT_ALLOWED",
        "message": "The file flight-payment.mp4 is not whitelisted for sandbox use",
        "percent": 100,
        "operation": "import\\/url",
        "result": {
          "files": [
            {
              "filename": "flight-payment.mp4",
              "md5": "3d5de4876bf14bcf6a57571e922ca50b"
            }
          ]
        },
        "created_at": "2023-09-05T14:54:39+00:00",
        "started_at": "2023-09-05T14:54:39+00:00",
        "ended_at": "2023-09-05T14:54:42+00:00",
        "retry_of_task_id": null,
        "copy_of_task_id": null,
        "user_id": 64955942,
        "priority": -10,
        "host_name": "katelynn",
        "storage": null,
        "region": "eu-central",
        "depends_on_task_ids": [],
        "links": {
          "self": "https:\\/\\/api.sandbox.cloudconvert.com\\/v2\\/tasks\\/4bb8e75e-8b8b-4cea-a252-8661c1e93f72"
        }
      }
    ],
    "links": {
      "self": "https:\\/\\/api.sandbox.cloudconvert.com\\/v2\\/jobs\\/c9d46d98-c3a6-44d5-b03a-247e1c7feae1"
    }
  }
}'''