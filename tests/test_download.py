import json
import json
import os
from dataclasses import dataclass

import botocore.stub
from botocore.stub import Stubber
from chalice.test import Client
from dataclasses_json import dataclass_json
from pytest import fixture
from requests_mock import Mocker

from chalicelib.entities.messages import DownloadTask
from chalicelib.entities.murf_ai import SynthesizeSpeechResponse

bucket_name = "another_test_bucket"
webhook_url = "https://www.blackhole.com"
status_queue_url = "https://sqs.us-east-1.amazonaws.com/statusQueue"
download_queue_url = "https://sqs.us-east-1.amazonaws.com/downloadQueue"
buffer_size_1mb = 1024 * 1024


@dataclass_json
@dataclass
class AwsStubs:
    s3: Stubber


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
    s3_stub = Stubber(app.get_s3_client())
    with s3_stub:
        yield AwsStubs(s3=s3_stub)


def test_download_success(monkeypatch, requests_mock: Mocker, test_client: Client, aws_stubs: AwsStubs):
    monkeypatch.setenv("INPUT_BUCKET_NAME", bucket_name)
    monkeypatch.setenv("WEBHOOK_URL", webhook_url)
    monkeypatch.setenv("MULTIPART_UPLOAD_BUFFER_SIZE", str(buffer_size_1mb))

    task = DownloadTask(
        destination_bucket=bucket_name,
        destination_key="out.mp3",
        speech_synthesized_response=SynthesizeSpeechResponse(audio_file="https://download.com/sample.mp3")
    )

    _setup_stubs_success(aws_stubs=aws_stubs, download_task=task)
    _setup_mock_success(mocker=requests_mock, download_task=task)

    event = test_client.events.generate_sqs_event(
        queue_name=download_queue_url.split("/")[-1], message_bodies=[task.to_json()]
    )
    test_client.lambda_.invoke('on_download_message', event)

    aws_stubs.s3.assert_no_pending_responses()

    assert len(requests_mock.request_history) == 2
    webhook_request = requests_mock.request_history[1]
    assert webhook_request.json() == json.dumps({
        "bucket": bucket_name, "key": "out.mp3"
    })


def test_source_download_failed(monkeypatch, requests_mock: Mocker, test_client: Client, aws_stubs: AwsStubs):
    monkeypatch.setenv("INPUT_BUCKET_NAME", bucket_name)
    monkeypatch.setenv("WEBHOOK_URL", webhook_url)
    monkeypatch.setenv("MULTIPART_UPLOAD_BUFFER_SIZE", str(buffer_size_1mb))

    task = DownloadTask(
        destination_bucket=bucket_name,
        destination_key="out.mp3",
        speech_synthesized_response=SynthesizeSpeechResponse(audio_file="https://download.com/sample.mp3")
    )

    _setup_stubs_success(aws_stubs=aws_stubs, download_task=task)
    _setup_mock_error(mocker=requests_mock, download_task=task)

    event = test_client.events.generate_sqs_event(
        queue_name=download_queue_url.split("/")[-1], message_bodies=[task.to_json()]
    )
    test_client.lambda_.invoke('on_download_message', event)

    assert len(requests_mock.request_history) == 2
    webhook_request = requests_mock.request_history[1]
    assert webhook_request.json() == json.dumps({
        "message": "Unable to fetch file for downloading.",
        "context": task.to_dict()
    })


def test_create_multipart_failed(monkeypatch, requests_mock: Mocker, test_client: Client, aws_stubs: AwsStubs):
    monkeypatch.setenv("INPUT_BUCKET_NAME", bucket_name)
    monkeypatch.setenv("WEBHOOK_URL", webhook_url)
    monkeypatch.setenv("MULTIPART_UPLOAD_BUFFER_SIZE", str(buffer_size_1mb))

    task = DownloadTask(
        destination_bucket=bucket_name,
        destination_key="out.mp3",
        speech_synthesized_response=SynthesizeSpeechResponse(audio_file="https://download.com/sample.mp3")
    )

    _setup_stubs_create_multipart_upload_error(aws_stubs=aws_stubs, download_task=task)
    _setup_mock_success(mocker=requests_mock, download_task=task)

    event = test_client.events.generate_sqs_event(
        queue_name=download_queue_url.split("/")[-1], message_bodies=[task.to_json()]
    )
    test_client.lambda_.invoke('on_download_message', event)

    aws_stubs.s3.assert_no_pending_responses()

    assert len(requests_mock.request_history) == 1
    webhook_request = requests_mock.request_history[0]
    assert webhook_request.json() == json.dumps({
        "message": "Unable to initialise multipart upload for converted speech file.",
        "context": task.to_dict()
    })


def test_upload_part_failed(monkeypatch, requests_mock: Mocker, test_client: Client, aws_stubs: AwsStubs):
    monkeypatch.setenv("INPUT_BUCKET_NAME", bucket_name)
    monkeypatch.setenv("WEBHOOK_URL", webhook_url)
    monkeypatch.setenv("MULTIPART_UPLOAD_BUFFER_SIZE", str(buffer_size_1mb))

    task = DownloadTask(
        destination_bucket=bucket_name,
        destination_key="out.mp3",
        speech_synthesized_response=SynthesizeSpeechResponse(audio_file="https://download.com/sample.mp3")
    )

    _setup_stubs_upload_part_failed(aws_stubs=aws_stubs, download_task=task)
    _setup_mock_success(mocker=requests_mock, download_task=task)

    event = test_client.events.generate_sqs_event(
        queue_name=download_queue_url.split("/")[-1], message_bodies=[task.to_json()]
    )
    test_client.lambda_.invoke('on_download_message', event)

    aws_stubs.s3.assert_no_pending_responses()

    assert len(requests_mock.request_history) == 2
    webhook_request = requests_mock.request_history[1]
    assert webhook_request.json() == json.dumps({
        "message": "Unable to finish multipart S3 upload and store converted speech file.",
        "context": task.to_dict()
    })


def _setup_mock_success(mocker: Mocker, download_task: DownloadTask) -> None:
    mocker.post(webhook_url, json={})
    pwd = os.path.dirname(os.path.realpath(__file__))
    with open(os.path.join(pwd, "sample.mp3"), 'rb') as f:
        binary_content = f.read()
    mocker.get(download_task.speech_synthesized_response.audio_file, content=binary_content)


def _setup_mock_error(mocker: Mocker, download_task: DownloadTask) -> None:
    mocker.post(webhook_url, json={})
    mocker.get(download_task.speech_synthesized_response.audio_file, status_code=404)


def _setup_stubs_success(aws_stubs: AwsStubs, download_task: DownloadTask) -> None:
    aws_stubs.s3.add_response(
        method='create_multipart_upload',
        expected_params={
            'Bucket': download_task.destination_bucket,
            'Key': download_task.destination_key,
        },
        service_response={
            'UploadId': 'u123'
        },
    )

    pwd = os.path.dirname(os.path.realpath(__file__))
    file_size = os.path.getsize(os.path.join(pwd, "sample.mp3"))

    chunks = (file_size + buffer_size_1mb - 1) // buffer_size_1mb

    for x in range(chunks):
        aws_stubs.s3.add_response(
            method='upload_part',
            expected_params={
                'Bucket': download_task.destination_bucket,
                'Key': download_task.destination_key,
                'PartNumber': x + 1,
                'UploadId': 'u123',
                'Body': botocore.stub.ANY,
            },
            service_response={
                'ETag': f'eTag{x + 1}'
            },
        )

    aws_stubs.s3.add_response(
        method='complete_multipart_upload',
        expected_params={
            'Bucket': download_task.destination_bucket,
            'Key': download_task.destination_key,
            'UploadId': 'u123',
            'MultipartUpload': {"Parts": [{'ETag': f'eTag{x + 1}', 'PartNumber': x + 1} for x in range(chunks)]}
        },
        service_response={
            'Bucket': download_task.destination_bucket,
            'Key': download_task.destination_key,
        },
    )


def _setup_stubs_upload_part_failed(aws_stubs: AwsStubs, download_task: DownloadTask) -> None:
    aws_stubs.s3.add_response(
        method='create_multipart_upload',
        expected_params={
            'Bucket': download_task.destination_bucket,
            'Key': download_task.destination_key,
        },
        service_response={
            'UploadId': 'u123'
        },
    )

    pwd = os.path.dirname(os.path.realpath(__file__))
    file_size = os.path.getsize(os.path.join(pwd, "sample.mp3"))

    chunks = (file_size // buffer_size_1mb) - 1

    for x in range(chunks):
        aws_stubs.s3.add_response(
            method='upload_part',
            expected_params={
                'Bucket': download_task.destination_bucket,
                'Key': download_task.destination_key,
                'PartNumber': x + 1,
                'UploadId': 'u123',
                'Body': botocore.stub.ANY,
            },
            service_response={
                'ETag': f'eTag{x + 1}'
            },
        )

    aws_stubs.s3.add_response(
        method='abort_multipart_upload',
        expected_params={
            'Bucket': download_task.destination_bucket,
            'Key': download_task.destination_key,
            'UploadId': 'u123'
        },
        service_response={},
    )


def _setup_stubs_create_multipart_upload_error(aws_stubs: AwsStubs, download_task: DownloadTask) -> None:
    aws_stubs.s3.add_client_error(
        method='create_multipart_upload',
        service_error_code='NoSuchBucket',
        service_message='The specified bucket does not exist.',
        http_status_code=404,
        expected_params={
            'Bucket': download_task.destination_bucket,
            'Key': download_task.destination_key,
        }
    )


response_job_status = {
    "voice": "Matthew",
    "converted": True,
    "audioDuration": 100,
    "audioUrl": "https://play.ht/output.mp3",
    "message": None,
    "error": False,
    "errorMessage": None
}
