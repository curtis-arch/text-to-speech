import io
import json
import os
from dataclasses import dataclass
from typing import Optional, Dict, Union

import pytest
from botocore.response import StreamingBody
from botocore.stub import Stubber
from chalice.test import Client
from dataclasses_json import dataclass_json
from pytest import fixture
from requests_mock import Mocker

from chalicelib.entities.engine_config import ConversionConfig, PlayHTConfig
from chalicelib.entities.messages import ConversionJob, ConversionJobConfig
from chalicelib.entities.play_ht import ConversionJobStatusResponse

bucket_name = "another_test_bucket"
webhook_url = "https://www.blackhole.com"
queue_url = "https://sqs.us-east-1.amazonaws.com/sampleQueue"


@dataclass_json
@dataclass
class AwsStubs:
    s3: Stubber


@fixture
def test_client() -> Client:
    os.environ["STATUS_POLLER_QUEUE_URL"] = queue_url

    import app
    with Client(app.app, stage_name="unit_tests") as client:
        yield client


@fixture
def aws_stubs() -> AwsStubs:
    import app
    s3_stub = Stubber(app.get_s3_client())
    with s3_stub:
        yield AwsStubs(s3=s3_stub)


def test_status_success(monkeypatch, requests_mock: Mocker, test_client: Client, aws_stubs: AwsStubs):
    monkeypatch.setenv("INPUT_BUCKET_NAME", bucket_name)
    monkeypatch.setenv("WEBHOOK_URL", webhook_url)

    config_object_key = "path/sub/john.json"
    transcription_id = "t123"
    engine_config = ConversionConfig(api_key="abc123", play_config=PlayHTConfig())

    response = ConversionJobStatusResponse.from_dict(response_job_status)
    _setup_stubs(aws_stubs=aws_stubs, config_object_key=config_object_key,engine_config=engine_config)
    _setup_mock_success(mocker=requests_mock, transcription_id=transcription_id, response=response)

    conversion_job = ConversionJob(
        job_id=transcription_id, config=ConversionJobConfig(bucket=bucket_name, config_object_key=config_object_key)
    )
    event = test_client.events.generate_sqs_event(
        queue_name=queue_url.split("/")[-1], message_bodies=[conversion_job.to_json()]
    )
    test_client.lambda_.invoke('on_conversion_job_message', event)

    aws_stubs.s3.assert_no_pending_responses()

    assert len(requests_mock.request_history) == 2
    webhook_request = requests_mock.request_history[1]
    assert webhook_request.json() == json.dumps({
        "audioFile": "https://play.ht/output.mp3", "audioLengthInSeconds": 100
    })


def test_status_conversion_error(monkeypatch, requests_mock: Mocker, test_client: Client, aws_stubs: AwsStubs):
    monkeypatch.setenv("INPUT_BUCKET_NAME", bucket_name)
    monkeypatch.setenv("WEBHOOK_URL", webhook_url)

    config_object_key = "path/sub/john.json"
    transcription_id = "t123"
    engine_config = ConversionConfig(api_key="abc123", play_config=PlayHTConfig())

    response = ConversionJobStatusResponse.from_dict({**response_job_status, "error": True, "errorMessage": "Invalid"})
    _setup_stubs(aws_stubs=aws_stubs, config_object_key=config_object_key,engine_config=engine_config)
    _setup_mock_success(mocker=requests_mock, transcription_id=transcription_id, response=response)

    conversion_job = ConversionJob(
        job_id=transcription_id, config=ConversionJobConfig(bucket=bucket_name, config_object_key=config_object_key)
    )
    event = test_client.events.generate_sqs_event(
        queue_name=queue_url.split("/")[-1], message_bodies=[conversion_job.to_json()]
    )
    test_client.lambda_.invoke('on_conversion_job_message', event)

    aws_stubs.s3.assert_no_pending_responses()

    assert len(requests_mock.request_history) == 2
    webhook_request = requests_mock.request_history[1]
    assert webhook_request.json() == json.dumps({
        "message": "Conversion job t123 failed", "context": {"error_message": "Invalid"}
    })


def test_status_conversion_incomplete(monkeypatch, requests_mock: Mocker, test_client: Client, aws_stubs: AwsStubs):
    monkeypatch.setenv("INPUT_BUCKET_NAME", bucket_name)
    monkeypatch.setenv("WEBHOOK_URL", webhook_url)

    config_object_key = "path/sub/john.json"
    transcription_id = "t123"
    engine_config = ConversionConfig(api_key="abc123", play_config=PlayHTConfig())

    response = ConversionJobStatusResponse.from_dict({**response_job_status, "converted": False})
    _setup_stubs(aws_stubs=aws_stubs, config_object_key=config_object_key,engine_config=engine_config)
    _setup_mock_success(mocker=requests_mock, transcription_id=transcription_id, response=response)

    conversion_job = ConversionJob(
        job_id=transcription_id, config=ConversionJobConfig(bucket=bucket_name, config_object_key=config_object_key)
    )
    event = test_client.events.generate_sqs_event(
        queue_name=queue_url.split("/")[-1], message_bodies=[conversion_job.to_json()]
    )

    from app import JobNotFinishedError
    with pytest.raises(JobNotFinishedError):
        test_client.lambda_.invoke('on_conversion_job_message', event)


def _setup_mock_success(mocker: Mocker, transcription_id: str, response: ConversionJobStatusResponse) -> None:
    mocker.post(webhook_url, json={})
    mocker.get(f'https://play.ht/api/v1/articleStatus?transcriptionId={transcription_id}', json=response.to_dict())


def _setup_mock_error(mocker: Mocker, status: int, response: Optional[Dict[str, Union[str, int]]] = None) -> None:
    mocker.post(webhook_url, json={})
    if response is not None:
        mocker.post(f'https://play.ht/api/v1/convert', json=response, status_code=status)
    else:
        mocker.post(f'https://play.ht/api/v1/convert', status_code=status)


def _setup_stubs(aws_stubs: AwsStubs, config_object_key: str, engine_config: ConversionConfig) -> None:
    json_config_encoded = engine_config.to_json().encode()
    aws_stubs.s3.add_response(
        method='get_object',
        expected_params={
            'Bucket': bucket_name,
            'Key': config_object_key,
        },
        service_response={
            'Body': StreamingBody(
                raw_stream=io.BytesIO(json_config_encoded),
                content_length=len(json_config_encoded)
            )
        },
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
