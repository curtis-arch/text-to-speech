import io
import json
import os
from dataclasses import dataclass
from typing import Optional, Dict, Union

from botocore.response import StreamingBody
from botocore.stub import Stubber
from chalice.test import Client
from dataclasses_json import dataclass_json
from pytest import fixture
from requests_mock import Mocker

from chalicelib.entities.engine_config import ConversionConfig, PlayHTConfig
from chalicelib.entities.play_ht import ConversionJobCreatedResponse

bucket_name = "another_test_bucket"
webhook_url = "https://www.blackhole.com"
queue_url = "https://sqs.us-east-1.amazonaws.com/sampleQueue"


@dataclass_json
@dataclass
class AwsStubs:
    s3: Stubber
    sqs: Stubber


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
    sqs_stub = Stubber(app.get_sqs_client())
    with s3_stub:
        with sqs_stub:
            yield AwsStubs(s3=s3_stub, sqs=sqs_stub)


def test_create_success(monkeypatch, requests_mock: Mocker, test_client: Client, aws_stubs: AwsStubs):
    monkeypatch.setenv("INPUT_BUCKET_NAME", bucket_name)
    monkeypatch.setenv("WEBHOOK_URL", webhook_url)
    monkeypatch.setenv("STATUS_POLLER_QUEUE_URL", queue_url)

    text_object_key = "path/sub/john.txt"
    engine_config = ConversionConfig(api_key="abc123", play_config=PlayHTConfig())

    response = ConversionJobCreatedResponse.from_dict(response_job_created)
    _setup_stubs(
        aws_stubs=aws_stubs, text_object_key=text_object_key,
        engine_config=engine_config
    )
    _setup_mock_success(mocker=requests_mock, response=response)

    event = test_client.events.generate_s3_event(bucket=bucket_name, key=text_object_key)
    response = test_client.lambda_.invoke('on_text_input_file', event)

    assert response.payload == {'result': 'created'}

    aws_stubs.s3.assert_no_pending_responses()
    aws_stubs.sqs.assert_no_pending_responses()

    assert len(requests_mock.request_history) == 1


def test_400(monkeypatch, requests_mock: Mocker, test_client: Client, aws_stubs: AwsStubs):
    monkeypatch.setenv("INPUT_BUCKET_NAME", bucket_name)
    monkeypatch.setenv("WEBHOOK_URL", webhook_url)
    monkeypatch.setenv("STATUS_POLLER_QUEUE_URL", queue_url)

    text_object_key = "path/sub/john.txt"
    engine_config = ConversionConfig(api_key="abc123", play_config=PlayHTConfig())

    _setup_stubs(
        aws_stubs=aws_stubs, text_object_key=text_object_key,
        engine_config=engine_config
    )
    _setup_mock_error(mocker=requests_mock, status=400, response={"error": "Something failed"})

    event = test_client.events.generate_s3_event(bucket=bucket_name, key=text_object_key)
    response = test_client.lambda_.invoke('on_text_input_file', event)

    assert response.payload == {'result': 'failure'}

    aws_stubs.s3.assert_no_pending_responses()

    assert len(requests_mock.request_history) == 2
    webhook_request = requests_mock.request_history[1]
    assert webhook_request.json() == json.dumps({
        "message": "Error while creating conversion job using play.ht",
        "context": {
            "voice": "en-US-JennyNeural",
            "content": ["A quick brown fox"],
            "error_message": "Something failed",
            "error_status": "400"
        }
    })


def test_403(monkeypatch, requests_mock: Mocker, test_client: Client, aws_stubs: AwsStubs):
    monkeypatch.setenv("INPUT_BUCKET_NAME", bucket_name)
    monkeypatch.setenv("WEBHOOK_URL", webhook_url)
    monkeypatch.setenv("STATUS_POLLER_QUEUE_URL", queue_url)

    text_object_key = "path/sub/john.txt"
    engine_config = ConversionConfig(api_key="abc123", play_config=PlayHTConfig())

    _setup_stubs(
        aws_stubs=aws_stubs, text_object_key=text_object_key,
        engine_config=engine_config
    )
    _setup_mock_error(mocker=requests_mock, status=403)

    event = test_client.events.generate_s3_event(bucket=bucket_name, key=text_object_key)
    response = test_client.lambda_.invoke('on_text_input_file', event)

    assert response.payload == {'result': 'failure'}

    aws_stubs.s3.assert_no_pending_responses()

    assert len(requests_mock.request_history) == 2
    webhook_request = requests_mock.request_history[1]
    assert webhook_request.json() == json.dumps({
        "message": "The provided play.ht api key's plan does not have access to the requested resource.",
        "context": {
            "voice": "en-US-JennyNeural",
            "content": ["A quick brown fox"],
            "error_message": "",
            "error_status": "403"
        }
    })


def _setup_mock_success(mocker: Mocker, response: ConversionJobCreatedResponse) -> None:
    mocker.post(webhook_url, json={})
    mocker.post(f'https://play.ht/api/v1/convert', json=response.to_dict())


def _setup_mock_error(mocker: Mocker, status: int, response: Optional[Dict[str, Union[str, int]]] = None) -> None:
    mocker.post(webhook_url, json={})
    if response is not None:
        mocker.post(f'https://play.ht/api/v1/convert', json=response, status_code=status)
    else:
        mocker.post(f'https://play.ht/api/v1/convert', status_code=status)


def _setup_stubs(aws_stubs: AwsStubs, text_object_key: str, engine_config: ConversionConfig) -> None:
    config_object_key = os.path.splitext(text_object_key)[0] + ".json"
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

    text_content_encoded = "A quick brown fox".encode()
    aws_stubs.s3.add_response(
        'get_object',
        expected_params={
            'Bucket': bucket_name,
            'Key': text_object_key,
        },
        service_response={
            'Body': StreamingBody(
                raw_stream=io.BytesIO(text_content_encoded),
                content_length=len(text_content_encoded)
            )
        },
    )

    aws_stubs.sqs.add_response(
        method='send_message',
        expected_params={
            'QueueUrl': queue_url,
            'MessageBody': json.dumps(
                {
                    "job_id": "t123",
                    "config": {
                        "bucket": bucket_name,
                        "config_object_key": config_object_key
                    }
                }
            )
        },
        service_response={
            'MessageId': 'test-message-id'
        }
    )


response_job_created = {
    "status": "CREATED",
    "transcriptionId": "t123",
    "content_length": 100,
    "word_count": 100
}
