import json
import os
from dataclasses import dataclass
from typing import Optional, Dict, List

from botocore.stub import Stubber
from chalice.test import Client
from dataclasses_json import dataclass_json
from pytest import fixture
from requests_mock import Mocker

from chalicelib.entities.secrets import Secrets
from chalicelib.utils.request_templates import cloud_convert_create_job, cloud_convert_create_webhook

bucket_name = "another_test_bucket"
webhook_url = "https://www.blackhole.com"
status_queue_url = "https://sqs.us-east-1.amazonaws.com/statusQueue"
download_queue_url = "https://sqs.us-east-1.amazonaws.com/downloadQueue"


@dataclass_json
@dataclass
class AwsStubs:
    ssm: Stubber


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
    ssm_stub = Stubber(app.get_ssm_client())
    with ssm_stub:
        yield AwsStubs(ssm=ssm_stub)


def test_create_conversion_success(monkeypatch, requests_mock: Mocker, test_client: Client, aws_stubs: AwsStubs):
    monkeypatch.setenv("INPUT_BUCKET_NAME", bucket_name)
    monkeypatch.setenv("WEBHOOK_URL", webhook_url)
    monkeypatch.setenv("STATUS_POLLER_QUEUE_URL", status_queue_url)
    monkeypatch.setenv("SECRETS_MANAGER_KEY_NAME", "key")
    monkeypatch.setenv("CLOUD_CONVERT_API_URL", "https://api.sandbox.cloudconvert.com")

    _setup_stubs(aws_stubs=aws_stubs)
    _setup_mock_success(mocker=requests_mock)

    event = test_client.events.generate_s3_event(bucket=bucket_name, key="videos/sample.mp4")
    response = test_client.lambda_.invoke('on_video_input_file', event)

    assert response.payload == {'result': 'success'}

    aws_stubs.ssm.assert_no_pending_responses()

    assert len(requests_mock.request_history) == 3
    assert requests_mock.request_history[1].json() == cloud_convert_create_webhook(url="http://localhost/v1/webhooks/cloud_convert")

    create_job_request = cloud_convert_create_job(url=f"https://{bucket_name}.s3.amazonaws.com/videos/sample.mp4")

    assert requests_mock.request_history[2].json() == create_job_request


def test_create_conversion_success_webhook_exists(monkeypatch, requests_mock: Mocker, test_client: Client, aws_stubs: AwsStubs):
    monkeypatch.setenv("INPUT_BUCKET_NAME", bucket_name)
    monkeypatch.setenv("WEBHOOK_URL", webhook_url)
    monkeypatch.setenv("STATUS_POLLER_QUEUE_URL", status_queue_url)
    monkeypatch.setenv("SECRETS_MANAGER_KEY_NAME", "key")
    monkeypatch.setenv("CLOUD_CONVERT_API_URL", "https://api.sandbox.cloudconvert.com")

    _setup_stubs(aws_stubs=aws_stubs)
    _setup_mock_success(
        mocker=requests_mock,
        webhooks_get_response=[
            {"url": "https://ping.com/webhook"}, {"url": "https://server.url/v1/webhooks/cloud_convert"}
        ]
    )

    event = test_client.events.generate_s3_event(bucket=bucket_name, key="videos/sample.mp4")
    response = test_client.lambda_.invoke('on_video_input_file', event)

    assert response.payload == {'result': 'success'}

    aws_stubs.ssm.assert_no_pending_responses()

    assert len(requests_mock.request_history) == 2
    assert requests_mock.request_history[1].json() == cloud_convert_create_job(url=f"https://{bucket_name}.s3.amazonaws.com/videos/sample.mp4")


def test_list_webhooks_failed(monkeypatch, requests_mock: Mocker, test_client: Client, aws_stubs: AwsStubs):
    monkeypatch.setenv("INPUT_BUCKET_NAME", bucket_name)
    monkeypatch.setenv("WEBHOOK_URL", webhook_url)
    monkeypatch.setenv("STATUS_POLLER_QUEUE_URL", status_queue_url)
    monkeypatch.setenv("SECRETS_MANAGER_KEY_NAME", "key")
    monkeypatch.setenv("CLOUD_CONVERT_API_URL", "https://api.sandbox.cloudconvert.com")

    _setup_stubs(aws_stubs=aws_stubs)
    _setup_mock_error_list_webhooks(mocker=requests_mock)

    event = test_client.events.generate_s3_event(bucket=bucket_name, key="videos/sample.mp4")
    response = test_client.lambda_.invoke('on_video_input_file', event)

    assert response.payload == {'result': 'failure'}

    aws_stubs.ssm.assert_no_pending_responses()

    assert len(requests_mock.request_history) == 2
    webhook_request = requests_mock.request_history[1]
    assert webhook_request.json() == json.dumps({
        "message": "Unable to list webhooks in cloud convert",
        "context": {"url": "https://api.sandbox.cloudconvert.com/v2/webhooks"}
    })


def test_create_webhooks_failed(monkeypatch, requests_mock: Mocker, test_client: Client, aws_stubs: AwsStubs):
    monkeypatch.setenv("INPUT_BUCKET_NAME", bucket_name)
    monkeypatch.setenv("WEBHOOK_URL", webhook_url)
    monkeypatch.setenv("STATUS_POLLER_QUEUE_URL", status_queue_url)
    monkeypatch.setenv("SECRETS_MANAGER_KEY_NAME", "key")
    monkeypatch.setenv("CLOUD_CONVERT_API_URL", "https://api.sandbox.cloudconvert.com")

    _setup_stubs(aws_stubs=aws_stubs)
    _setup_mock_error_create_webhook(mocker=requests_mock)

    event = test_client.events.generate_s3_event(bucket=bucket_name, key="videos/sample.mp4")
    response = test_client.lambda_.invoke('on_video_input_file', event)

    assert response.payload == {'result': 'failure'}

    aws_stubs.ssm.assert_no_pending_responses()

    assert len(requests_mock.request_history) == 3
    webhook_request = requests_mock.request_history[2]
    assert webhook_request.json() == json.dumps({
        "message": "Failed to create cloud convert webhook.",
        "context": {"data": cloud_convert_create_webhook(url="http://localhost/v1/webhooks/cloud_convert")}
    })


def test_create_job_failed(monkeypatch, requests_mock: Mocker, test_client: Client, aws_stubs: AwsStubs):
    monkeypatch.setenv("INPUT_BUCKET_NAME", bucket_name)
    monkeypatch.setenv("WEBHOOK_URL", webhook_url)
    monkeypatch.setenv("STATUS_POLLER_QUEUE_URL", status_queue_url)
    monkeypatch.setenv("SECRETS_MANAGER_KEY_NAME", "key")
    monkeypatch.setenv("CLOUD_CONVERT_API_URL", "https://api.sandbox.cloudconvert.com")

    _setup_stubs(aws_stubs=aws_stubs)
    _setup_mock_error_create_job(mocker=requests_mock)

    event = test_client.events.generate_s3_event(bucket=bucket_name, key="videos/sample.mp4")
    response = test_client.lambda_.invoke('on_video_input_file', event)

    assert response.payload == {'result': 'failure'}

    aws_stubs.ssm.assert_no_pending_responses()

    assert len(requests_mock.request_history) == 4
    webhook_request = requests_mock.request_history[3]
    assert webhook_request.json() == json.dumps({
        "message": "Failed to start converting input using cloud convert.",
        "context": {"data": cloud_convert_create_job(url=f"https://{bucket_name}.s3.amazonaws.com/videos/sample.mp4")}
    })


def _setup_mock_success(mocker: Mocker, webhooks_get_response: Optional[List[Dict[str, str]]] = None) -> None:
    mocker.post(webhook_url, json={})
    if webhooks_get_response is None:
        mocker.get(f'https://api.sandbox.cloudconvert.com/v2/webhooks', json={"data": []})
        mocker.post(f'https://api.sandbox.cloudconvert.com/v2/webhooks', json={})
    else:
        mocker.get(f'https://api.sandbox.cloudconvert.com/v2/webhooks', json={"data": webhooks_get_response})

    mocker.post(f'https://api.sandbox.cloudconvert.com/v2/jobs', json={"data": {"id": "newJobId"}})


def _setup_mock_error_list_webhooks(mocker: Mocker) -> None:
    mocker.post(webhook_url, json={})
    mocker.get(f'https://api.sandbox.cloudconvert.com/v2/webhooks', status_code=400)


def _setup_mock_error_create_webhook(mocker: Mocker) -> None:
    mocker.post(webhook_url, json={})
    mocker.get(f'https://api.sandbox.cloudconvert.com/v2/webhooks', json={"data": []})
    mocker.post(f'https://api.sandbox.cloudconvert.com/v2/webhooks', status_code=400)


def _setup_mock_error_create_job(mocker: Mocker) -> None:
    mocker.post(webhook_url, json={})
    mocker.get(f'https://api.sandbox.cloudconvert.com/v2/webhooks', json={"data": []})
    mocker.post(f'https://api.sandbox.cloudconvert.com/v2/webhooks', json={})
    mocker.post(f'https://api.sandbox.cloudconvert.com/v2/jobs', status_code=500)


def _setup_stubs(aws_stubs: AwsStubs) -> None:
    aws_stubs.ssm.add_response(
        method='get_parameter',
        expected_params={
            'Name': '/aws/reference/secretsmanager/key',
            'WithDecryption': True
        },
        service_response={
            'Parameter': {
                'Value': Secrets(cloud_conversion_api_key='foo').to_json()
            }
        },
    )


response_job_created = {
    "status": "CREATED",
    "transcriptionId": "t123",
    "content_length": 100,
    "word_count": 100
}
