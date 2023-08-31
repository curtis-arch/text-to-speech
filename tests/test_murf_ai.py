import io
import json
import os
from typing import Optional, Dict, Union

from botocore.response import StreamingBody
from botocore.stub import Stubber
from chalice.test import Client
from pytest import fixture
from requests_mock import Mocker

from chalicelib.entities.engine_config import ConversionConfig, MurfAIConfig
from chalicelib.entities.murf_ai import SynthesizeSpeechResponse

bucket_name = "test_bucket"
webhook_url = "https://blackhole.com"
status_queue_url = "https://sqs.us-east-1.amazonaws.com/statusQueue"
download_queue_url = "https://sqs.us-east-1.amazonaws.com/downloadQueue"


@fixture
def test_client() -> Client:
    os.environ["STATUS_POLLER_QUEUE_URL"] = status_queue_url
    os.environ["DOWNLOADER_QUEUE_URL"] = download_queue_url

    import app
    with Client(app.app, stage_name="unit_tests") as client:
        yield client


@fixture
def s3_stub() -> Stubber:
    import app
    client = app.get_s3_client()
    stubbed_client = Stubber(client)
    with stubbed_client:
        yield stubbed_client


def test_success(monkeypatch, requests_mock: Mocker, test_client: Client, s3_stub: Stubber):
    monkeypatch.setenv("INPUT_BUCKET_NAME", bucket_name)
    monkeypatch.setenv("WEBHOOK_URL", webhook_url)

    text_object_key = "path/sub/john.txt"
    engine_config = ConversionConfig(api_key="abc123", murf_config=MurfAIConfig())

    synthesize_speech_response = SynthesizeSpeechResponse.from_dict(response_synth_speech)
    _setup_stubs(
        stubbed_client=s3_stub, text_object_key=text_object_key, engine_config=engine_config
    )
    _setup_mock_success(mocker=requests_mock, response=synthesize_speech_response)

    event = test_client.events.generate_s3_event(bucket=bucket_name, key=text_object_key)
    response = test_client.lambda_.invoke('on_text_input_file', event)

    assert response.payload == {'result': 'success'}

    s3_stub.assert_no_pending_responses()

    assert len(requests_mock.request_history) == 2
    webhook_request = requests_mock.request_history[1]
    assert webhook_request.json() == synthesize_speech_response.to_json()


def test_500(monkeypatch, requests_mock: Mocker, test_client: Client, s3_stub: Stubber):
    monkeypatch.setenv("INPUT_BUCKET_NAME", bucket_name)
    monkeypatch.setenv("WEBHOOK_URL", webhook_url)

    text_object_key = "path/sub/john.txt"
    engine_config = ConversionConfig(api_key="abc123", murf_config=MurfAIConfig())

    _setup_stubs(
        stubbed_client=s3_stub, text_object_key=text_object_key, engine_config=engine_config
    )
    _setup_mock_error(mocker=requests_mock, status=500)

    event = test_client.events.generate_s3_event(bucket=bucket_name, key=text_object_key)
    response = test_client.lambda_.invoke('on_text_input_file', event)

    assert response.payload == {'result': 'failure'}

    s3_stub.assert_no_pending_responses()

    assert len(requests_mock.request_history) == 2
    webhook_request = requests_mock.request_history[1]
    assert webhook_request.json() == json.dumps({
        "message": "Something went wrong generating speech using murf.ai.",
        "context": {
            "voiceId": "en-US-cooper", "style": "Conversational", "rate": 0, "pitch": 0, "sampleRate": 24000,
            "format": "MP3", "channelType": "STEREO", "text": "A quick brown fox", "error_status": 500
        }
    })


def test_400(monkeypatch, requests_mock: Mocker, test_client: Client, s3_stub: Stubber):
    monkeypatch.setenv("INPUT_BUCKET_NAME", bucket_name)
    monkeypatch.setenv("WEBHOOK_URL", webhook_url)

    text_object_key = "path/sub/john.txt"
    engine_config = ConversionConfig(api_key="abc123", murf_config=MurfAIConfig())

    _setup_stubs(
        stubbed_client=s3_stub, text_object_key=text_object_key, engine_config=engine_config
    )
    _setup_mock_error(
        mocker=requests_mock,
        status=400,
        response={
            "errorMessage": "Text passed is 2056 characters long. Max length allowed is 1000 characters",
            "errorCode": 400
        }
    )

    event = test_client.events.generate_s3_event(bucket=bucket_name, key=text_object_key)
    response = test_client.lambda_.invoke('on_text_input_file', event)

    assert response.payload == {'result': 'failure'}

    s3_stub.assert_no_pending_responses()

    assert len(requests_mock.request_history) == 2
    webhook_request = requests_mock.request_history[1]
    assert webhook_request.json() == json.dumps({
        "message": "Something went wrong generating speech using murf.ai.",
        "context": {
            "voiceId": "en-US-cooper", "style": "Conversational", "rate": 0, "pitch": 0, "sampleRate": 24000,
            "format": "MP3", "channelType": "STEREO", "text": "A quick brown fox", "error_message": "Text passed is 2056 characters long. Max length allowed is 1000 characters",
            "error_status": 400
        }
    })


def _setup_mock_success(mocker: Mocker, response: SynthesizeSpeechResponse) -> None:
    mocker.post(webhook_url, json={})
    mocker.post(f'https://api.murf.ai/v1/speech/generate-with-key', json=response.to_dict())


def _setup_mock_error(mocker: Mocker, status: int, response: Optional[Dict[str, Union[str, int]]] = None) -> None:
    mocker.post(webhook_url, json={})
    if response is not None:
        mocker.post(f'https://api.murf.ai/v1/speech/generate-with-key', json=response, status_code=status)
    else:
        mocker.post(f'https://api.murf.ai/v1/speech/generate-with-key', status_code=status)


def _setup_stubs(stubbed_client: Stubber, text_object_key: str, engine_config: ConversionConfig) -> None:
    config_object_key = os.path.splitext(text_object_key)[0] + ".json"
    json_config_encoded = engine_config.to_json().encode()
    stubbed_client.add_response(
        'get_object',
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
    stubbed_client.add_response(
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


response_synth_speech = {
    "audioFile": "https://murf.ai/user-upload/one-day-temp/1834b12f-d2e2-4a72-8097-c3fedb19056c.mp3",
    "audioLengthInSeconds": 1.694417
}
