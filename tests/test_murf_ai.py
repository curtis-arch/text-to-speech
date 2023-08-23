import io
import json
import os

from botocore.response import StreamingBody
from botocore.stub import Stubber
from chalice.test import Client
from pytest import fixture

import app
from chalicelib.entities.engine_config import EngineConfig

bucket_name = "test_bucket"


@fixture
def test_client() -> Client:
    with Client(app.app) as client:
        yield client


@fixture
def s3_stub() -> Stubber:
    client = app.get_s3_client()
    stubbed_client = Stubber(client)
    with stubbed_client:
        yield stubbed_client


def test_success(monkeypatch, test_client: Client, s3_stub: Stubber):
    monkeypatch.setenv("INPUT_BUCKET_NAME", bucket_name)

    text_object_key = "path/sub/john.txt"
    engine_config = EngineConfig(engine='murf.ai', voice_id="male")
    _setup_stubs(
        stubbed_client=s3_stub, text_object_key=text_object_key, engine_config=engine_config
    )

    event = test_client.events.generate_s3_event(bucket=bucket_name, key=text_object_key)
    response = test_client.lambda_.invoke('on_text_input_file', event)
    assert response.payload == {'result': 'success'}
    s3_stub.assert_no_pending_responses()


def _setup_stubs(stubbed_client: Stubber, text_object_key: str, engine_config: EngineConfig) -> None:
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

