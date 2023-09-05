import json
import logging
import os
from json import JSONDecodeError

import pytest
import requests
from requests import HTTPError

from chalicelib.entities.engine_config import ConversionConfig, MurfAIConfig
from chalicelib.entities.murf_ai import SynthesizeSpeechResponse

logger = logging.getLogger()
logger.setLevel(logging.INFO)


@pytest.mark.skip(reason="requires python3.3")
def test_multipart():
    parts = []
    buffer_size = int(os.environ.get("MULTIPART_UPLOAD_BUFFER_SIZE", str(10 * 1024 * 1024)))  # 10 MB default
    buffer = bytearray(buffer_size)
    file_url = "https://storage.cloudconvert.com/tasks-sandbox/85fd2dac-a0e7-4c9e-ae2c-2c58ae83ca7f/mov_test.mp4?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=cloudconvert-sandbox%2F20230905%2Ffra%2Fs3%2Faws4_request&X-Amz-Date=20230905T192932Z&X-Amz-Expires=86400&X-Amz-Signature=cf7610f9d6dd268371a43c7fcae522ceb9bd0e94931df00a614feb34290b972e&X-Amz-SignedHeaders=host&response-content-disposition=attachment%3B%20filename%3D%22mov_test.mp4%22&response-content-type=video%2Fmp4&x-id=GetObject"
    logger.info(f"About to GET {file_url}")
    with requests.get(file_url, stream=True) as response:
        response.raise_for_status()
        logger.info("Starting to stream response ..")

        for count, chunk in enumerate(response.iter_content(chunk_size=buffer_size)):
            if chunk:
                buffer[:len(chunk)] = chunk

                part_number = len(parts) + 1
                logger.info(f"Uploading part {part_number}")

                parts.append({
                    "PartNumber": part_number,
                    "ETag": "1"
                })


@pytest.mark.skip(reason="requires python3.3")
def test_murf():
    api_key = "api_ca9f5035-b4ae-4cd6-a304-0ac934743bc3"
    config = MurfAIConfig(voice_id="en-UK-hazel")

    url = "https://api.murf.ai/v1/speech/generate-with-key"
    headers = {
        "Content-Type": "application/json",
        "api-key": api_key,
        "Accept": "application/json",
    }

    data = {**config.to_dict(), "text": "A quick brown fox jumps over a lazy dog.", "pronunciationDictionary": {}, "encodeAsBase64": False}

    logger.info(f"About to POST to {url}. data: {data}")
    try:
        pass
        #response = requests.post(url, json=data, headers=headers)
        #response.raise_for_status()
    except HTTPError as err:
        logger.exception("Failed to generate speech using murf.ai.")
        if err.response is not None:
            logger.info(err.response.text)
            try:
                error_response = json.loads(err.response.text)
                raise ValueError("Failed (json)")
            except JSONDecodeError:
                raise ValueError("Failed (no json)")
        else:
            raise ValueError("Failed (data)")
    else:
        logger.info(f"Response: {response.text}")
        try:
            response = SynthesizeSpeechResponse.from_dict(response.json())
            logger.info(response.to_dict())
        except Exception:
            logger.exception("Response does not map to a SynthesizeSpeechResponse.")
            raise ValueError("Failed (responded)")
