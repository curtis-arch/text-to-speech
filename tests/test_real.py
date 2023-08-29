import json
import logging
from json import JSONDecodeError

import pytest
import requests
from requests import HTTPError

from chalicelib.entities.engine_config import ConversionConfig, MurfAIConfig
from chalicelib.entities.murf_ai import SynthesizeSpeechResponse

logger = logging.getLogger()
logger.setLevel(logging.INFO)


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
