from dataclasses import dataclass, field
from typing import Optional

from dataclasses_json import dataclass_json, Undefined, config

from chalicelib.entities.cloud_convert import JobResultFile
from chalicelib.entities.murf_ai import SynthesizeSpeechResponse


@dataclass_json(undefined=Undefined.EXCLUDE)
@dataclass
class ConversionJobConfig:
    bucket: str
    config_object_key: str


@dataclass_json(undefined=Undefined.EXCLUDE)
@dataclass
class ConversionJob:
    job_id: str
    config: ConversionJobConfig


@dataclass_json(undefined=Undefined.EXCLUDE)
@dataclass
class DownloadTask:
    destination_bucket: str
    destination_key: str
    speech_synthesized_response: Optional[SynthesizeSpeechResponse] = field(
        default=None, metadata=config(exclude=lambda value: value is None))
    job_result_file: Optional[JobResultFile] = field(
        default=None, metadata=config(exclude=lambda value: value is None))
