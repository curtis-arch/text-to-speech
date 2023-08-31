from dataclasses import dataclass

from dataclasses_json import dataclass_json, Undefined

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
    source: SynthesizeSpeechResponse
