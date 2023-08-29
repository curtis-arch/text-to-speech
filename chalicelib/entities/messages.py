from dataclasses import dataclass

from dataclasses_json import dataclass_json, Undefined


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
