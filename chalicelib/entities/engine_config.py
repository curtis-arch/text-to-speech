from dataclasses import field, dataclass
from typing import Optional

from dataclasses_json import dataclass_json, Undefined, config


@dataclass_json(undefined=Undefined.EXCLUDE)
@dataclass
class MurfAIConfig:
    voice_id: str = field(default="en-US-cooper", metadata=config(field_name="voiceId"))
    style: str = field(default="Conversational")
    rate: int = field(default=0)
    pitch: int = field(default=0)
    sample_rate: int = field(default=24000, metadata=config(field_name="sampleRate"))
    format: str = field(default="MP3")
    channel_type: str = field(default="STEREO", metadata=config(field_name="channelType"))


@dataclass_json(undefined=Undefined.EXCLUDE)
@dataclass
class ConversionConfig:
    token: str
    murf_config: Optional[MurfAIConfig]
