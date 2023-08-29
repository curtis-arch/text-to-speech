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
class PlayHTConfig:
    voice: str = field(default="en-US-JennyNeural")
    global_speed: Optional[int] = field(
        default=None, metadata=config(field_name="globalSpeed", exclude=lambda value: value is None))
    trim_silence: Optional[bool] = field(
        default=None, metadata=config(field_name="trimSilence", exclude=lambda value: value is None))
    narration_style: Optional[str] = field(
        default=None, metadata=config(field_name="narrationStyle", exclude=lambda value: value is None))


@dataclass_json(undefined=Undefined.EXCLUDE)
@dataclass
class ConversionConfig:
    api_key: str
    user_id: Optional[str] = field(default=None)
    murf_config: Optional[MurfAIConfig] = field(default=None)
    play_config: Optional[PlayHTConfig] = field(default=None)
