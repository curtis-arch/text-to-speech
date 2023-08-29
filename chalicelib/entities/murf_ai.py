from dataclasses import field, dataclass
from typing import Optional, List

from dataclasses_json import dataclass_json, Undefined, config


@dataclass_json(undefined=Undefined.EXCLUDE)
@dataclass
class WordDuration:
    word: Optional[str] = field(default=None, metadata=config(field_name="audioFile"))
    start_ms: Optional[int] = field(default=None, metadata=config(field_name="audioFile"))
    end_ms: Optional[int] = field(default=None, metadata=config(field_name="audioFile"))


@dataclass_json(undefined=Undefined.EXCLUDE)
@dataclass
class SynthesizeSpeechResponse:
    audio_file: str = field(metadata=config(field_name="audioFile"))
    audio_length_seconds: Optional[int] = field(default=None, metadata=config(field_name="audioLengthInSeconds"))