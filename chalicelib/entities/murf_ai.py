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
    encoded_audio: Optional[str] = field(default=None, metadata=config(field_name="encodedAudio"))
    audio_length_seconds: Optional[int] = field(default=None, metadata=config(field_name="audioLengthInSeconds"))
    word_durations: Optional[List[WordDuration]] = field(default=None, metadata=config(field_name="wordDurations"))
    consumed_character_count: Optional[int] = field(default=None, metadata=config(field_name="consumedCharacterCount"))
    remaining_character_count: Optional[int] = field(default=None, metadata=config(field_name="remainingCharacterCount"))