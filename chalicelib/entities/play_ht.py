from dataclasses import field, dataclass
from typing import Optional

from dataclasses_json import dataclass_json, Undefined, config


@dataclass_json(undefined=Undefined.EXCLUDE)
@dataclass
class ConversionJobStatusResponse:
    voice: str
    converted: bool
    audio_duration_seconds: Optional[int] = field(default=None, metadata=config(field_name="audioDuration"))
    audio_url: Optional[str] = field(default=None, metadata=config(field_name="audioUrl"))
    message: Optional[str] = field(default=None)
    error: Optional[bool] = field(default=None)
    error_message: Optional[str] = field(default=None, metadata=config(field_name="errorMessage"))


@dataclass_json(undefined=Undefined.EXCLUDE)
@dataclass
class ConversionJobCreatedResponse:
    status: str
    transcription_id: Optional[str] = field(default=None, metadata=config(field_name="transcriptionId"))
    content_length: Optional[int] = field(default=None, metadata=config(field_name="contentLength"))
    word_count: Optional[int] = field(default=None, metadata=config(field_name="wordCount"))
