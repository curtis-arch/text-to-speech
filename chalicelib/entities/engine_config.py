from dataclasses import field, dataclass
from typing import Optional

from dataclasses_json import dataclass_json, Undefined


@dataclass_json(undefined=Undefined.EXCLUDE)
@dataclass
class EngineConfig:
    engine: str
    voice_id: str
    style: Optional[str] = field(default=None)