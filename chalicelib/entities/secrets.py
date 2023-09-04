from dataclasses import dataclass, field
from typing import Optional

from dataclasses_json import dataclass_json, Undefined, config


@dataclass_json(undefined=Undefined.EXCLUDE)
@dataclass
class Secrets:
    cloud_conversion_api_key: Optional[str] = field(default=None, metadata=config(field_name="cloudConversionApiKey"))