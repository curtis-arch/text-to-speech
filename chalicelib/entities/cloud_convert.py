from dataclasses import field, dataclass
from typing import Optional, List

from dataclasses_json import dataclass_json, Undefined


@dataclass_json(undefined=Undefined.EXCLUDE)
@dataclass
class JobResultFile:
    filename: Optional[str] = field(default=None)
    size: Optional[int] = field(default=None)
    url: Optional[str] = field(default=None)


@dataclass_json(undefined=Undefined.EXCLUDE)
@dataclass
class JobResult:
    files: Optional[List[JobResultFile]] = field(default=None)


@dataclass_json(undefined=Undefined.EXCLUDE)
@dataclass
class JobTasks:
    name: str
    status: str
    operation: Optional[str] = field(default=None)
    result: Optional[JobResult] = field(default=None)


@dataclass_json(undefined=Undefined.EXCLUDE)
@dataclass
class JobStatusDetails:
    id: str
    status: str
    tasks: List[JobTasks]
    created_at: Optional[str] = field(default=None)
    started_at: Optional[str] = field(default=None)
    ended_at: Optional[str] = field(default=None)


@dataclass_json(undefined=Undefined.EXCLUDE)
@dataclass
class JobStatusEvent:
    event: str
    job: JobStatusDetails
