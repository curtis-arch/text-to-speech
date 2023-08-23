from dataclasses import field, dataclass
from typing import Optional

from dataclasses_json import config, dataclass_json, Undefined


@dataclass_json(undefined=Undefined.EXCLUDE)
@dataclass
class UserProfile:
    id: str
    nickname: Optional[str] = field(default=None)
    about: Optional[str] = field(default=None)
    personal_page: Optional[str] = field(default=None, metadata=config(field_name="personalPage"))
    personal_photo: Optional[str] = field(default=None, metadata=config(field_name="personalPhoto"))
    creator_logo: Optional[str] = field(default=None, metadata=config(field_name="creatorLogo"))
    subscription_id: Optional[str] = field(default=None, metadata=config(field_name="subscriptionId"))
    subscription_name: Optional[str] = field(default=None, metadata=config(field_name="subscriptionName"))
    subscription_expiration: Optional[str] = field(default=None, metadata=config(field_name="subscriptionExpiration"))
    experimental: Optional[bool] = field(default=None)
    administrator: Optional[bool] = field(default=None)
    editor: Optional[bool] = field(default=None)
    website: Optional[str] = field(default=None)
    tip_html: Optional[str] = field(default=None, metadata=config(field_name="tipHTML"))
    cognito_username: Optional[str] = field(default=None, metadata=config(field_name="cognitoUsername"))
    created_at: Optional[str] = field(default=None, metadata=config(field_name="createdAt"))
    updated_at: Optional[str] = field(default=None, metadata=config(field_name="updatedAt"))