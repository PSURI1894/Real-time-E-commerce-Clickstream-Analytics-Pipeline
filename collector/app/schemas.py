from pydantic import BaseModel, Field
from typing import Dict, Optional
class ClientPayload(BaseModel):
    user_id: str
    session_id: str
    event_type: str
