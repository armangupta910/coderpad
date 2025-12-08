from pydantic import BaseModel
from typing import Optional

class CreateRoomRequest(BaseModel):
    language: Optional[str] = "python"
    initial_code: Optional[str] = None


class CreateRoomResponse(BaseModel):
    room_id: str
    room_code: str
    language: str
    code: str
    max_participants: int
    websocket_url: str


class LeaveRoomRequest(BaseModel):
    participant_id: str


class RunCodeRequest(BaseModel):
    code: Optional[str] = None
    language: Optional[str] = "python"
    input: Optional[str] = None