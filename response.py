from pydantic import BaseModel
from typing import Optional

class RoomStatusResponse(BaseModel):
    exists: bool
    room_id: Optional[str] = None
    language: Optional[str] = None
    participants: int
    max_participants: int
    is_full: bool


class JoinRoomRequest(BaseModel):
    client_id: Optional[str] = None


class JoinRoomResponse(BaseModel):
    participant_id: str
    role: str
    code: str
    version: int


class RunCodeResponse(BaseModel):
    stdout: str
    stderr: str
    exitCode: int
    timeMs: int