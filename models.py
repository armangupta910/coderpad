from fastapi import FastAPI, HTTPException, status, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict, List
from datetime import datetime, timedelta
import random
import string
import uuid
import json
from models import *
from request import *
from response import *


class Participant:
    def __init__(self, participant_id: str, room_id: str, client_id: str):
        self.id = participant_id
        self.room_id = room_id
        self.client_id = client_id
        self.connected = True
        self.last_seen_at = datetime.now()
        self.cursor_position = 0
        
class Room:
    def __init__(self, max_participants: int,room_code: str, language: str = "python", initial_code: str = None):
        self.id = str(uuid.uuid4())
        self.room_code = room_code
        self.language = language
        self.code = initial_code or self.get_initial_template(language)
        self.version = 0
        self.participants_count = 0
        self.max_participants = max_participants
        self.created_at = datetime.now()
        self.expires_at = datetime.now() + timedelta(hours=24)
        self.participants: Dict[str, 'Participant'] = {}
        self.websocket_connections: Dict[str, WebSocket] = {}
    
    @staticmethod
    def get_initial_template(language: str) -> str:
        templates = {
            "python": "def solution():\n    # Write your code here\n    pass\n\n# Test your solution\nif __name__ == '__main__':\n    result = solution()\n    print(result)"
        }
        return templates.get(language, "# Start coding here")