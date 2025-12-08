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

def generate_room_code(rooms: Dict[str, Room], length: int = 6) -> str:
    """Generate a random room code"""
    while True:
        code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))
        if code not in rooms:
            return code


def cleanup_expired_rooms(rooms: Dict[str, Room]):
    """Remove expired rooms (can be called periodically)"""
    now = datetime.now()
    expired = [code for code, room in rooms.items() if room.expires_at < now]
    for code in expired:
        del rooms[code]
    return len(expired)


async def broadcast_to_room(rooms: Dict[str, Room], room_code: str, message: dict, exclude_client: str = None):
    """Broadcast a message to all participants in a room"""
    room = rooms.get(room_code)
    if not room:
        return
    
    disconnected_clients = []
    
    for client_id, ws in room.websocket_connections.items():
        if exclude_client and client_id == exclude_client:
            continue
        
        try:
            await ws.send_json(message)
        except Exception as e:
            print(f"Error sending to {client_id}: {e}")
            disconnected_clients.append(client_id)
    
    # Clean up disconnected clients
    for client_id in disconnected_clients:
        if client_id in room.websocket_connections:
            del room.websocket_connections[client_id]