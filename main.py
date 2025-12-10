from fastapi import FastAPI, HTTPException, status, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict, List, Any
from datetime import datetime, timedelta
import random
import string
import uuid
import json
import uvicorn
import asyncio
import ssl, certifi
from models import Room, Participant    # <-- your model classes
from request import *
from response import *
from utils import *
import redis.asyncio as aioredis

ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

UPSTASH_URL = "rediss://default:AZloAAIncDI1MzZlYTUxOTEwZTg0MmNlOTM5ZmU4NWY3MjVkYjdhYXAyMzkyNzI@excited-piranha-39272.upstash.io:6379"
ssl_context = ssl.create_default_context(cafile=certifi.where())
app = FastAPI(title="Collaborative Code Editor API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-process map to hold WebSocket connections (not persisted)
# mapping: room_code -> { client_id: WebSocket }
in_memory_ws: Dict[str, Dict[str, WebSocket]] = {}

redis_client: Optional[aioredis.Redis] = None
REDIS_ROOM_KEY = "room:{}"  # format room_code

def participant_to_dict(p: Participant) -> dict:
    return {
        "id": p.id,
        "room_id": p.room_id,
        "client_id": p.client_id,
        "connected": getattr(p, "connected", True),
        "last_seen_at": getattr(p, "last_seen_at", datetime.utcnow()).isoformat(),
        "cursor_position": getattr(p, "cursor_position", 0),
    }

def participant_from_dict(d: dict) -> Participant:
    p = Participant(d["id"], d["room_id"], d["client_id"])
    p.connected = d.get("connected", True)
    if d.get("last_seen_at"):
        p.last_seen_at = datetime.fromisoformat(d["last_seen_at"])
    p.cursor_position = d.get("cursor_position", 0)
    return p

def room_to_dict(r: Room) -> dict:
    # Keep only serializable fields; do NOT include websocket objects
    return {
        "id": getattr(r, "id", str(uuid.uuid4())),
        "room_code": r.room_code,
        "language": getattr(r, "language", "python"),
        "code": getattr(r, "code", None),
        "version": getattr(r, "version", 0),
        "participants_count": getattr(r, "participants_count", 0),
        "max_participants": getattr(r, "max_participants", 2),
        "created_at": getattr(r, "created_at", datetime.utcnow()).isoformat(),
        "expires_at": getattr(r, "expires_at", (datetime.utcnow() + timedelta(hours=24))).isoformat(),
        "participants": {pid: participant_to_dict(p) for pid, p in getattr(r, "participants", {}).items()},
    }

def room_from_dict(d: dict) -> Room:
    max_participants = d.get("max_participants", 2)
    room_code = d["room_code"]
    r = Room(max_participants=max_participants, room_code=room_code, language=d.get("language", "python"), initial_code=d.get("code"))
    r.id = d.get("id", r.id)
    r.version = d.get("version", 0)
    r.participants_count = d.get("participants_count", len(d.get("participants", {})))
    r.created_at = datetime.fromisoformat(d["created_at"]) if d.get("created_at") else datetime.utcnow()
    r.expires_at = datetime.fromisoformat(d["expires_at"]) if d.get("expires_at") else (datetime.utcnow() + timedelta(hours=24))
    # reconstruct participants
    r.participants = {pid: participant_from_dict(pd) for pid, pd in d.get("participants", {}).items()}
    # websocket_connections will be created from in_memory_ws when needed — do not set on Room
    if hasattr(r, "websocket_connections"):
        delattr(r, "websocket_connections")
    return r


# -------------------------
# Redis helpers
# -------------------------
async def create_redis_client():
    return aioredis.from_url(
        UPSTASH_URL,
        encoding="utf-8",
        decode_responses=True,
        ssl_cert_reqs="required",
        ssl_ca_certs=certifi.where()
    )

async def save_room_to_redis(r: Room):
    key = REDIS_ROOM_KEY.format(r.room_code) # key = room:ABC123
    payload = json.dumps(room_to_dict(r)) # value = Room class object
    await redis_client.set(key, payload)
    # set expiry
    await redis_client.expireat(key, int(r.expires_at.timestamp()))
    return True

async def fetch_room_from_redis(room_code: str) -> Optional[Room]:
    key = REDIS_ROOM_KEY.format(room_code) # key = room:ABC123
    raw = await redis_client.get(key)
    if raw is None:
        return None
    data = json.loads(raw)
    return room_from_dict(data)

async def delete_room_from_redis(room_code: str):
    key = REDIS_ROOM_KEY.format(room_code)
    await redis_client.delete(key)

# atomic update (version check) for code edits
async def update_room_code_with_version(room_code: str, new_code: str, expected_version: int):
    key = REDIS_ROOM_KEY.format(room_code)
    # Use WATCH / MULTI to implement optimistic locking
    async with redis_client.pipeline() as pipe:
        while True:
            try:
                await pipe.watch(key)
                raw = await pipe.get(key)
                if raw is None:
                    await pipe.unwatch()
                    return False, "room-not-found", None
                data = json.loads(raw)
                current_version = data.get("version", 0)
                # if current_version != expected_version:
                #     await pipe.unwatch()
                #     return False, "version-mismatch", current_version
                # mutate
                data["code"] = new_code
                data["version"] = current_version + 1
                # start transaction
                pipe.multi()
                pipe.set(key, json.dumps(data))
                if data.get("expires_at"):
                    pipe.expireat(key, int(datetime.fromisoformat(data["expires_at"]).timestamp()))
                await pipe.execute()
                # success
                return True, None, data["version"]
            except aioredis.WatchError:
                # someone changed between watch and exec, retry
                continue
            finally:
                try:
                    await pipe.reset()
                except Exception:
                    pass

# participant helper
async def add_participant_to_room(room_code: str, participant: Participant):
    room = await fetch_room_from_redis(room_code) # Again Fetching from Resis
    if room is None:
        return False, "room-not-found", None
    if room.participants_count >= room.max_participants:
        return False, "room-full", None
    room.participants[participant.id] = participant
    room.participants_count = len(room.participants)
    room.version += 1
    await save_room_to_redis(room)
    return True, None, room

async def remove_participant_from_room(room_code: str, participant_id: str):
    room = await fetch_room_from_redis(room_code)
    if room is None:
        return False, "room-not-found", None
    if participant_id in room.participants:
        del room.participants[participant_id]
        room.participants_count = len(room.participants)
        room.version += 1
        await save_room_to_redis(room)
    return True, None, room


# -------------------------
# Startup / Shutdown
# -------------------------
@app.on_event("startup")
async def startup():
    global redis_client
    redis_client = await create_redis_client()
    print("Connected to Redis")


@app.on_event("shutdown")
async def shutdown():
    global redis_client
    if redis_client:
        await redis_client.close()


# -------------------------
# API endpoints (async)
# -------------------------
@app.get("/")
def root():
    return {
        "message": "Collaborative Code Editor API",
        "version": "1.0.0",
        "endpoints": {
            "create_room": "POST /rooms",
            "get_room_status": "GET /rooms/{room_code}/status",
            "join_room": "POST /rooms/{room_code}/join",
            "leave_room": "POST /rooms/{room_code}/leave",
            "run_code": "POST /rooms/{room_code}/run",
            "websocket": "WS /ws/rooms/{room_code}"
        }
    }


@app.post("/rooms/{max_participants}", response_model=CreateRoomResponse, status_code=status.HTTP_201_CREATED)
async def create_room(max_participants: int, request: CreateRoomRequest):
    """Create a new collaborative coding room"""
    room_code = generate_room_code({})  # pass empty, generation must just ensure uniqueness or check redis
    room = Room(
        max_participants=max_participants,
        room_code=room_code,
        language=request.language,
        initial_code=request.initial_code
    )
    # Persist to Redis
    await save_room_to_redis(room)

    # Initialize in-memory websocket map for this room
    in_memory_ws.setdefault(room_code, {})

    websocket_url = f"ws://localhost:8000/rooms/{room_code}"
    return CreateRoomResponse(
        room_id=room.id,
        room_code=room.room_code,
        language=room.language,
        code=room.code,
        max_participants=room.max_participants,
        websocket_url=websocket_url
    )


@app.get("/rooms/{room_code}/status", response_model=RoomStatusResponse)
async def get_room_status(room_code: str):
    print("Trying to get room status")
    room = await fetch_room_from_redis(room_code)
    print("Room Status - " + str(room))
    if not room:
        return RoomStatusResponse(
            exists=False,
            participants=0,
            max_participants=3,
            is_full=False
        )
    return RoomStatusResponse(
        exists=True,
        room_id=room.id,
        language=room.language,
        participants=room.participants_count,
        max_participants=room.max_participants,
        is_full=room.participants_count >= room.max_participants
    )


@app.post("/rooms/{room_code}/join", response_model=JoinRoomResponse)
async def join_room(room_code: str, request: JoinRoomRequest):
    """Join an existing room"""
    room = await fetch_room_from_redis(room_code)
    if not room:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Room not found")

    if room.participants_count >= room.max_participants:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail={"error": "ROOM_FULL", "message": "Room is full."})

    participant_id = request.client_id or str(uuid.uuid4())
    client_id = request.client_id or participant_id

    participant = Participant(participant_id=participant_id, room_id=room.id, client_id=client_id)
    ok, reason, new_room = await add_participant_to_room(room_code, participant)
    if not ok:
        if reason == "room-full":
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Room full")
        else:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=reason)

    role = "owner" if new_room.participants_count == 1 else "participant"
    return JoinRoomResponse(
        participant_id=participant_id,
        role=role,
        code=new_room.code,
        version=new_room.version
    )


@app.post("/rooms/{room_code}/leave")
async def leave_room(room_code: str, request: LeaveRoomRequest):
    room = await fetch_room_from_redis(room_code)
    if not room:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Room not found")

    ok, reason, updated_room = await remove_participant_from_room(room_code, request.participant_id)
    if not ok:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=reason)

    # Remove WebSocket in-memory if present
    conns = in_memory_ws.get(room_code, {})
    # participant may use client_id or participant_id in your code; try both
    conns.pop(request.participant_id, None)
    # cleanup if no participants and no websockets
    if updated_room.participants_count == 0:
        await delete_room_from_redis(room_code)
        in_memory_ws.pop(room_code, None)

    return {"message": "Left room successfully", "participants_remaining": updated_room.participants_count}


@app.post("/rooms/{room_code}/run", response_model=RunCodeResponse)
async def run_code(room_code: str, request: RunCodeRequest):
    room = await fetch_room_from_redis(room_code)
    if not room:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Room not found")
    code = request.code if request.code is not None else room.code
    # simulate execution (same as your original)
    import time, sys
    from io import StringIO

    stdout_capture = StringIO()
    stderr_capture = StringIO()
    exit_code = 0
    start_time = time.time()
    try:
        old_stdout, old_stderr = sys.stdout, sys.stderr
        sys.stdout, sys.stderr = stdout_capture, stderr_capture
        exec(code, {"__name__": "__main__"})
    except Exception as e:
        stderr_capture.write(f"{type(e).__name__}: {str(e)}")
        exit_code = 1
    finally:
        sys.stdout, sys.stderr = old_stdout, old_stderr

    end_time = time.time()
    execution_time_ms = int((end_time - start_time) * 1000)
    return RunCodeResponse(stdout=stdout_capture.getvalue(), stderr=stderr_capture.getvalue(), exitCode=exit_code, timeMs=execution_time_ms)


@app.delete("/rooms/{room_code}")
async def delete_room(room_code: str):
    room = await fetch_room_from_redis(room_code)
    if not room:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Room not found")
    await delete_room_from_redis(room_code)
    in_memory_ws.pop(room_code, None)
    return {"message": f"Room {room_code} deleted successfully"}


@app.get("/rooms")
async def list_rooms():
    """
    NOTE: listing all rooms requires scanning Redis keys (SCAN).
    This is intended for debug / admin only.
    """
    rooms_list = []
    cursor = "0"
    # SCAN - use the async scan_iter helper
    async for key in redis_client.scan_iter(match="room:*"):
        raw = await redis_client.get(key)
        if not raw:
            continue
        d = json.loads(raw)
        rooms_list.append({
            "room_code": d.get("room_code"),
            "room_id": d.get("id"),
            "language": d.get("language"),
            "participants": d.get("participants_count"),
            "connected_ws": len(in_memory_ws.get(d.get("room_code"), {})),
            "created_at": d.get("created_at"),
            "expires_at": d.get("expires_at")
        })
    return {"total_rooms": len(rooms_list), "rooms": rooms_list}


# -------------------------
# WebSocket endpoint
# -------------------------
@app.websocket("/ws/rooms/{room_code}")
async def websocket_endpoint(websocket: WebSocket, room_code: str):
    await websocket.accept()
    print("Web Socket connection accepted - ", str(websocket))
    room = await fetch_room_from_redis(room_code)
    if not room:
        await websocket.send_json({"type": "ERROR", "message": "Room not found"})
        await websocket.close()
        return

    # ensure in_memory map exists
    in_memory_ws.setdefault(room_code, {})
    client_id = None
    participant_id = None

    try:
        init_message = await websocket.receive_json()
        if init_message.get("type") != "INIT":
            await websocket.send_json({"type": "ERROR", "message": "Expected INIT message"})
            await websocket.close()
            return

        client_id = init_message.get("clientId")
        participant_id = init_message.get("participantId") or client_id

        if not client_id:
            await websocket.send_json({"type": "ERROR", "message": "clientId required"})
            await websocket.close()
            return

        # Check if participant exists in Redis; if not, add (auto-join)
        current_room = await fetch_room_from_redis(room_code) # Fetching the room secind time, even if you fetched it already above, why not reuse it?
        if participant_id not in current_room.participants:
            if current_room.participants_count >= current_room.max_participants:
                await websocket.send_json({"type": "ERROR", "message": "Room is full"})
                await websocket.close()
                return
            participant = Participant(participant_id=participant_id, room_id=current_room.id, client_id=client_id)
            ok, reason, current_room = await add_participant_to_room(room_code, participant)
            if not ok:
                await websocket.send_json({"type": "ERROR", "message": reason})
                await websocket.close()
                return

        # register ws in-memory
        in_memory_ws[room_code][client_id] = websocket

        # Send current state
        await websocket.send_json({
            "type": "STATE",
            "code": current_room.code,
            "version": current_room.version,
            "participants": current_room.participants_count
        })

        # Notify others
        async def broadcast(data: dict, exclude_client: Optional[str] = None):
            conns = in_memory_ws.get(room_code, {})
            to_remove = []
            for cid, ws in conns.items():
                if cid == exclude_client:
                    continue
                try:
                    await ws.send_json(data)
                except Exception:
                    to_remove.append(cid)
            for cid in to_remove:
                conns.pop(cid, None)

        await broadcast({"type": "PARTICIPANT_JOINED", "clientId": client_id, "participantCount": current_room.participants_count}, exclude_client=client_id)

        import time

        # Listen loop
        while True:
            message = await websocket.receive_json()
            message_type = message.get("type")

            if message_type == "EDIT":
                start = time.perf_counter()   # ⏱ Start timing
                new_code = message.get("code")
                expected_version = message.get("expected_version", current_room.version)
                if new_code is None:
                    continue

                ok, reason, new_version_or_current = await update_room_code_with_version(room_code, new_code, expected_version)
                current_room = await fetch_room_from_redis(room_code)
                end1 = time.perf_counter()
                print(f"[Redis Write block executed in {end1 - start:.6f} seconds]")
                await broadcast({
                    "type": "PATCH",
                    "code": current_room.code,
                    "version": current_room.version,
                        "clientId": client_id
                    }, exclude_client=client_id)
            
                end = time.perf_counter()     # ⏱ End timing
                print(f"[Broadcast block executed in {end - end1:.6f} seconds]")

            elif message_type == "CURSOR":
                # broadcast cursor update
                print("cursor position received - ", str(message.get("position")), client_id)
                await broadcast({
                    "type": "CURSOR",
                    "clientId": client_id,
                    "position": message.get("position"),
                    "selection": message.get("selection")
                }, exclude_client=client_id)

            elif message_type == "PING":
                await websocket.send_json({"type": "PONG"})

    except WebSocketDisconnect:
        pass
    except Exception as e:
        print("WebSocket error:", e)
    finally:
        # cleanup
        if client_id:
            # remove from in-memory ws map
            conns = in_memory_ws.get(room_code, {})
            conns.pop(client_id, None)

            # try removing participant from Redis
            try:
                await remove_participant_from_room(room_code, participant_id or client_id)
                # reload room to check count
                updated_room = await fetch_room_from_redis(room_code)
                if not updated_room or updated_room.participants_count == 0:
                    await delete_room_from_redis(room_code)
                    in_memory_ws.pop(room_code, None)
                else:
                    # notify others
                    conns = in_memory_ws.get(room_code, {})
                    for cid, ws in conns.items():
                        try:
                            await ws.send_json({"type": "PARTICIPANT_LEFT", "clientId": client_id, "participantCount": updated_room.participants_count})
                        except Exception:
                            pass
            except Exception:
                pass


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
