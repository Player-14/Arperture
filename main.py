from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Depends, UploadFile, File, status
from fastapi.staticfiles import StaticFiles
from fastapi.responses import StreamingResponse
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, Table, DateTime, Text
from sqlalchemy.orm import declarative_base, sessionmaker, relationship, Session
from passlib.context import CryptContext
from jose import JWTError, jwt
import asyncio
import httpx
import qbittorrentapi
from pyarr import LidarrAPI
import re
import time
from typing import Dict, List, Optional, Tuple
import json
import os
import shutil
from datetime import datetime, timedelta

# --- 1. Security & Database Setup ---
SECRET_KEY = os.getenv("SECRET_KEY", "super-secret-spectra-key-change-this-in-production")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24 * 7

# Store all files and databases inside the config/arperture mount folder
CONFIG_DIR = "/app/config/arperture"
os.makedirs(CONFIG_DIR, exist_ok=True)

DATABASE_URL = f"sqlite:///{CONFIG_DIR}/spectra.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")

# Association Tables
friendship_table = Table(
    "friendships",
    Base.metadata,
    Column("user_id", Integer, ForeignKey("users.id"), primary_key=True),
    Column("friend_id", Integer, ForeignKey("users.id"), primary_key=True)
)

request_requesters = Table(
    "request_requesters",
    Base.metadata,
    Column("request_id", Integer, ForeignKey("game_requests.id"), primary_key=True),
    Column("user_id", Integer, ForeignKey("users.id"), primary_key=True)
)

class UserDB(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True)
    hashed_password = Column(String)
    status = Column(String, default="Offline")
    current_activity = Column(String, default="")
    
    friends = relationship(
        "UserDB",
        secondary=friendship_table,
        primaryjoin=id==friendship_table.c.user_id,
        secondaryjoin=id==friendship_table.c.friend_id,
    )

class GameDB(Base):
    __tablename__ = "games"
    id = Column(Integer, primary_key=True, index=True)
    title = Column(String, nullable=False)
    platform = Column(String, nullable=False)
    nexus_domain = Column(String, nullable=True) # Automatic slug mapping for Nexus Mods
    executable_path = Column(String)
    rom_path = Column(String)
    emulator_id = Column(Integer, default=0)
    is_installed = Column(Integer, default=0)
    is_smb = Column(Integer, default=0)
    smb_unc_path = Column(String)
    cover_art = Column(String)
    hero_banner = Column(String)
    logo_url = Column(String)
    icon_url = Column(String)
    trailer_url = Column(String)
    micro_trailer_path = Column(String)
    hltb_main = Column(Float, default=0.0)
    hltb_extra = Column(Float, default=0.0)
    hltb_comp = Column(Float, default=0.0)
    req_cpu = Column(String)
    req_gpu = Column(String)
    req_ram_mb = Column(Integer, default=8192)
    install_size_bytes = Column(Integer, default=0)
    last_played = Column(Integer, default=0)
    total_playtime_minutes = Column(Integer, default=0)
    tags = Column(Text)

class GameModDB(Base):
    __tablename__ = "game_mods"
    id = Column(Integer, primary_key=True, index=True)
    game_id = Column(Integer, ForeignKey("games.id"))
    nexus_mod_id = Column(Integer, nullable=True)
    nexus_domain = Column(String, nullable=True)
    mod_name = Column(String, nullable=False)
    version = Column(String, default="1.0")
    author = Column(String, default="Unknown")
    thumbnail_url = Column(String, nullable=True)
    file_path = Column(String, nullable=False)
    enabled = Column(Integer, default=1)

class GameRequestDB(Base):
    __tablename__ = "game_requests"
    id = Column(Integer, primary_key=True, index=True)
    title = Column(String, nullable=False)
    platform = Column(String, nullable=False)
    status = Column(String, default="Pending")
    created_at = Column(DateTime, default=datetime.utcnow)
    requesters = relationship("UserDB", secondary=request_requesters, backref="game_requests")

class TelemetryDB(Base):
    __tablename__ = "telemetry"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    game_title = Column(String)
    duration_seconds = Column(Integer)
    timestamp = Column(DateTime, default=datetime.utcnow)

Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)) -> UserDB:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id: int = payload.get("sub")
        if user_id is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    
    user = db.query(UserDB).filter(UserDB.id == user_id).first()
    if user is None:
        raise credentials_exception
    return user

# --- 2. Environment & External Clients ---
QBIT_HOST = os.getenv("QBIT_HOST", "http://localhost:8080")
QBIT_USER = os.getenv("QBIT_USER", "admin")
QBIT_PASS = os.getenv("QBIT_PASS", "adminpassword")

PROWLARR_HOST = os.getenv("PROWLARR_HOST", "http://localhost:9696")
PROWLARR_API_KEY = os.getenv("PROWLARR_API_KEY", "your_prowlarr_api_key")

LIDARR_HOST = os.getenv("LIDARR_HOST", "http://localhost:8686")
LIDARR_API_KEY = os.getenv("LIDARR_API_KEY", "your_lidarr_api_key")

SEERR_HOST = os.getenv("SEERR_HOST", "http://localhost:5055")
SEERR_API_KEY = os.getenv("SEERR_API_KEY", "your_seerr_api_key")

JELLYFIN_HOST = os.getenv("JELLYFIN_HOST", "http://localhost:8096")
JELLYFIN_API_KEY = os.getenv("JELLYFIN_API_KEY", "your_jellyfin_api_key")

NEXUS_API_KEY = os.getenv("NEXUS_API_KEY", "your_nexus_personal_api_key")
NEXUS_REST_BASE = "https://api.nexusmods.com/v1"
NEXUS_GRAPHQL_URL = "https://api.nexusmods.com/v2/graphql"

qbt_client = None
try:
    qbt_client = qbittorrentapi.Client(host=QBIT_HOST, username=QBIT_USER, password=QBIT_PASS)
    qbt_client.auth_log_in()
except Exception:
    pass

lidarr_client = None
try:
    lidarr_client = LidarrAPI(LIDARR_HOST, LIDARR_API_KEY)
except Exception:
    pass

app = FastAPI(title="Spectra Ecosystem Server", version="9.0.0")

# Directories mapped inside config/arperture and shared storage
GAME_STORAGE_DIR = "/app/shared_games"
ASSET_STORAGE_DIR = f"{CONFIG_DIR}/assets"
MOD_STORAGE_DIR = f"{CONFIG_DIR}/mods"
os.makedirs(GAME_STORAGE_DIR, exist_ok=True)
os.makedirs(ASSET_STORAGE_DIR, exist_ok=True)
os.makedirs(MOD_STORAGE_DIR, exist_ok=True)

app.mount("/downloads", StaticFiles(directory=GAME_STORAGE_DIR), name="downloads")
app.mount("/assets", StaticFiles(directory=ASSET_STORAGE_DIR), name="assets")
app.mount("/config/mods", StaticFiles(directory=MOD_STORAGE_DIR), name="mods")

nexus_cache: Dict[str, Tuple[float, dict]] = {}
CACHE_TTL_SECONDS = 1800


# --- Helper: Automatic Game Title to Nexus Domain Slug Mapper ---
def generate_nexus_domain(title: str) -> str:
    """
    Converts a game title into a clean Nexus Mods domain slug.
    Example: 'Cyberpunk 2077' -> 'cyberpunk2077'
    Example: 'The Witcher 3: Wild Hunt' -> 'thewitcher3wildhunt' (or manual mapping override)
    """
    # Remove special characters and lowercase
    slug = re.sub(r'[^a-zA-Z0-9]', '', title).lower()
    return slug


# --- 3. Pydantic Schemas ---
class UserCreate(BaseModel):
    username: str
    password: str

class UserLogin(BaseModel):
    username: str
    password: str

class GameAddRequest(BaseModel):
    title: str
    platform: str
    cover_art: Optional[str] = None
    hero_banner: Optional[str] = None
    trailer_url: Optional[str] = None

class GameRequestCreate(BaseModel):
    title: str
    platform: str

class TorrentAddRequest(BaseModel):
    magnet_url: str

class TorrentHashRequest(BaseModel):
    torrent_hash: str

class MediaRequest(BaseModel):
    media_id: int
    media_type: str
    seasons: List[int] = []

class NexusInstallRequest(BaseModel):
    game_id: int
    nexus_domain: str
    mod_id: int
    file_id: int
    mod_name: str
    version: str = "1.0"
    author: str = "Unknown"
    thumbnail_url: Optional[str] = None

class TelemetryReport(BaseModel):
    game_title: str
    duration_seconds: int


# --- 4. WebSocket Connection Manager ---
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[int, WebSocket] = {}

    async def connect(self, user_id: int, websocket: WebSocket):
        await websocket.accept()
        self.active_connections[user_id] = websocket

    def disconnect(self, user_id: int):
        if user_id in self.active_connections:
            del self.active_connections[user_id]

    async def broadcast_to_friends(self, user_id: int, payload: dict, db: Session):
        user = db.query(UserDB).filter(UserDB.id == user_id).first()
        if not user:
            return
        for friend in user.friends:
            if friend.id in self.active_connections:
                await self.active_connections[friend.id].send_json(payload)

manager = ConnectionManager()


# --- 5. Background Polling Tasks ---
async def poll_torrent_progress():
    while True:
        try:
            if qbt_client:
                torrents = qbt_client.torrents_info()
                progress_list = [{
                    "hash": t.hash,
                    "name": t.name,
                    "progress": round(t.progress * 100, 2),
                    "dlspeed": t.dlspeed,
                    "state": t.state,
                    "eta": t.eta
                } for t in torrents]
                
                if progress_list:
                    payload = {"type": "download_progress_sync", "torrents": progress_list}
                    for ws in manager.active_connections.values():
                        await ws.send_json(payload)
        except Exception:
            pass
        await asyncio.sleep(2)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(poll_torrent_progress())


# --- 6. Auth Endpoints ---
@app.post("/api/auth/register")
def register_user(payload: UserCreate, db: Session = Depends(get_db)):
    existing = db.query(UserDB).filter(UserDB.username == payload.username).first()
    if existing:
        raise HTTPException(status_code=400, detail="Username already taken")
    
    hashed_pw = pwd_context.hash(payload.password)
    user = UserDB(username=payload.username, hashed_password=hashed_pw)
    db.add(user)
    db.commit()
    db.refresh(user)
    return {"status": "success", "user_id": user.id, "username": user.username}

@app.post("/api/auth/login")
def login_user(payload: UserLogin, db: Session = Depends(get_db)):
    user = db.query(UserDB).filter(UserDB.username == payload.username).first()
    if not user or not pwd_context.verify(payload.password, user.hashed_password):
        raise HTTPException(status_code=401, detail="Invalid username or password")
    
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode = {"sub": user.id, "exp": expire}
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    
    return {
        "access_token": encoded_jwt,
        "token_type": "bearer",
        "user_id": user.id,
        "username": user.username
    }


# --- 7. Social & Friend System ---
@app.post("/api/friends/add")
def add_friend(friend_username: str, current_user: UserDB = Depends(get_current_user), db: Session = Depends(get_db)):
    friend = db.query(UserDB).filter(UserDB.username == friend_username).first()
    if not friend:
        raise HTTPException(status_code=404, detail="Friend username not found")
    if friend.id == current_user.id:
        raise HTTPException(status_code=400, detail="Cannot add yourself as a friend")
    
    if friend not in current_user.friends:
        current_user.friends.append(friend)
        friend.friends.append(current_user)
        db.commit()
    return {"status": "success", "message": f"Added {friend_username} as a friend."}

@app.get("/api/friends/list")
def get_friends_list(current_user: UserDB = Depends(get_current_user)):
    return [{
        "id": f.id, 
        "username": f.username, 
        "status": f.status, 
        "activity": f.current_activity
    } for f in current_user.friends]


# --- 8. Game Library & Asset Endpoints ---
@app.get("/api/games/library")
def get_games_library(current_user: UserDB = Depends(get_current_user), db: Session = Depends(get_db)):
    games = db.query(GameDB).all()
    return [{
        "id": g.id,
        "title": g.title,
        "platform": g.platform,
        "nexus_domain": g.nexus_domain,
        "is_installed": g.is_installed,
        "cover_art": g.cover_art,
        "hero_banner": g.hero_banner,
        "trailer_url": g.trailer_url,
        "hltb_main": g.hltb_main,
        "install_size_bytes": g.install_size_bytes,
        "tags": g.tags
    } for g in games]

@app.post("/api/games/add")
def add_stored_game(payload: GameAddRequest, current_user: UserDB = Depends(get_current_user), db: Session = Depends(get_db)):
    """Automatically maps game title to Nexus domain slug upon registration."""
    domain_slug = generate_nexus_domain(payload.title)
    new_game = GameDB(
        title=payload.title,
        platform=payload.platform,
        nexus_domain=domain_slug,
        cover_art=payload.cover_art,
        hero_banner=payload.hero_banner,
        trailer_url=payload.trailer_url,
        is_installed=1
    )
    db.add(new_game)
    db.commit()
    db.refresh(new_game)
    return {"status": "success", "game_id": new_game.id, "nexus_domain": domain_slug}

@app.delete("/api/games/{game_id}")
def delete_stored_game(game_id: int, current_user: UserDB = Depends(get_current_user), db: Session = Depends(get_db)):
    game = db.query(GameDB).filter(GameDB.id == game_id).first()
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")
    
    for path in [game.executable_path, game.rom_path, game.micro_trailer_path]:
        if path and os.path.exists(path):
            try:
                if os.path.isdir(path):
                    shutil.rmtree(path)
                else:
                    os.remove(path)
            except Exception:
                pass

    db.delete(game)
    db.commit()
    return {"status": "success", "message": f"Deleted game {game.title} and cleared local storage."}


# --- 9. Community Game Requests (Seerr-style) ---
@app.get("/api/requests/list")
def list_game_requests(current_user: UserDB = Depends(get_current_user), db: Session = Depends(get_db)):
    requests = db.query(GameRequestDB).all()
    result = []
    for req in requests:
        result.append({
            "id": req.id,
            "title": req.title,
            "platform": req.platform,
            "status": req.status,
            "created_at": req.created_at,
            "requesters": [{"id": u.id, "username": u.username} for u in req.requesters]
        })
    return result

@app.post("/api/requests/add")
def create_game_request(payload: GameRequestCreate, current_user: UserDB = Depends(get_current_user), db: Session = Depends(get_db)):
    existing = db.query(GameRequestDB).filter(
        GameRequestDB.title.ilike(payload.title), 
        GameRequestDB.platform.ilike(payload.platform)
    ).first()
    
    if existing:
        if current_user not in existing.requesters:
            existing.requesters.append(current_user)
            db.commit()
        return {"status": "success", "message": "Added your vote/request to this game.", "request_id": existing.id}
    
    new_req = GameRequestDB(title=payload.title, platform=payload.platform, status="Pending")
    new_req.requesters.append(current_user)
    db.add(new_req)
    db.commit()
    db.refresh(new_req)
    return {"status": "success", "message": "Game request created successfully.", "request_id": new_req.id}


# --- 10. Jellyfin & Seerr Discovery ---
@app.get("/api/jellyfin/library")
async def get_jellyfin_library(current_user: UserDB = Depends(get_current_user)):
    headers = {"Authorization": f"MediaBrowser Token={JELLYFIN_API_KEY}"}
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(
                f"{JELLYFIN_HOST}/Items",
                headers=headers,
                params={"Recursive": "true", "IncludeItemTypes": "Movie,Series", "Limit": 50},
                timeout=10.0
            )
            items = response.json().get("Items", [])
            return [{
                "id": i.get("Id"),
                "title": i.get("Name"),
                "type": i.get("Type"),
                "year": i.get("ProductionYear"),
                "overview": i.get("Overview", "No description available."),
                "poster_url": f"/api/jellyfin/artwork/{i.get('Id')}/poster",
                "backdrop_url": f"/api/jellyfin/artwork/{i.get('Id')}/backdrop"
            } for i in items]
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/jellyfin/artwork/{item_id}/{art_type}")
async def get_jellyfin_artwork(item_id: str, art_type: str):
    image_endpoint = "Primary" if art_type == "poster" else "Backdrop"
    headers = {"Authorization": f"MediaBrowser Token={JELLYFIN_API_KEY}"}
    async with httpx.AsyncClient() as client:
        try:
            image_url = f"{JELLYFIN_HOST}/Items/{item_id}/Images/{image_endpoint}"
            req = client.build_request("GET", image_url, headers=headers)
            r = await client.send(req, stream=True)
            return StreamingResponse(r.aiter_bytes(), media_type=r.headers.get("content-type"))
        except Exception:
            raise HTTPException(status_code=404, detail="Artwork not found")

@app.get("/api/seerr/trending")
async def get_seerr_trending(current_user: UserDB = Depends(get_current_user)):
    headers = {"X-Api-Key": SEERR_API_KEY}
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(
                f"{SEERR_HOST}/api/v1/discover/trending",
                headers=headers,
                timeout=10.0
            )
            return response.json()
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to fetch Seerr trending: {str(e)}")

@app.post("/api/seerr/request")
async def create_seerr_request(payload: MediaRequest, current_user: UserDB = Depends(get_current_user)):
    headers = {"X-Api-Key": SEERR_API_KEY}
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{SEERR_HOST}/api/v1/request",
            json={"mediaId": payload.media_id, "mediaType": payload.media_type, "seasons": payload.seasons},
            headers=headers,
            timeout=10.0
        )
        return response.json()


# --- 11. Lidarr Music Library ---
@app.get("/api/lidarr/artists")
async def get_lidarr_artists(current_user: UserDB = Depends(get_current_user)):
    if not lidarr_client:
        raise HTTPException(status_code=500, detail="Lidarr client not connected")
    try:
        return lidarr_client.get_artist()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/lidarr/albums")
async def get_lidarr_albums(current_user: UserDB = Depends(get_current_user)):
    if not lidarr_client:
        raise HTTPException(status_code=500, detail="Lidarr client not connected")
    try:
        return lidarr_client.get_album()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# --- 12. Nexus Mods Integration ---
def get_nexus_headers() -> dict:
    return {
        "apikey": NEXUS_API_KEY,
        "Application-Name": "SpectraLauncher",
        "Application-Version": "1.0.0",
        "Content-Type": "application/json"
    }

@app.get("/api/mods/nexus/search")
async def search_nexus_mods(domain_name: str, query: str, current_user: UserDB = Depends(get_current_user)):
    cache_key = f"search:{domain_name}:{query.lower().strip()}"
    now = time.time()
    if cache_key in nexus_cache and (now - nexus_cache[cache_key][0]) < CACHE_TTL_SECONDS:
        return nexus_cache[cache_key][1]

    graphql_query = {
        "query": """
        query SearchMods($filter: ModsFilter, $count: Int) {
            mods(filter: $filter, count: $count) {
                nodes {
                    modId
                    name
                    summary
                    author
                    version
                    pictureUrl
                    endorsements
                    downloads
                }
            }
        }
        """,
        "variables": {
            "count": 25,
            "filter": {
                "gameDomainName": [{"value": domain_name, "op": "EQUALS"}],
                "name": [{"value": query, "op": "WILDCARD"}]
            }
        }
    }

    async with httpx.AsyncClient() as client:
        try:
            resp = await client.post(NEXUS_GRAPHQL_URL, json=graphql_query, headers=get_nexus_headers(), timeout=12.0)
            resp.raise_for_status()
            data = resp.json()
            nodes = data.get("data", {}).get("mods", {}).get("nodes", [])
            
            results = [{
                "mod_id": m.get("modId"),
                "name": m.get("name"),
                "summary": m.get("summary"),
                "author": m.get("author"),
                "version": m.get("version"),
                "thumbnail_url": m.get("pictureUrl"),
                "endorsements": m.get("endorsements"),
                "downloads": m.get("downloads"),
                "domain_name": domain_name
            } for m in nodes]

            nexus_cache[cache_key] = (now, results)
            return results
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                raise HTTPException(status_code=429, detail="Nexus API rate limit exceeded.")
            raise HTTPException(status_code=502, detail=f"Nexus API error: {str(e)}")

@app.get("/api/mods/nexus/{domain_name}/{mod_id}/files")
async def get_nexus_mod_files(domain_name: str, mod_id: int, current_user: UserDB = Depends(get_current_user)):
    cache_key = f"files:{domain_name}:{mod_id}"
    now = time.time()
    if cache_key in nexus_cache and (now - nexus_cache[cache_key][0]) < CACHE_TTL_SECONDS:
        return nexus_cache[cache_key][1]

    url = f"{NEXUS_REST_BASE}/games/{domain_name}/mods/{mod_id}/files.json"
    async with httpx.AsyncClient() as client:
        resp = await client.get(url, headers=get_nexus_headers(), timeout=10.0)
        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail="Failed to fetch mod files.")
        
        files = resp.json().get("files", [])
        formatted_files = [{
            "file_id": f.get("file_id"),
            "name": f.get("name"),
            "version": f.get("version"),
            "category_name": f.get("category_name"),
            "size_kb": f.get("size_kb"),
            "description": f.get("description")
        } for f in files if f.get("category_name") in ["MAIN", "OPTIONAL", "UPDATE"]]

        nexus_cache[cache_key] = (now, formatted_files)
        return formatted_files

@app.post("/api/mods/nexus/install")
async def install_nexus_mod(payload: NexusInstallRequest, current_user: UserDB = Depends(get_current_user), db: Session = Depends(get_db)):
    link_url = f"{NEXUS_REST_BASE}/games/{payload.nexus_domain}/mods/{payload.mod_id}/files/{payload.file_id}/download_link.json"
    
    async with httpx.AsyncClient() as client:
        resp = await client.get(link_url, headers=get_nexus_headers(), timeout=10.0)
        if resp.status_code == 403:
            return {
                "status": "manual_required",
                "message": "Direct CDN download requires a Nexus Premium API key.",
                "fallback_url": f"https://www.nexusmods.com/{payload.nexus_domain}/mods/{payload.mod_id}?tab=files"
            }
        elif resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail="Failed to generate download link.")

        links = resp.json()
        if not links:
            raise HTTPException(status_code=404, detail="No CDN mirrors returned.")
        
        cdn_url = links[0]["URI"]
        mod_dir = f"{MOD_STORAGE_DIR}/{payload.game_id}"
        os.makedirs(mod_dir, exist_ok=True)
        
        safe_name = "".join(c for c in payload.mod_name if c.isalnum() or c in (" ", "-", "_")).rstrip()
        file_location = os.path.join(mod_dir, f"{safe_name}_{payload.file_id}.zip")

        async with client.stream("GET", cdn_url, timeout=60.0) as download_stream:
            download_stream.raise_for_status()
            with open(file_location, "wb") as f:
                async for chunk in download_stream.aiter_bytes(chunk_size=1024 * 1024):
                    f.write(chunk)

    new_mod = GameModDB(
        game_id=payload.game_id,
        nexus_mod_id=payload.mod_id,
        nexus_domain=payload.nexus_domain,
        mod_name=payload.mod_name,
        version=payload.version,
        author=payload.author,
        thumbnail_url=payload.thumbnail_url,
        file_path=file_location,
        enabled=1
    )
    db.add(new_mod)
    db.commit()
    db.refresh(new_mod)

    return {
        "status": "installed",
        "mod_id": new_mod.id,
        "download_url": f"/config/mods/{payload.game_id}/{os.path.basename(file_location)}"
    }

@app.get("/api/mods/game/{game_id}")
def get_game_mods(game_id: int, current_user: UserDB = Depends(get_current_user), db: Session = Depends(get_db)):
    mods = db.query(GameModDB).filter(GameModDB.game_id == game_id).all()
    return [{
        "id": m.id,
        "mod_name": m.mod_name,
        "version": m.version,
        "author": m.author,
        "thumbnail_url": m.thumbnail_url,
        "enabled": bool(m.enabled),
        "file_url": f"/config/mods/{game_id}/{os.path.basename(m.file_path)}"
    } for m in mods]

@app.post("/api/mods/{mod_id}/toggle")
def toggle_game_mod(mod_id: int, current_user: UserDB = Depends(get_current_user), db: Session = Depends(get_db)):
    mod = db.query(GameModDB).filter(GameModDB.id == mod_id).first()
    if not mod:
        raise HTTPException(status_code=404, detail="Mod not found")
    mod.enabled = 0 if mod.enabled else 1
    db.commit()
    return {"status": "success", "mod_id": mod.id, "enabled": bool(mod.enabled)}


# --- 13. Downloads & Torrent Management ---
@app.post("/api/downloads/add")
async def add_torrent(payload: TorrentAddRequest, current_user: UserDB = Depends(get_current_user)):
    if not qbt_client:
        raise HTTPException(status_code=500, detail="qBittorrent not connected")
    qbt_client.torrents_add(urls=payload.magnet_url, savepath=GAME_STORAGE_DIR)
    return {"status": "success", "message": "Torrent added."}

@app.post("/api/downloads/pause")
def pause_torrent(payload: TorrentHashRequest, current_user: UserDB = Depends(get_current_user)):
    if not qbt_client:
        raise HTTPException(status_code=500, detail="qBittorrent not connected")
    qbt_client.torrents_pause(torrent_hashes=payload.torrent_hash)
    return {"status": "success", "message": "Torrent paused."}

@app.post("/api/downloads/resume")
def resume_torrent(payload: TorrentHashRequest, current_user: UserDB = Depends(get_current_user)):
    if not qbt_client:
        raise HTTPException(status_code=500, detail="qBittorrent not connected")
    qbt_client.torrents_resume(torrent_hashes=payload.torrent_hash)
    return {"status": "success", "message": "Torrent resumed."}

@app.post("/api/downloads/cancel")
def cancel_torrent(payload: TorrentHashRequest, current_user: UserDB = Depends(get_current_user)):
    if not qbt_client:
        raise HTTPException(status_code=500, detail="qBittorrent not connected")
    qbt_client.torrents_delete(delete_files=True, torrent_hashes=payload.torrent_hash)
    return {"status": "success", "message": "Torrent cancelled and deleted."}


# --- 14. Telemetry & Cloud Saves ---
SAVE_DIR = f"{CONFIG_DIR}/saves"
os.makedirs(SAVE_DIR, exist_ok=True)

@app.post("/api/telemetry/session")
def log_session(payload: TelemetryReport, current_user: UserDB = Depends(get_current_user), db: Session = Depends(get_db)):
    telemetry = TelemetryDB(user_id=current_user.id, game_title=payload.game_title, duration_seconds=payload.duration_seconds)
    db.add(telemetry)
    db.commit()
    return {"status": "success", "logged_seconds": payload.duration_seconds}

@app.post("/api/saves/upload")
async def upload_game_save(game_title: str, file: UploadFile = File(...), current_user: UserDB = Depends(get_current_user)):
    user_save_path = os.path.join(SAVE_DIR, str(current_user.id))
    os.makedirs(user_save_path, exist_ok=True)
    file_location = os.path.join(user_save_path, f"{game_title}.zip")
    with open(file_location, "wb+") as buffer:
        shutil.copyfileobj(file.file, buffer)
    return {"status": "success", "message": f"Cloud save synced for {game_title}"}

@app.get("/api/saves/download")
async def download_game_save(game_title: str, current_user: UserDB = Depends(get_current_user)):
    file_path = os.path.join(SAVE_DIR, str(current_user.id), f"{game_title}.zip")
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="No cloud save found.")
    return StreamingResponse(open(file_path, "rb"), media_type="application/zip")


# --- 15. WebSocket Hub ---
@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: int, db: Session = Depends(get_db)):
    await manager.connect(user_id, websocket)
    user = db.query(UserDB).filter(UserDB.id == user_id).first()
    if user:
        user.status = "Online"
        db.commit()

    try:
        while True:
            data = await websocket.receive_text()
            message = json.loads(data)
            msg_type = message.get("type")
            
            if msg_type == "activity_update":
                activity = message.get("activity", "")
                user.current_activity = activity
                db.commit()
                await manager.broadcast_to_friends(user_id, {
                    "type": "friend_status_update",
                    "user_id": user_id,
                    "username": user.username,
                    "activity": activity
                }, db)

            elif msg_type == "chat_message":
                recipient_id = message.get("recipient_id")
                content = message.get("content")
                if recipient_id in manager.active_connections:
                    await manager.active_connections[recipient_id].send_json({
                        "type": "chat_message",
                        "sender_id": user_id,
                        "sender_username": user.username,
                        "content": content
                    })
                    
    except WebSocketDisconnect:
        manager.disconnect(user_id)
        if user:
            user.status = "Offline"
            user.current_activity = ""
            db.commit()
