# -*- coding: utf-8 -*-
"""
==========================================================================
 МЕССЕНДЖЕР «ИВАН» — СЕРВЕРНАЯ ЧАСТЬ (main.py)
 FastAPI + SQLite. Бесплатный ИИ Google Gemini, личные чаты, ГРУППЫ и КАНАЛЫ.
 Готов к бесплатному деплою на Render.com.
==========================================================================

ЛОКАЛЬНЫЙ ЗАПУСК:
    pip install -r requirements.txt
    uvicorn main:app --reload          # http://127.0.0.1:8000  (+ /docs)

ДЕПЛОЙ НА RENDER.COM (бесплатный Web Service):
    Build Command:  pip install -r requirements.txt
    Start Command:  uvicorn main:app --host 0.0.0.0 --port $PORT
    Environment:
        GEMINI_API_KEY = ...   (ключ из Google AI Studio — для бота «ai»)
        GEMINI_MODEL   = gemini-2.5-flash   (необязательно)

ВАЖНО ПРО МОДЕЛЬ:
    gemini-1.5-flash снят Google и возвращает 404, поэтому по умолчанию
    используется бесплатная gemini-2.5-flash. Сменить можно переменной
    окружения GEMINI_MODEL.

ВАЖНО ПРО БЕСПЛАТНЫЙ RENDER:
    Диск НЕ постоянный: messenger.db обнуляется при перезапуске/«засыпании»
    сервиса (аккаунты, группы и история пропадут — нужно зарегистрироваться
    заново). Для вечного хранения подключите внешний Postgres или платный диск.
    Сервис «засыпает» после ~15 мин простоя; первый запрос будит его ~30–60 сек.
"""

import os
import re
import time
import random
import base64
import sqlite3
import asyncio
import hashlib
import secrets
from contextlib import closing
from typing import Optional

import httpx
from fastapi import FastAPI, BackgroundTasks, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ------------------------------------------------------------------ #
# 1. КОНФИГУРАЦИЯ                                                     #
# ------------------------------------------------------------------ #
DB_PATH         = os.environ.get("DB_PATH", "messenger.db")
GEMINI_API_KEY  = os.environ.get("GEMINI_API_KEY")
GEMINI_MODEL    = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")
GEMINI_URL      = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
CHAT_SEP        = "~"            # разделитель участников в chat_id личного чата (в username запрещён)
TCK_INTERVAL_MS = 30_000        # ТЦК нагнетает не чаще раза в 30 секунд

# Загрузка файлов (голосовые и т.п.)
MAX_UPLOAD_BYTES = int(os.environ.get("MAX_UPLOAD_BYTES", str(8 * 1024 * 1024)))  # 8 МБ по умолчанию
# Внешний адрес сервиса для построения ссылок на файлы.
# На Render автоматически доступна переменная RENDER_EXTERNAL_URL.
PUBLIC_BASE_URL  = (os.environ.get("PUBLIC_BASE_URL") or os.environ.get("RENDER_EXTERNAL_URL") or "").rstrip("/")

SYSTEM_USERS = {
    "ai":  {"display_name": "Ассистент (ИИ)", "status": "Google Gemini"},
    "tck": {"display_name": "ТЦК",            "status": "повістка вже в дорозі"},
}
RESERVED_HANDLES = set(SYSTEM_USERS.keys())

AI_SYSTEM_PROMPT = (
    "Ты — дружелюбный и полезный ассистент внутри мессенджера «Иван». "
    "Отвечай естественно и на том языке, на котором пишет пользователь "
    "(по-русски или по-украински). Будь лаконичным, но содержательным; "
    "лёгкий уместный юмор приветствуется."
)

# Фразы бота «ТЦК» — остаются без изменений (украиноязычная сатира).
TCK_PHRASES = [
    "Вітаємо! Ваш бусик уже виїхав, чекайте біля під'їзду 🚐",
    "Оновіть дані в «Резерв+», бо ми вже оновили їх за вас.",
    "Кава у Львові — це добре, але Бахмут сумує без вас.",
    "Не ховайтеся за шторкою, ми бачили, як ви відкрили цей чат.",
    "Доброго вечора! Ми з ТЦК. Ваша черга в історії вже підсвічена.",
    "Повістку доставлено подумки. Юридично ви вже попереджені 📜",
    "Виходьте на вулицю — у нас якраз є вільне місце біля вікна.",
    "Спортзал — це чудово. У нас теж є фізпідготовка, цілком безкоштовно.",
    "Ваш статус: «уважно роздивляється стелю». Час оновити на «захисник».",
    "Ми не телефонуємо двічі. Ми просто приїжджаємо.",
    "Доброго ранку! За вами вже закріплено персонального водія бусика.",
    "ВЛК зачекалась. Приходьте — у нас і чай є, і ехокардіограф.",
    "Загубили повістку? Не хвилюйтесь, у нас є ще одна. І ще одна.",
    "Ваш сусід уже з нами. Передавав вітання і просив зайняти вам місце.",
    "Геолокацію вимкнено? Дуже мило. Бусик орієнтується по запаху кави.",
    "Це не погроза, це запрошення. Просто дуже наполегливе.",
    "Не біжіть, ви ж марафонець — у нас якраз є дистанція до військкомату.",
    "Доброго вечора, ми шукаємо саме вас. Так-так, того, хто це читає.",
    "Чай охолов, бусик прогрівся. Усе як ви любите.",
    "Ви ще тут? А бусик уже там. Дивовижний збіг.",
]

# ------------------------------------------------------------------ #
# 2. ПРИЛОЖЕНИЕ + CORS                                                #
# ------------------------------------------------------------------ #
app = FastAPI(title="Ivan Messenger API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],      # фронтенд на GitHub Pages сможет слать запросы
    allow_methods=["*"],
    allow_headers=["*"],
)

# ------------------------------------------------------------------ #
# 3. БАЗА ДАННЫХ (sqlite3)                                            #
# ------------------------------------------------------------------ #
def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=5.0)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with closing(db()) as conn, conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                username      TEXT UNIQUE NOT NULL,
                password_hash TEXT,
                display_name  TEXT,
                status        TEXT,
                custom_bg     TEXT DEFAULT '',   -- тема жидкого градиента
                custom_color  TEXT DEFAULT '',   -- цвет моих сообщений
                custom_blur   TEXT DEFAULT ''    -- степень размытия стекла
            );

            CREATE TABLE IF NOT EXISTS contacts (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id          INTEGER NOT NULL,
                contact_user_id  INTEGER NOT NULL,
                UNIQUE(user_id, contact_user_id)
            );

            -- Группы и каналы (общая таблица комнат)
            CREATE TABLE IF NOT EXISTS rooms (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                room_username     TEXT UNIQUE NOT NULL,        -- @юзернейм группы/канала
                type              TEXT NOT NULL,               -- 'group' | 'channel'
                name              TEXT NOT NULL,
                creator_username  TEXT NOT NULL,
                created_at        INTEGER NOT NULL
            );

            -- Участники комнат
            CREATE TABLE IF NOT EXISTS room_members (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                room_username TEXT NOT NULL,
                username      TEXT NOT NULL,
                role          TEXT NOT NULL DEFAULT 'member',  -- 'admin' | 'member'
                UNIQUE(room_username, username)
            );

            CREATE TABLE IF NOT EXISTS messages (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id          TEXT    NOT NULL,
                sender_username  TEXT    NOT NULL,
                text             TEXT    NOT NULL,
                timestamp        INTEGER NOT NULL,   -- epoch миллисекунды
                is_read          INTEGER NOT NULL DEFAULT 0,
                audio_url        TEXT    NOT NULL DEFAULT ''   -- ссылка на голосовое/вложение
            );

            -- Хранилище загруженных файлов (голосовые и пр.).
            -- ВНИМАНИЕ: на бесплатном Render диск эфемерный — содержимое
            -- пропадает при перезапуске/засыпании. Для продакшена см. комментарии.
            CREATE TABLE IF NOT EXISTS uploads (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                mime        TEXT    NOT NULL,
                data        BLOB    NOT NULL,
                created_at  INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_messages_chat ON messages(chat_id, id);
            CREATE INDEX IF NOT EXISTS idx_members_user  ON room_members(username);
            """
        )
        # --- мини-миграция: добавить audio_url, если БД создана старой версией ---
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(messages)").fetchall()}
        if "audio_url" not in cols:
            conn.execute("ALTER TABLE messages ADD COLUMN audio_url TEXT NOT NULL DEFAULT ''")
    # Системные боты (войти под ними нельзя — пароль невалидный).
    for uname, info in SYSTEM_USERS.items():
        with closing(db()) as conn, conn:
            conn.execute(
                """
                INSERT INTO users (username, password_hash, display_name, status, custom_bg, custom_color, custom_blur)
                VALUES (?, '!', ?, ?, '', '', '')
                ON CONFLICT(username) DO UPDATE SET
                    display_name = excluded.display_name,
                    status       = excluded.status
                """,
                (uname, info["display_name"], info["status"]),
            )


# ---- пароли: PBKDF2 + соль ----
def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), 100_000)
    return f"{salt}${dk.hex()}"


def verify_password(password: str, stored: Optional[str]) -> bool:
    if not stored or "$" not in stored:
        return False
    salt, h = stored.split("$", 1)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), 100_000)
    return secrets.compare_digest(dk.hex(), h)


# ---- пользователи ----
def public_user(username: str) -> Optional[dict]:
    with closing(db()) as conn:
        row = conn.execute(
            "SELECT username, display_name, status, custom_bg, custom_color, custom_blur "
            "FROM users WHERE username = ?",
            (username,),
        ).fetchone()
    return dict(row) if row else None


def user_id(username: str) -> Optional[int]:
    with closing(db()) as conn:
        row = conn.execute("SELECT id FROM users WHERE username = ?", (username,)).fetchone()
    return row["id"] if row else None


def add_edge(user: str, contact: str) -> None:
    uid, cid = user_id(user), user_id(contact)
    if not uid or not cid or uid == cid:
        return
    with closing(db()) as conn, conn:
        conn.execute("INSERT OR IGNORE INTO contacts (user_id, contact_user_id) VALUES (?, ?)", (uid, cid))


def add_contact_both_ways(a: str, b: str) -> None:
    add_edge(a, b)
    add_edge(b, a)


# ---- комнаты (группы/каналы) ----
def room_get(handle: str) -> Optional[dict]:
    with closing(db()) as conn:
        row = conn.execute("SELECT * FROM rooms WHERE room_username = ?", (handle,)).fetchone()
    return dict(row) if row else None


def room_member_count(handle: str) -> int:
    with closing(db()) as conn:
        row = conn.execute("SELECT COUNT(*) AS c FROM room_members WHERE room_username = ?", (handle,)).fetchone()
    return row["c"] if row else 0


def is_member(handle: str, username: str) -> bool:
    with closing(db()) as conn:
        row = conn.execute(
            "SELECT 1 FROM room_members WHERE room_username = ? AND username = ?",
            (handle, username),
        ).fetchone()
    return bool(row)


def add_member(handle: str, username: str, role: str = "member") -> None:
    with closing(db()) as conn, conn:
        conn.execute(
            "INSERT OR IGNORE INTO room_members (room_username, username, role) VALUES (?, ?, ?)",
            (handle, username, role),
        )


def room_entry(room: dict) -> dict:
    """Единый формат записи о комнате для клиента."""
    return {
        "type": room["type"],
        "username": room["room_username"],
        "name": room["name"],
        "status": None,
        "creator": room["creator_username"],
        "member_count": room_member_count(room["room_username"]),
    }


# ---- сообщения ----
def save_message(chat_id: str, sender: str, text: str, audio_url: str = "") -> dict:
    ts = int(time.time() * 1000)
    with closing(db()) as conn, conn:
        cur = conn.execute(
            "INSERT INTO messages (chat_id, sender_username, text, timestamp, is_read, audio_url) "
            "VALUES (?, ?, ?, ?, 0, ?)",
            (chat_id, sender, text, ts, audio_url),
        )
        mid = cur.lastrowid
    return {"id": mid, "chat_id": chat_id, "sender": sender, "text": text,
            "timestamp": ts, "is_read": 0, "audio_url": audio_url}


def fetch_messages(chat_id: str) -> list:
    with closing(db()) as conn:
        rows = conn.execute(
            "SELECT id, chat_id, sender_username AS sender, text, timestamp, is_read, audio_url "
            "FROM messages WHERE chat_id = ? ORDER BY id ASC",
            (chat_id,),
        ).fetchall()
    return [dict(r) for r in rows]


# ------------------------------------------------------------------ #
# 4. УТИЛИТЫ                                                          #
# ------------------------------------------------------------------ #
def clean_username(raw: Optional[str]) -> Optional[str]:
    u = (raw or "").strip().lower().lstrip("@")
    return u if re.fullmatch(r"[a-z0-9_]{2,20}", u) else None


def chat_participants(chat_id: str) -> list:
    return chat_id.split(CHAT_SEP)


# ------------------------------------------------------------------ #
# 5. МОДЕЛИ ЗАПРОСОВ                                                  #
# ------------------------------------------------------------------ #
class RegisterIn(BaseModel):
    username: str
    display_name: Optional[str] = None
    password: str


class LoginIn(BaseModel):
    username: str
    password: str


class AddIn(BaseModel):
    username: str           # текущий пользователь
    handle: str             # кого/что добавляем (друг, бот, группа или канал)


class CreateRoomIn(BaseModel):
    username: str           # создатель
    handle: str             # @юзернейм комнаты
    name: str
    type: str               # 'group' | 'channel'


class MessageIn(BaseModel):
    chat_id: str
    sender_username: str
    text: str
    audio_url: Optional[str] = None     # ссылка на голосовое (получена из /api/upload)


class UploadIn(BaseModel):
    kind: Optional[str] = None          # напр. "audio"
    data: str                           # data-URL: "data:audio/webm;base64,...."


class ReadIn(BaseModel):
    username: str


class SettingsIn(BaseModel):
    username: str
    custom_bg: Optional[str] = None
    custom_color: Optional[str] = None
    custom_blur: Optional[str] = None
    display_name: Optional[str] = None
    status: Optional[str] = None


# ------------------------------------------------------------------ #
# 6. ЭНДПОИНТЫ                                                        #
# ------------------------------------------------------------------ #
@app.get("/")
def root():
    return {"service": "Ivan Messenger API", "ok": True, "ai_enabled": bool(GEMINI_API_KEY), "model": GEMINI_MODEL}


@app.post("/api/auth/register")
def register(data: RegisterIn):
    uname = clean_username(data.username)
    if not uname:
        raise HTTPException(400, "Некорректный юзернейм. Разрешены: a-z, 0-9, _ (2–20).")
    if uname in RESERVED_HANDLES:
        raise HTTPException(400, "Этот юзернейм зарезервирован системой (ai / tck).")
    if not data.password or len(data.password) < 4:
        raise HTTPException(400, "Пароль должен содержать минимум 4 символа.")
    if user_id(uname) or room_get(uname):
        raise HTTPException(409, "Такой юзернейм уже занят.")

    display = (data.display_name or uname).strip()[:40] or uname
    with closing(db()) as conn, conn:
        conn.execute(
            "INSERT INTO users (username, password_hash, display_name, status, custom_bg, custom_color, custom_blur) "
            "VALUES (?, ?, ?, 'В сети', '', '', '')",
            (uname, hash_password(data.password), display),
        )
    # Сразу добавляем ботов ai и tck в список чатов новичка.
    add_edge(uname, "ai")
    add_edge(uname, "tck")
    return public_user(uname)


@app.post("/api/auth/login")
def login(data: LoginIn):
    uname = clean_username(data.username)
    if not uname:
        raise HTTPException(400, "Некорректный юзернейм.")
    with closing(db()) as conn:
        row = conn.execute("SELECT password_hash FROM users WHERE username = ?", (uname,)).fetchone()
    if not row or not verify_password(data.password, row["password_hash"]):
        raise HTTPException(401, "Неверный юзернейм или пароль.")
    return public_user(uname)


@app.post("/api/users/settings")
def save_settings(data: SettingsIn):
    me = clean_username(data.username)
    if not me or not public_user(me):
        raise HTTPException(400, "Пользователь не найден.")
    fields, values = [], []
    if data.custom_bg is not None:
        fields.append("custom_bg = ?");    values.append(data.custom_bg.strip()[:32])
    if data.custom_color is not None:
        fields.append("custom_color = ?"); values.append(data.custom_color.strip()[:32])
    if data.custom_blur is not None:
        fields.append("custom_blur = ?");  values.append(data.custom_blur.strip()[:8])
    if data.display_name is not None and data.display_name.strip():
        fields.append("display_name = ?"); values.append(data.display_name.strip()[:40])
    if data.status is not None:
        fields.append("status = ?");       values.append(data.status.strip()[:60])
    if fields:
        values.append(me)
        with closing(db()) as conn, conn:
            conn.execute(f"UPDATE users SET {', '.join(fields)} WHERE username = ?", values)
    return public_user(me)


@app.post("/api/rooms/create")
def create_room(data: CreateRoomIn):
    me = clean_username(data.username)
    handle = clean_username(data.handle)
    name = (data.name or "").strip()[:60]
    rtype = (data.type or "").strip()
    if not me or not public_user(me):
        raise HTTPException(400, "Сначала войдите в аккаунт.")
    if not handle:
        raise HTTPException(400, "Некорректный @юзернейм комнаты (a-z, 0-9, _, 2–20).")
    if not name:
        raise HTTPException(400, "Укажите название.")
    if rtype not in ("group", "channel"):
        raise HTTPException(400, "Тип должен быть group или channel.")
    if handle in RESERVED_HANDLES:
        raise HTTPException(400, "Этот юзернейм зарезервирован.")
    if user_id(handle) or room_get(handle):
        raise HTTPException(409, "Такой @юзернейм уже занят.")

    with closing(db()) as conn, conn:
        conn.execute(
            "INSERT INTO rooms (room_username, type, name, creator_username, created_at) VALUES (?, ?, ?, ?, ?)",
            (handle, rtype, name, me, int(time.time() * 1000)),
        )
    add_member(handle, me, role="admin")     # создатель — администратор
    return room_entry(room_get(handle))


@app.post("/api/chats/add")
def add_chat(data: AddIn):
    """Универсальное добавление по @юзернейму: друг/бот ИЛИ вступление в группу/канал."""
    me = clean_username(data.username)
    handle = clean_username(data.handle)
    if not me or not handle:
        raise HTTPException(400, "Некорректный юзернейм.")
    if not public_user(me):
        raise HTTPException(400, "Сначала войдите в аккаунт.")
    if me == handle:
        raise HTTPException(400, "Нельзя добавить самого себя 🙂")

    user = public_user(handle)
    if user:                                  # это пользователь или бот → личный чат
        add_contact_both_ways(me, handle)
        return {"type": "direct", "username": handle, "name": user["display_name"],
                "status": user["status"], "creator": None, "member_count": None}

    room = room_get(handle)
    if room:                                  # это группа/канал → вступаем
        add_member(handle, me, role="member")
        return room_entry(room)

    raise HTTPException(404, "Пользователь или канал не найден")


@app.get("/api/chats/{username}")
def list_chats(username: str):
    """Единый список: личные контакты + группы/каналы, где состоит пользователь."""
    me = clean_username(username)
    if not me:
        raise HTTPException(400, "Некорректный юзернейм.")
    out = []
    with closing(db()) as conn:
        # личные контакты
        for r in conn.execute(
            """
            SELECT u.username AS username, u.display_name AS name, u.status AS status
            FROM contacts c JOIN users u ON u.id = c.contact_user_id
            WHERE c.user_id = (SELECT id FROM users WHERE username = ?)
            ORDER BY u.display_name COLLATE NOCASE
            """,
            (me,),
        ).fetchall():
            out.append({"type": "direct", "username": r["username"], "name": r["name"],
                        "status": r["status"], "creator": None, "member_count": None})
        # группы и каналы
        for r in conn.execute(
            """
            SELECT r.room_username AS username, r.type AS type, r.name AS name, r.creator_username AS creator,
                   (SELECT COUNT(*) FROM room_members m2 WHERE m2.room_username = r.room_username) AS member_count
            FROM rooms r JOIN room_members m ON m.room_username = r.room_username
            WHERE m.username = ?
            ORDER BY r.name COLLATE NOCASE
            """,
            (me,),
        ).fetchall():
            out.append({"type": r["type"], "username": r["username"], "name": r["name"],
                        "status": None, "creator": r["creator"], "member_count": r["member_count"]})
    return out


@app.get("/api/messages/{chat_id}")
def get_messages(chat_id: str):
    parts = chat_participants(chat_id)
    if "tck" in parts:
        maybe_tck_autoreply(chat_id)
    return fetch_messages(chat_id)


@app.post("/api/messages")
def post_message(data: MessageIn, background: BackgroundTasks):
    sender    = clean_username(data.sender_username)
    text      = (data.text or "").strip()[:4000]
    chat_id   = (data.chat_id or "").strip()
    audio_url = (data.audio_url or "").strip()[:500]
    # сообщение валидно, если есть отправитель и хотя бы текст ИЛИ голосовое
    if not sender or (not text and not audio_url):
        raise HTTPException(400, "Пустое сообщение или некорректный отправитель.")

    if CHAT_SEP in chat_id:
        # ---- личный чат ----
        msg = save_message(chat_id, sender, text, audio_url)
        parts = chat_participants(chat_id)
        if "ai" in parts and sender != "ai":
            background.add_task(handle_ai_reply, chat_id)
        if "tck" in parts and sender != "tck":
            background.add_task(handle_tck_reply, chat_id)
        return msg

    # ---- группа или канал ----
    room = room_get(chat_id)
    if not room:
        raise HTTPException(400, "Чат не найден.")
    if not is_member(chat_id, sender):
        raise HTTPException(403, "Вы не участник этого чата.")
    if room["type"] == "channel" and sender != room["creator_username"]:
        raise HTTPException(403, "Только администраторы могут писать в этот канал")
    return save_message(chat_id, sender, text, audio_url)


@app.post("/api/messages/{chat_id}/read")
def mark_read(chat_id: str, data: ReadIn):
    me = clean_username(data.username)
    if not me:
        raise HTTPException(400, "Некорректный юзернейм.")
    with closing(db()) as conn, conn:
        conn.execute(
            "UPDATE messages SET is_read = 1 WHERE chat_id = ? AND sender_username <> ?",
            (chat_id, me),
        )
    return {"ok": True}


# ------------------------------------------------------------------ #
# 6b. ЗАГРУЗКА ФАЙЛОВ (голосовые сообщения)                           #
# ------------------------------------------------------------------ #
def public_base(request: Request) -> str:
    """Внешний https-адрес сервиса для построения ссылок на файлы."""
    if PUBLIC_BASE_URL:
        return PUBLIC_BASE_URL
    base = str(request.base_url).rstrip("/")
    # за обратным прокси (Render) принудительно используем https, кроме локалки
    if base.startswith("http://") and "localhost" not in base and "127.0.0.1" not in base:
        base = "https://" + base[len("http://"):]
    return base


def parse_data_url(raw: str):
    """Разбирает data-URL → (mime, bytes). Принимает и «голый» base64."""
    raw = (raw or "").strip()
    mime = "application/octet-stream"
    b64 = raw
    if raw.startswith("data:"):
        header, _, b64 = raw.partition(",")
        m = re.match(r"data:([^;,]+)", header)
        if m:
            mime = m.group(1)
    try:
        blob = base64.b64decode(b64, validate=False)
    except Exception:
        raise HTTPException(400, "Некорректные данные файла.")
    return mime, blob


@app.post("/api/upload")
def upload_file(data: UploadIn, request: Request):
    """
    Принимает аудио (data-URL в JSON), сохраняет в БД и возвращает {"url", "id"}.
    Клиент кладёт этот url в поле audio_url сообщения.
    """
    mime, blob = parse_data_url(data.data)
    if not blob:
        raise HTTPException(400, "Пустой файл.")
    if len(blob) > MAX_UPLOAD_BYTES:
        raise HTTPException(413, f"Файл слишком большой (> {MAX_UPLOAD_BYTES // (1024 * 1024)} МБ).")
    # на всякий случай ограничим тип голосовыми/аудио, но не строго
    if not (mime.startswith("audio/") or mime.startswith("image/")):
        mime = "audio/webm"
    with closing(db()) as conn, conn:
        cur = conn.execute(
            "INSERT INTO uploads (mime, data, created_at) VALUES (?, ?, ?)",
            (mime, blob, int(time.time() * 1000)),
        )
        fid = cur.lastrowid
    return {"id": fid, "url": f"{public_base(request)}/api/file/{fid}"}


@app.get("/api/file/{file_id}")
def get_file(file_id: int):
    """Отдаёт ранее загруженный файл по id."""
    with closing(db()) as conn:
        row = conn.execute("SELECT mime, data FROM uploads WHERE id = ?", (file_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Файл не найден.")
    return Response(
        content=bytes(row["data"]),
        media_type=row["mime"],
        headers={"Cache-Control": "public, max-age=31536000, immutable"},
    )


# ------------------------------------------------------------------ #
# 7. ЛОГИКА БОТОВ                                                     #
# ------------------------------------------------------------------ #
def maybe_tck_autoreply(chat_id: str) -> None:
    """Приветствие при входе в чат с ТЦК и нагнетание не чаще раза в 30 секунд."""
    rows = fetch_messages(chat_id)
    now = int(time.time() * 1000)
    if not rows:
        save_message(chat_id, "tck", random.choice(TCK_PHRASES))
        return
    tck_times = [r["timestamp"] for r in rows if r["sender"] == "tck"]
    if tck_times and (now - max(tck_times)) >= TCK_INTERVAL_MS:
        save_message(chat_id, "tck", random.choice(TCK_PHRASES))


async def handle_tck_reply(chat_id: str) -> None:
    """Ответ ТЦК на сообщение пользователя — через 1–2 секунды."""
    await asyncio.sleep(random.uniform(1.0, 2.0))
    save_message(chat_id, "tck", random.choice(TCK_PHRASES))


def build_gemini_contents(rows: list) -> list:
    """
    История чата -> формат Gemini 'contents'.
    sender == 'ai' => role 'model', иначе 'user'.
    Склеиваем подряд идущие одинаковые роли и начинаем с роли user.
    """
    contents = []
    for r in rows:
        role = "model" if r["sender"] == "ai" else "user"
        if contents and contents[-1]["role"] == role:
            contents[-1]["parts"][0]["text"] += "\n" + r["text"]
        else:
            contents.append({"role": role, "parts": [{"text": r["text"]}]})
    while contents and contents[0]["role"] != "user":
        contents.pop(0)
    return contents


async def handle_ai_reply(chat_id: str) -> None:
    """Запрос к Google Gemini и сохранение ответа в базу."""
    if not GEMINI_API_KEY:
        save_message(chat_id, "ai", "⚠️ ИИ недоступен: на сервере не задана переменная GEMINI_API_KEY.")
        return

    contents = build_gemini_contents(fetch_messages(chat_id)[-40:])
    if not contents:
        return

    payload = {
        "systemInstruction": {"parts": [{"text": AI_SYSTEM_PROMPT}]},
        "contents": contents,
        "generationConfig": {"maxOutputTokens": 1024, "temperature": 0.9},
    }
    headers = {"x-goog-api-key": GEMINI_API_KEY, "Content-Type": "application/json"}

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(GEMINI_URL, headers=headers, json=payload)
        if resp.status_code != 200:
            save_message(chat_id, "ai", f"⚠️ Ошибка Gemini API ({resp.status_code}). {resp.text[:300]}")
            return
        data = resp.json()
        candidates = data.get("candidates", [])
        if not candidates:
            save_message(chat_id, "ai", "⚠️ Пустой ответ от Gemini (возможно, сработал фильтр безопасности).")
            return
        parts = candidates[0].get("content", {}).get("parts", [])
        text = "".join(p.get("text", "") for p in parts).strip()
        save_message(chat_id, "ai", text or "(пустой ответ)")
    except Exception as exc:                       # noqa: BLE001
        save_message(chat_id, "ai", f"⚠️ Не удалось связаться с ИИ: {exc}")


# ------------------------------------------------------------------ #
# 8. СТАРТ                                                            #
# ------------------------------------------------------------------ #
init_db()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
