# -*- coding: utf-8 -*-
"""
==========================================================================
 МЕССЕНДЖЕР «ІВАН» — СЕРВЕРНАЯ ЧАСТЬ (БЛОК 1)
 FastAPI + встроенный SQLite. Аккаунты, друзья, кастомизация, боты ai/tck.
 Готов к бесплатному деплою на Render.com.
==========================================================================

ЛОКАЛЬНЫЙ ЗАПУСК:
    pip install -r requirements.txt
    uvicorn main:app --reload          # http://127.0.0.1:8000  (+ /docs)

ДЕПЛОЙ НА RENDER.COM (бесплатный Web Service):
    Build Command:  pip install -r requirements.txt
    Start Command:  uvicorn main:app --host 0.0.0.0 --port $PORT
    Environment:
        CLAUDE_API_KEY = sk-ant-...                      (для чата с ботом 'ai')
        CLAUDE_MODEL   = claude-3-5-sonnet-20241022      (необязательно)

ВАЖНО ПРО БЕСПЛАТНЫЙ ТАРИФ RENDER:
    • Диск НЕ постоянный: файл messenger.db обнуляется при деплое/перезапуске/
      «засыпании» сервиса — аккаунты и история пропадут (нужно будет
      зарегистрироваться заново). Для вечного хранения подключите внешнюю БД
      (бесплатный Postgres на Neon/Supabase) или платный диск Render.
    • Сервис «засыпает» после ~15 мин простоя; первый запрос будит его ~30–60 сек.

ПРО БЕЗОПАСНОСТЬ (MVP):
    • Пароли НЕ хранятся в открытом виде — только PBKDF2-хэш с солью.
    • Сессионных токенов нет: после входа клиент работает по username (как в
      типовом хобби-проекте). Не используйте для действительно приватных данных.
"""

import os
import re
import time
import random
import sqlite3
import asyncio
import hashlib
import secrets
from contextlib import closing
from typing import Optional

import httpx
from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ------------------------------------------------------------------ #
# 1. КОНФИГУРАЦИЯ                                                     #
# ------------------------------------------------------------------ #
DB_PATH        = os.environ.get("DB_PATH", "messenger.db")
CLAUDE_API_KEY = os.environ.get("CLAUDE_API_KEY")
CLAUDE_MODEL   = os.environ.get("CLAUDE_MODEL", "claude-3-5-sonnet-20241022")
ANTHROPIC_URL  = "https://api.anthropic.com/v1/messages"
CHAT_SEP       = "~"                      # разделитель участников в chat_id (в username запрещён)
TCK_INTERVAL_MS = 30_000                  # ТЦК нагнетает не чаще раза в 30 секунд

SYSTEM_USERS = {
    "ai":  {"display_name": "Клод · ШІ", "status": "штучний інтелект"},
    "tck": {"display_name": "ТЦК",       "status": "повістка вже в дорозі"},
}
RESERVED_USERNAMES = set(SYSTEM_USERS.keys())

# Украиноязычные фразы бота «ТЦК» (сатира/абсурд) — выбираются случайно.
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
    conn = sqlite3.connect(DB_PATH, timeout=5.0)   # timeout спасает от "database is locked"
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
                custom_bg     TEXT DEFAULT '',
                custom_color  TEXT DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS contacts (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id          INTEGER NOT NULL,
                contact_user_id  INTEGER NOT NULL,
                UNIQUE(user_id, contact_user_id)
            );

            CREATE TABLE IF NOT EXISTS messages (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id          TEXT    NOT NULL,
                sender_username  TEXT    NOT NULL,
                text             TEXT    NOT NULL,
                timestamp        INTEGER NOT NULL,   -- epoch миллисекунды
                is_read          INTEGER NOT NULL DEFAULT 0
            );

            CREATE INDEX IF NOT EXISTS idx_messages_chat ON messages(chat_id, id);
            """
        )
    # Системные боты (войти под ними нельзя — пароль невалидный).
    for uname, info in SYSTEM_USERS.items():
        with closing(db()) as conn, conn:
            conn.execute(
                """
                INSERT INTO users (username, password_hash, display_name, status, custom_bg, custom_color)
                VALUES (?, '!', ?, ?, '', '')
                ON CONFLICT(username) DO UPDATE SET
                    display_name = excluded.display_name,
                    status       = excluded.status
                """,
                (uname, info["display_name"], info["status"]),
            )


# ---- пароли: PBKDF2 + соль (только стандартная библиотека) ----
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
    """Профиль без пароля — то, что отдаём клиенту."""
    with closing(db()) as conn:
        row = conn.execute(
            "SELECT username, display_name, status, custom_bg, custom_color FROM users WHERE username = ?",
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


# ---- сообщения ----
def save_message(chat_id: str, sender: str, text: str) -> dict:
    ts = int(time.time() * 1000)
    with closing(db()) as conn, conn:
        cur = conn.execute(
            "INSERT INTO messages (chat_id, sender_username, text, timestamp, is_read) VALUES (?, ?, ?, ?, 0)",
            (chat_id, sender, text, ts),
        )
        mid = cur.lastrowid
    return {"id": mid, "chat_id": chat_id, "sender": sender, "text": text, "timestamp": ts, "is_read": 0}


def fetch_messages(chat_id: str) -> list:
    with closing(db()) as conn:
        rows = conn.execute(
            "SELECT id, chat_id, sender_username AS sender, text, timestamp, is_read "
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


class AddContactIn(BaseModel):
    username: str
    contact_username: str


class MessageIn(BaseModel):
    chat_id: str
    sender_username: str
    text: str


class ReadIn(BaseModel):
    username: str


class SettingsIn(BaseModel):
    username: str
    custom_bg: Optional[str] = None
    custom_color: Optional[str] = None
    display_name: Optional[str] = None
    status: Optional[str] = None


# ------------------------------------------------------------------ #
# 6. ЭНДПОИНТЫ                                                        #
# ------------------------------------------------------------------ #
@app.get("/")
def root():
    return {"service": "Ivan Messenger API", "ok": True, "ai_enabled": bool(CLAUDE_API_KEY)}


@app.post("/api/auth/register")
def register(data: RegisterIn):
    uname = clean_username(data.username)
    if not uname:
        raise HTTPException(400, "Некоректний username. Дозволені символи: a-z, 0-9, _ (2–20).")
    if uname in RESERVED_USERNAMES:
        raise HTTPException(400, "Цей username зарезервовано системою (ai / tck).")
    if not data.password or len(data.password) < 4:
        raise HTTPException(400, "Пароль має містити щонайменше 4 символи.")
    if user_id(uname):
        raise HTTPException(409, "Такий username уже зайнятий.")

    display = (data.display_name or uname).strip()[:40] or uname
    with closing(db()) as conn, conn:
        conn.execute(
            "INSERT INTO users (username, password_hash, display_name, status, custom_bg, custom_color) "
            "VALUES (?, ?, ?, 'У мережі', '', '')",
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
        raise HTTPException(400, "Некоректний username.")
    with closing(db()) as conn:
        row = conn.execute("SELECT password_hash FROM users WHERE username = ?", (uname,)).fetchone()
    if not row or not verify_password(data.password, row["password_hash"]):
        raise HTTPException(401, "Невірний username або пароль.")
    return public_user(uname)


@app.post("/api/contacts/add")
def add_contact(data: AddContactIn):
    me     = clean_username(data.username)
    target = clean_username(data.contact_username)
    if not me or not target:
        raise HTTPException(400, "Некоректний username.")
    if not public_user(me):
        raise HTTPException(400, "Спочатку увійдіть у свій акаунт.")
    if me == target:
        raise HTTPException(400, "Не можна додати самого себе 🙂")
    target_user = public_user(target)
    if not target_user:
        raise HTTPException(404, "Користувача не знайдено")   # фронтенд покажет это уведомлением
    add_contact_both_ways(me, target)
    return target_user


@app.get("/api/contacts/{username}")
def list_contacts(username: str):
    me = clean_username(username)
    if not me:
        raise HTTPException(400, "Некоректний username.")
    with closing(db()) as conn:
        rows = conn.execute(
            """
            SELECT u.username, u.display_name, u.status, u.custom_bg, u.custom_color
            FROM contacts c JOIN users u ON u.id = c.contact_user_id
            WHERE c.user_id = (SELECT id FROM users WHERE username = ?)
            ORDER BY u.display_name COLLATE NOCASE
            """,
            (me,),
        ).fetchall()
    return [dict(r) for r in rows]


@app.post("/api/users/settings")
def save_settings(data: SettingsIn):
    me = clean_username(data.username)
    if not me or not public_user(me):
        raise HTTPException(400, "Користувача не знайдено.")
    fields, values = [], []
    if data.custom_bg is not None:
        fields.append("custom_bg = ?");    values.append(data.custom_bg.strip()[:32])
    if data.custom_color is not None:
        fields.append("custom_color = ?"); values.append(data.custom_color.strip()[:32])
    if data.display_name is not None and data.display_name.strip():
        fields.append("display_name = ?"); values.append(data.display_name.strip()[:40])
    if data.status is not None:
        fields.append("status = ?");       values.append(data.status.strip()[:60])
    if fields:
        values.append(me)
        with closing(db()) as conn, conn:
            conn.execute(f"UPDATE users SET {', '.join(fields)} WHERE username = ?", values)
    return public_user(me)


@app.get("/api/messages/{chat_id}")
def get_messages(chat_id: str):
    """История чата. Для чата с ТЦК: приветствие при входе и нагнетание раз в 30 сек."""
    parts = chat_participants(chat_id)
    if "tck" in parts:
        maybe_tck_autoreply(chat_id)
    return fetch_messages(chat_id)


@app.post("/api/messages")
def post_message(data: MessageIn, background: BackgroundTasks):
    sender  = clean_username(data.sender_username)
    text    = (data.text or "").strip()[:4000]
    chat_id = (data.chat_id or "").strip()
    if not sender or not text:
        raise HTTPException(400, "Порожнє повідомлення або некоректний відправник.")
    if CHAT_SEP not in chat_id:
        raise HTTPException(400, "Некоректний chat_id.")

    msg = save_message(chat_id, sender, text)
    parts = chat_participants(chat_id)

    if "ai" in parts and sender != "ai":
        background.add_task(handle_ai_reply, chat_id)
    if "tck" in parts and sender != "tck":
        background.add_task(handle_tck_reply, chat_id)
    return msg


@app.post("/api/messages/{chat_id}/read")
def mark_read(chat_id: str, data: ReadIn):
    me = clean_username(data.username)
    if not me:
        raise HTTPException(400, "Некоректний username.")
    with closing(db()) as conn, conn:
        conn.execute(
            "UPDATE messages SET is_read = 1 WHERE chat_id = ? AND sender_username <> ?",
            (chat_id, me),
        )
    return {"ok": True}


# ------------------------------------------------------------------ #
# 7. ЛОГИКА БОТОВ (ai и tck)                                          #
# ------------------------------------------------------------------ #
def maybe_tck_autoreply(chat_id: str) -> None:
    """
    Вызывается при GET чата с ТЦК.
    • Пустой чат  -> ТЦК здоровается (вход в чат).
    • Иначе       -> если с последней реплики ТЦК прошло >= 30 сек, добавляет новую.
    Так как клиент опрашивает чат каждые 2 сек, пока он открыт, ТЦК «капает»
    примерно раз в 30 секунд и затихает, когда чат закрыт.
    """
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


def build_anthropic_messages(rows: list) -> list:
    """История чата -> формат Anthropic. sender 'ai' => assistant, иначе user.
    Склеиваем подряд идущие одинаковые роли и начинаем с роли user."""
    msgs = []
    for r in rows:
        role = "assistant" if r["sender"] == "ai" else "user"
        if msgs and msgs[-1]["role"] == role:
            msgs[-1]["content"] += "\n" + r["text"]
        else:
            msgs.append({"role": role, "content": r["text"]})
    while msgs and msgs[0]["role"] != "user":
        msgs.pop(0)
    return msgs


async def handle_ai_reply(chat_id: str) -> None:
    """Запрос к Claude (Anthropic API) и сохранение ответа в базу."""
    if not CLAUDE_API_KEY:
        save_message(chat_id, "ai", "⚠️ ШІ недоступний: на сервері не задано змінну CLAUDE_API_KEY.")
        return

    messages = build_anthropic_messages(fetch_messages(chat_id)[-40:])
    if not messages:
        return

    payload = {
        "model": CLAUDE_MODEL,
        "max_tokens": 1024,
        "system": (
            "Ти — Клод, доброзичливий і корисний асистент усередині месенджера «Іван». "
            "Відповідай природно й тією ж мовою, якою пише користувач (українською або російською). "
            "Будь лаконічним, але змістовним; можна з легким гумором."
        ),
        "messages": messages,
        # temperature НЕ передаём — новые модели возвращают 400 на этот параметр.
    }
    headers = {
        "x-api-key": CLAUDE_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(ANTHROPIC_URL, headers=headers, json=payload)
        if resp.status_code != 200:
            save_message(chat_id, "ai", f"⚠️ Помилка Anthropic API ({resp.status_code}). {resp.text[:300]}")
            return
        data = resp.json()
        text = "".join(b.get("text", "") for b in data.get("content", []) if b.get("type") == "text").strip()
        save_message(chat_id, "ai", text or "(порожня відповідь)")
    except Exception as exc:                       # noqa: BLE001
        save_message(chat_id, "ai", f"⚠️ Не вдалося звʼязатися з ШІ: {exc}")


# ------------------------------------------------------------------ #
# 8. СТАРТ                                                            #
# ------------------------------------------------------------------ #
init_db()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
