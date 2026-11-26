from __future__ import annotations

import asyncio
import html
import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import aiosqlite
from aiohttp import ClientSession, ClientTimeout
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("awesomevpn")

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
PANEL_API_TOKEN = os.getenv("PANEL_API_TOKEN", "").strip()
PANEL_API_URL = os.getenv("PANEL_API_URL", "").strip().rstrip("/")
if PANEL_API_URL.endswith("/api"):
    PANEL_API_URL = PANEL_API_URL[:-4]
PLATEGA_MERCHANT_ID = os.getenv("PLATEGA_MERCHANT_ID", "").strip()
PLATEGA_SECRET = (os.getenv("PLATEGA_SECRET", "") or os.getenv("PLATEGA_API", "")).strip()
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").replace(" ", "").split(",") if x.isdigit()}
DB_PATH = os.getenv("DB_PATH", "data/awesomevpn.sqlite3")
PLATEGA_API = os.getenv("PLATEGA_API_URL", "https://app.platega.io").rstrip("/")
PAYMENT_METHOD = int(os.getenv("PLATEGA_PAYMENT_METHOD", "2"))
CURRENCY = os.getenv("CURRENCY", "RUB")
DEFAULT_TRAFFIC_GB = float(os.getenv("TRAFFIC_LIMIT_GB", "0"))
DEFAULT_CHANNELS = [x.strip() for x in os.getenv("PANEL_CHANNELS", "main,reality").split(",") if x.strip()]
LOCATIONS = json.loads(os.getenv("PANEL_LOCATIONS_JSON", "{}") or "{}")
if not isinstance(LOCATIONS, dict):
    LOCATIONS = {}
# If PANEL_LOCATIONS_JSON is empty, the bot discovers named inbounds from the
# primary H1 panel and its linked federation nodes using read-only API calls.
PLANS = {
    "3d": {"title": "3 дня", "days": 3, "price": 29, "devices": 2},
    "30d": {"title": "1 месяц", "days": 30, "price": 139, "devices": 3},
    "90d": {"title": "3 месяца", "days": 90, "price": 360, "devices": 3},
    "365d": {"title": "12 месяцев", "days": 365, "price": 1299, "devices": 5},
}

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is required")
if not PANEL_API_TOKEN or not PANEL_API_URL:
    raise RuntimeError("PANEL_API_TOKEN and PANEL_API_URL are required")
if not PLATEGA_MERCHANT_ID or not PLATEGA_SECRET:
    # Never log credentials; report presence flags only to diagnose missing Relax Dev variables.
    log.error(
        "Platega environment check: PLATEGA_MERCHANT_ID=%s, PLATEGA_SECRET/PLATEGA_API=%s",
        bool(PLATEGA_MERCHANT_ID), bool(PLATEGA_SECRET)
    )
    raise RuntimeError(
        "Platega credentials are not visible to this process. In Relax Dev, save "
        "PLATEGA_MERCHANT_ID and PLATEGA_SECRET, then restart/redeploy the latest commit."
    )

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
http: ClientSession | None = None
db: aiosqlite.Connection | None = None
user_locks: dict[int, asyncio.Lock] = {}
payment_locks: dict[str, asyncio.Lock] = {}


def kb(rows: list[list[tuple[str, str]]]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=label, callback_data=data) for label, data in row]
        for row in rows
    ])


def home_kb() -> InlineKeyboardMarkup:
    return kb([
        [("🟢 Купить VPN", "buy"), ("👤 Мой VPN", "profile")],
        [("➕ Добавить ключ", "addkey"), ("📲 Перенести в приложение", "apps")],
        [("🌍 Локации", "locations"), ("💳 Пополнить баланс", "balance")],
        [("🎁 Пригласить друга", "invite")],
    ])


async def init_db() -> None:
    global db
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    await db.executescript("""
    PRAGMA journal_mode=WAL;
    CREATE TABLE IF NOT EXISTS users(
      tg_id INTEGER PRIMARY KEY, username TEXT, balance REAL NOT NULL DEFAULT 0,
      client_name TEXT, client_uuid TEXT, sub_url TEXT, expires_at INTEGER,
      device_limit INTEGER NOT NULL DEFAULT 0, selected_locations TEXT NOT NULL DEFAULT '[]',
      created_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS payments(
      order_id TEXT PRIMARY KEY, tg_id INTEGER NOT NULL, amount REAL NOT NULL,
      plan_key TEXT, locations TEXT NOT NULL DEFAULT '[]', status TEXT NOT NULL, provider_id TEXT, created_at INTEGER NOT NULL,
      processed_at INTEGER
    );
    CREATE TABLE IF NOT EXISTS processed_callbacks(
      payment_id TEXT PRIMARY KEY, received_at INTEGER NOT NULL
    );
    """)
    await db.commit()
    try:
        await db.execute("ALTER TABLE payments ADD COLUMN locations TEXT NOT NULL DEFAULT '[]'")
        await db.commit()
    except Exception:
        pass


async def ensure_user(tg_id: int, username: str | None) -> aiosqlite.Row:
    assert db
    await db.execute("INSERT INTO users(tg_id,username,created_at) VALUES(?,?,?) ON CONFLICT(tg_id) DO UPDATE SET username=excluded.username",
                     (tg_id, username, int(time.time())))
    await db.commit()
    cur = await db.execute("SELECT * FROM users WHERE tg_id=?", (tg_id,))
    return await cur.fetchone()


async def panel_request(method: str, path: str, payload: dict | None = None) -> dict:
    assert http
    url = f"{PANEL_API_URL}/api{path}"
    async with http.request(method, url, headers={"Authorization": f"Bearer {PANEL_API_TOKEN}", "Accept": "application/json"},
                            json=payload, timeout=ClientTimeout(total=35)) as resp:
        data = await resp.json(content_type=None)
        if resp.status >= 400 or (isinstance(data, dict) and (data.get("ok") is False or data.get("success") is False)):
            message = (data.get("msg") or data.get("message") or "request rejected") if isinstance(data, dict) else "request rejected"
            raise RuntimeError(f"Panel API {resp.status}: {message}")
        if not isinstance(data, dict):
            raise RuntimeError(f"Unexpected panel response: {data!r}")
        return data


async def platega_request(method: str, path: str, payload: dict | None = None) -> dict:
    assert http
    async with http.request(method, f"{PLATEGA_API}{path}",
        headers={"X-MerchantId": PLATEGA_MERCHANT_ID, "X-Secret": PLATEGA_SECRET, "Content-Type": "application/json"},
        json=payload, timeout=ClientTimeout(total=25)) as resp:
        data = await resp.json(content_type=None)
        if resp.status >= 400:
            raise RuntimeError(f"Platega API {resp.status}: {data}")
        return data


def location_keyboard(prefix: str = "loc") -> InlineKeyboardMarkup:
    rows = [[(name, f"{prefix}:{key}")] for key, name in enumerate(LOCATIONS.keys())]
    rows.append([("🌐 Все локации", f"{prefix}:all")])
    rows.append([("⬅️ Назад", "home")])
    return kb(rows)


def _panel_safe_summary(value, depth: int = 0):
    """Keep diagnostics useful while never exposing client credentials or secrets."""
    if depth > 5:
        return "…"
    if isinstance(value, list):
        return [_panel_safe_summary(item, depth + 1) for item in value[:30]]
    if isinstance(value, dict):
        hidden = ("token", "secret", "password", "uuid", "client", "private", "key")
        result = {}
        for key, item in value.items():
            low = str(key).lower()
            if any(word in low for word in hidden):
                result[str(key)] = "[скрыто]"
            elif isinstance(item, (dict, list)):
                result[str(key)] = _panel_safe_summary(item, depth + 1)
            elif isinstance(item, (str, int, float, bool)) or item is None:
                result[str(key)] = item
        return result
    return value


def _panel_items(data):
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("obj", "data", "items", "nodes", "inbounds", "servers"):
            value = data.get(key)
            if isinstance(value, list):
                return value
            if isinstance(value, dict):
                nested = _panel_items(value)
                if nested:
                    return nested
    return []


def _first_field(item: dict, names: tuple[str, ...]):
    for name in names:
        value = item.get(name)
        if value not in (None, ""):
            return value
    return None


def _inbound_location_label(item: dict) -> tuple[str, str] | None:
    """Map only explicit inbound names to a country/feature; never infer by port."""
    text_value = " ".join(
        str(item.get(key, "")) for key in
        ("remark", "tag", "name", "label", "title", "description", "country", "listen")
    ).casefold()
    compact = text_value.replace("_", " ").replace("-", " ")
    rules = (
        (("нидер", "netherlands", "holland", " nl "), "🇳🇱 Нидерланды", "main"),
        (("польш", "poland", " pl "), "🇵🇱 Польша", "main"),
        (("финлянд", "finland", " fi "), "🇫🇮 Финляндия", "main"),
        (("герман", "germany", "deutschland", " de "), "🇩🇪 Германия", "main"),
        (("сша", "usa", "united states", "america", " us "), "🇺🇸 США", "main"),
        (("япони", "japan", " jp "), "🇯🇵 Япония", "main"),
        (("обход бел", "white list", "whitelist", "bypass", " белых спис"), "🌍 Обход белых списков (IP)", "bs"),
    )
    padded = f" {compact} "
    for needles, label, channel in rules:
        if any(needle in padded for needle in needles):
            return label, channel
    return None


async def discover_h1_locations() -> tuple[dict, list[str]]:
    """Discover country inbounds on the primary panel and all linked H1 nodes."""
    discovered: dict[str, dict] = {}
    report: list[str] = []
    node_rows: list[tuple[str, str]] = [("", "Основной сервер")]
    federation = await panel_request("GET", "/fed/lagg")
    for node in _panel_items(federation):
        if not isinstance(node, dict):
            continue
        node_id = _first_field(node, ("node_id", "nodeId", "server_id", "serverId", "id"))
        if node_id is None:
            continue
        node_name = str(_first_field(node, ("name", "remark", "title", "label", "address", "host")) or f"Узел {node_id}")
        node_rows.append((str(node_id), node_name))

    for node_id, node_name in node_rows:
        path = f"/fed/lproxy/{quote(node_id, safe='')}/inbounds" if node_id else "/inbounds"
        try:
            response = await panel_request("GET", path)
        except Exception as exc:
            report.append(f"⚠️ {html.escape(node_name)}: не удалось получить инбаунды ({html.escape(str(exc)[:180])})")
            continue
        inbounds = _panel_items(response)
        found_here = 0
        for inbound in inbounds:
            if not isinstance(inbound, dict):
                continue
            inbound_id = _first_field(inbound, ("id", "inbound_id", "inboundId", "inboundID"))
            mapped = _inbound_location_label(inbound)
            if inbound_id is None or mapped is None:
                continue
            label, channel = mapped
            cfg = discovered.setdefault(label, {"node_id": node_id, "inbound_ids": [], "channels": list(DEFAULT_CHANNELS)})
            # One subscription is created per H1 node, with all matching local inbounds.
            if cfg["node_id"] == node_id and str(inbound_id) not in [str(x) for x in cfg["inbound_ids"]]:
                cfg["inbound_ids"].append(str(inbound_id))
                if channel == "bs" and "bs" not in cfg["channels"]:
                    cfg["channels"].append("bs")
                found_here += 1
        report.append(f"• {html.escape(node_name)}: просмотрено {len(inbounds)} инбаундов, распознано {found_here}")
    return discovered, report


@dp.message(Command("h1discover"))
async def h1_discover_command(message: Message):
    """Admin-only read-only H1 discovery and in-memory location refresh."""
    if message.from_user.id not in ADMIN_IDS:
        return await message.answer("Команда доступна только администратору.")
    await message.answer("🔎 Ищу локации в основной панели и связке H1. Выполняю только GET-запросы.")
    try:
        discovered, report = await discover_h1_locations()
        if discovered:
            LOCATIONS.clear()
            LOCATIONS.update(discovered)
            names = "\n".join(f"• {html.escape(name)} — узел <code>{html.escape(str(cfg['node_id']) or 'primary')}</code>, inbound <code>{html.escape(','.join(cfg['inbound_ids']))}</code>" for name, cfg in discovered.items())
            text = "✅ <b>Найдены локации:</b>\n" + names + "\n\n" + "\n".join(report) + "\n\nТеперь новые покупки будут включать все найденные локации в одну подписку."
        else:
            text = "⚠️ Не удалось автоматически распознать названия локаций.\n" + "\n".join(report) + "\n\nВыполни /h1diag и пришли ответ — по нему подстрою сопоставление."
        await message.answer(text[:3900])
    except Exception as exc:
        log.exception("H1 discovery failed")
        await message.answer(f"❌ Ошибка чтения H1 API: <code>{html.escape(str(exc)[:700])}</code>")


@dp.message(Command("h1diag"))
async def h1_diagnostics(message: Message):
    """Admin-only read-only discovery of connected H1 nodes and their inbounds."""
    if message.from_user.id not in ADMIN_IDS:
        return await message.answer("Команда доступна только администратору.")
    await message.answer("🔎 Проверяю подключённые узлы и инбаунды H1. Это только GET-запросы, настройки не изменяются.")
    reports = []
    node_ids = []
    for path, title in (("/fed/lagg", "Связанные серверы"), ("/inbounds", "Инбаунды основной панели")):
        try:
            data = await panel_request("GET", path)
            safe = _panel_safe_summary(data)
            reports.append(f"<b>{html.escape(title)}</b> <code>{html.escape(path)}</code>\n<pre>{html.escape(json.dumps(safe, ensure_ascii=False)[:2200])}</pre>")
            if path == "/fed/lagg":
                for item in _panel_items(data):
                    if not isinstance(item, dict):
                        continue
                    node_id = next((item.get(k) for k in ("node_id", "nodeId", "id", "server_id", "serverId", "uuid") if item.get(k) not in (None, "")), None)
                    if node_id is not None:
                        node_ids.append(str(node_id))
        except Exception as exc:
            reports.append(f"<b>{html.escape(title)}</b> <code>{html.escape(path)}</code>\nОшибка: <code>{html.escape(str(exc)[:700])}</code>")
    for node_id in dict.fromkeys(node_ids):
        path = f"/fed/lproxy/{quote(node_id, safe='')}/inbounds"
        try:
            data = await panel_request("GET", path)
            safe = _panel_safe_summary(data)
            reports.append(f"<b>Инбаунды связанного узла {html.escape(node_id)}</b> <code>{html.escape(path)}</code>\n<pre>{html.escape(json.dumps(safe, ensure_ascii=False)[:2200])}</pre>")
        except Exception as exc:
            reports.append(f"<b>Инбаунды связанного узла {html.escape(node_id)}</b>\nОшибка <code>{html.escape(str(exc)[:700])}</code>")
    output = "\n\n".join(reports) if reports else "Панель не вернула данные."
    chunks = []
    while len(output) > 3800:
        cut = output.rfind("\n\n", 0, 3800)
        if cut < 1:
            cut = 3800
        chunks.append(output[:cut])
        output = output[cut:].lstrip()
    chunks.append(output)
    for chunk in chunks:
        await message.answer(chunk)


async def show_home(target: Message | CallbackQuery) -> None:
    text = (
        "🚀 <b>Awesome VPN</b>\n\n"
        "Быстрый и стабильный VPN для телефона и компьютера.\n"
        "Выберите нужный раздел ниже."
    )
    if isinstance(target, CallbackQuery):
        await target.message.edit_text(text, reply_markup=home_kb())
        await target.answer()
    else:
        await target.answer(text, reply_markup=home_kb())


@dp.message(CommandStart())
async def start(message: Message):
    await ensure_user(message.from_user.id, message.from_user.username)
    await show_home(message)


@dp.message(Command("help"))
async def help_cmd(message: Message):
    await message.answer("Используйте кнопки меню: купить VPN, посмотреть подписку или перенести ключ в приложение.", reply_markup=home_kb())


@dp.callback_query(F.data == "home")
async def home_cb(c: CallbackQuery):
    await show_home(c)


@dp.callback_query(F.data == "buy")
async def buy(c: CallbackQuery):
    rows = [[(f"📦 {p['title']} — {p['price']} ₽", f"plan:{key}")] for key, p in PLANS.items()]
    rows.append([("⬅️ В меню", "home")])
    await c.message.edit_text("🛒 <b>Выберите тариф</b>\n\nБезлимитный трафик. Устройства указаны для каждого тарифа.", reply_markup=kb(rows))
    await c.answer()


async def create_plan_payment(c: CallbackQuery, plan_key: str, selected: list[str]) -> None:
    plan = PLANS[plan_key]
    order_id = str(uuid.uuid4())
    assert db
    await db.execute("INSERT INTO payments(order_id,tg_id,amount,plan_key,status,created_at) VALUES(?,?,?,?,?,?)",
                     (order_id, c.from_user.id, plan["price"], plan_key, "CREATING", int(time.time())))
    await db.execute("UPDATE payments SET locations=? WHERE order_id=?", (json.dumps(selected), order_id))
    await db.commit()
    try:
        result = await platega_request("POST", "/transaction/process", {
            "paymentMethod": PAYMENT_METHOD,
            "paymentDetails": {"amount": plan["price"], "currency": CURRENCY},
            "description": f"Awesome VPN — {plan['title']}",
            "payload": order_id,
            "orderId": order_id,
            "metadata": {"userId": str(c.from_user.id), "userName": c.from_user.username or "",
                         "user_email": f"{c.from_user.id}@telegram.org"}
        })
        provider_id = result.get("transactionId") or result.get("id")
        pay_url = result.get("redirect") or result.get("url")
        if not provider_id or not pay_url:
            raise RuntimeError(f"Unexpected Platega create response: {result}")
        await db.execute("UPDATE payments SET status='PENDING',provider_id=? WHERE order_id=?", (provider_id, order_id))
        await db.commit()
        await c.message.edit_text(
            f"💳 <b>Заказ создан</b>\n\nТариф: {plan['title']}\nСумма: <b>{plan['price']} ₽</b>\n\n"
            "После оплаты бот активирует единую подписку со всеми доступными локациями.",
            reply_markup=kb([[("⬅️ В меню", "home")]]))
        await c.message.answer("Нажмите кнопку ниже для оплаты.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Оплатить", url=pay_url)],
            [InlineKeyboardButton(text="Проверить оплату", callback_data=f"check:{order_id}")]
        ]))
    except Exception:
        log.exception("Payment creation failed")
        await db.execute("UPDATE payments SET status='ERROR' WHERE order_id=?", (order_id,))
        await db.commit()
        await c.message.edit_text("Не удалось создать платёж. Попробуйте позже или обратитесь в поддержку.", reply_markup=home_kb())
    await c.answer()


@dp.callback_query(F.data.startswith("plan:"))
async def choose_plan(c: CallbackQuery):
    key = c.data.split(":", 1)[1]
    if key not in PLANS:
        return await c.answer("Тариф не найден", show_alert=True)
    if not LOCATIONS:
        try:
            discovered, _ = await discover_h1_locations()
            if discovered:
                LOCATIONS.clear()
                LOCATIONS.update(discovered)
        except Exception:
            log.exception("On-demand H1 location discovery failed")
    if not LOCATIONS:
        return await c.answer("Не удалось получить локации H1. Напишите администратору.", show_alert=True)
    await ensure_user(c.from_user.id, c.from_user.username)
    # One purchase creates one client identity across every discovered node/inbound.
    await create_plan_payment(c, key, list(LOCATIONS.keys()))


@dp.callback_query(F.data.startswith("check:"))
async def check_payment(c: CallbackQuery):
    order_id = c.data.split(":", 1)[1]
    assert db
    cur = await db.execute("SELECT * FROM payments WHERE order_id=? AND tg_id=?", (order_id, c.from_user.id))
    order = await cur.fetchone()
    if not order:
        return await c.answer("Заказ не найден", show_alert=True)
    if order["status"] == "CONFIRMED":
        return await c.answer("Оплата уже подтверждена", show_alert=True)
    try:
        state = await verify_and_process(order["provider_id"], order_id)
        if state == "CONFIRMED":
            await c.answer("Оплата подтверждена. Доступ или баланс обновлён!")
        elif state == "CANCELED":
            await c.answer("Платёж отменён.", show_alert=True)
        else:
            await c.answer("Платёж пока не подтверждён. Бот проверит его автоматически.")
    except Exception:
        log.exception("Payment status check failed")
        await c.answer("Не удалось проверить платёж. Попробуйте ещё раз.", show_alert=True)


async def provision_paid_order(order_id: str) -> None:
    assert db
    cur = await db.execute("SELECT * FROM payments WHERE order_id=?", (order_id,))
    order = await cur.fetchone()
    if not order or order["status"] == "CONFIRMED" and order["processed_at"]:
        return
    plan = PLANS.get(order["plan_key"])
    if not plan:
        raise RuntimeError("Unknown paid plan")
    lock = user_locks.setdefault(order["tg_id"], asyncio.Lock())
    async with lock:
        cur = await db.execute("SELECT * FROM users WHERE tg_id=?", (order["tg_id"],))
        user = await cur.fetchone()
        client_name = user["client_name"] if user and user["client_name"] else f"tg_{order['tg_id']}"
        client_uuid = user["client_uuid"] if user and user["client_uuid"] else str(uuid.uuid4())
        selected = json.loads(order["locations"] or "[]") or list(LOCATIONS.keys())
        missing_locations = [name for name in selected if name not in LOCATIONS]
        if missing_locations:
            # Pending orders may be confirmed after a bot restart; rebuild the in-memory
            # map before provisioning so payment reconciliation never depends on RAM state.
            discovered, _ = await discover_h1_locations()
            LOCATIONS.update(discovered)
            still_missing = [name for name in selected if name not in LOCATIONS]
            if still_missing:
                raise RuntimeError("H1 locations are not discoverable: " + ", ".join(still_missing))
        expires_at = max(int(user["expires_at"] or 0) if user else 0, int(time.time())) + plan["days"] * 86400
        # Group all selected country inbounds by H1 node. A single create request per
        # node is important: it attaches the same client UUID to every local inbound.
        node_configs: dict[str, dict] = {}
        for loc in selected:
            cfg = LOCATIONS[loc]
            node_id = str(cfg.get("node_id", "")).strip()
            grouped = node_configs.setdefault(node_id, {"inbound_ids": [], "channels": []})
            for inbound_id in cfg.get("inbound_ids", []):
                if str(inbound_id) not in [str(x) for x in grouped["inbound_ids"]]:
                    grouped["inbound_ids"].append(str(inbound_id))
            for channel in cfg.get("channels", DEFAULT_CHANNELS):
                if channel not in grouped["channels"]:
                    grouped["channels"].append(channel)
        success_nodes = []
        for node_id, cfg in node_configs.items():
            inbounds = cfg["inbound_ids"]
            if not inbounds:
                raise RuntimeError(f"No inbound IDs discovered for H1 node {node_id or 'primary'}")
            path = f"/fed/lproxy/{quote(node_id, safe='')}/create" if node_id else "/create"
            existing_path = f"/fed/lproxy/{quote(node_id, safe='')}/clients/{quote(client_name, safe='')}" if node_id else f"/clients/{quote(client_name, safe='')}"
            existing_client = None
            try:
                existing = await panel_request("GET", existing_path)
                candidate = existing.get("client", existing)
                if isinstance(candidate, dict) and candidate.get("uuid") == client_uuid:
                    existing_client = candidate
            except Exception:
                existing_client = None
            if existing_client:
                await panel_request("PATCH", existing_path, {
                    "expires_at": expires_at, "device_limit": plan["devices"],
                    "traffic_limit_gb": DEFAULT_TRAFFIC_GB
                })
            else:
                payload = {
                    "name": client_name, "uuid": client_uuid, "expires_at": expires_at,
                    "days": plan["days"], "device_limit": plan["devices"],
                    "traffic_limit_gb": DEFAULT_TRAFFIC_GB, "manual": True,
                    "channels": cfg["channels"] or DEFAULT_CHANNELS,
                    "inbound_ids": inbounds
                }
                await panel_request("POST", path, payload)
            success_nodes.append(node_id or "primary")
        # Read the canonical client on the primary panel to get the panel-generated unified subscription URL.
        canonical = await panel_request("GET", f"/clients/{quote(client_name, safe='')}")
        client = canonical.get("client", canonical)
        sub_url = client.get("subscription_url") or client.get("sub_url") or ""
        if not sub_url:
            raise RuntimeError("H1 panel did not return subscription_url/sub_url")
        await db.execute("""UPDATE users SET client_name=?,client_uuid=?,sub_url=?,expires_at=?,device_limit=?,selected_locations=? WHERE tg_id=?""",
                         (client_name, client_uuid, sub_url, expires_at, plan["devices"], json.dumps(selected), order["tg_id"]))
        await db.execute("UPDATE payments SET status='CONFIRMED',processed_at=? WHERE order_id=?", (int(time.time()), order_id))
        await db.commit()
        await bot.send_message(order["tg_id"], "🎉 <b>Оплата получена!</b>\n\nVPN активирован. Нажмите «Мой VPN», чтобы получить ссылку для подключения.", reply_markup=home_kb())


async def verify_and_process(provider_id: str, order_id: str) -> str:
    """Query Platega directly; no public webhook/domain is required."""
    if not provider_id:
        raise RuntimeError("Missing Platega transaction id")
    lock = payment_locks.setdefault(order_id, asyncio.Lock())
    async with lock:
        assert db
        cur = await db.execute("SELECT * FROM payments WHERE order_id=?", (order_id,))
        order = await cur.fetchone()
        if not order:
            raise RuntimeError("Unknown local payment order")
        if order["status"] == "CONFIRMED" and order["processed_at"]:
            return "CONFIRMED"

        data = await platega_request("GET", f"/transaction/{quote(provider_id, safe='')}")
        state = str(data.get("status", "PENDING")).upper()
        if state == "CANCELED":
            await db.execute(
                "UPDATE payments SET status='CANCELED' WHERE order_id=? AND status!='CONFIRMED'",
                (order_id,)
            )
            await db.commit()
            return "CANCELED"
        if state != "CONFIRMED":
            return state

        actual = (data.get("paymentDetails") or {}).get("amount")
        if actual is not None and float(actual) != float(order["amount"]):
            raise RuntimeError("Payment amount mismatch")
        actual_currency = (data.get("paymentDetails") or {}).get("currency")
        if actual_currency and actual_currency != CURRENCY:
            raise RuntimeError("Payment currency mismatch")

        if str(order["plan_key"] or "").startswith("balance:"):
            if not order["processed_at"]:
                await db.execute(
                    "UPDATE users SET balance=balance+? WHERE tg_id=?",
                    (float(order["amount"]), order["tg_id"])
                )
                await db.execute(
                    "UPDATE payments SET status='CONFIRMED',processed_at=? WHERE order_id=? AND processed_at IS NULL",
                    (int(time.time()), order_id)
                )
                await db.commit()
                await bot.send_message(
                    order["tg_id"],
                    f"✅ Баланс пополнен на <b>{order['amount']:.0f} ₽</b>.",
                    reply_markup=home_kb()
                )
            return "CONFIRMED"

        await provision_paid_order(order_id)
        return "CONFIRMED"


async def poll_pending_payments() -> None:
    """Background reconciliation for pending Platega transactions."""
    interval = max(10, int(os.getenv("PAYMENT_POLL_INTERVAL", "20")))
    log.info("Platega payment polling enabled (interval=%ss)", interval)
    while True:
        try:
            assert db
            cur = await db.execute(
                "SELECT order_id,provider_id FROM payments "
                "WHERE status IN ('PENDING','CREATING') AND provider_id IS NOT NULL "
                "ORDER BY created_at ASC LIMIT 50"
            )
            pending = await cur.fetchall()
            for row in pending:
                try:
                    state = await verify_and_process(row["provider_id"], row["order_id"])
                    if state == "CONFIRMED":
                        log.info("Platega order %s confirmed by polling", row["order_id"])
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("Platega polling failed for order %s", row["order_id"])
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Platega payment polling loop failed")
        await asyncio.sleep(interval)


@dp.callback_query(F.data == "profile")
async def profile(c: CallbackQuery):
    user = await ensure_user(c.from_user.id, c.from_user.username)
    if not user["client_name"]:
        await c.message.edit_text("👤 <b>Мой VPN</b>\n\nАктивной подписки пока нет. Выберите тариф, чтобы подключиться.", reply_markup=kb([[("🟢 Купить VPN", "buy")], [("⬅️ В меню", "home")]]))
        return await c.answer()
    try:
        data = await panel_request("GET", f"/clients/{quote(user['client_name'], safe='')}")
        client = data.get("client", data)
        used = float(client.get("traffic_used_gb") or 0)
        limit = float(client.get("traffic_limit_gb") or 0)
        expiry = int(client.get("expires_at") or user["expires_at"] or 0)
        expiry_text = datetime.fromtimestamp(expiry, timezone.utc).astimezone().strftime("%d.%m.%Y %H:%M") if expiry else "бессрочно"
        status = "🟢 Активна" if client.get("enable", True) else "🔴 Отключена"
        text = (f"👤 <b>Мой VPN</b>\n\n💰 Баланс: {user['balance']:.0f} ₽\n\n{status}\n"
                f"⏳ До: {expiry_text}\n📊 Использовано: {used:.2f} ГБ" + (f" из {limit:g} ГБ" if limit else " • безлимит"))
        sub_url = client.get("subscription_url") or client.get("sub_url") or user["sub_url"]
        await c.message.edit_text(text, reply_markup=profile_kb(sub_url))
    except Exception:
        log.exception("Profile lookup failed")
        await c.message.edit_text("Не удалось получить данные подписки. Попробуйте позже.", reply_markup=home_kb())
    await c.answer()


def profile_kb(sub_url: str) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if sub_url.startswith("https://"):
        base = sub_url.rstrip("/")
        rows.append([InlineKeyboardButton(text="🔗 Подписка", url=sub_url)])
        rows.append([
            InlineKeyboardButton(text="➕ Happ", url=base + "/open/happ"),
            InlineKeyboardButton(text="➕ v2rayTun", url=base + "/open/v2raytun"),
        ])
        rows.append([InlineKeyboardButton(text="➕ INCI", url=base + "/open/inci")])
    rows.extend([
        [InlineKeyboardButton(text="📲 Мои устройства", callback_data="apps")],
        [InlineKeyboardButton(text="🌍 Локации", callback_data="locations")],
        [InlineKeyboardButton(text="⬅️ В меню", callback_data="home")],
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@dp.callback_query(F.data == "apps")
async def apps(c: CallbackQuery):
    user = await ensure_user(c.from_user.id, c.from_user.username)
    sub = user["sub_url"] or ""
    if not sub:
        return await c.answer("Сначала подключите тариф.", show_alert=True)
    base = sub.rstrip("/")
    await c.message.edit_text("📲 <b>Перенос ключа в приложение</b>\n\nВыберите приложение — откроется ссылка импорта подписки.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Happ", url=base + "/open/happ")],
        [InlineKeyboardButton(text="v2rayTun", url=base + "/open/v2raytun")],
        [InlineKeyboardButton(text="INCI", url=base + "/open/inci")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="profile")]
    ]))
    await c.answer()


@dp.callback_query(F.data == "locations")
async def locations(c: CallbackQuery):
    user = await ensure_user(c.from_user.id, c.from_user.username)
    names = json.loads(user["selected_locations"] or "[]")
    text = "🌍 <b>Ваши локации</b>\n\n" + ("\n".join(names) if names else "Нет активных локаций.")
    await c.message.edit_text(text, reply_markup=kb([[("⬅️ В меню", "home")]]))
    await c.answer()


@dp.callback_query(F.data == "balance")
async def balance(c: CallbackQuery):
    await c.message.edit_text("💳 <b>Пополнение баланса</b>\\n\\nВыберите сумму:", reply_markup=kb([
        [("100 ₽", "balance_amt:100"), ("300 ₽", "balance_amt:300")],
        [("500 ₽", "balance_amt:500"), ("1000 ₽", "balance_amt:1000")],
        [("⬅️ В меню", "home")]
    ]))
    await c.answer()


@dp.callback_query(F.data.startswith("balance_amt:"))
async def balance_amount(c: CallbackQuery):
    amount = int(c.data.split(":", 1)[1])
    order_id = str(uuid.uuid4())
    assert db
    await db.execute("INSERT INTO payments(order_id,tg_id,amount,plan_key,status,created_at) VALUES(?,?,?,?,?,?)",
                     (order_id, c.from_user.id, amount, f"balance:{amount}", "CREATING", int(time.time())))
    await db.commit()
    try:
        result = await platega_request("POST", "/transaction/process", {
            "paymentMethod": PAYMENT_METHOD,
            "paymentDetails": {"amount": amount, "currency": CURRENCY},
            "description": "Пополнение баланса Awesome VPN",
            "payload": order_id, "orderId": order_id,
            "metadata": {"userId": str(c.from_user.id), "userName": c.from_user.username or "",
                         "user_email": f"{c.from_user.id}@telegram.org"}
        })
        provider_id = result.get("transactionId") or result.get("id")
        pay_url = result.get("redirect") or result.get("url")
        if not provider_id or not pay_url:
            raise RuntimeError(f"Unexpected Platega response: {result}")
        await db.execute("UPDATE payments SET status='PENDING',provider_id=? WHERE order_id=?", (provider_id, order_id))
        await db.commit()
        await c.message.edit_text(f"💳 Пополнение на <b>{amount} ₽</b>. После оплаты баланс обновится автоматически.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Оплатить", url=pay_url)],
                [InlineKeyboardButton(text="Проверить оплату", callback_data=f"check:{order_id}")],
                [InlineKeyboardButton(text="⬅️ В меню", callback_data="home")]
            ]))
    except Exception:
        log.exception("Balance payment creation failed")
        await c.message.edit_text("Не удалось создать платёж. Попробуйте позже.", reply_markup=home_kb())
    await c.answer()


@dp.callback_query(F.data == "invite")
async def invite(c: CallbackQuery):
    me = await bot.get_me()
    await c.message.edit_text(f"🎁 Приглашайте друзей в Awesome VPN:\nhttps://t.me/{me.username}?start=ref_{c.from_user.id}", reply_markup=kb([[("⬅️ В меню", "home")]]))
    await c.answer()


@dp.callback_query(F.data == "addkey")
async def addkey(c: CallbackQuery):
    await c.answer("Для создания нового подключения выберите тариф в разделе «Купить VPN».", show_alert=True)


@dp.callback_query(F.data == "noop")
async def noop(c: CallbackQuery):
    await c.answer()


async def main():
    global http
    await init_db()
    http = ClientSession(timeout=ClientTimeout(total=35))
    payment_poller = asyncio.create_task(poll_pending_payments(), name="platega-payment-poller")
    try:
        log.info("Awesome VPN Telegram bot started; no public domain/webhook required")
        await dp.start_polling(bot)
    finally:
        payment_poller.cancel()
        try:
            await payment_poller
        except asyncio.CancelledError:
            pass
        await http.close()
        if db:
            await db.close()


if __name__ == "__main__":
    asyncio.run(main())
