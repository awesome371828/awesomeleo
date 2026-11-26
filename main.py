from __future__ import annotations

import asyncio
import html
import json
import logging
import os
import secrets
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote, urlsplit, urlunsplit

import aiosqlite
from aiohttp import ClientSession, ClientTimeout, web
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
PLATEGA_MERCHANT_ID = os.getenv("PLATEGA_MERCHANT_ID", "").strip()
PLATEGA_SECRET = os.getenv("PLATEGA_SECRET", "").strip()
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").strip().rstrip("/")
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").replace(" ", "").split(",") if x.isdigit()}
DB_PATH = os.getenv("DB_PATH", "data/awesomevpn.sqlite3")
PLATEGA_API = os.getenv("PLATEGA_API_URL", "https://app.platega.io").rstrip("/")
PAYMENT_METHOD = int(os.getenv("PLATEGA_PAYMENT_METHOD", "2"))
CURRENCY = os.getenv("CURRENCY", "RUB")
DEFAULT_TRAFFIC_GB = float(os.getenv("TRAFFIC_LIMIT_GB", "0"))
DEFAULT_CHANNELS = [x.strip() for x in os.getenv("PANEL_CHANNELS", "main,reality").split(",") if x.strip()]
LOCATIONS = json.loads(os.getenv("PANEL_LOCATIONS_JSON", "{}") or "{}")
# Example: {"🇩🇪 Германия":{"node_id":"123","inbound_ids":["7"]},"🇳🇱 Нидерланды":{"node_id":"456","inbound_ids":["3"]}}
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
    raise RuntimeError("PLATEGA_MERCHANT_ID and PLATEGA_SECRET are required")
if not PUBLIC_BASE_URL.startswith("https://"):
    raise RuntimeError("PUBLIC_BASE_URL must be a public HTTPS URL")

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
http: ClientSession | None = None
db: aiosqlite.Connection | None = None
user_locks: dict[int, asyncio.Lock] = {}


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
        if resp.status >= 400 or (isinstance(data, dict) and data.get("ok") is False):
            raise RuntimeError(f"Panel API {resp.status}: {data}")
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


@dp.callback_query(F.data.startswith("plan:"))
async def choose_plan(c: CallbackQuery):
    key = c.data.split(":", 1)[1]
    plan = PLANS.get(key)
    if not plan:
        return await c.answer("Тариф не найден", show_alert=True)
    if not LOCATIONS:
        return await c.answer("Локации ещё не настроены администратором.", show_alert=True)
    await ensure_user(c.from_user.id, c.from_user.username)
    await c.message.edit_text(
        f"📦 <b>{plan['title']} — {plan['price']} ₽</b>\n"
        f"⏳ Срок: {plan['days']} дней\n📱 Устройств: {plan['devices']}\n\n"
        "Выберите локации для подключения:",
        reply_markup=location_keyboard("buy_loc:" + key))
    await c.answer()


@dp.callback_query(F.data.startswith("buy_loc:"))
async def choose_locations(c: CallbackQuery):
    _, plan_key, selection = c.data.split(":", 2)
    if selection == "all":
        selected = list(LOCATIONS.keys())
    else:
        try:
            selected = [list(LOCATIONS.keys())[int(selection)]]
        except (ValueError, IndexError):
            return await c.answer("Локация не найдена", show_alert=True)
    plan = PLANS.get(plan_key)
    if not plan:
        return await c.answer("Тариф не найден", show_alert=True)
    order_id = str(uuid.uuid4())
    assert db
    await db.execute("INSERT INTO payments(order_id,tg_id,amount,plan_key,status,created_at) VALUES(?,?,?,?,?,?)",
                     (order_id, c.from_user.id, plan["price"], plan_key, "CREATING", int(time.time())))
    await db.execute("UPDATE payments SET locations=? WHERE order_id=?", (json.dumps(selected), order_id))
    await db.commit()
    try:
        # Payment is created server-side. The opaque order id is the callback payload.
        result = await platega_request("POST", "/transaction/process", {
            "paymentMethod": PAYMENT_METHOD,
            "paymentDetails": {"amount": plan["price"], "currency": CURRENCY},
            "description": f"Awesome VPN — {plan['title']}",
            "return": PUBLIC_BASE_URL + "/payment/return",
            "failedUrl": PUBLIC_BASE_URL + "/payment/failed",
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
            "После оплаты VPN подключится автоматически.",
            reply_markup=kb([[("💳 Перейти к оплате", "noop")], [("⬅️ В меню", "home")]]))
        # URL button is emitted separately because Telegram callback_data is not a URL.
        await c.message.answer("Нажмите кнопку ниже для оплаты.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Оплатить", url=pay_url)],
            [InlineKeyboardButton(text="Проверить оплату", callback_data=f"check:{order_id}")]
        ]))
    except Exception as e:
        log.exception("Payment creation failed")
        await db.execute("UPDATE payments SET status='ERROR' WHERE order_id=?", (order_id,))
        await db.commit()
        await c.message.edit_text("Не удалось создать платёж. Попробуйте позже или обратитесь в поддержку.", reply_markup=home_kb())
    await c.answer()


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
        await verify_and_process(order["provider_id"], order_id)
        cur = await db.execute("SELECT status FROM payments WHERE order_id=?", (order_id,))
        state = (await cur.fetchone())["status"]
        await c.answer("Оплата подтверждена!" if state == "CONFIRMED" else "Платёж пока не подтверждён.")
        if state == "CONFIRMED":
            await c.message.answer("✅ Оплата получена! VPN-подписка активирована. Откройте «Мой VPN».", reply_markup=home_kb())
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
        expires_at = max(int(user["expires_at"] or 0) if user else 0, int(time.time())) + plan["days"] * 86400
        success_nodes = []
        for loc in selected:
            cfg = LOCATIONS[loc]
            node_id = str(cfg.get("node_id", "")).strip()
            inbounds = cfg.get("inbound_ids", [])
            path = f"/fed/lproxy/{quote(node_id, safe='')}/create" if node_id else "/create"
            # On retry, check whether this node already has the client before creating it again.
            try:
                existing_path = f"/fed/lproxy/{quote(node_id, safe='')}/clients/{quote(client_name, safe='')}" if node_id else f"/clients/{quote(client_name, safe='')}"
                existing = await panel_request("GET", existing_path)
                client = existing.get("client", existing)
                if client.get("uuid") == client_uuid:
                    success_nodes.append(loc)
                    continue
            except Exception:
                pass
            payload = {
                "name": client_name, "uuid": client_uuid, "expires_at": expires_at,
                "days": plan["days"], "device_limit": plan["devices"],
                "traffic_limit_gb": DEFAULT_TRAFFIC_GB, "manual": True,
                "channels": cfg.get("channels", DEFAULT_CHANNELS),
                "inbound_ids": inbounds
            }
            await panel_request("POST", path, payload)
            success_nodes.append(loc)
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


async def verify_and_process(provider_id: str, order_id: str) -> None:
    if not provider_id:
        raise RuntimeError("Missing Platega transaction id")
    data = await platega_request("GET", f"/transaction/{quote(provider_id, safe='')}")
    if data.get("status") != "CONFIRMED":
        return
    assert db
    cur = await db.execute("SELECT amount FROM payments WHERE order_id=?", (order_id,))
    order = await cur.fetchone()
    actual = (data.get("paymentDetails") or {}).get("amount")
    if order and actual is not None and float(actual) != float(order["amount"]):
        raise RuntimeError("Payment amount mismatch")
    await provision_paid_order(order_id)


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
    await c.answer("Пополнение баланса будет добавлено отдельным сценарием. Сейчас оплата доступна при покупке тарифа.", show_alert=True)


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


async def platega_callback(request: web.Request) -> web.Response:
    # Platega callback authentication: compare both merchant headers; never trust body alone.
    if request.headers.get("X-MerchantId", "") != PLATEGA_MERCHANT_ID or not secrets.compare_digest(
        request.headers.get("X-Secret", ""), PLATEGA_SECRET
    ):
        raise web.HTTPUnauthorized(text="invalid callback credentials")
    payload = await request.json()
    payment_id = str(payload.get("id") or "")
    order_id = str(payload.get("payload") or "")
    if not payment_id or not order_id:
        raise web.HTTPBadRequest(text="missing payment id/payload")
    assert db
    cur = await db.execute("SELECT * FROM payments WHERE order_id=?", (order_id,))
    order = await cur.fetchone()
    if not order:
        raise web.HTTPNotFound(text="unknown order")
    if float(payload.get("amount", -1)) != float(order["amount"]) or payload.get("currency") != CURRENCY:
        raise web.HTTPBadRequest(text="amount/currency mismatch")
    # Verify final status with Platega API before activating access.
    if payload.get("status") == "CONFIRMED":
        await verify_and_process(order["provider_id"], order_id)
    await db.execute("INSERT OR IGNORE INTO processed_callbacks(payment_id,received_at) VALUES(?,?)",
                     (payment_id, int(time.time())))
    await db.commit()
    return web.json_response({"ok": True})


async def health(_: web.Request) -> web.Response:
    return web.json_response({"ok": True, "service": "awesomevpn"})


async def main():
    global http
    await init_db()
    http = ClientSession(timeout=ClientTimeout(total=35))
    app = web.Application(client_max_size=1024 * 1024)
    app.router.add_get("/health", health)
    app.router.add_get("/payment/return", health)
    app.router.add_get("/payment/failed", health)
    app.router.add_post("/platega/callback", platega_callback)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", "8080")))
    await site.start()
    log.info("HTTP callback server started on port %s", os.getenv("PORT", "8080"))
    try:
        await dp.start_polling(bot)
    finally:
        await runner.cleanup()
        await http.close()
        if db:
            await db.close()


if __name__ == "__main__":
    asyncio.run(main())
