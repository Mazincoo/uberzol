import re
import uuid
import json
import asyncio
import random
from pathlib import Path
from typing import Dict, Any, Optional, Set, List, Tuple
from datetime import datetime
from collections import Counter

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    CallbackQueryHandler,
    filters,
)

# =========================================================
# CONFIG
# =========================================================
TOKEN = "PUT_YOUR_BOT_TOKEN_HERE"
GROUP_CHAT_ID = -1003693470503
BOT_USERNAME = "uberzol_bot"
DRIVERS_INVITE_LINK = "https://t.me/+0hJrK5ecU-05NDRk"

PLATFORM_COMMISSION_PCT = 11
CLIENT_REF_PCT = 1
DRIVER_REF_PCT = 1
DRIVER_DEBT_LIMIT = 5000
ORDER_TIMEOUT_SECONDS = 40

ADMIN_USER_IDS = {737883669}

# =========================================================
# AREAS
# =========================================================
CITY_AREAS = {
    "بحري": [
        "المزاد", "الصافية", "شمبات", "الحلفايا", "كافوري",
        "الدروشاب", "الكدرو", "العزبة", "السامراب", "الاملاك"
    ],
    "الخرطوم": [
        "العمارات", "الصحافة", "الرياض", "الطائف", "الجريف",
        "بري", "الامتداد", "السوق العربي", "اركويت", "الكلاكلة"
    ],
    "أم درمان": [
        "الثورة", "المهندسين", "أبو روف", "بيت المال", "الملازمين",
        "الفتيحاب", "ود نوباوي", "أمبدة", "كرري", "البوستة"
    ],
}
CITIES = list(CITY_AREAS.keys())

CITY_ICONS = {
    "بحري": "🌉",
    "الخرطوم": "🏙️",
    "أم درمان": "🕌",
}
AREA_ICON = "🏘️"

# =========================================================
# FILES
# =========================================================
DATA_DIR = Path("./data")
DATA_DIR.mkdir(exist_ok=True)

CLIENTS_FILE = DATA_DIR / "clients.json"
DRIVERS_FILE = DATA_DIR / "drivers.json"
ONLINE_FILE = DATA_DIR / "online_drivers.json"
TRIPS_FILE = DATA_DIR / "trips.json"
WALLETS_FILE = DATA_DIR / "wallets.json"
APP_STATE_FILE = DATA_DIR / "app_state.json"
CANCELLATIONS_FILE = DATA_DIR / "cancellations.json"

# =========================================================
# JSON HELPERS
# =========================================================
def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_json(path: Path, data: Dict[str, Any]) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


CLIENTS_DB: Dict[str, Any] = _load_json(CLIENTS_FILE)
DRIVERS_DB: Dict[str, Any] = _load_json(DRIVERS_FILE)
TRIPS_DB: Dict[str, Any] = _load_json(TRIPS_FILE)
WALLETS_DB: Dict[str, Any] = _load_json(WALLETS_FILE)
CANCELLATIONS_DB: Dict[str, Any] = _load_json(CANCELLATIONS_FILE)


def _load_online_set() -> Set[int]:
    data = _load_json(ONLINE_FILE)
    ids = data.get("online_driver_ids", [])
    out: Set[int] = set()
    for x in ids:
        try:
            out.add(int(x))
        except Exception:
            pass
    return out


def _save_online_set(s: Set[int]) -> None:
    _save_json(ONLINE_FILE, {"online_driver_ids": sorted(list(s))})


def save_clients():
    _save_json(CLIENTS_FILE, CLIENTS_DB)


def save_drivers():
    _save_json(DRIVERS_FILE, DRIVERS_DB)


def save_trips():
    _save_json(TRIPS_FILE, TRIPS_DB)


def save_wallets():
    _save_json(WALLETS_FILE, WALLETS_DB)


def save_cancellations():
    _save_json(CANCELLATIONS_FILE, CANCELLATIONS_DB)

# =========================================================
# APP STATE
# =========================================================
def _load_app_state() -> Dict[str, Any]:
    data = _load_json(APP_STATE_FILE)
    if not data:
        data = {"enabled": True}
        _save_json(APP_STATE_FILE, data)
    data.setdefault("enabled", True)
    return data


APP_STATE: Dict[str, Any] = _load_app_state()


def save_app_state():
    _save_json(APP_STATE_FILE, APP_STATE)


def app_is_enabled() -> bool:
    return bool(APP_STATE.get("enabled", True))


def set_app_enabled(value: bool):
    APP_STATE["enabled"] = bool(value)
    save_app_state()


def app_status_text() -> str:
    return "✅ شغال" if app_is_enabled() else "🚧 قيد الإنشاء"


def app_under_construction_text() -> str:
    return (
        "🚧 أوبر زول لسه تحت اللمسات الأخيرة يا جميل ✨\n\n"
        "التسجيل شغال والتجربة مفتوحة لحدي آخر خطوة، "
        "والتشغيل الكامل قريب شديد جدًا إن شاء الله 🚕🔥"
    )

# =========================================================
# UPGRADE
# =========================================================
def _upgrade_wallet(w: Dict[str, Any]) -> Dict[str, Any]:
    w.setdefault("balance", 0)
    w.setdefault("referrals_count", 0)
    w.setdefault("ref_code", "UZ" + str(random.randint(100000, 999999)))
    w.setdefault("used_codes", [])
    w.setdefault("history", [])
    w.setdefault("history_all", [])
    w.setdefault("history_client_ref", [])
    w.setdefault("history_driver_ref", [])
    w.setdefault("client_ref_summary", {})
    w.setdefault("driver_ref_summary", {})
    return w


def _upgrade_client(c: Dict[str, Any]) -> Dict[str, Any]:
    c.setdefault("name_first", "")
    c.setdefault("name_full", "")
    c.setdefault("gender", None)
    c.setdefault("invited_by_client", None)
    c.setdefault("pending_driver_inviter", None)
    c.setdefault("home_city", None)
    c.setdefault("home_area", None)
    return c


def _upgrade_driver(d: Dict[str, Any]) -> Dict[str, Any]:
    d.setdefault("name_full", "")
    d.setdefault("phone", "")
    d.setdefault("car", "")
    d.setdefault("color", "")
    d.setdefault("plate", "")
    d.setdefault("gender", None)
    d.setdefault("home_city", None)
    d.setdefault("home_area", None)
    d.setdefault("work_city", None)
    d.setdefault("work_areas", [])
    d.setdefault("invited_by_driver", None)
    d.setdefault("commission_due", 0)
    d.setdefault("commission_paid", 0)
    d.setdefault("commission_total", 0)
    d.setdefault("blocked_due", False)
    d.setdefault("finance_history", [])
    return d


for k, v in list(WALLETS_DB.items()):
    WALLETS_DB[k] = _upgrade_wallet(v or {})
for k, v in list(CLIENTS_DB.items()):
    CLIENTS_DB[k] = _upgrade_client(v or {})
for k, v in list(DRIVERS_DB.items()):
    DRIVERS_DB[k] = _upgrade_driver(v or {})

CANCELLATIONS_DB.setdefault("records", [])

save_wallets()
save_clients()
save_drivers()
save_cancellations()

# =========================================================
# RUNTIME STATE
# =========================================================
ONLINE_DRIVERS: Set[int] = _load_online_set()
PENDING_ORDERS: Dict[str, Dict[str, Any]] = {}
CLIENT_SESSIONS: Dict[int, Dict[str, Any]] = {}
TIMEOUT_TASKS: Dict[str, asyncio.Task] = {}
DRIVER_OTP_WAIT: Dict[int, str] = {}
DRIVER_SESSIONS: Dict[int, Dict[str, Any]] = {}

# =========================================================
# STATUS
# =========================================================
ST_SENT = "SENT"
ST_ACCEPTED = "ACCEPTED"
ST_ON_THE_WAY = "ON_THE_WAY"
ST_ARRIVED = "ARRIVED"
ST_TRIP_START = "TRIP_STARTED"
ST_TRIP_END = "TRIP_ENDED"
ST_CANCELLED = "CANCELLED"

STATUS_LABEL = {
    ST_SENT: "📤 الطلب اتحرّك",
    ST_ACCEPTED: "✅ في سواق قبل الطلب",
    ST_ON_THE_WAY: "🚗 السواق جايك",
    ST_ARRIVED: "📍 السواق وصل",
    ST_TRIP_START: "▶️ الرحلة بدأت",
    ST_TRIP_END: "✅ الرحلة خلصت",
    ST_CANCELLED: "❌ الرحلة اتلغت",
}

STATUS_PROGRESS = {
    ST_SENT: 20,
    ST_ACCEPTED: 40,
    ST_ON_THE_WAY: 60,
    ST_ARRIVED: 80,
    ST_TRIP_START: 90,
    ST_TRIP_END: 100,
    ST_CANCELLED: 0,
}

# =========================================================
# CANCEL REASONS
# =========================================================
CLIENT_CANCEL_REASONS_PENDING = {
    "ALT": "🚕 لقيت مشوار تاني",
    "PRICE": "💰 السعر ما مناسب",
    "TIME": "⏳ غيرت رأيي",
    "OTHER": "✍️ أخرى",
}

CLIENT_CANCEL_REASONS_ACCEPTED = {
    "LATE": "⏳ السواق اتأخر",
    "CHANGE": "🤷 غيرت رأيي",
    "OUT": "📞 اتفقت برا",
    "OTHER": "✍️ أخرى",
}

DRIVER_CANCEL_REASONS = {
    "NO_ANSWER": "📵 العميل ما بيرد",
    "FAR": "📍 الطلب بعيد",
    "PRICE": "💰 السعر ما مناسب",
    "OTHER": "✍️ أخرى",
}

# =========================================================
# HELPERS
# =========================================================
def first_name(full_name: str) -> str:
    parts = (full_name or "").strip().split()
    return parts[0] if parts else "زولنا"


def _now_local() -> datetime:
    return datetime.now()


def is_admin(user_id: int) -> bool:
    return int(user_id) in ADMIN_USER_IDS


def is_driver_registered(user_id: int) -> bool:
    return str(user_id) in DRIVERS_DB


def _ensure_client(user) -> None:
    uid = str(user.id)
    if uid not in CLIENTS_DB:
        CLIENTS_DB[uid] = _upgrade_client({
            "name_first": first_name(user.full_name),
            "name_full": user.full_name,
        })
    else:
        CLIENTS_DB[uid] = _upgrade_client(CLIENTS_DB[uid] or {})
        CLIENTS_DB[uid]["name_first"] = CLIENTS_DB[uid].get("name_first") or first_name(user.full_name)
        CLIENTS_DB[uid]["name_full"] = CLIENTS_DB[uid].get("name_full") or user.full_name

    save_clients()
    _get_wallet(user.id)


def get_client_first(user) -> str:
    c = CLIENTS_DB.get(str(user.id), {})
    return c.get("name_first") or first_name(user.full_name)


def get_gender(user_id: int) -> Optional[str]:
    return (CLIENTS_DB.get(str(user_id), {}) or {}).get("gender")


def is_female(user_id: int) -> bool:
    return get_gender(user_id) == "F"


def needs_gender(user_id: int) -> bool:
    return not bool(get_gender(user_id))


def needs_home_area(user_id: int) -> bool:
    c = CLIENTS_DB.get(str(user_id), {}) or {}
    return not c.get("home_city") or not c.get("home_area")


def word_write(user_id: int) -> str:
    return "اكتبي" if is_female(user_id) else "اكتب"


def word_choose(user_id: int) -> str:
    return "اختاري" if is_female(user_id) else "اختار"


def word_done(user_id: int) -> str:
    return "خلصنا يا زولة 😄" if is_female(user_id) else "خلصنا يا زول 😄"


def only_digits(s: str) -> bool:
    return bool(re.fullmatch(r"\d+", (s or "").strip()))


def phone_ok(s: str) -> bool:
    return bool(re.fullmatch(r"\d{7,15}", (s or "").strip()))


def set_stage(chat_id: int, stage: str, order_id: Optional[str] = None):
    sess = CLIENT_SESSIONS.get(chat_id, {})
    sess["stage"] = stage
    if order_id is not None:
        sess["order_id"] = order_id
    CLIENT_SESSIONS[chat_id] = sess


def get_stage(chat_id: int) -> Optional[str]:
    return (CLIENT_SESSIONS.get(chat_id) or {}).get("stage")


def get_order_id(chat_id: int) -> Optional[str]:
    return (CLIENT_SESSIONS.get(chat_id) or {}).get("order_id")


def clear_stage(chat_id: int):
    CLIENT_SESSIONS.pop(chat_id, None)


def set_driver_stage(user_id: int, stage: str, **kwargs):
    data = DRIVER_SESSIONS.get(user_id, {})
    data["stage"] = stage
    data.update(kwargs)
    DRIVER_SESSIONS[user_id] = data


def get_driver_stage(user_id: int) -> Optional[str]:
    return (DRIVER_SESSIONS.get(user_id) or {}).get("stage")


def clear_driver_stage(user_id: int):
    DRIVER_SESSIONS.pop(user_id, None)


def online_count() -> int:
    return len(ONLINE_DRIVERS)


def _gen_code() -> str:
    return f"{random.randint(1000, 9999)}"


def _pct_amount(price: int, pct: int) -> int:
    return int(round(price * (pct / 100.0)))

# =========================================================
# AREA HELPERS
# =========================================================
def city_inline(prefix: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(f"{CITY_ICONS['بحري']} بحري", callback_data=f"{prefix}:CITY:بحري"),
            InlineKeyboardButton(f"{CITY_ICONS['الخرطوم']} الخرطوم", callback_data=f"{prefix}:CITY:الخرطوم"),
            InlineKeyboardButton(f"{CITY_ICONS['أم درمان']} أم درمان", callback_data=f"{prefix}:CITY:أم درمان"),
        ]
    ])


def area_inline(prefix: str, city: str) -> InlineKeyboardMarkup:
    rows = []
    areas = CITY_AREAS.get(city, [])
    for i in range(0, len(areas), 2):
        row = []
        for area in areas[i:i + 2]:
            row.append(InlineKeyboardButton(f"{AREA_ICON} {area}", callback_data=f"{prefix}:AREA:{city}:{area}"))
        rows.append(row)
    rows.append([InlineKeyboardButton("↩️ رجوع", callback_data=f"{prefix}:BACKCITY")])
    return InlineKeyboardMarkup(rows)


def work_area_toggle_inline(user_id: int, city: str) -> InlineKeyboardMarkup:
    d = DRIVERS_DB.get(str(user_id), {}) or {}
    selected = set(d.get("work_areas", []))
    rows = []
    areas = CITY_AREAS.get(city, [])
    for i in range(0, len(areas), 2):
        row = []
        for area in areas[i:i + 2]:
            key = f"{city}|{area}"
            mark = "✅" if key in selected else "⬜"
            row.append(InlineKeyboardButton(f"{mark} {area}", callback_data=f"WORK:TOGGLE:{city}:{area}"))
        rows.append(row)

    rows.append([InlineKeyboardButton("✅ حفظ مناطق العمل", callback_data=f"WORK:SAVE:{city}")])
    rows.append([InlineKeyboardButton("🗑️ مسح الكل", callback_data=f"WORK:CLEAR:{city}")])
    rows.append([InlineKeyboardButton("↩️ رجوع", callback_data="WORK:BACKCITY")])
    return InlineKeyboardMarkup(rows)


def work_city_inline() -> InlineKeyboardMarkup:
    return city_inline("WORK")

# =========================================================
# WALLET HELPERS
# =========================================================
def _get_wallet(user_id: int) -> Dict[str, Any]:
    uid = str(user_id)
    if uid not in WALLETS_DB:
        WALLETS_DB[uid] = _upgrade_wallet({})
        save_wallets()
    else:
        WALLETS_DB[uid] = _upgrade_wallet(WALLETS_DB[uid] or {})
    return WALLETS_DB[uid]


def _ref_link_for(code: str) -> str:
    return f"https://t.me/{BOT_USERNAME}?start=ref{code}"


def _find_user_id_by_ref_code(code: str) -> Optional[int]:
    code = (code or "").strip()
    for uid, w in WALLETS_DB.items():
        if (w or {}).get("ref_code") == code:
            try:
                return int(uid)
            except Exception:
                return None
    return None


def _entry_to_text(entry: Any) -> str:
    if isinstance(entry, dict):
        return str(entry.get("text", ""))
    return str(entry or "")


def _extract_plus_amount(text: str) -> int:
    nums = re.findall(r"\+(\d+)", text or "")
    if nums:
        try:
            return sum(int(x) for x in nums)
        except Exception:
            return 0
    return 0


def wallet_add_entry(owner_user_id: int, text: str, kind: str = "client") -> None:
    w = _get_wallet(owner_user_id)
    entry = {"text": text, "kind": kind, "time": _now_local().isoformat()}

    w.setdefault("history_all", []).append(entry)

    if kind == "client":
        w.setdefault("history_client_ref", []).append(entry)
        w["history_client_ref"] = w["history_client_ref"][-100:]
    elif kind == "driver":
        w.setdefault("history_driver_ref", []).append(entry)
        w["history_driver_ref"] = w["history_driver_ref"][-100:]

    w["history_all"] = w["history_all"][-200:]
    save_wallets()


def wallet_add_summary_signup(owner_user_id: int, invited_name: str, kind: str = "client") -> None:
    w = _get_wallet(owner_user_id)
    summary = w.setdefault("client_ref_summary", {}) if kind == "client" else w.setdefault("driver_ref_summary", {})
    key = invited_name.strip().lower() or "غير معروف"
    if key not in summary:
        summary[key] = {"name": invited_name.strip() or "غير معروف", "total": 0}
    save_wallets()


def wallet_add_summary_commission(owner_user_id: int, invited_name: str, amount: int, kind: str = "client") -> None:
    w = _get_wallet(owner_user_id)
    summary = w.setdefault("client_ref_summary", {}) if kind == "client" else w.setdefault("driver_ref_summary", {})
    key = invited_name.strip().lower() or "غير معروف"
    if key not in summary:
        summary[key] = {"name": invited_name.strip() or "غير معروف", "total": 0}
    summary[key]["total"] = int(summary[key].get("total", 0)) + int(amount)
    save_wallets()


def repair_wallets_consistency():
    changed = False
    for uid, w in list(WALLETS_DB.items()):
        w = _upgrade_wallet(w or {})
        total_client = sum(_extract_plus_amount(_entry_to_text(x)) for x in w.get("history_client_ref", []))
        total_driver = sum(_extract_plus_amount(_entry_to_text(x)) for x in w.get("history_driver_ref", []))

        csum = w.get("client_ref_summary", {}) or {}
        dsum = w.get("driver_ref_summary", {}) or {}

        if total_client > 0:
            cur = sum(int((x or {}).get("total", 0)) for x in csum.values())
            if not csum:
                csum["legacy_client"] = {"name": "إحالات قديمة", "total": total_client}
            elif cur != total_client and len(csum) == 1:
                key = next(iter(csum))
                csum[key]["total"] = total_client

        if total_driver > 0:
            cur = sum(int((x or {}).get("total", 0)) for x in dsum.values())
            if not dsum:
                dsum["legacy_driver"] = {"name": "إحالات سواقين قديمة", "total": total_driver}
            elif cur != total_driver and len(dsum) == 1:
                key = next(iter(dsum))
                dsum[key]["total"] = total_driver

        fixed_balance = total_client + total_driver
        if int(w.get("balance", 0)) != fixed_balance:
            w["balance"] = fixed_balance

        w["client_ref_summary"] = csum
        w["driver_ref_summary"] = dsum
        WALLETS_DB[uid] = w
        changed = True

    if changed:
        save_wallets()


repair_wallets_consistency()

# =========================================================
# FINANCE HELPERS
# =========================================================
def _ensure_driver_finance(user_id: int):
    uid = str(user_id)
    if uid in DRIVERS_DB:
        DRIVERS_DB[uid] = _upgrade_driver(DRIVERS_DB[uid] or {})
        save_drivers()


def driver_finance_summary(user_id: int) -> Dict[str, int]:
    _ensure_driver_finance(user_id)
    d = DRIVERS_DB.get(str(user_id), {}) or {}
    return {
        "commission_total": int(d.get("commission_total", 0)),
        "commission_paid": int(d.get("commission_paid", 0)),
        "commission_due": int(d.get("commission_due", 0)),
        "blocked_due": 1 if d.get("blocked_due") else 0,
    }


def driver_is_blocked_due(user_id: int) -> bool:
    d = DRIVERS_DB.get(str(user_id), {}) or {}
    return bool(d.get("blocked_due")) or int(d.get("commission_due", 0)) >= DRIVER_DEBT_LIMIT


def driver_finance_add_due(user_id: int, amount: int, trip_id: str):
    _ensure_driver_finance(user_id)
    d = DRIVERS_DB[str(user_id)]
    d["commission_total"] = int(d.get("commission_total", 0)) + int(amount)
    d["commission_due"] = int(d.get("commission_due", 0)) + int(amount)
    d.setdefault("finance_history", []).append(f"➕ عمولة رحلة {trip_id}: +{amount} | المتبقي: {d['commission_due']}")
    d["finance_history"] = d["finance_history"][-150:]
    if int(d.get("commission_due", 0)) >= DRIVER_DEBT_LIMIT:
        d["blocked_due"] = True
        ONLINE_DRIVERS.discard(int(user_id))
        _save_online_set(ONLINE_DRIVERS)
    save_drivers()


def driver_debt_warning_text(due: int) -> str:
    return (
        "⚠️ يا بطل، العمولة المستحقة عليك وصلت الحد المسموح.\n\n"
        f"المتبقي عليك: {due}\n"
        f"الحد: {DRIVER_DEBT_LIMIT}\n\n"
        "تم إيقاف الطلبات مؤقتًا لحدي السداد ✅"
    )


def compute_trip_financials(fare: int, client_user_id: int, driver_user_id: int) -> Dict[str, int]:
    platform_commission = _pct_amount(fare, PLATFORM_COMMISSION_PCT)
    driver_due = fare - platform_commission

    client_inviter = (CLIENTS_DB.get(str(client_user_id), {}) or {}).get("invited_by_client")
    driver_inviter = (DRIVERS_DB.get(str(driver_user_id), {}) or {}).get("invited_by_driver")

    client_ref_paid = _pct_amount(fare, CLIENT_REF_PCT) if client_inviter else 0
    driver_ref_paid = _pct_amount(fare, DRIVER_REF_PCT) if driver_inviter else 0
    platform_net = platform_commission - client_ref_paid - driver_ref_paid

    return {
        "fare": fare,
        "platform_commission": platform_commission,
        "driver_due": driver_due,
        "client_ref_paid": client_ref_paid,
        "driver_ref_paid": driver_ref_paid,
        "platform_net_profit": platform_net,
        "client_inviter_id": int(client_inviter) if client_inviter else 0,
        "driver_inviter_id": int(driver_inviter) if driver_inviter else 0,
    }

# =========================================================
# DELETE ENGINE
# =========================================================
async def delete_message_after(bot, chat_id: int, message_id: int, delay: float = 0.0):
    try:
        if delay > 0:
            await asyncio.sleep(delay)
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass


def _get_prompt_ids(context: ContextTypes.DEFAULT_TYPE) -> List[Tuple[int, int]]:
    return context.user_data.get("prompt_msg_ids", [])


def _add_prompt_id(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int):
    ids = _get_prompt_ids(context)
    ids.append((chat_id, message_id))
    context.user_data["prompt_msg_ids"] = ids[-25:]


def _get_user_ids(context: ContextTypes.DEFAULT_TYPE) -> List[Tuple[int, int]]:
    return context.user_data.get("user_msg_ids", [])


def _add_user_id(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int):
    ids = _get_user_ids(context)
    ids.append((chat_id, message_id))
    context.user_data["user_msg_ids"] = ids[-35:]


async def delete_previous_step_messages(context: ContextTypes.DEFAULT_TYPE):
    pids = _get_prompt_ids(context)
    uids = _get_user_ids(context)
    context.user_data["prompt_msg_ids"] = []
    context.user_data["user_msg_ids"] = []
    for cid, mid in pids:
        asyncio.create_task(delete_message_after(context.bot, cid, mid))
    for cid, mid in uids:
        asyncio.create_task(delete_message_after(context.bot, cid, mid))


async def send_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, **kwargs):
    msg = update.effective_message
    m = await msg.reply_text(text, **kwargs)
    if m:
        _add_prompt_id(context, m.chat_id, m.message_id)
    return m


async def toast(msg, context: ContextTypes.DEFAULT_TYPE, text: str, seconds: float = 2.5, **kwargs):
    m = await msg.reply_text(text, **kwargs)
    if m:
        asyncio.create_task(delete_message_after(context.bot, m.chat_id, m.message_id, seconds))
    return m

# =========================================================
# WELCOME / UI CLEANUP
# =========================================================
def welcome_text_for_user(user) -> str:
    n = get_client_first(user)
    if app_is_enabled():
        body = "أوبر زول جاهز يوديك وين ما داير/ة 🚕✨"
    else:
        body = "أوبر زول قيد الإنشاء حاليًا 🚧\nلكن تقدر/ي تتفرج وتجرّب لحدي آخر خطوة 👌"
    return (
        f"يا هلا يا {n} {'😍' if is_female(user.id) else '😎🔥'}\n\n"
        f"{body}\n\n"
        "اختار/ي الزر المناسب تحت وخلي الباقي علينا 👇"
    )


def ask_gender_text(user) -> str:
    return f"يا {get_client_first(user)} 😄\n\nكيف نخاطبك؟"


def welcome_menu_inline() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🧑‍✈️ تسجيل كسائق", callback_data="MENU:DRIVER_START"),
            InlineKeyboardButton("🚗 طلب مشوار", callback_data="MENU:CLIENT_START"),
        ],
        [InlineKeyboardButton("💼 محفظتي", callback_data="WALLET:OPEN")],
    ])


def gender_inline() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("👨 ولد", callback_data="GENDER:M"),
            InlineKeyboardButton("👩 بنت", callback_data="GENDER:F"),
        ]
    ])


async def dismiss_cancel_reason_prompt(context: ContextTypes.DEFAULT_TYPE):
    cm = context.user_data.get("cancel_reason_msg_id")
    if cm and isinstance(cm, (list, tuple)) and len(cm) == 2:
        asyncio.create_task(delete_message_after(context.bot, cm[0], cm[1]))
    context.user_data["cancel_reason_msg_id"] = None


async def set_cancel_reason_prompt(context: ContextTypes.DEFAULT_TYPE, chat_id: int, text: str, reply_markup=None):
    await dismiss_cancel_reason_prompt(context)
    m = await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup)
    context.user_data["cancel_reason_msg_id"] = (m.chat_id, m.message_id)
    return m


async def dismiss_welcome(context: ContextTypes.DEFAULT_TYPE):
    ids = context.user_data.get("welcome_msg_ids", [])
    for cid, mid in ids:
        asyncio.create_task(delete_message_after(context.bot, cid, mid))
    context.user_data["welcome_msg_ids"] = []
    context.user_data["welcome_msg_id"] = None


async def dismiss_end_menu(context: ContextTypes.DEFAULT_TYPE):
    em = context.user_data.get("end_menu_msg_id")
    if em:
        asyncio.create_task(delete_message_after(context.bot, em[0], em[1]))
    context.user_data["end_menu_msg_id"] = None


async def dismiss_wallet_detail(context: ContextTypes.DEFAULT_TYPE):
    dm = context.user_data.get("wallet_detail_msg_id")
    if dm:
        asyncio.create_task(delete_message_after(context.bot, dm[0], dm[1]))
    context.user_data["wallet_detail_msg_id"] = None


async def dismiss_admin(context: ContextTypes.DEFAULT_TYPE):
    am = context.user_data.get("admin_msg_id")
    if am:
        asyncio.create_task(delete_message_after(context.bot, am[0], am[1]))
    context.user_data["admin_msg_id"] = None


async def dismiss_finish_banner(context: ContextTypes.DEFAULT_TYPE):
    fm = context.user_data.get("finish_banner_msg_id")
    if fm and isinstance(fm, (list, tuple)) and len(fm) == 2:
        asyncio.create_task(delete_message_after(context.bot, fm[0], fm[1]))
    context.user_data["finish_banner_msg_id"] = None


async def dismiss_home_family(context: ContextTypes.DEFAULT_TYPE):
    await dismiss_welcome(context)
    await dismiss_end_menu(context)
    await dismiss_finish_banner(context)


async def show_single_home(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user):
    await dismiss_home_family(context)
    await upsert_welcome(context, chat_id, user)


async def upsert_welcome(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user):
    text = welcome_text_for_user(user)

    await dismiss_end_menu(context)
    await dismiss_finish_banner(context)

    wid = context.user_data.get("welcome_msg_id")

    if wid and isinstance(wid, (list, tuple)) and len(wid) == 2 and wid[0] == chat_id:
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=wid[1],
                text=text,
                reply_markup=welcome_menu_inline()
            )
            context.user_data["welcome_msg_ids"] = [wid]
            return
        except Exception:
            try:
                await context.bot.delete_message(chat_id=wid[0], message_id=wid[1])
            except Exception:
                pass
            context.user_data["welcome_msg_id"] = None
            context.user_data["welcome_msg_ids"] = []

    await dismiss_welcome(context)
    m = await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=welcome_menu_inline())
    context.user_data["welcome_msg_id"] = (m.chat_id, m.message_id)
    context.user_data["welcome_msg_ids"] = [(m.chat_id, m.message_id)]


async def ask_gender_screen(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await dismiss_welcome(context)
    set_stage(update.effective_chat.id, "ASK_GENDER")
    await delete_previous_step_messages(context)
    await send_prompt(update, context, ask_gender_text(update.effective_user), reply_markup=gender_inline())


async def ask_home_city_screen(chat_id: int, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    choose_word = word_choose(user_id)
    await context.bot.send_message(
        chat_id=chat_id,
        text=f"📍 قبل نبدأ يا جميل، {choose_word} مدينة سكنك:",
        reply_markup=city_inline("HOME")
    )


async def ask_home_area_screen(chat_id: int, context: ContextTypes.DEFAULT_TYPE, city: str, user_id: int):
    choose_word = word_choose(user_id)
    await context.bot.send_message(
        chat_id=chat_id,
        text=f"🏘️ كويس يا حلو، {choose_word} الحي في {city}:",
        reply_markup=area_inline("HOME", city)
    )

# =========================================================
# DRIVER REGISTER FLOW
# =========================================================
async def open_work_areas_editor(chat_id: int, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    if not is_driver_registered(user_id):
        await context.bot.send_message(chat_id=chat_id, text="⚠️ أول حاجة سجّل كسائق وبعدها تعال عدّل مناطق العمل 👌")
        return
    await context.bot.send_message(
        chat_id=chat_id,
        text="📍 اختار مدينة العمل:",
        reply_markup=work_city_inline()
    )


async def begin_driver_register(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    _ensure_client(user)

    if str(user.id) in DRIVERS_DB:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="✅ إنت مسجل كسائق من قبل.\nلو داير تغيّر مناطق العمل استخدم زر مناطق العمل 👌",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📍 مناطق العمل", url=f"https://t.me/{BOT_USERNAME}?start=workareas")],
                [InlineKeyboardButton("🔗 قروب السواقين", url=DRIVERS_INVITE_LINK)],
            ])
        )
        return

    if needs_gender(user.id):
        await ask_gender_screen(update, context)
        return

    if needs_home_area(user.id):
        await ask_home_city_screen(update.effective_chat.id, context, user.id)
        return

    clear_stage(update.effective_chat.id)
    set_driver_stage(user.id, "DRV_NAME")
    await context.bot.send_message(chat_id=update.effective_chat.id, text="📝 يلا يا بطل، أرسل اسمك الكامل.")


async def handle_driver_register_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    stage = get_driver_stage(user.id)
    if not stage:
        return False

    text = (update.effective_message.text or "").strip()
    uid = str(user.id)

    if stage == "DRV_NAME":
        context.user_data["drv_name_full"] = text
        set_driver_stage(user.id, "DRV_PHONE")
        await update.effective_message.reply_text("📞 أرسل رقم جوالك:")
        return True

    if stage == "DRV_PHONE":
        if not phone_ok(text):
            await update.effective_message.reply_text("⚠️ الرقم ما راكب، أرسله أرقام بس.")
            return True
        context.user_data["drv_phone"] = text
        set_driver_stage(user.id, "DRV_CAR")
        await update.effective_message.reply_text("🚘 أرسل نوع العربية:")
        return True

    if stage == "DRV_CAR":
        context.user_data["drv_car"] = text
        set_driver_stage(user.id, "DRV_COLOR")
        await update.effective_message.reply_text("🎨 أرسل لون العربية:")
        return True

    if stage == "DRV_COLOR":
        context.user_data["drv_color"] = text
        set_driver_stage(user.id, "DRV_PLATE")
        await update.effective_message.reply_text("🔢 أرسل رقم اللوحة:")
        return True

    if stage == "DRV_PLATE":
        pending_driver_inviter = (CLIENTS_DB.get(uid, {}) or {}).get("pending_driver_inviter")
        home_city = CLIENTS_DB.get(uid, {}).get("home_city")
        home_area = CLIENTS_DB.get(uid, {}).get("home_area")

        DRIVERS_DB[uid] = _upgrade_driver({
            "name_full": context.user_data.get("drv_name_full", user.full_name),
            "phone": context.user_data.get("drv_phone", ""),
            "car": context.user_data.get("drv_car", ""),
            "color": context.user_data.get("drv_color", ""),
            "plate": text,
            "gender": get_gender(user.id),
            "home_city": home_city,
            "home_area": home_area,
            "invited_by_driver": pending_driver_inviter,
            "work_city": None,
            "work_areas": [],
        })
        save_drivers()

        if uid in CLIENTS_DB:
            CLIENTS_DB[uid]["pending_driver_inviter"] = None
            save_clients()

        _get_wallet(user.id)

        set_driver_stage(user.id, "DRV_WORK_CITY")
        await update.effective_message.reply_text(
            "📍 حلو شديد.\nاختار المدينة البتشتغل فيها غالبًا:",
            reply_markup=work_city_inline()
        )
        return True

    return False

# =========================================================
# WALLET UI
# =========================================================
def wallet_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("👥 عمولات العملاء", callback_data="WALLET:H_CLIENT"),
            InlineKeyboardButton("🧑‍✈️ عمولات السواقين", callback_data="WALLET:H_DRIVER"),
        ],
        [InlineKeyboardButton("↩️ رجوع", callback_data="WALLET:BACK")],
    ])


def wallet_detail_back_inline(kind: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("↩️ رجوع", callback_data=f"WALLET:DETAIL_BACK:{kind}")]])


def wallet_text(user) -> str:
    repair_wallets_consistency()
    w = _get_wallet(user.id)
    code = w.get("ref_code", "")
    link = _ref_link_for(code)
    bal = w.get("balance", 0)
    refs = w.get("referrals_count", 0)

    title = "💼 محفظتك كسائق 🧑‍✈️" if is_driver_registered(user.id) else f"💼 محفظتك يا {get_client_first(user)} 😄"
    extra = ""
    if is_driver_registered(user.id):
        fin = driver_finance_summary(user.id)
        blocked_txt = "⛔ موقوف مؤقتًا" if fin["blocked_due"] else "✅ شغال"
        extra = (
            f"\n🧾 إجمالي عمولاتك: {fin['commission_total']}\n"
            f"✅ المسدد: {fin['commission_paid']}\n"
            f"⚠️ المتبقي عليك: {fin['commission_due']}\n"
            f"📌 الحالة: {blocked_txt}\n"
        )

    return (
        f"{title}\n\n"
        f"💰 الرصيد: {bal}\n"
        f"👥 المشتركين عن طريقك: {refs}\n"
        f"{extra}\n"
        "🔗 رابط دعوتك: انسخه وأرسله لناسَك 👇\n"
        f"{link}\n\n"
        "اختار من الأزرار تحت 👇"
    )


def wallet_detail_text(user, kind: str) -> str:
    repair_wallets_consistency()
    w = _get_wallet(user.id)

    if kind == "client":
        summary = w.get("client_ref_summary", {})
        title = f"👥 عمولات العملاء يا {get_client_first(user)} 😄"
        icon = "👥"
    else:
        summary = w.get("driver_ref_summary", {})
        title = f"🧑‍✈️ عمولات السواقين يا {get_client_first(user)} 😄"
        icon = "🧑‍✈️"

    if not summary:
        return f"{title}\n\nلسه ما في بيانات هنا."

    total = 0
    rows = []
    for item in sorted(summary.values(), key=lambda x: int(x.get("total", 0)), reverse=True):
        total += int(item.get("total", 0))
        rows.append(
            f"• {icon} الاسم: {item.get('name', 'غير معروف')}\n"
            f"💰 مجموع العمولة: {int(item.get('total', 0))}"
        )
    return title + f"\n\n💰 الإجمالي: {total}\n\n" + "\n\n".join(rows)


async def open_wallet_screen(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user, autoclose_seconds: float = 0.0):
    m = await context.bot.send_message(chat_id=chat_id, text=wallet_text(user), reply_markup=wallet_kb())
    context.user_data["wallet_main_msg_id"] = (m.chat_id, m.message_id)
    if autoclose_seconds > 0:
        asyncio.create_task(delete_message_after(context.bot, m.chat_id, m.message_id, autoclose_seconds))

# =========================================================
# ORDER TEXT
# =========================================================
def ask_pickup_city_text(user) -> str:
    return (
        f"📍😄 يا {get_client_first(user)}\n\n"
        "نجيك وين الليلة يا بطل/ة؟ 🚕\n"
        f"{word_choose(user.id)} المدينة أول 👇"
    )


def ask_pickup_area_text(user, city: str) -> str:
    return (
        f"🏘️✨ تمام يا {get_client_first(user)}\n\n"
        f"في أي حي نلقاك في {city}؟\n"
        f"{word_choose(user.id)} الحي المناسب 👇"
    )


def ask_pickup_desc_text(user, city: str, area: str) -> str:
    return (
        f"📌😁 يا {get_client_first(user)}\n\n"
        f"{word_write(user.id)} لينا وصف سريع للمكان في {area} - {city} عشان السواق يلقاك بسرعة 🚗💨\n\n"
        "مثال:\n"
        "جنب مخبز فلان\n"
        "قريب محطة فلان\n"
        "قدام الصيدلية"
    )


def ask_destination_text(user) -> str:
    return (
        f"🎯😎 يا {get_client_first(user)}\n\n"
        f"المشوار الليلة على وين؟ {word_write(user.id)} الوجهة بشكل واضح وخفيف 👇\n\n"
        "مثال: بحري الصافية"
    )


def ask_payment_text(user) -> str:
    return (
        f"💳✨ يا {get_client_first(user)}\n\n"
        f"باقي خطوة خفيفة بس... {word_choose(user.id)} طريقة الدفع 👇"
    )


def price_prompt(user) -> str:
    n = get_client_first(user)
    return random.choice([
        (
            f"💰🔥 يا {n}\n\n"
            f"{word_write(user.id)} السعر البتشوفو مناسب للمشوار بالأرقام فقط.\n"
            "دا السعر الحنرسلو للسواقين، ولو مناسب غالبًا يقبلوا بسرعة إن شاء الله 🚕✨\n\n"
            "مثال: 2000"
        ),
        (
            f"💸😄 يا {n}\n\n"
            "ورّينا سعرك للمشوار بالأرقام فقط.\n"
            "حنرسلو للسواقين زي ما هو، ولو لقوه مناسب بقبلوا سريع 👌🚗\n\n"
            "مثال: 2000"
        ),
        (
            f"🤑🚕 يا {n}\n\n"
            f"{word_write(user.id)} السعر المقترح للمشوار.\n"
            "نفس الرقم دا الحيمشي للسواقين، فخلو مناسب عشان الطلب يتحرك بسرعة 😎\n\n"
            "مثال: 2000"
        ),
    ])


def timeout_choice_text(reject_count: int, price: str) -> str:
    return (
        "😅 يا زولنا، الطلب ما لقى سواق خلال المهلة المحددة.\n\n"
        f"❌ عدد الرفضات: {reject_count}\n"
        f"💰 السعر الحالي: {price}\n\n"
        "داير نعيدو بنفس السعر؟ ولا نعدّل السعر عشان يزيد حظو؟ 👇"
    )


def client_summary_text(order: Dict[str, Any], client_first: str) -> str:
    eligible = len(order.get("eligible_driver_ids", []))
    pickup_full = f"{order.get('pickup_area', '')} - {order.get('pickup_city', '')}\n{order.get('pickup_desc', '')}".strip()
    return (
        f"👌😎 يا {client_first}\n\n"
        "طلبك بقى مرتب وجاهز للإرسال 🚕✨\n\n"
        "راجع التفاصيل دي بسرعة 👇\n\n"
        f"🚘 النوع: {order.get('ride_type', 'عربية')}\n"
        f"📍 الانطلاق:\n{pickup_full}\n"
        f"🎯 الوجهة: {order['destination']}\n"
        f"💰 السعر: {order['price']}\n"
        f"💳 الدفع: {order['payment']}\n"
        f"🧑‍✈️ السواقين المطابقين للحي حالياً: {eligible}\n\n"
        "لو تمام، رسلو للسواقين 👇"
    )


def _progress_bar(pct: int, total: int = 10) -> str:
    filled = max(0, min(total, int(round((pct / 100) * total))))
    return "🟩" * filled + "⬛" * (total - filled)


def _build_client_status_text(order: Dict[str, Any]) -> str:
    label = STATUS_LABEL.get(order.get("status", ST_SENT), "ℹ️")
    pct = STATUS_PROGRESS.get(order.get("status", ST_SENT), 20)
    bar = _progress_bar(pct)

    driver_line = "—"
    if order.get("accepted_by"):
        d = DRIVERS_DB.get(str(order["accepted_by"]), {})
        driver_line = (
            f"{d.get('name_full', 'سواقنا')}\n"
            f"📞 {d.get('phone', 'غير متوفر')}\n"
            f"🚗 {d.get('car', 'غير متوفر')} — {d.get('color', 'غير متوفر')}\n"
            f"🔢 {d.get('plate', 'غير متوفر')}"
        )

    code_block = ""
    if order.get("accepted_by") and order.get("otp_code") and not order.get("otp_verified") and order.get("status") in (ST_ACCEPTED, ST_ON_THE_WAY, ST_ARRIVED):
        code_block = f"\n🟨 الكود بتاعك: 🔐 {order['otp_code']}\nسلّمو للسواق أول ما تركب/ي 😎👌\n"

    pickup_full = f"{order.get('pickup_area', '')} - {order.get('pickup_city', '')}\n{order.get('pickup_desc', '')}".strip()

    return (
        "🚗 لوحة متابعة المشوار\n\n"
        f"{bar}  {pct}%\n"
        f"📌 الحالة: {label}\n"
        f"{code_block}\n"
        f"👤 السائق:\n{driver_line}\n\n"
        f"📍 الانطلاق:\n{pickup_full}\n"
        f"🎯 الوجهة: {order['destination']}\n"
        f"💰 السعر: {order['price']}\n"
        f"💳 الدفع: {order['payment']}\n"
    )


def _build_driver_ctrl_text(order: Dict[str, Any], driver_name: str) -> str:
    pickup_full = f"{order.get('pickup_area', '')} - {order.get('pickup_city', '')}\n{order.get('pickup_desc', '')}".strip()
    otp_line = ""
    if order.get("otp_pending") and not order.get("otp_verified") and order.get("status") == ST_ARRIVED:
        otp_line = "\n🔐 اكتب الكود في الشات (أرقام فقط) ✅\n"

    return (
        "🚗 لوحة الرحلة يا بطل\n\n"
        f"👤 السائق: {driver_name}\n"
        f"📌 الحالة: {STATUS_LABEL.get(order.get('status', ST_SENT), 'ℹ️')}\n"
        f"{otp_line}\n"
        f"📍 الانطلاق:\n{pickup_full}\n"
        f"🎯 الوجهة: {order['destination']}\n"
        f"💰 السعر: {order['price']}\n"
        f"💳 الدفع: {order['payment']}\n\n"
        "اختار الخطوة الجاية 👇"
    )

# =========================================================
# ORDER HELPERS
# =========================================================
def make_order(chat_id: int, user_id: int) -> Dict[str, Any]:
    oid = str(uuid.uuid4())[:8]
    return {
        "order_id": oid,
        "client_chat_id": chat_id,
        "client_user_id": user_id,
        "ride_type": "عربية",
        "pickup_city": "",
        "pickup_area": "",
        "pickup_desc": "",
        "destination": "",
        "price": "",
        "payment": "",
        "accepted_by": None,
        "rejects": [],
        "offer_message_ids": {},
        "eligible_driver_ids": [],
        "status": ST_SENT,
        "client_status_msg_id": None,
        "driver_ctrl_msg_id": None,
        "driver_settlement_msg_id": None,
        "rating_msg_id": None,
        "otp_code": None,
        "otp_verified": False,
        "otp_pending": False,
        "created_at": _now_local().isoformat(),
        "timeout_finalized": False,
        "timeout_choice_msg_id": None,
    }


def eligible_drivers_for_order(order: Dict[str, Any]) -> List[int]:
    city = order.get("pickup_city")
    area = order.get("pickup_area")
    if not city or not area:
        return []

    key = f"{city}|{area}"
    out = []
    for driver_id in ONLINE_DRIVERS:
        d = DRIVERS_DB.get(str(driver_id), {}) or {}
        if driver_is_blocked_due(driver_id):
            continue
        if key in (d.get("work_areas") or []):
            out.append(int(driver_id))
    return out

# =========================================================
# ORDER BUTTONS
# =========================================================
def payment_inline(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("💵 كاش", callback_data=f"PAY:CASH:{order_id}"),
            InlineKeyboardButton("🏦 تحويل بنكك", callback_data=f"PAY:BANK:{order_id}"),
        ],
        [InlineKeyboardButton("❌ إلغاء", callback_data="CLIENT:CANCEL")],
    ])


def client_summary_inline(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ إرسال للسواقين", callback_data=f"CLIENT:SEND:{order_id}"),
            InlineKeyboardButton("❌ إلغاء", callback_data=f"CLIENT:CANCEL:{order_id}"),
        ]
    ])


def retry_choice_inline(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔁 إعادة الإرسال بنفس السعر", callback_data=f"CLIENT:RESEND_SAME:{order_id}"),
            InlineKeyboardButton("✏️ تعديل السعر", callback_data=f"CLIENT:EDIT_PRICE:{order_id}"),
        ],
        [InlineKeyboardButton("❌ إلغاء", callback_data=f"CLIENT:CANCEL:{order_id}")],
    ])


def driver_offer_inline(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ قبول", callback_data=f"ORDER:ACCEPT:{order_id}"),
            InlineKeyboardButton("❌ رفض", callback_data=f"ORDER:REJECT:{order_id}"),
        ]
    ])


def driver_trip_controls_inline(order_id: str, status: str, otp_pending: bool, client_user_id: int, otp_verified: bool) -> InlineKeyboardMarkup:
    buttons = []

    if status in (ST_ACCEPTED, ST_SENT):
        buttons.append([InlineKeyboardButton("🚗 أنا في الطريق", callback_data=f"TRIP:OTW:{order_id}")])

    if status == ST_ON_THE_WAY:
        buttons.append([InlineKeyboardButton("📍 وصلت", callback_data=f"TRIP:ARR:{order_id}")])

    if status == ST_ARRIVED and not otp_pending and not otp_verified:
        buttons.append([InlineKeyboardButton("▶️ بدء الرحلة (بالكود)", callback_data=f"TRIP:START:{order_id}")])

    if status == ST_TRIP_START:
        buttons.append([InlineKeyboardButton("✅ إنهاء الرحلة", callback_data=f"TRIP:END:{order_id}")])

    if status in (ST_ACCEPTED, ST_ON_THE_WAY, ST_ARRIVED):
        buttons.append([InlineKeyboardButton("❌ إلغاء الرحلة", callback_data=f"DRV:CANCEL:{order_id}")])

    buttons.append([InlineKeyboardButton("ℹ️ تحديث/عرض الحالة", callback_data=f"TRIP:REFRESH:{order_id}")])
    buttons.append([InlineKeyboardButton("📞 تواصل مع العميل", url=f"tg://user?id={client_user_id}")])
    return InlineKeyboardMarkup(buttons)


def rating_inline(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("⭐1", callback_data=f"RATE:{order_id}:1"),
        InlineKeyboardButton("⭐2", callback_data=f"RATE:{order_id}:2"),
        InlineKeyboardButton("⭐3", callback_data=f"RATE:{order_id}:3"),
        InlineKeyboardButton("⭐4", callback_data=f"RATE:{order_id}:4"),
        InlineKeyboardButton("⭐5", callback_data=f"RATE:{order_id}:5"),
    ]])


def driver_after_trip_inline() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔙 الرجوع لقروب السواقين", callback_data="DRV:BACK_TO_GROUP"),
            InlineKeyboardButton("💼 محفظتي", callback_data="DRV:OPEN_WALLET"),
        ]
    ])


def driver_trip_settlement_inline(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("✅ إنهاء", callback_data=f"DRV:SETTLEMENT_CLOSE:{order_id}")]])


def cancellation_reasons_inline(actor_role: str, order_id: str, accepted: bool) -> InlineKeyboardMarkup:
    if actor_role == "client":
        reasons = CLIENT_CANCEL_REASONS_ACCEPTED if accepted else CLIENT_CANCEL_REASONS_PENDING
        prefix = "CXLSEL:CLIENT"
    else:
        reasons = DRIVER_CANCEL_REASONS
        prefix = "CXLSEL:DRIVER"

    rows = []
    for code, label in reasons.items():
        rows.append([InlineKeyboardButton(label, callback_data=f"{prefix}:{order_id}:{code}")])
    rows.append([InlineKeyboardButton("↩️ رجوع", callback_data=f"CXLBACK:{actor_role.upper()}:{order_id}")])
    return InlineKeyboardMarkup(rows)

# =========================================================
# STATUS PANELS + TIMEOUT MESSAGE HELPERS
# =========================================================
async def clear_timeout_choice_message(context: ContextTypes.DEFAULT_TYPE, order: Dict[str, Any]):
    mid = order.get("timeout_choice_msg_id")
    if mid:
        try:
            await context.bot.delete_message(chat_id=order["client_chat_id"], message_id=mid)
        except Exception:
            pass
    order["timeout_choice_msg_id"] = None


async def clear_client_status_panel(context: ContextTypes.DEFAULT_TYPE, order: Dict[str, Any]):
    mid = order.get("client_status_msg_id")
    if mid:
        try:
            await context.bot.delete_message(chat_id=order["client_chat_id"], message_id=mid)
        except Exception:
            pass
    order["client_status_msg_id"] = None


async def upsert_client_status(context: ContextTypes.DEFAULT_TYPE, order: Dict[str, Any]):
    rows = [[InlineKeyboardButton("ℹ️ تحديث/عرض الحالة", callback_data=f"CLIENT:REFRESH:{order['order_id']}")]]
    if order.get("status") in (ST_SENT, ST_ACCEPTED, ST_ON_THE_WAY, ST_ARRIVED):
        rows.append([InlineKeyboardButton("❌ إلغاء الرحلة", callback_data=f"CLIENT:CANCEL_TRIP:{order['order_id']}")])
    kb = InlineKeyboardMarkup(rows)

    mid = order.get("client_status_msg_id")
    txt = _build_client_status_text(order)
    if mid:
        try:
            await context.bot.edit_message_text(
                chat_id=order["client_chat_id"],
                message_id=mid,
                text=txt,
                reply_markup=kb
            )
            return
        except Exception:
            try:
                await context.bot.delete_message(chat_id=order["client_chat_id"], message_id=mid)
            except Exception:
                pass
            order["client_status_msg_id"] = None

    m = await context.bot.send_message(chat_id=order["client_chat_id"], text=txt, reply_markup=kb)
    order["client_status_msg_id"] = m.message_id


async def upsert_driver_controls(context: ContextTypes.DEFAULT_TYPE, order: Dict[str, Any], driver_user_id: int, driver_name: str):
    mid = order.get("driver_ctrl_msg_id")
    txt = _build_driver_ctrl_text(order, driver_name)
    kb = driver_trip_controls_inline(
        order_id=order["order_id"],
        status=order.get("status", ST_ACCEPTED),
        otp_pending=bool(order.get("otp_pending") and not order.get("otp_verified")),
        client_user_id=int(order.get("client_user_id")),
        otp_verified=bool(order.get("otp_verified")),
    )

    if mid:
        try:
            await context.bot.edit_message_text(
                chat_id=driver_user_id,
                message_id=mid,
                text=txt,
                reply_markup=kb
            )
            return
        except Exception:
            try:
                await context.bot.delete_message(chat_id=driver_user_id, message_id=mid)
            except Exception:
                pass
            order["driver_ctrl_msg_id"] = None

    m = await context.bot.send_message(chat_id=driver_user_id, text=txt, reply_markup=kb)
    order["driver_ctrl_msg_id"] = m.message_id

# =========================================================
# DRIVER DIRECT OFFERING
# =========================================================
def _driver_offer_text(order: Dict[str, Any]) -> str:
    pickup_full = f"{order.get('pickup_area', '')} - {order.get('pickup_city', '')}\n{order.get('pickup_desc', '')}".strip()
    return (
        "🚗 طلب جديد يا بطل\n\n"
        f"📍 الانطلاق:\n{pickup_full}\n"
        f"🎯 الوجهة: {order.get('destination')}\n"
        f"💰 السعر: {order.get('price')}\n"
        f"💳 الدفع: {order.get('payment')}\n\n"
        "لو مناسبك اضغط قبول 👇"
    )


async def clear_driver_offer_messages(context: ContextTypes.DEFAULT_TYPE, order: Dict[str, Any]):
    offer_ids = order.get("offer_message_ids", {}) or {}
    for uid, mid in list(offer_ids.items()):
        try:
            await context.bot.delete_message(chat_id=int(uid), message_id=mid)
        except Exception:
            pass
    order["offer_message_ids"] = {}


async def send_order_to_matching_drivers(context: ContextTypes.DEFAULT_TYPE, order_id: str):
    order = PENDING_ORDERS.get(order_id)
    if not order:
        return

    await clear_timeout_choice_message(context, order)
    await clear_client_status_panel(context, order)

    matched = eligible_drivers_for_order(order)
    order["eligible_driver_ids"] = matched
    order["offer_message_ids"] = {}
    order["rejects"] = []
    order["accepted_by"] = None
    order["status"] = ST_SENT
    order["timeout_finalized"] = False
    order["otp_pending"] = False
    order["otp_verified"] = False

    await upsert_client_status(context, order)

    for driver_id in matched:
        try:
            m = await context.bot.send_message(
                chat_id=driver_id,
                text=_driver_offer_text(order),
                reply_markup=driver_offer_inline(order_id)
            )
            order["offer_message_ids"][str(driver_id)] = m.message_id
        except Exception:
            pass

    old = TIMEOUT_TASKS.get(order_id)
    if old and not old.done():
        old.cancel()

    async def _timeout():
        try:
            await asyncio.sleep(ORDER_TIMEOUT_SECONDS)
            cur = PENDING_ORDERS.get(order_id)
            if cur and not cur.get("accepted_by") and not cur.get("timeout_finalized"):
                await finalize_timeout_choice(context, order_id)
        except asyncio.CancelledError:
            return

    TIMEOUT_TASKS[order_id] = asyncio.create_task(_timeout())


async def finalize_timeout_choice(context: ContextTypes.DEFAULT_TYPE, order_id: str):
    order = PENDING_ORDERS.get(order_id)
    if not order or order.get("accepted_by") or order.get("timeout_finalized"):
        return

    order["timeout_finalized"] = True
    await clear_driver_offer_messages(context, order)

    set_stage(order["client_chat_id"], "CL_RETRY", order_id=order_id)
    reject_count = len(order.get("rejects", []))
    msg = await context.bot.send_message(
        chat_id=order["client_chat_id"],
        text=timeout_choice_text(reject_count, str(order.get("price", ""))),
        reply_markup=retry_choice_inline(order_id)
    )
    order["timeout_choice_msg_id"] = msg.message_id

# =========================================================
# CANCELLATION HELPERS
# =========================================================
def save_cancellation_record(order: Dict[str, Any], actor_role: str, actor_user_id: int, actor_name: str, reason: str):
    rec = {
        "cancel_id": str(uuid.uuid4())[:10],
        "order_id": order.get("order_id"),
        "actor_role": actor_role,
        "actor_user_id": int(actor_user_id),
        "actor_name": actor_name,
        "reason": reason,
        "pickup_city": order.get("pickup_city"),
        "pickup_area": order.get("pickup_area"),
        "pickup_desc": order.get("pickup_desc"),
        "destination": order.get("destination"),
        "price": order.get("price"),
        "accepted_by": int(order["accepted_by"]) if order.get("accepted_by") else 0,
        "created_at": _now_local().isoformat(),
    }
    CANCELLATIONS_DB.setdefault("records", []).append(rec)
    CANCELLATIONS_DB["records"] = CANCELLATIONS_DB["records"][-500:]
    save_cancellations()


async def delete_order_artifacts(context: ContextTypes.DEFAULT_TYPE, order: Dict[str, Any]):
    await clear_driver_offer_messages(context, order)
    await clear_timeout_choice_message(context, order)

    try:
        if order.get("client_status_msg_id"):
            await context.bot.delete_message(chat_id=order["client_chat_id"], message_id=order["client_status_msg_id"])
    except Exception:
        pass

    try:
        if order.get("accepted_by") and order.get("driver_ctrl_msg_id"):
            await context.bot.delete_message(chat_id=order["accepted_by"], message_id=order["driver_ctrl_msg_id"])
    except Exception:
        pass

    order["client_status_msg_id"] = None
    order["driver_ctrl_msg_id"] = None


async def finalize_order_cancellation(context: ContextTypes.DEFAULT_TYPE, order_id: str, actor_role: str, actor_user, reason: str):
    order = PENDING_ORDERS.get(order_id)
    if not order:
        return False

    t = TIMEOUT_TASKS.get(order_id)
    if t and not t.done():
        t.cancel()

    save_cancellation_record(order, actor_role, actor_user.id, actor_user.full_name, reason)
    driver_id = order.get("accepted_by")
    client_chat_id = order.get("client_chat_id")
    client_user_id = order.get("client_user_id")

    order["status"] = ST_CANCELLED

    try:
        if actor_role == "client":
            if driver_id:
                msg_d = await context.bot.send_message(
                    chat_id=driver_id,
                    text=f"❌ العميل ألغى الرحلة\n\n📌 السبب: {reason}"
                )
                asyncio.create_task(delete_message_after(context.bot, msg_d.chat_id, msg_d.message_id, 7))

            msg_c = await context.bot.send_message(
                chat_id=client_chat_id,
                text=f"✅ تم إلغاء الرحلة\n\n📌 السبب المسجل: {reason}"
            )
            asyncio.create_task(delete_message_after(context.bot, msg_c.chat_id, msg_c.message_id, 7))
        else:
            msg_c = await context.bot.send_message(
                chat_id=client_chat_id,
                text=f"❌ السائق ألغى الرحلة\n\n📌 السبب: {reason}\nبنعتذر ليك 🌷"
            )
            asyncio.create_task(delete_message_after(context.bot, msg_c.chat_id, msg_c.message_id, 7))
            if driver_id:
                msg_d = await context.bot.send_message(
                    chat_id=driver_id,
                    text=f"✅ تم تسجيل إلغاء الرحلة\n\n📌 السبب المسجل: {reason}"
                )
                asyncio.create_task(delete_message_after(context.bot, msg_d.chat_id, msg_d.message_id, 7))
    except Exception:
        pass

    for admin_id in ADMIN_USER_IDS:
        try:
            admin_msg = await context.bot.send_message(
                chat_id=admin_id,
                text=(
                    "🛑 إلغاء جديد\n\n"
                    f"🆔 الطلب: {order_id}\n"
                    f"👤 الملغي: {actor_user.full_name}\n"
                    f"🎭 النوع: {'عميل' if actor_role == 'client' else 'سائق'}\n"
                    f"📌 السبب: {reason}\n"
                    f"📍 {order.get('pickup_area', '-')}\n"
                    f"🎯 {order.get('destination', '-')}\n"
                    f"💰 {order.get('price', '-')}"
                )
            )
            asyncio.create_task(delete_message_after(context.bot, admin_msg.chat_id, admin_msg.message_id, 7))
        except Exception:
            pass

    await delete_order_artifacts(context, order)
    await dismiss_cancel_reason_prompt(context)

    clear_stage(client_chat_id)
    if driver_id:
        DRIVER_OTP_WAIT.pop(driver_id, None)

    PENDING_ORDERS.pop(order_id, None)
    TIMEOUT_TASKS.pop(order_id, None)

    dummy_user = type("Dummy", (), {
        "id": client_user_id,
        "full_name": CLIENTS_DB.get(str(client_user_id), {}).get("name_full", "زولنا")
    })()
    await show_single_home(context, client_chat_id, dummy_user)
    return True

# =========================================================
# ADMIN UI
# =========================================================
def admin_panel_inline() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📊 الإحصائيات", callback_data="ADMIN:STATS"),
            InlineKeyboardButton("🚗 الرحلات", callback_data="ADMIN:TRIPS"),
        ],
        [
            InlineKeyboardButton("👥 العملاء", callback_data="ADMIN:CLIENTS"),
            InlineKeyboardButton("🧑‍✈️ السواقين", callback_data="ADMIN:DRIVERS"),
        ],
        [
            InlineKeyboardButton("💼 المحافظ", callback_data="ADMIN:WALLETS"),
            InlineKeyboardButton("🔎 البحث", callback_data="ADMIN:SEARCH"),
        ],
        [
            InlineKeyboardButton("🛑 الإلغاءات", callback_data="ADMIN:CANCELS"),
            InlineKeyboardButton("📈 أكثر الملغين", callback_data="ADMIN:CANCELS_TOP"),
        ],
        [
            InlineKeyboardButton("✅ تشغيل المشروع", callback_data="ADMIN:APP_ON"),
            InlineKeyboardButton("🚧 قيد الإنشاء", callback_data="ADMIN:APP_OFF"),
        ],
        [InlineKeyboardButton("❌ إغلاق", callback_data="ADMIN:CLOSE")],
    ])


def admin_back_inline() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("⬅️ رجوع", callback_data="ADMIN:HOME"),
            InlineKeyboardButton("❌ إغلاق", callback_data="ADMIN:CLOSE"),
        ]
    ])


def admin_search_menu_inline() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("👥 بحث عميل", callback_data="ADMIN:SEARCH_CLIENT"),
            InlineKeyboardButton("🧑‍✈️ بحث سائق", callback_data="ADMIN:SEARCH_DRIVER"),
        ],
        [InlineKeyboardButton("🚗 بحث رحلة", callback_data="ADMIN:SEARCH_TRIP")],
        [
            InlineKeyboardButton("⬅️ رجوع", callback_data="ADMIN:HOME"),
            InlineKeyboardButton("❌ إغلاق", callback_data="ADMIN:CLOSE"),
        ]
    ])


def admin_home_text() -> str:
    return f"🛠️ لوحة الأدمن — أوبر زول\n\n📌 حالة المشروع: {app_status_text()}\n\nاختَر من تحت 👇"


def admin_stats_text() -> str:
    total_clients = len(CLIENTS_DB)
    total_drivers = len(DRIVERS_DB)
    total_online = len(ONLINE_DRIVERS)
    total_trips = len(TRIPS_DB)
    total_pending = len(PENDING_ORDERS)
    total_cancels = len(CANCELLATIONS_DB.get("records", []))

    total_wallet_balance = sum(int((w or {}).get("balance", 0)) for w in WALLETS_DB.values())
    total_driver_due = sum(int((d or {}).get("commission_due", 0)) for d in DRIVERS_DB.values())

    return (
        "📊 الإحصائيات العامة\n\n"
        f"👥 العملاء: {total_clients}\n"
        f"🧑‍✈️ السواقين: {total_drivers}\n"
        f"🟢 الأونلاين الآن: {total_online}\n"
        f"🚗 الرحلات المكتملة: {total_trips}\n"
        f"⏳ الطلبات الحالية: {total_pending}\n"
        f"🛑 الإلغاءات: {total_cancels}\n"
        f"💼 إجمالي أرصدة المحافظ: {total_wallet_balance}\n"
        f"⚠️ إجمالي مديونية السواقين: {total_driver_due}"
    )


def admin_trips_text(limit: int = 15) -> str:
    if not TRIPS_DB:
        return "🚗 لا توجد رحلات محفوظة."

    trips = sorted(TRIPS_DB.values(), key=lambda x: x.get("ended_at", ""), reverse=True)[:limit]
    rows = []
    for t in trips:
        rows.append(
            f"• الرحلة: {t.get('order_id')}\n"
            f"📍 {t.get('pickup_area', '-')}, {t.get('pickup_city', '-')}\n"
            f"🎯 {t.get('destination', '-')}\n"
            f"💰 {t.get('fare', t.get('price', '-'))}\n"
            f"⭐ التقييم: {t.get('rating', '—')}"
        )
    return "🚗 آخر الرحلات\n\n" + "\n\n".join(rows)


def admin_clients_text(limit: int = 20) -> str:
    if not CLIENTS_DB:
        return "👥 لا يوجد عملاء."

    rows = []
    for uid, c in list(CLIENTS_DB.items())[:limit]:
        rows.append(
            f"• {c.get('name_full', uid)}\n"
            f"🆔 {uid}\n"
            f"📍 {c.get('home_area', '-')}, {c.get('home_city', '-')}"
        )
    return "👥 العملاء\n\n" + "\n\n".join(rows)


def admin_drivers_text(limit: int = 20) -> str:
    if not DRIVERS_DB:
        return "🧑‍✈️ لا يوجد سواقين."

    rows = []
    for uid, d in list(DRIVERS_DB.items())[:limit]:
        areas = ", ".join([x.split("|", 1)[1] for x in d.get("work_areas", [])]) or "—"
        status = "🟢 أونلاين" if int(uid) in ONLINE_DRIVERS else "🔴 أوفلاين"
        rows.append(
            f"• {d.get('name_full', uid)}\n"
            f"🆔 {uid}\n"
            f"📞 {d.get('phone', '-')}\n"
            f"📍 مناطق العمل: {areas}\n"
            f"⚠️ المتبقي: {int(d.get('commission_due', 0))}\n"
            f"📌 الحالة: {status}"
        )
    return "🧑‍✈️ السواقين\n\n" + "\n\n".join(rows)


def admin_wallets_text(limit: int = 20) -> str:
    repair_wallets_consistency()
    if not WALLETS_DB:
        return "💼 لا توجد محافظ."

    rows = []
    for uid, w in list(WALLETS_DB.items())[:limit]:
        rows.append(
            f"• المستخدم: {uid}\n"
            f"💰 الرصيد: {w.get('balance', 0)}\n"
            f"👥 عدد الإحالات: {w.get('referrals_count', 0)}\n"
            f"🔗 الكود: {w.get('ref_code', '-')}"
        )
    return "💼 المحافظ\n\n" + "\n\n".join(rows)


def _client_search_text(term: str) -> str:
    term = (term or "").strip().lower()
    rows = []
    for uid, c in CLIENTS_DB.items():
        name = str(c.get("name_full", "")).lower()
        if term in name or term == uid:
            rows.append(
                f"• {c.get('name_full', uid)}\n"
                f"🆔 {uid}\n"
                f"📍 {c.get('home_area', '-')}, {c.get('home_city', '-')}"
            )
    return "👥 نتيجة بحث العميل\n\n" + ("\n\n".join(rows) if rows else "ما لقينا نتيجة.")


def _driver_search_text(term: str) -> str:
    term = (term or "").strip().lower()
    rows = []
    for uid, d in DRIVERS_DB.items():
        name = str(d.get("name_full", "")).lower()
        phone = str(d.get("phone", "")).lower()
        if term in name or term in phone or term == uid:
            rows.append(
                f"• {d.get('name_full', uid)}\n"
                f"🆔 {uid}\n"
                f"📞 {d.get('phone', '-')}\n"
                f"⚠️ المتبقي: {int(d.get('commission_due', 0))}"
            )
    return "🧑‍✈️ نتيجة بحث السائق\n\n" + ("\n\n".join(rows) if rows else "ما لقينا نتيجة.")


def _trip_search_text(term: str) -> str:
    t = TRIPS_DB.get((term or "").strip())
    if not t:
        return "🚗 ما لقينا الرحلة."
    return (
        "🚗 بيانات الرحلة\n\n"
        f"🆔 {t.get('order_id', '-')}\n"
        f"📍 {t.get('pickup_area', '-')}, {t.get('pickup_city', '-')}\n"
        f"🎯 {t.get('destination', '-')}\n"
        f"💰 {t.get('fare', t.get('price', '-'))}\n"
        f"⭐ {t.get('rating', '—')}"
    )


def admin_cancellations_text(limit: int = 20) -> str:
    recs = CANCELLATIONS_DB.get("records", [])
    if not recs:
        return "🛑 لا توجد إلغاءات."
    rows = []
    for r in recs[-limit:][::-1]:
        rows.append(
            f"• الطلب: {r.get('order_id')}\n"
            f"👤 {r.get('actor_name')}\n"
            f"🎭 {'عميل' if r.get('actor_role') == 'client' else 'سائق'}\n"
            f"📌 السبب: {r.get('reason')}\n"
            f"📍 {r.get('pickup_area', '-')}\n"
            f"💰 {r.get('price', '-')}"
        )
    return "🛑 آخر الإلغاءات\n\n" + "\n\n".join(rows)


def admin_cancellations_top_text(limit: int = 10) -> str:
    recs = CANCELLATIONS_DB.get("records", [])
    if not recs:
        return "📈 لا توجد بيانات بعد."
    counter = Counter()
    names = {}
    for r in recs:
        key = (r.get("actor_role"), int(r.get("actor_user_id", 0)))
        counter[key] += 1
        names[key] = r.get("actor_name", str(key[1]))
    rows = []
    for (role, uid), cnt in counter.most_common(limit):
        rows.append(
            f"• {names[(role, uid)]}\n"
            f"🎭 {'عميل' if role == 'client' else 'سائق'}\n"
            f"🆔 {uid}\n"
            f"🛑 عدد الإلغاءات: {cnt}"
        )
    return "📈 الأكثر إلغاءً\n\n" + "\n\n".join(rows)


async def admin_open_panel(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    text = admin_home_text()
    old = context.user_data.get("admin_msg_id")

    if old and isinstance(old, (list, tuple)) and len(old) == 2 and old[0] == chat_id:
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=old[1],
                text=text,
                reply_markup=admin_panel_inline()
            )
            return
        except Exception:
            try:
                await context.bot.delete_message(chat_id=old[0], message_id=old[1])
            except Exception:
                pass
            context.user_data["admin_msg_id"] = None

    m = await context.bot.send_message(
        chat_id=chat_id,
        text=text,
        reply_markup=admin_panel_inline()
    )
    context.user_data["admin_msg_id"] = (m.chat_id, m.message_id)


async def admin_show_section(query, context: ContextTypes.DEFAULT_TYPE, title: str):
    if title == "HOME":
        text = admin_home_text()
        kb = admin_panel_inline()
    elif title == "STATS":
        text = admin_stats_text()
        kb = admin_back_inline()
    elif title == "TRIPS":
        text = admin_trips_text()
        kb = admin_back_inline()
    elif title == "CLIENTS":
        text = admin_clients_text()
        kb = admin_back_inline()
    elif title == "DRIVERS":
        text = admin_drivers_text()
        kb = admin_back_inline()
    elif title == "WALLETS":
        text = admin_wallets_text()
        kb = admin_back_inline()
    elif title == "CANCELS":
        text = admin_cancellations_text()
        kb = admin_back_inline()
    elif title == "CANCELS_TOP":
        text = admin_cancellations_top_text()
        kb = admin_back_inline()
    elif title == "SEARCH":
        text = "🔎 البحث — لوحة الأدمن\n\nاختَر النوع 👇"
        kb = admin_search_menu_inline()
    else:
        text = admin_home_text()
        kb = admin_panel_inline()

    try:
        await query.edit_message_text(text=text, reply_markup=kb)
        if query.message:
            context.user_data["admin_msg_id"] = (query.message.chat_id, query.message.message_id)
        return
    except Exception:
        pass

    try:
        if query.message:
            m = await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=text,
                reply_markup=kb
            )
            context.user_data["admin_msg_id"] = (m.chat_id, m.message_id)
    except Exception:
        pass

# =========================================================
# COMMANDS
# =========================================================
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        asyncio.create_task(delete_message_after(context.bot, update.effective_chat.id, update.effective_message.message_id, 0.2))
    except Exception:
        pass

    if update.effective_chat.id == GROUP_CHAT_ID:
        return

    user = update.effective_user
    _ensure_client(user)

    if context.args:
        arg0 = context.args[0]

        if arg0 == "wallet":
            if needs_gender(user.id):
                await ask_gender_screen(update, context)
                return
            if needs_home_area(user.id):
                await ask_home_city_screen(update.effective_chat.id, context, user.id)
                return
            await open_wallet_screen(context, update.effective_chat.id, user)
            return

        if arg0 == "walletg":
            if needs_gender(user.id):
                await ask_gender_screen(update, context)
                return
            if needs_home_area(user.id):
                await ask_home_city_screen(update.effective_chat.id, context, user.id)
                return
            await open_wallet_screen(context, update.effective_chat.id, user, autoclose_seconds=5)
            return

        if arg0 == "driver_register":
            await begin_driver_register(update, context)
            return

        if arg0 == "workareas":
            if needs_gender(user.id):
                await ask_gender_screen(update, context)
                return
            if needs_home_area(user.id):
                await ask_home_city_screen(update.effective_chat.id, context, user.id)
                return
            await open_work_areas_editor(update.effective_chat.id, context, user.id)
            return

        if arg0.startswith("ref") and len(arg0) > 3:
            code = arg0[3:].strip()
            inviter_id = _find_user_id_by_ref_code(code)
            if inviter_id and int(inviter_id) != int(user.id):
                if not CLIENTS_DB[str(user.id)].get("invited_by_client"):
                    CLIENTS_DB[str(user.id)]["invited_by_client"] = int(inviter_id)
                    save_clients()
                    w = _get_wallet(int(inviter_id))
                    w["referrals_count"] = int(w.get("referrals_count", 0)) + 1
                    save_wallets()
                    wallet_add_entry(int(inviter_id), f"➕ مشترك جديد عبر رابطك: {user.full_name}", kind="client")
                    wallet_add_summary_signup(int(inviter_id), user.full_name, "client")
                    repair_wallets_consistency()

                if str(int(inviter_id)) in DRIVERS_DB and not CLIENTS_DB[str(user.id)].get("pending_driver_inviter"):
                    CLIENTS_DB[str(user.id)]["pending_driver_inviter"] = int(inviter_id)
                    save_clients()

    if needs_gender(user.id):
        await ask_gender_screen(update, context)
        return
    if needs_home_area(user.id):
        await ask_home_city_screen(update.effective_chat.id, context, user.id)
        return

    await show_single_home(context, update.effective_chat.id, user)


async def admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.effective_message.reply_text("⛔ ما عندك صلاحية.")
        return
    await admin_open_panel(context, update.effective_chat.id)


async def panel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = await context.bot.send_message(
        chat_id=GROUP_CHAT_ID,
        text=(
            "🚖 لوحة السواقين — أوبر زول\n\n"
            "🟢 بدء العمل = أونلاين\n"
            "🔴 إنهاء العمل = أوفلاين\n"
            "💼 محفظتي = في الخاص\n"
            "📝 التسجيل = في الخاص\n"
            "📍 مناطق العمل = في الخاص"
        ),
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🟢 بدء العمل", callback_data="PANEL:ON"),
                InlineKeyboardButton("🔴 إنهاء العمل", callback_data="PANEL:OFF"),
            ],
            [
                InlineKeyboardButton("💼 محفظتي", url=f"https://t.me/{BOT_USERNAME}?start=walletg"),
                InlineKeyboardButton("📝 التسجيل", url=f"https://t.me/{BOT_USERNAME}?start=driver_register"),
            ],
            [
                InlineKeyboardButton("📍 مناطق العمل", url=f"https://t.me/{BOT_USERNAME}?start=workareas"),
            ],
        ])
    )
    try:
        await context.bot.pin_chat_message(chat_id=GROUP_CHAT_ID, message_id=msg.message_id)
    except Exception:
        pass


async def id_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(f"Chat ID: {update.effective_chat.id}")

# =========================================================
# TEXT HANDLER
# =========================================================
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id == GROUP_CHAT_ID:
        return

    user = update.effective_user
    _ensure_client(user)
    chat_id = update.effective_chat.id
    text = (update.effective_message.text or "").strip()

    if needs_gender(user.id):
        await ask_gender_screen(update, context)
        return

    if needs_home_area(user.id):
        await ask_home_city_screen(chat_id, context, user.id)
        return

    if get_stage(chat_id) == "CL_CANCEL_OTHER":
        order_id = get_order_id(chat_id)
        if not order_id or order_id not in PENDING_ORDERS:
            clear_stage(chat_id)
            await dismiss_cancel_reason_prompt(context)
            await show_single_home(context, chat_id, user)
            return
        if len(text) < 3:
            await toast(update.effective_message, context, "⚠️ اكتبي/اكتب سبب واضح شوية.")
            return
        await dismiss_cancel_reason_prompt(context)
        await finalize_order_cancellation(context, order_id, "client", user, text)
        clear_stage(chat_id)
        await show_single_home(context, chat_id, user)
        return

    if get_driver_stage(user.id) == "DRV_CANCEL_OTHER":
        order_id = context.user_data.get("driver_cancel_order_id")
        if not order_id or order_id not in PENDING_ORDERS:
            clear_driver_stage(user.id)
            context.user_data.pop("driver_cancel_order_id", None)
            await dismiss_cancel_reason_prompt(context)
            return
        if len(text) < 3:
            await toast(update.effective_message, context, "⚠️ اكتب سبب واضح شوية.")
            return
        await dismiss_cancel_reason_prompt(context)
        await finalize_order_cancellation(context, order_id, "driver", user, text)
        clear_driver_stage(user.id)
        context.user_data.pop("driver_cancel_order_id", None)
        return

    if is_admin(user.id):
        admin_stage = context.user_data.get("admin_stage")
        if admin_stage == "SEARCH_CLIENT":
            context.user_data["admin_stage"] = None
            await dismiss_admin(context)
            m = await context.bot.send_message(chat_id=chat_id, text=_client_search_text(text), reply_markup=admin_back_inline())
            context.user_data["admin_msg_id"] = (m.chat_id, m.message_id)
            return
        if admin_stage == "SEARCH_DRIVER":
            context.user_data["admin_stage"] = None
            await dismiss_admin(context)
            m = await context.bot.send_message(chat_id=chat_id, text=_driver_search_text(text), reply_markup=admin_back_inline())
            context.user_data["admin_msg_id"] = (m.chat_id, m.message_id)
            return
        if admin_stage == "SEARCH_TRIP":
            context.user_data["admin_stage"] = None
            await dismiss_admin(context)
            m = await context.bot.send_message(chat_id=chat_id, text=_trip_search_text(text), reply_markup=admin_back_inline())
            context.user_data["admin_msg_id"] = (m.chat_id, m.message_id)
            return

    if user.id in DRIVER_OTP_WAIT:
        order_id = DRIVER_OTP_WAIT.get(user.id)
        order = PENDING_ORDERS.get(order_id)
        try:
            asyncio.create_task(delete_message_after(context.bot, chat_id, update.effective_message.message_id, 0.2))
        except Exception:
            pass

        if not order:
            DRIVER_OTP_WAIT.pop(user.id, None)
            return

        if not re.fullmatch(r"\d{4,6}", text):
            await toast(update.effective_message, context, "⚠️ اكتب الكود أرقام بس 😄")
            return

        if text != str(order.get("otp_code")):
            await toast(update.effective_message, context, "❌ الكود غلط… جرّب تاني 😅")
            return

        order["otp_verified"] = True
        order["otp_pending"] = False
        DRIVER_OTP_WAIT.pop(user.id, None)
        order["status"] = ST_TRIP_START

        d = DRIVERS_DB.get(str(user.id), {}) or {}
        await upsert_client_status(context, order)
        await upsert_driver_controls(context, order, user.id, d.get("name_full", user.full_name))
        return

    handled = await handle_driver_register_text(update, context)
    if handled:
        return

    _add_user_id(context, chat_id, update.effective_message.message_id)

    stage = get_stage(chat_id)
    if not stage:
        await show_single_home(context, chat_id, user)
        return

    if stage == "CL_PICKUP_DESC":
        if len(text) < 2:
            await toast(update.effective_message, context, "⚠️ اكتب/اكتبي وصف واضح شوية.")
            return
        context.user_data["pickup_desc"] = text
        set_stage(chat_id, "CL_DEST")
        await delete_previous_step_messages(context)
        await send_prompt(update, context, ask_destination_text(user))
        return

    if stage == "CL_DEST":
        if len(text) < 2:
            await toast(update.effective_message, context, "⚠️ اكتب/اكتبي الوجهة بشكل واضح.")
            return
        context.user_data["destination"] = text
        set_stage(chat_id, "CL_PRICE")
        await delete_previous_step_messages(context)
        await send_prompt(update, context, price_prompt(user))
        return

    if stage == "CL_PRICE":
        if not only_digits(text):
            await toast(update.effective_message, context, "⚠️ السعر لازم يكون أرقام بس.")
            return

        order = make_order(chat_id, user.id)
        order_id = order["order_id"]
        order["pickup_city"] = context.user_data.get("pickup_city", "")
        order["pickup_area"] = context.user_data.get("pickup_area", "")
        order["pickup_desc"] = context.user_data.get("pickup_desc", "")
        order["destination"] = context.user_data.get("destination", "")
        order["price"] = text
        order["eligible_driver_ids"] = eligible_drivers_for_order(order)

        PENDING_ORDERS[order_id] = order
        set_stage(chat_id, "CL_PAY", order_id=order_id)

        await delete_previous_step_messages(context)
        await send_prompt(update, context, ask_payment_text(user), reply_markup=payment_inline(order_id))
        return

    if stage == "CL_NEW_PRICE":
        order_id = get_order_id(chat_id)
        if not order_id or order_id not in PENDING_ORDERS:
            clear_stage(chat_id)
            await show_single_home(context, chat_id, user)
            return

        if not only_digits(text):
            await toast(update.effective_message, context, "⚠️ اكتب/اكتبي السعر الجديد أرقام بس.")
            return

        old_price = int(PENDING_ORDERS[order_id]["price"])
        new_price = int(text)
        if new_price <= old_price:
            await toast(update.effective_message, context, f"⚠️ لازم السعر الجديد يكون أعلى من {old_price}.")
            return

        PENDING_ORDERS[order_id]["price"] = str(new_price)
        PENDING_ORDERS[order_id]["eligible_driver_ids"] = eligible_drivers_for_order(PENDING_ORDERS[order_id])

        set_stage(chat_id, "CL_SUMMARY", order_id=order_id)
        await delete_previous_step_messages(context)
        await send_prompt(update, context, client_summary_text(PENDING_ORDERS[order_id], get_client_first(user)), reply_markup=client_summary_inline(order_id))
        return

# =========================================================
# CALLBACKS
# =========================================================
async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data or ""
    user = query.from_user
    _ensure_client(user)
    chat_id = query.message.chat_id if query.message else None

    try:
        await query.answer()
    except Exception:
        pass

    # ---------------- ADMIN ----------------
    if data.startswith("ADMIN:"):
        if not is_admin(user.id):
            await query.answer("⛔ ما عندك صلاحية", show_alert=True)
            return

        action = data.split(":", 1)[1]

        if action == "APP_ON":
            set_app_enabled(True)
            await admin_show_section(query, context, "HOME")
            return
        if action == "APP_OFF":
            set_app_enabled(False)
            await admin_show_section(query, context, "HOME")
            return
        if action == "CLOSE":
            await dismiss_admin(context)
            context.user_data["admin_stage"] = None
            return
        if action == "SEARCH":
            await admin_show_section(query, context, "SEARCH")
            return
        if action == "SEARCH_CLIENT":
            context.user_data["admin_stage"] = "SEARCH_CLIENT"
            try:
                await query.edit_message_text("👥 اكتب اسم العميل أو آيديه.", reply_markup=admin_back_inline())
            except Exception:
                pass
            return
        if action == "SEARCH_DRIVER":
            context.user_data["admin_stage"] = "SEARCH_DRIVER"
            try:
                await query.edit_message_text("🧑‍✈️ اكتب اسم السائق أو رقمه.", reply_markup=admin_back_inline())
            except Exception:
                pass
            return
        if action == "SEARCH_TRIP":
            context.user_data["admin_stage"] = "SEARCH_TRIP"
            try:
                await query.edit_message_text("🚗 اكتب رقم الرحلة.", reply_markup=admin_back_inline())
            except Exception:
                pass
            return

        await admin_show_section(query, context, action)
        return

    # ---------------- GENDER ----------------
    if data.startswith("GENDER:"):
        g = data.split(":", 1)[1]
        CLIENTS_DB[str(user.id)]["gender"] = "F" if g == "F" else "M"
        save_clients()

        if is_driver_registered(user.id):
            DRIVERS_DB[str(user.id)]["gender"] = CLIENTS_DB[str(user.id)]["gender"]
            save_drivers()

        try:
            if query.message:
                asyncio.create_task(delete_message_after(context.bot, query.message.chat_id, query.message.message_id))
        except Exception:
            pass

        if chat_id:
            clear_stage(chat_id)

        if needs_home_area(user.id):
            await ask_home_city_screen(chat_id, context, user.id)
        else:
            await show_single_home(context, chat_id, user)
        return

    # ---------------- HOME CITY/AREA ----------------
    if data.startswith("HOME:CITY:"):
        city = data.split(":", 2)[2]
        try:
            if query.message:
                asyncio.create_task(delete_message_after(context.bot, query.message.chat_id, query.message.message_id))
        except Exception:
            pass
        await ask_home_area_screen(chat_id, context, city, user.id)
        return

    if data == "HOME:BACKCITY":
        try:
            if query.message:
                asyncio.create_task(delete_message_after(context.bot, query.message.chat_id, query.message.message_id))
        except Exception:
            pass
        await ask_home_city_screen(chat_id, context, user.id)
        return

    if data.startswith("HOME:AREA:"):
        _, _, city, area = data.split(":", 3)
        CLIENTS_DB[str(user.id)]["home_city"] = city
        CLIENTS_DB[str(user.id)]["home_area"] = area
        save_clients()

        if is_driver_registered(user.id):
            DRIVERS_DB[str(user.id)]["home_city"] = city
            DRIVERS_DB[str(user.id)]["home_area"] = area
            save_drivers()

        try:
            if query.message:
                asyncio.create_task(delete_message_after(context.bot, query.message.chat_id, query.message.message_id))
        except Exception:
            pass

        if get_driver_stage(user.id):
            await begin_driver_register(update, context)
            return

        await show_single_home(context, chat_id, user)
        return

    # ---------------- DRIVER WORK AREAS ----------------
    if data.startswith("WORK:CITY:"):
        city = data.split(":", 2)[2]
        if str(user.id) in DRIVERS_DB:
            DRIVERS_DB[str(user.id)]["work_city"] = city
            save_drivers()
        try:
            if query.message:
                asyncio.create_task(delete_message_after(context.bot, query.message.chat_id, query.message.message_id))
        except Exception:
            pass
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"📍 اختار الأحياء البتشتغل فيها في {city}.\nكل ضغطة بتفعّل أو تلغي الحي 👇",
            reply_markup=work_area_toggle_inline(user.id, city)
        )
        return

    if data == "WORK:BACKCITY":
        try:
            if query.message:
                asyncio.create_task(delete_message_after(context.bot, query.message.chat_id, query.message.message_id))
        except Exception:
            pass
        await context.bot.send_message(chat_id=chat_id, text="📍 اختار مدينة العمل:", reply_markup=work_city_inline())
        return

    if data.startswith("WORK:TOGGLE:"):
        _, _, city, area = data.split(":", 3)
        d = DRIVERS_DB.get(str(user.id), {}) or {}
        areas = set(d.get("work_areas", []))
        key = f"{city}|{area}"
        if key in areas:
            areas.remove(key)
        else:
            areas.add(key)
        d["work_city"] = city
        d["work_areas"] = sorted(list(areas))
        DRIVERS_DB[str(user.id)] = _upgrade_driver(d)
        save_drivers()

        try:
            await query.edit_message_reply_markup(reply_markup=work_area_toggle_inline(user.id, city))
        except Exception:
            pass
        return

    if data.startswith("WORK:CLEAR:"):
        city = data.split(":", 2)[2]
        d = DRIVERS_DB.get(str(user.id), {}) or {}
        d["work_areas"] = []
        d["work_city"] = city
        DRIVERS_DB[str(user.id)] = _upgrade_driver(d)
        save_drivers()
        try:
            await query.edit_message_reply_markup(reply_markup=work_area_toggle_inline(user.id, city))
        except Exception:
            pass
        return

    if data.startswith("WORK:SAVE:"):
        city = data.split(":", 2)[2]
        d = DRIVERS_DB.get(str(user.id), {}) or {}
        if not d.get("work_areas"):
            await query.answer("اختَر/اختاري حي واحد على الأقل.", show_alert=True)
            return

        clear_driver_stage(user.id)
        for key in ["drv_name_full", "drv_phone", "drv_car", "drv_color"]:
            context.user_data.pop(key, None)

        try:
            if query.message:
                asyncio.create_task(delete_message_after(context.bot, query.message.chat_id, query.message.message_id))
        except Exception:
            pass

        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                f"✅ تم حفظ مناطق العمل يا بطل\n\n"
                f"📍 مدينة العمل: {city}\n"
                f"🧭 عدد الأحياء المختارة: {len(d.get('work_areas', []))}"
            )
        )
        return

    # ---------------- NEEDS GENDER/HOME ----------------
    if needs_gender(user.id):
        await query.answer("اختَر/اختاري النوع أول 👇", show_alert=True)
        return

    if needs_home_area(user.id):
        await query.answer("حدد/حددي مكان السكن أول 👇", show_alert=True)
        return

    # ---------------- WALLET ----------------
    if data == "WALLET:OPEN":
        await dismiss_end_menu(context)
        await dismiss_welcome(context)
        await open_wallet_screen(context, chat_id, user)
        return

    if data == "WALLET:H_CLIENT":
        await dismiss_wallet_detail(context)
        m = await context.bot.send_message(chat_id=chat_id, text=wallet_detail_text(user, "client"), reply_markup=wallet_detail_back_inline("client"))
        context.user_data["wallet_detail_msg_id"] = (m.chat_id, m.message_id)
        return

    if data == "WALLET:H_DRIVER":
        await dismiss_wallet_detail(context)
        m = await context.bot.send_message(chat_id=chat_id, text=wallet_detail_text(user, "driver"), reply_markup=wallet_detail_back_inline("driver"))
        context.user_data["wallet_detail_msg_id"] = (m.chat_id, m.message_id)
        return

    if data.startswith("WALLET:DETAIL_BACK:") or data == "WALLET:BACK":
        await dismiss_wallet_detail(context)
        wm = context.user_data.get("wallet_main_msg_id")
        if wm:
            asyncio.create_task(delete_message_after(context.bot, wm[0], wm[1]))
            context.user_data["wallet_main_msg_id"] = None

        finish_banner = context.user_data.get("finish_banner_msg_id")
        if finish_banner:
            return

        await show_single_home(context, chat_id, user)
        return

    # ---------------- MAIN MENU ----------------
    if data == "MENU:CLIENT_START":
        await dismiss_end_menu(context)
        await dismiss_wallet_detail(context)
        await dismiss_welcome(context)

        set_stage(chat_id, "CL_PICKUP_CITY")
        await context.bot.send_message(chat_id=chat_id, text=ask_pickup_city_text(user), reply_markup=city_inline("PICKUP"))
        return

    if data.startswith("PICKUP:CITY:"):
        city = data.split(":", 2)[2]
        context.user_data["pickup_city"] = city
        set_stage(chat_id, "CL_PICKUP_AREA")
        try:
            if query.message:
                asyncio.create_task(delete_message_after(context.bot, query.message.chat_id, query.message.message_id))
        except Exception:
            pass
        await context.bot.send_message(chat_id=chat_id, text=ask_pickup_area_text(user, city), reply_markup=area_inline("PICKUP", city))
        return

    if data == "PICKUP:BACKCITY":
        set_stage(chat_id, "CL_PICKUP_CITY")
        try:
            if query.message:
                asyncio.create_task(delete_message_after(context.bot, query.message.chat_id, query.message.message_id))
        except Exception:
            pass
        await context.bot.send_message(chat_id=chat_id, text=ask_pickup_city_text(user), reply_markup=city_inline("PICKUP"))
        return

    if data.startswith("PICKUP:AREA:"):
        _, _, city, area = data.split(":", 3)
        context.user_data["pickup_city"] = city
        context.user_data["pickup_area"] = area
        set_stage(chat_id, "CL_PICKUP_DESC")
        try:
            if query.message:
                asyncio.create_task(delete_message_after(context.bot, query.message.chat_id, query.message.message_id))
        except Exception:
            pass
        await delete_previous_step_messages(context)
        await send_prompt(update, context, ask_pickup_desc_text(user, city, area))
        return

    if data == "MENU:DRIVER_START":
        await begin_driver_register(update, context)
        return

    # ---------------- CLIENT CANCEL GENERIC ----------------
    if data.startswith("CLIENT:CANCEL") and not data.startswith("CLIENT:CANCEL_TRIP"):
        parts = data.split(":")
        order_id = parts[2] if len(parts) >= 3 else None
        if order_id and order_id in PENDING_ORDERS:
            t = TIMEOUT_TASKS.get(order_id)
            if t and not t.done():
                t.cancel()
            await clear_timeout_choice_message(context, PENDING_ORDERS[order_id])
            PENDING_ORDERS.pop(order_id, None)
        try:
            if query.message:
                asyncio.create_task(delete_message_after(context.bot, query.message.chat_id, query.message.message_id))
        except Exception:
            pass
        if chat_id:
            clear_stage(chat_id)
        await dismiss_cancel_reason_prompt(context)
        await delete_previous_step_messages(context)
        await show_single_home(context, chat_id, user)
        return

    # ---------------- PAYMENT ----------------
    if data.startswith("PAY:"):
        _, pay_type, order_id = data.split(":", 2)
        order = PENDING_ORDERS.get(order_id)
        if not order:
            await query.answer("الطلب غير موجود", show_alert=True)
            return
        order["payment"] = "كاش" if pay_type == "CASH" else "تحويل بنكك"
        order["eligible_driver_ids"] = eligible_drivers_for_order(order)
        set_stage(chat_id, "CL_SUMMARY", order_id=order_id)
        await delete_previous_step_messages(context)
        await send_prompt(update, context, client_summary_text(order, get_client_first(user)), reply_markup=client_summary_inline(order_id))
        return

    # ---------------- SEND TO DRIVERS ----------------
    if data.startswith("CLIENT:SEND:"):
        order_id = data.split(":", 2)[2]
        order = PENDING_ORDERS.get(order_id)
        if not order:
            await query.answer("الطلب انتهى", show_alert=True)
            return

        if not app_is_enabled():
            await context.bot.send_message(chat_id=chat_id, text=app_under_construction_text())
            PENDING_ORDERS.pop(order_id, None)
            clear_stage(chat_id)
            await show_single_home(context, chat_id, user)
            return

        matched = eligible_drivers_for_order(order)
        order["eligible_driver_ids"] = matched

        if not matched:
            await query.answer("حالياً ما في سواقين في نفس الحي أونلاين 😔", show_alert=True)
            return

        await delete_previous_step_messages(context)
        await toast(query.message, context, f"✅ طلبك اتحرّك لعدد {len(matched)} سواق في نفس الحي 👀🚗", 3)
        set_stage(chat_id, "CL_WAIT_DRIVER", order_id=order_id)
        await send_order_to_matching_drivers(context, order_id)
        return

    if data.startswith("CLIENT:REFRESH:"):
        order_id = data.split(":", 2)[2]
        order = PENDING_ORDERS.get(order_id)
        if order:
            await dismiss_cancel_reason_prompt(context)
            await upsert_client_status(context, order)
        return

    if data.startswith("CLIENT:EDIT_PRICE:"):
        order_id = data.split(":", 2)[2]
        if order_id not in PENDING_ORDERS:
            await query.answer("الطلب انتهى", show_alert=True)
            return

        await clear_timeout_choice_message(context, PENDING_ORDERS[order_id])
        try:
            if query.message:
                asyncio.create_task(delete_message_after(context.bot, query.message.chat_id, query.message.message_id))
        except Exception:
            pass

        set_stage(chat_id, "CL_NEW_PRICE", order_id=order_id)
        await delete_previous_step_messages(context)
        await send_prompt(update, context, f"🔥 {word_write(user.id)} السعر الجديد بالأرقام فقط — ولازم يكون أعلى من {PENDING_ORDERS[order_id]['price']}")
        return

    if data.startswith("CLIENT:RESEND_SAME:"):
        order_id = data.split(":", 2)[2]
        order = PENDING_ORDERS.get(order_id)
        if not order:
            await query.answer("الطلب انتهى", show_alert=True)
            return

        await clear_timeout_choice_message(context, order)
        try:
            if query.message:
                asyncio.create_task(delete_message_after(context.bot, query.message.chat_id, query.message.message_id))
        except Exception:
            pass

        matched = eligible_drivers_for_order(order)
        order["eligible_driver_ids"] = matched
        if not matched:
            await query.answer("حالياً ما في سواقين مطابقين للحي أونلاين", show_alert=True)
            return
        set_stage(chat_id, "CL_WAIT_DRIVER", order_id=order_id)
        await toast(query.message, context, "🔁 تمام، أعدنا إرسال الطلب 😎", 2.5)
        await send_order_to_matching_drivers(context, order_id)
        return

    # ---------------- CLIENT/DRIVER CANCEL WIZARD ----------------
    if data.startswith("CLIENT:CANCEL_TRIP:"):
        order_id = data.split(":", 2)[2]
        order = PENDING_ORDERS.get(order_id)
        if not order:
            return
        await dismiss_cancel_reason_prompt(context)
        await set_cancel_reason_prompt(
            context,
            chat_id,
            "📌 اختَر/اختاري سبب الإلغاء:",
            cancellation_reasons_inline("client", order_id, bool(order.get("accepted_by")))
        )
        return

    if data.startswith("DRV:CANCEL:"):
        order_id = data.split(":", 2)[2]
        order = PENDING_ORDERS.get(order_id)
        if not order or order.get("accepted_by") != user.id:
            return
        await dismiss_cancel_reason_prompt(context)
        await set_cancel_reason_prompt(
            context,
            chat_id,
            "📌 اختَر سبب الإلغاء:",
            cancellation_reasons_inline("driver", order_id, True)
        )
        return

    if data.startswith("CXLBACK:"):
        _, actor, order_id = data.split(":", 2)
        await dismiss_cancel_reason_prompt(context)
        order = PENDING_ORDERS.get(order_id)
        if not order:
            return
        if actor == "CLIENT":
            await upsert_client_status(context, order)
        else:
            d = DRIVERS_DB.get(str(user.id), {}) or {}
            await upsert_driver_controls(context, order, user.id, d.get("name_full", user.full_name))
        return

    if data.startswith("CXLSEL:CLIENT:"):
        _, _, order_id, code = data.split(":", 3)
        order = PENDING_ORDERS.get(order_id)
        if not order:
            return

        if code == "OTHER":
            await dismiss_cancel_reason_prompt(context)
            set_stage(chat_id, "CL_CANCEL_OTHER", order_id=order_id)
            await set_cancel_reason_prompt(context, chat_id, "✍️ اكتبي/اكتب سبب الإلغاء:")
            return

        reasons = CLIENT_CANCEL_REASONS_ACCEPTED if order.get("accepted_by") else CLIENT_CANCEL_REASONS_PENDING
        reason = reasons.get(code, "سبب غير محدد")
        await dismiss_cancel_reason_prompt(context)
        await finalize_order_cancellation(context, order_id, "client", user, reason)
        clear_stage(chat_id)
        await show_single_home(context, chat_id, user)
        return

    if data.startswith("CXLSEL:DRIVER:"):
        _, _, order_id, code = data.split(":", 3)
        order = PENDING_ORDERS.get(order_id)
        if not order or order.get("accepted_by") != user.id:
            return

        if code == "OTHER":
            await dismiss_cancel_reason_prompt(context)
            set_driver_stage(user.id, "DRV_CANCEL_OTHER")
            context.user_data["driver_cancel_order_id"] = order_id
            await set_cancel_reason_prompt(context, chat_id, "✍️ اكتب سبب الإلغاء:")
            return

        reason = DRIVER_CANCEL_REASONS.get(code, "سبب غير محدد")
        await dismiss_cancel_reason_prompt(context)
        await finalize_order_cancellation(context, order_id, "driver", user, reason)
        clear_driver_stage(user.id)
        context.user_data.pop("driver_cancel_order_id", None)
        return

    # ---------------- PANEL ----------------
    if data == "PANEL:ON":
        if not is_driver_registered(user.id):
            await query.answer("لازم تسجل أولاً 📝", show_alert=True)
            return
        if driver_is_blocked_due(user.id):
            due = driver_finance_summary(user.id)["commission_due"]
            await query.answer(f"عليك عمولة {due} — سدد أول ✅", show_alert=True)
            return
        ONLINE_DRIVERS.add(user.id)
        _save_online_set(ONLINE_DRIVERS)
        await query.answer("تم، بقيت أونلاين ✅", show_alert=False)
        return

    if data == "PANEL:OFF":
        ONLINE_DRIVERS.discard(user.id)
        _save_online_set(ONLINE_DRIVERS)
        await query.answer("تم، بقيت أوفلاين ⛔", show_alert=False)
        return

    # ---------------- ORDER ACCEPT/REJECT ----------------
    if data.startswith("ORDER:"):
        _, action, order_id = data.split(":", 2)
        order = PENDING_ORDERS.get(order_id)
        if not order:
            await query.answer("الطلب انتهى", show_alert=True)
            return

        if not is_driver_registered(user.id):
            await query.answer("لازم تسجل كسائق أول.", show_alert=True)
            return

        if user.id not in ONLINE_DRIVERS:
            await query.answer("اضغط بدء العمل أول.", show_alert=True)
            return

        if driver_is_blocked_due(user.id):
            due = driver_finance_summary(user.id)["commission_due"]
            await query.answer(f"عليك عمولة {due} — سدد أول.", show_alert=True)
            return

        if user.id not in order.get("eligible_driver_ids", []):
            await query.answer("الطلب دا ما من منطقتك.", show_alert=True)
            return

        if order.get("accepted_by"):
            await query.answer("سائق تاني سبقك", show_alert=True)
            return

        if action == "REJECT":
            if user.id not in order["rejects"]:
                order["rejects"].append(user.id)
            await query.answer("تم الرفض", show_alert=False)
            if len(order["rejects"]) >= len(order.get("eligible_driver_ids", [])):
                t = TIMEOUT_TASKS.get(order_id)
                if t and not t.done():
                    t.cancel()
                await finalize_timeout_choice(context, order_id)
            return

        # ACCEPT
        order["accepted_by"] = user.id
        order["status"] = ST_ACCEPTED
        order["otp_code"] = _gen_code()
        order["otp_verified"] = False
        order["otp_pending"] = False
        order["timeout_finalized"] = False

        t = TIMEOUT_TASKS.get(order_id)
        if t and not t.done():
            t.cancel()

        await clear_driver_offer_messages(context, order)
        await clear_timeout_choice_message(context, order)
        await clear_client_status_panel(context, order)

        d = DRIVERS_DB.get(str(user.id), {}) or {}
        await upsert_client_status(context, order)
        await upsert_driver_controls(context, order, user.id, d.get("name_full", user.full_name))

        try:
            if query.message:
                asyncio.create_task(delete_message_after(context.bot, query.message.chat_id, query.message.message_id))
        except Exception:
            pass
        return

    # ---------------- TRIP FLOW ----------------
    if data.startswith("TRIP:"):
        _, step, order_id = data.split(":", 2)
        order = PENDING_ORDERS.get(order_id)
        if not order or order.get("accepted_by") != user.id:
            return

        d = DRIVERS_DB.get(str(user.id), {}) or {}
        driver_name = d.get("name_full", user.full_name)

        if step == "REFRESH":
            await dismiss_cancel_reason_prompt(context)
            await upsert_driver_controls(context, order, user.id, driver_name)
            return

        if step == "OTW":
            order["status"] = ST_ON_THE_WAY

        elif step == "ARR":
            order["status"] = ST_ARRIVED
            order["otp_pending"] = False
            order["otp_verified"] = False

        elif step == "START":
            order["status"] = ST_ARRIVED
            order["otp_pending"] = True
            DRIVER_OTP_WAIT[user.id] = order_id
            await upsert_client_status(context, order)
            await upsert_driver_controls(context, order, user.id, driver_name)
            return

        elif step == "END":
            order["status"] = ST_TRIP_END
            order["ended_at"] = _now_local().isoformat()
        else:
            return

        await upsert_client_status(context, order)
        await upsert_driver_controls(context, order, user.id, driver_name)

        if order["status"] == ST_TRIP_END:
            try:
                fare = int(str(order["price"]).strip())
            except Exception:
                fare = 0

            fin = compute_trip_financials(fare, int(order["client_user_id"]), int(order["accepted_by"]))

            TRIPS_DB[order_id] = {
                "order_id": order_id,
                "client_user_id": order["client_user_id"],
                "driver_user_id": order["accepted_by"],
                "pickup_city": order["pickup_city"],
                "pickup_area": order["pickup_area"],
                "pickup_desc": order["pickup_desc"],
                "destination": order["destination"],
                "price": order["price"],
                "fare": fare,
                "payment": order["payment"],
                "ride_type": order.get("ride_type", "عربية"),
                "rating": None,
                "client_ref_paid_amount": fin["client_ref_paid"],
                "driver_ref_paid_amount": fin["driver_ref_paid"],
                "platform_commission_amount": fin["platform_commission"],
                "platform_net_profit_amount": fin["platform_net_profit"],
                "driver_due_amount": fin["driver_due"],
                "created_at": order.get("created_at"),
                "ended_at": order.get("ended_at", _now_local().isoformat()),
            }
            save_trips()

            if fin["client_ref_paid"] > 0 and fin["client_inviter_id"] > 0:
                client_name = CLIENTS_DB.get(str(order["client_user_id"]), {}).get("name_full", "عميل")
                w = _get_wallet(fin["client_inviter_id"])
                w["balance"] = int(w.get("balance", 0)) + fin["client_ref_paid"]
                save_wallets()
                wallet_add_entry(fin["client_inviter_id"], f"👥 عمولة عميل (+{fin['client_ref_paid']})", kind="client")
                wallet_add_summary_commission(fin["client_inviter_id"], client_name, fin["client_ref_paid"], "client")

            if fin["driver_ref_paid"] > 0 and fin["driver_inviter_id"] > 0:
                d2 = DRIVERS_DB.get(str(order["accepted_by"]), {}) or {}
                w = _get_wallet(fin["driver_inviter_id"])
                w["balance"] = int(w.get("balance", 0)) + fin["driver_ref_paid"]
                save_wallets()
                wallet_add_entry(fin["driver_inviter_id"], f"🧑‍✈️ عمولة سائق (+{fin['driver_ref_paid']})", kind="driver")
                wallet_add_summary_commission(fin["driver_inviter_id"], d2.get("name_full", "سائق"), fin["driver_ref_paid"], "driver")

            repair_wallets_consistency()
            driver_finance_add_due(int(order["accepted_by"]), fin["platform_commission"], order_id)

            current_fin = driver_finance_summary(int(order["accepted_by"]))
            if current_fin["blocked_due"]:
                try:
                    await context.bot.send_message(chat_id=user.id, text=driver_debt_warning_text(current_fin["commission_due"]))
                except Exception:
                    pass

            await clear_client_status_panel(context, order)

            c_first = CLIENTS_DB.get(str(order["client_user_id"]), {}).get("name_first", "زولنا")
            msg = await context.bot.send_message(
                chat_id=order["client_chat_id"],
                text=(
                    f"✨ يا {c_first}، المشوار خلص تمام!\n\n"
                    "أدينا تقييم سريع من 1 إلى 5 ⭐"
                ),
                reply_markup=rating_inline(order_id)
            )
            order["rating_msg_id"] = msg.message_id

            try:
                if order.get("driver_ctrl_msg_id"):
                    asyncio.create_task(delete_message_after(context.bot, user.id, order["driver_ctrl_msg_id"]))
                    order["driver_ctrl_msg_id"] = None
            except Exception:
                pass

            try:
                sm = await context.bot.send_message(
                    chat_id=user.id,
                    text=(
                        "✅ تم إنهاء الرحلة\n\n"
                        f"💰 قيمة الرحلة: {fin['fare']}\n"
                        f"🧾 عمولة التطبيق: {fin['platform_commission']}\n"
                        f"💵 استحقاقك: {fin['driver_due']}"
                    ),
                    reply_markup=driver_trip_settlement_inline(order_id)
                )
                order["driver_settlement_msg_id"] = sm.message_id
            except Exception:
                pass

        return

    # ---------------- AFTER DRIVER END ----------------
    if data.startswith("DRV:SETTLEMENT_CLOSE:"):
        if query.message:
            asyncio.create_task(delete_message_after(context.bot, query.message.chat_id, query.message.message_id))
        m = await context.bot.send_message(chat_id=chat_id, text="✅ اختار 👇", reply_markup=driver_after_trip_inline())
        context.user_data["driver_end_panel"] = (m.chat_id, m.message_id)
        return

    if data == "DRV:OPEN_WALLET":
        if query.message:
            asyncio.create_task(delete_message_after(context.bot, query.message.chat_id, query.message.message_id))
        await open_wallet_screen(context, chat_id, user)
        return

    if data == "DRV:BACK_TO_GROUP":
        if query.message:
            asyncio.create_task(delete_message_after(context.bot, query.message.chat_id, query.message.message_id))
        m = await context.bot.send_message(
            chat_id=chat_id,
            text="🔗 قروب السواقين 👇",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("فتح القروب", url=DRIVERS_INVITE_LINK)]])
        )
        asyncio.create_task(delete_message_after(context.bot, chat_id, m.message_id, 5))
        return

    # ---------------- RATING ----------------
    if data.startswith("RATE:"):
        _, order_id, stars = data.split(":", 2)
        if order_id in TRIPS_DB:
            TRIPS_DB[order_id]["rating"] = int(stars)
            save_trips()

        try:
            asyncio.create_task(delete_message_after(context.bot, chat_id, query.message.message_id))
        except Exception:
            pass

        msg = await context.bot.send_message(
            chat_id=chat_id,
            text=(
                f"🌟 {('تسلّمي' if is_female(user.id) else 'تسلّم')} يا {get_client_first(user)}\n\n"
                f"تقييمك {stars}⭐ وصلنا وفرّحنا شديد 🤍"
            )
        )
        asyncio.create_task(delete_message_after(context.bot, chat_id, msg.message_id, 5))

        async def _send_menu_after():
            await asyncio.sleep(5)
            await dismiss_home_family(context)
            m = await context.bot.send_message(
                chat_id=chat_id,
                text=f"✅ {word_done(user.id)} — {word_choose(user.id)} من تحت 👇",
                reply_markup=welcome_menu_inline()
            )
            context.user_data["finish_banner_msg_id"] = (m.chat_id, m.message_id)

        asyncio.create_task(_send_menu_after())

        TIMEOUT_TASKS.pop(order_id, None)
        PENDING_ORDERS.pop(order_id, None)
        return

# =========================================================
# FALLBACK
# =========================================================
async def any_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id == GROUP_CHAT_ID:
        return

    user = update.effective_user
    _ensure_client(user)

    if needs_gender(user.id):
        await ask_gender_screen(update, context)
        return

    if needs_home_area(user.id):
        await ask_home_city_screen(update.effective_chat.id, context, user.id)
        return

    if get_stage(update.effective_chat.id):
        return

    await show_single_home(context, update.effective_chat.id, user)

# =========================================================
# RUN
# =========================================================
def main():
    print("Loaded online drivers:", ONLINE_DRIVERS)
    print("App status:", app_status_text())

    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("admin", admin_cmd))
    app.add_handler(CommandHandler("panel", panel_cmd))
    app.add_handler(CommandHandler("id", id_cmd))

    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(MessageHandler(filters.ALL, any_message))

    print("UberZol Bot is running...")
    app.run_polling()


if __name__ == "__main__":
    main()