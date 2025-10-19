import os
import re
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional
from pathlib import Path
from html import escape as html_escape  # безопасный HTML
import asyncio

# ===== TZ: безопасная работа с часовыми поясами =====
try:
    from zoneinfo import ZoneInfo
except Exception:  # старые окружения
    ZoneInfo = None  # будем использовать фолбэк
# ====================================================

from dotenv import load_dotenv
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.constants import ParseMode
from telegram.helpers import mention_html
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    TypeHandler,
    filters,
    PicklePersistence,
)

# ---------------------------------------------------------
# ЛОГИРОВАНИЕ
# ---------------------------------------------------------
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
    force=True
)
print(">>> bot.py starting…", flush=True)

# ---------------------------------------------------------
# .env — грузим из папки файла
# ---------------------------------------------------------
dotenv_path = Path(__file__).with_name(".env")
print(f">>> loading .env from: {dotenv_path}", flush=True)
load_dotenv(dotenv_path=dotenv_path)

BOT_TOKEN = os.getenv("BOT_TOKEN", "")

# ===== TZ: получаем локальную таймзону с фолбэком =====
LOCAL_TZ_NAME = os.getenv("LOCAL_TZ", "Europe/Moscow")

def _resolve_local_tz(name: str):
    """
    Пытаемся создать ZoneInfo(name). Если базы нет (Windows без tzdata) —
    пробуем импортировать tzdata. Если снова не вышло — даём фиксированный
    оффсет для известных зон (МСК = UTC+3). В крайнем случае — UTC.
    Возвращаем кортеж: (tzinfo, source_str)
    """
    # 1) Прямая попытка ZoneInfo
    if ZoneInfo is not None:
        try:
            return ZoneInfo(name), f"ZoneInfo({name})"
        except Exception:
            pass

    # 2) Попробовать подтянуть базу tzdata (если установлена)
    try:
        import tzdata  # noqa: F401
        if ZoneInfo is not None:
            try:
                return ZoneInfo(name), f"ZoneInfo({name}) via tzdata"
            except Exception:
                pass
    except Exception:
        pass

    # 3) Фиксированные оффсеты для популярных зон
    fixed_map = {
        "Europe/Moscow": timezone(timedelta(hours=3)),   # МСК, без переходов
        "UTC": timezone.utc,
    }
    if name in fixed_map:
        return fixed_map[name], f"fixed-offset({name})"

    # 4) Совсем крайний случай
    return timezone.utc, "fallback=UTC"

LOCAL_TZ, LOCAL_TZ_SRC = _resolve_local_tz(LOCAL_TZ_NAME)
# ========================================================

def _parse_admin_ids(env_value: str) -> set[int]:
    ids = set()
    for part in (env_value or "").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            ids.add(int(part))
        except ValueError:
            pass
    return ids

ADMIN_IDS = _parse_admin_ids(os.getenv("ADMIN_USER_IDS", "")) or _parse_admin_ids(os.getenv("ADMIN_USER_ID", ""))

THREADS_CHAT_ID = int(os.getenv("THREADS_CHAT_ID", "0") or "0")
ENTRY_THREAD_ID = int(os.getenv("ENTRY_THREAD_ID", "0") or "0")
REPORT_THREAD_ID = int(os.getenv("REPORT_THREAD_ID", "0") or "0")  # Тема для отчётов

print(">>> ENV CHECK:",
      "BOT_TOKEN set" if bool(BOT_TOKEN) else "BOT_TOKEN MISSING",
      f"ADMIN_IDS={sorted(ADMIN_IDS)}",
      f"THREADS_CHAT_ID={THREADS_CHAT_ID}",
      f"ENTRY_THREAD_ID={ENTRY_THREAD_ID}",
      f"REPORT_THREAD_ID={REPORT_THREAD_ID}",
      f"LOCAL_TZ={LOCAL_TZ_NAME} ({LOCAL_TZ_SRC})",
      sep=" | ", flush=True)

if not BOT_TOKEN:
    raise RuntimeError("Не задан BOT_TOKEN в .env")
if not ADMIN_IDS:
    raise RuntimeError("Не заданы ADMIN_USER_IDS в .env (список ID через запятую)")
if not THREADS_CHAT_ID:
    raise RuntimeError("Не задан THREADS_CHAT_ID — это супергруппа с включёнными Темами (Forum)")
if not ENTRY_THREAD_ID:
    raise RuntimeError("Не задан ENTRY_THREAD_ID — это id темы «Использовать бота» в вашей супергруппе")
if not REPORT_THREAD_ID:
    raise RuntimeError("Не задан REPORT_THREAD_ID — это id темы для публикации отчётов")

VEHICLE_TYPES = ["Kia Ceed", "Sitrak"]

# список пар (текст, код)
TASK_CHOICES = [
    ("Команда \"Emergency Brake\" с последующим ее отключением", "emergency_brake"),
    ("Команда \"Safe Brake\" с последующим ее отключением", "safe_brake"),
    ("Построение траектории телеоператором", "teleop_path"),
]

# Пресет «стандартных проверок»
PRESET_TASKS = ["emergency_brake", "safe_brake", "teleop_path"]

# Регулярки
# Разрешаем Drv12345 и Drv-12345
DRV_RE = re.compile(r"^[A-Za-z]{3}-?\d{3,10}$")
NUM_RE = re.compile(r"^\d{1,6}$")  # только цифры 1..6

# --------- ТЕКСТ для входной темы ----------
ENTRY_PROMPT = "Чтобы связаться с телеоператором, перейди в чат с ботом."

# ==========================================================
# ПАМЯТЬ + простой JSON-персист (на случай рестартов)
# ==========================================================
STATE_PATH = Path(__file__).with_name("bot_state.json")

AUTH_DRIVERS: Dict[int, Dict] = {}  # на будущее
DRIVERS: Dict[int, Dict] = {}
NEXT_REQUEST_ID = 1
REQUESTS: Dict[int, Dict] = {}
PENDING_ADMIN_COMMENT: Dict[int, int] = {}
# Локи для заявок (защита от гонок «Подключиться» — на будущее)
REQUEST_LOCKS: Dict[int, asyncio.Lock] = {}

def _load_state() -> None:
    global DRIVERS, NEXT_REQUEST_ID, REQUESTS, PENDING_ADMIN_COMMENT
    if not STATE_PATH.exists():
        return
    try:
        data = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        DRIVERS = data.get("DRIVERS", {})
        NEXT_REQUEST_ID = int(data.get("NEXT_REQUEST_ID", 1))
        REQUESTS = data.get("REQUESTS", {})
        PENDING_ADMIN_COMMENT = data.get("PENDING_ADMIN_COMMENT", {})
        logging.info("State loaded from %s (reqs=%d, drivers=%d)", STATE_PATH, len(REQUESTS), len(DRIVERS))
    except Exception as e:
        logging.warning("Failed to load state: %s", e)

def _save_state() -> None:
    try:
        data = {
            "DRIVERS": DRIVERS,
            "NEXT_REQUEST_ID": NEXT_REQUEST_ID,
            "REQUESTS": REQUESTS,
            "PENDING_ADMIN_COMMENT": PENDING_ADMIN_COMMENT,
        }
        STATE_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        logging.warning("Failed to save state: %s", e)

_load_state()

# ==========================================================
# ВСПОМОГАТЕЛЬНЫЕ
# ==========================================================
def set_driver_seen(u) -> None:
    DRIVERS[u.id] = {
        "user_id": u.id,
        "first_name": u.first_name or "",
        "last_name": u.last_name or "",
        "username": u.username or "",
    }
    _save_state()

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

def new_request_id() -> int:
    global NEXT_REQUEST_ID
    rid = NEXT_REQUEST_ID
    NEXT_REQUEST_ID += 1
    _save_state()
    return rid

def create_request(driver_user_id: int, task_code: str, vehicle_type: str,
                   vehicle_number: str, tasks: List[str]) -> int:
    rid = new_request_id()
    now_iso = datetime.now(timezone.utc).isoformat()
    REQUESTS[rid] = {
        "id": rid,
        "driver_user_id": driver_user_id,
        "task_code": task_code,
        "vehicle_type": vehicle_type,
        "vehicle_number": vehicle_number,
        "tasks": tasks[:],
        "status": "new",
        "operator_user_id": None,
        "operator_comment": "",
        "created_at": now_iso,           # время создания водителем (UTC ISO, aware)
        "accepted_at": None,             # время принятия оператором (UTC ISO)
        "closed_at": None,               # время закрытия (UTC ISO)
        "updated_at": now_iso,
        # рассылки адм. (для удаления «Требуется оператор»)
        "admin_message_ids": [],       # список {admin_id, message_id}
        # форумная тема
        "thread_id": None,             # message_thread_id темы
        "thread_message_id": None,     # id первого сообщения в теме
    }
    _save_state()
    return rid

def save_request_message_ids(request_id: int, admin_msg_ids: Optional[Dict[int, int]] = None, thread_msg_id: Optional[int] = None):
    req = REQUESTS.get(request_id)
    if not req:
        return
    if admin_msg_ids is not None:
        req["admin_message_ids"] = [{"admin_id": k, "message_id": v} for k, v in admin_msg_ids.items()]
    if thread_msg_id is not None:
        req["thread_message_id"] = thread_msg_id
    req["updated_at"] = datetime.now(timezone.utc).isoformat()
    _save_state()

def set_request_status(request_id: int, status: str, operator_user_id: Optional[int]):
    req = REQUESTS.get(request_id)
    if not req: return
    req["status"] = status
    if operator_user_id is not None:
        req["operator_user_id"] = operator_user_id
    req["updated_at"] = datetime.now(timezone.utc).isoformat()
    _save_state()

def set_request_comment(request_id: int, operator_user_id: int, comment: Optional[str]):
    req = REQUESTS.get(request_id)
    if not req: return
    req["operator_comment"] = comment or ""
    req["operator_user_id"] = operator_user_id
    req["updated_at"] = datetime.now(timezone.utc).isoformat()
    _save_state()

def mark_accepted(request_id: int, operator_user_id: int):
    req = REQUESTS.get(request_id)
    if not req: return
    req["operator_user_id"] = operator_user_id
    req["accepted_at"] = datetime.now(timezone.utc).isoformat()
    req["updated_at"] = req["accepted_at"]
    _save_state()

def mark_closed(request_id: int, operator_user_id: int):
    req = REQUESTS.get(request_id)
    if not req: return
    req["operator_user_id"] = operator_user_id
    req["closed_at"] = datetime.now(timezone.utc).isoformat()
    req["updated_at"] = req["closed_at"]
    _save_state()

def load_request(request_id: int) -> Optional[Dict]:
    return REQUESTS.get(request_id)

def load_driver(user_id: int) -> Optional[Dict]:
    return DRIVERS.get(user_id)

# ---------- НОРМАЛИЗАЦИЯ КОДА ЗАДАЧИ ----------
def normalize_task_code(raw: str) -> str:
    """
    'Drv12345' / 'Drv-12345' → 'DRV-12345'
    Любой другой текст возвращаем как есть (upper для букв, дефис добавляем при необходимости).
    """
    s = (raw or "").strip()
    if not s:
        return "-"
    m = re.fullmatch(r"([A-Za-z]{3})-?(\d{3,10})", s)
    if not m:
        return s[:100]  # принимаем произвольное
    return f"{m.group(1).upper()}-{m.group(2)}"

# ==========================================================
# ФОРМАТЫ
# ==========================================================
def tasks_human_readable(codes: List[str]) -> str:
    mapping = {code: text for (text, code) in TASK_CHOICES}
    return "; ".join(mapping.get(c, c) for c in codes) if codes else "—"

def vehicle_bort(vehicle_type: str, vehicle_number: str) -> str:
    number = (vehicle_number or "").strip()
    if not number:
        return "—"
    if vehicle_type == "Kia Ceed":
        return f"kc2-{number}"
    if vehicle_type == "Sitrak":
        return f"St-{number}"
    return f"{vehicle_type} {number}"

# ===== TZ: UTC ISO -> локальная строка
def _parse_iso(iso_str: Optional[str]) -> Optional[datetime]:
    """Безопасно парсим ISO8601 и приводим к UTC-aware."""
    if not iso_str:
        return None
    try:
        dt = datetime.fromisoformat(iso_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def _to_local(dt_utc: Optional[datetime]) -> Optional[datetime]:
    """UTC → локальная зона (из LOCAL_TZ)."""
    if dt_utc is None:
        return None
    try:
        return dt_utc.astimezone(LOCAL_TZ)
    except Exception:
        return None

def _fmt_hhmm_from_iso(iso_str: Optional[str]) -> str:
    dt_utc = _parse_iso(iso_str)
    dt_local = _to_local(dt_utc)
    return dt_local.strftime("%H:%M") if dt_local else "--:--"

def _fmt_date_from_iso(iso_str: Optional[str]) -> str:
    dt_utc = _parse_iso(iso_str)
    dt_local = _to_local(dt_utc)
    return dt_local.strftime("%d.%m.%Y") if dt_local else "--.--.----"

def _driver_display_name(driver: Dict) -> str:
    first = (driver.get("first_name") or "").strip()
    last = (driver.get("last_name") or "").strip()
    return (first + (" " + last if last else "")).strip() or "Водитель"

def request_summary_text(req: Dict) -> str:
    dt_local = _fmt_date_from_iso(req.get("created_at")) + " " + _fmt_hhmm_from_iso(req.get("created_at"))

    driver = load_driver(req["driver_user_id"]) or {}
    driver_name = _driver_display_name(driver)
    username = driver.get("username") or ""
    mention = mention_html(driver.get("user_id", 0), driver_name or "Водитель")

    # ЭКРАНИРУЕМ пользовательские поля:
    task_code = html_escape((req.get("task_code") or "").strip() or "-")
    bort = html_escape(vehicle_bort(req.get("vehicle_type",""), req.get("vehicle_number","")))
    tasks_line = html_escape(tasks_human_readable(req.get("tasks", [])))

    return (
        f"<b>Заявка от {dt_local}</b>\n"
        f"Задача в Jira: <b>{task_code}</b>\n"
        f"ВАТС: <b>{bort}</b>\n"
        f"Водитель: {mention}" + (f" (@{username})" if username else "") + "\n"
        f"Задачи: <b>{tasks_line}</b>"
    )

# ====== безопасное экранирование в HTML ======
def _esc(s: Optional[str]) -> str:
    return html_escape("" if s is None else str(s), quote=False)

# ====== вычисление времени решения в минутах ======
def _resolution_minutes(created_iso: Optional[str], closed_iso: Optional[str]) -> Optional[int]:
    if not (created_iso and closed_iso):
        return None
    try:
        created = datetime.fromisoformat(created_iso)
        closed = datetime.fromisoformat(closed_iso)
        delta = closed - created
        if delta.total_seconds() < 0:
            return None
        return int(delta.total_seconds() // 60)
    except Exception:
        return None

# ====== ТЕКСТЫ ПУЛЕЙ ДЛЯ ЗАДАЧ ======
BULLET_TEXTS: Dict[str, str] = {
    "emergency_brake": 'команда "Emergency Brake" от телеоператора приводит к экстренной остановке со значительным ускорением торможения, после отключения "Emergency Brake" телеоператором, ВАТС продолжает движение;',
    "safe_brake": 'команда "Safe Brake" от телеоператора приводит к плавной остановке, после отключения "Safe Brake" телеоператором, ВАТС продолжает движение;',
    "teleop_path": 'ВАТС проезжает по траектории, построенной телеоператором, останавливается в конце нарисованной дороги и начинает движение после удаления телеоператором нарисованной дороги.',
}

def _punctuate_bullet(text: str, is_last: bool) -> str:
    """
    Для пунктов списка:
    - у всех, кроме последнего, заканчиваем на ';'
    - у последнего — на '.'
    Нормализуем вне зависимости от того, что было в шаблоне.
    """
    t = (text or "").strip()
    if is_last:
        t = t.rstrip(";").rstrip(".") + "."
    else:
        t = t.rstrip(".").rstrip(";") + ";"
    return t

def _build_description(req: Dict) -> str:
    """Генерируем блок 'Описание' с корректной пунктуацией последнего пункта."""
    task_code = (req.get("task_code") or "-").strip()
    tasks = [c for c in (req.get("tasks", []) or []) if BULLET_TEXTS.get(c)]
    comment = (req.get("operator_comment") or "").strip()

    # Шапка
    lines = [f"Задача {task_code}."]
    if tasks:
        lines.append("Выполнили проверку следующих команд:")

        # Буллеты с нормализацией финального знака
        last_idx = len(tasks) - 1
        for idx, code in enumerate(tasks):
            raw = BULLET_TEXTS[code]
            lines.append(f"- {_punctuate_bullet(raw, is_last=(idx == last_idx))}")

    # Комментарий оператора (если есть)
    if comment:
        lines.append(comment)

    return "\n".join(lines)

# ====== ОБНОВЛЁННЫЙ ОТЧЁТ (без 'Результат') ======
def report_text(req: Dict) -> str:
    """
    Отчет от (dd.mm.yyyy)
    Время: HH:MM
    ВАТС: <борта>
    Описание: <многострочный блок как в ТЗ>
    Время решения: "~X мин" / "<1 мин" / "-"
    Всё после ":" — в <code>…</code> с HTML-экранированием.
    """
    created_iso = req.get("created_at")
    date_line = _fmt_date_from_iso(created_iso)
    time_line = _fmt_hhmm_from_iso(created_iso)

    vts = vehicle_bort(req.get("vehicle_type",""), req.get("vehicle_number",""))

    mins = _resolution_minutes(req.get("created_at"), req.get("closed_at"))
    if mins is None:
        solve = "-"
    else:
        solve = "<1 мин" if mins < 1 else f"~{mins} мин"

    descr_block = _build_description(req)

    return (
        f"Отчет от <code>{_esc(date_line)}</code>\n"
        f"Время: <code>{_esc(time_line)}</code>\n"
        f"ВАТС: <code>{_esc(vts)}</code>\n"
        f"Описание: <code>{_esc(descr_block)}</code>\n"
        # f"Результат: <code>{_esc(result)}</code>\n"   # <-- удалено по ТЗ (оставлено закомментированным)
        f"Время решения: <code>{_esc(solve)}</code>"
    )

def skip_task_code_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("Не указывать", callback_data="skip_task_code")]])

def back_keyboard(target_stage: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data=f"back_to:{target_stage}")]])

def vehicle_type_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Kia Ceed", callback_data="vehicle:Kia Ceed"),
            InlineKeyboardButton("Sitrak",  callback_data="vehicle:Sitrak"),
        ],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back_to:task_code")],
    ])

def tasks_keyboard(selected: Optional[List[str]] = None) -> InlineKeyboardMarkup:
    selected = selected or []
    rows = []
    for text, code in TASK_CHOICES:
        mark = "✅" if code in selected else "⬜️"
        rows.append([InlineKeyboardButton(f"{mark} {text}", callback_data=f"task_toggle:{code}")])
    rows.append([
        InlineKeyboardButton("Готово", callback_data="tasks_done"),
        InlineKeyboardButton("Отмена", callback_data="tasks_cancel"),
    ])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to:vehicle_number")])
    return InlineKeyboardMarkup(rows)

def tasks_choice_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Стандартные проверки", callback_data="tasks_preset")],
        [InlineKeyboardButton("📝 Выбрать вручную", callback_data="tasks_manual")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back_to:vehicle_number")],
    ])

# Панель оператора (для того, кто «подключился»)
def operator_controls_keyboard(request_id: int, current_status: str, deep_link: Optional[str] = None) -> InlineKeyboardMarkup:
    status_line = {
        "done":"✅ Задача выполнена",
        "not_done":"❌ Задача не выполнена",
        "new":"⏳ Ожидает выполнения",
        "closed":"🔒 Закрыта"
    }.get(current_status,current_status)
    rows = [
        [InlineKeyboardButton("✅ Задача выполнена", callback_data=f"op_status:{request_id}:done")],
        [InlineKeyboardButton("❌ Задача не выполнена", callback_data=f"op_status:{request_id}:not_done")],
        [InlineKeyboardButton("🗄 Закрыть заявку",      callback_data=f"op_close:{request_id}")],
    ]
    if deep_link:
        rows.append([InlineKeyboardButton("🔗 Открыть диалог в теме", url=deep_link)])
    rows.append([InlineKeyboardButton(f"ℹ️ Текущий статус: {status_line}", callback_data="noop")])
    return InlineKeyboardMarkup(rows)

# Кнопка «Подключиться» в уведомлении для админов
def operator_claim_keyboard(request_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🧑‍💻 Подключиться", callback_data=f"op_claim:{request_id}")]])

# --- Клавиатуры для ответа водителю после создания заявки ---
def _driver_open_url_keyboard(url: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("👨‍💼 Перейти к диалогу с оператором", url=url)]])

def _driver_join_group_keyboard(invite_url: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("✅ Вступить в группу", url=invite_url)]])

# --------- Хелперы: членство и deeplink ----------
async def _is_member_of_threads_chat(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> bool:
    try:
        member = await context.bot.get_chat_member(THREADS_CHAT_ID, user_id)
        status = getattr(member, "status", "")
        return (
            status in ("creator", "administrator", "member")
            or (status == "restricted" and bool(getattr(member, "is_member", False)))
        )
    except Exception:
        return False

def _topic_message_link(msg_id: int) -> Optional[str]:
    if not msg_id:
        return None
    chat_id_str = str(THREADS_CHAT_ID)
    if chat_id_str.startswith("-100"):
        internal = chat_id_str[4:]
    else:
        internal = str(abs(THREADS_CHAT_ID))
    return f"https://t.me/c/{internal}/{msg_id}"

# ==========================================================
# ТОПИКИ (FORUM TOPICS)
# ==========================================================
async def ensure_forum_topic(context: ContextTypes.DEFAULT_TYPE, req: Dict) -> Optional[int]:
    if req.get("thread_id"):
        return req["thread_id"]
    # Название темы — "Заявка от dd.mm.yyyy HH:MM"
    created = req.get("created_at")
    title_dt = _fmt_date_from_iso(created) + " " + _fmt_hhmm_from_iso(created)
    title = f"Заявка от {title_dt}"
    try:
        topic = await context.bot.create_forum_topic(chat_id=THREADS_CHAT_ID, name=title)
        thread_id = topic.message_thread_id
        req["thread_id"] = thread_id
        req["updated_at"] = datetime.now(timezone.utc).isoformat()
        _save_state()
        logging.info("Forum topic created: thread_id=%s for request #%s", thread_id, req["id"])
        return thread_id
    except Exception as e:
        logging.exception("create_forum_topic failed", exc_info=e)
        return None

async def post_intro_in_topic(context: ContextTypes.DEFAULT_TYPE, req: Dict, text: str) -> Optional[int]:
    if not req.get("thread_id"):
        return None
    try:
        m = await context.bot.send_message(
            chat_id=THREADS_CHAT_ID,
            message_thread_id=req["thread_id"],
            text=text,
            parse_mode=ParseMode.HTML
        )
        req["thread_message_id"] = m.message_id
        req["updated_at"] = datetime.now(timezone.utc).isoformat()
        _save_state()
        logging.info("Intro posted in thread %s (msg_id=%s)", req["thread_id"], m.message_id)
        return m.message_id
    except Exception as e:
        logging.exception("post_intro_in_topic failed", exc_info=e)
        return None

# Пересылка «якоря» темы/инвайта водителю
async def send_topic_jump(context: ContextTypes.DEFAULT_TYPE, user_id: int, req: Dict):
    is_member = await _is_member_of_threads_chat(context, user_id)
    msg_id = req.get("thread_message_id") or 0

    if is_member and msg_id:
        try:
            await context.bot.forward_message(chat_id=user_id, from_chat_id=THREADS_CHAT_ID, message_id=msg_id)
        except Exception as e:
            logging.exception("forward topic intro failed", exc_info=e)
        return

    try:
        link = await context.bot.create_chat_invite_link(chat_id=THREADS_CHAT_ID, creates_join_request=False)
        await context.bot.send_message(chat_id=user_id, text="Сначала вступи в группу:", reply_markup=_driver_join_group_keyboard(link.invite_link))
    except Exception as e:
        logging.exception("send invite failed", exc_info=e)

# ==========================================================
# ЭКРАНЫ (СТАДИИ)
# ==========================================================
async def stage_task_code(message, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.setdefault("request", {"task_code":"","vehicle_type":"","vehicle_number":"","tasks":[]})
    context.user_data["await"] = "task_code"
    await message.reply_text(
        "Привет! Укажи название задачи в Jira (например, Drv-12345)",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Не указывать", callback_data="skip_task_code")]])
    )

async def stage_vehicle_type(message, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["await"] = None
    await message.reply_text("Выбери тип ТС", reply_markup=vehicle_type_keyboard())

async def stage_vehicle_number(message, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["await"] = "vehicle_number"
    await message.reply_text("Введи номер борта (только цифры)", reply_markup=back_keyboard("vehicle_type"))

async def stage_tasks(message, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["await"] = None
    kb = tasks_choice_keyboard()
    await message.reply_text("Что будем проверять?", reply_markup=kb)

# ==========================================================
# КОМАНДЫ
# ==========================================================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    set_driver_seen(user)

    if update.effective_chat and update.effective_chat.type != "private":
        msg_thread_id = update.effective_message.message_thread_id if update.effective_message else None
        if msg_thread_id == ENTRY_THREAD_ID:
            me = await context.bot.get_me()
            btn = InlineKeyboardMarkup([[
                InlineKeyboardButton("Открыть чат с ботом", url=f"https://t.me/{me.username}?start=start")
            ]])
            await update.effective_message.reply_text(ENTRY_PROMPT, reply_markup=btn)
        return

    await stage_task_code(update.effective_message, context)

async def cmd_whoami(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(f"Твой Telegram ID: <code>{update.effective_user.id}</code>", parse_mode=ParseMode.HTML)

async def cmd_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    thread_id = update.effective_message.message_thread_id
    text = f"ID этого чата: <code>{chat_id}</code>"
    if thread_id:
        text += f"\nID темы: <code>{thread_id}</code>"
    await update.effective_message.reply_text(text, parse_mode=ParseMode.HTML)

async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text("pong")

async def cmd_state(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(f"user_data: {dict(context.user_data)}")

# Диагностика
async def on_any_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try: d = update.to_dict()
    except Exception: d = str(update)
    logging.info("UPDATE INBOUND: %s", d)

# ==========================================================
# ДИАЛОГ ВОДИТЕЛЯ
# ==========================================================
async def on_user_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat and update.effective_chat.type != "private":
        return

    # Игнорируем только когда админ пишет комментарий к закрытию
    if is_admin(update.effective_user.id) and PENDING_ADMIN_COMMENT.get(update.effective_user.id):
        return

    msg_text = (update.effective_message.text or "").strip() if update.effective_message else ""
    logging.info("on_user_text <- %r | await=%r", msg_text, context.user_data.get("await"))

    if "await" not in context.user_data or context.user_data.get("await") is None:
        await stage_task_code(update.effective_message, context)
        return

    step = context.user_data.get("await")
    msg = msg_text

    if step == "task_code":
        msg = msg.strip()
        if msg and not DRV_RE.match(msg):
            await update.effective_message.reply_text(
                "Формат задачи в Jira: три латинские буквы и 3–10 цифр, дефис допускается (например, Drv12345 или Drv-12345). "
                "Можно нажать «Не указывать».",
                reply_markup=skip_task_code_keyboard()
            )
            return
        context.user_data["request"]["task_code"] = normalize_task_code(msg) if msg else "-"
        await stage_vehicle_type(update.effective_message, context)
        return

    if step == "vehicle_number":
        if not msg or not NUM_RE.match(msg):
            await update.effective_message.reply_text(
                "Номер борта должен содержать только цифры (например, 030). Попробуйте ещё раз.",
                reply_markup=back_keyboard("vehicle_type")
            )
            return
        context.user_data["request"]["vehicle_number"] = msg
        await stage_tasks(update.effective_message, context)
        return

async def on_skip_task_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data.setdefault("request", {})
    context.user_data["request"]["task_code"] = "-"
    await stage_vehicle_type(query.message, context)

# ==========================================================
# САБМИТ ЗАЯВКИ + РАССЫЛКА АДМИНАМ (с «Подключиться»)
# ==========================================================
async def _submit_request_and_notify(query, context: ContextTypes.DEFAULT_TYPE):
    req_local = context.user_data.get("request", {})
    if not (req_local.get("vehicle_type") and req_local.get("vehicle_number")):
        await query.answer("Не все поля заполнены.", show_alert=True); return

    request_id = create_request(
        driver_user_id=query.from_user.id,
        task_code=req_local.get("task_code") or "-",
        vehicle_type=req_local["vehicle_type"],
        vehicle_number=req_local["vehicle_number"],
        tasks=req_local.get("tasks", []),
    )
    req = load_request(request_id)
    summary = request_summary_text(req)

    # Создаём тему заранее (чтобы был deep_link)
    thread_id = await ensure_forum_topic(context, req)

    # Первый пост в теме заявки (карточка)
    thread_msg_id = None
    if thread_id:
        mid = await post_intro_in_topic(context, req, summary)
        thread_msg_id = mid

    # deep-link на первое сообщение темы (если есть)
    deep_link = _topic_message_link(thread_msg_id) if thread_msg_id else None

    # 1) ЛС админам — «Требуется оператор для <@Водитель>» + кнопка «Подключиться»
    admin_msg_ids: Dict[int, int] = {}
    driver = load_driver(req["driver_user_id"]) or {}
    driver_name = _driver_display_name(driver)
    driver_mention = mention_html(driver.get("user_id", 0), driver_name or "Водитель")
    call_text = f"🚨 Требуется оператор для {driver_mention}"

    for admin_id in ADMIN_IDS:
        try:
            m = await context.bot.send_message(
                chat_id=admin_id,
                text=call_text,
                parse_mode=ParseMode.HTML,  # кликабельный тег-профиля
                reply_markup=operator_claim_keyboard(request_id)
            )
            admin_msg_ids[admin_id] = m.message_id
        except Exception as e:
            logging.exception("send CALL to ADMIN failed (id=%s)", admin_id, exc_info=e)

    save_request_message_ids(request_id, admin_msg_ids=admin_msg_ids, thread_msg_id=thread_msg_id)

    # === Ответ водителю (два сообщения в нужном порядке) ===
    is_member = await _is_member_of_threads_chat(context, query.from_user.id)

    await context.bot.send_message(chat_id=query.from_user.id, text="✅ Заявка создана")

    if is_member and deep_link:
        await context.bot.send_message(
            chat_id=query.from_user.id,
            text=summary,
            parse_mode=ParseMode.HTML,
            reply_markup=_driver_open_url_keyboard(deep_link)
        )
    else:
        try:
            link = await context.bot.create_chat_invite_link(chat_id=THREADS_CHAT_ID, creates_join_request=False)
            await context.bot.send_message(
                chat_id=query.from_user.id,
                text=summary,
                parse_mode=ParseMode.HTML,
                reply_markup=_driver_join_group_keyboard(link.invite_link)
            )
        except Exception as e:
            logging.exception("send invite to driver failed", exc_info=e)

    # сброс локального состояния
    context.user_data.pop("request", None)
    context.user_data["await"] = None

# ==========================================================
# ОБРАБОТКА «ПОДКЛЮЧИТЬСЯ» (кто успел — тот оператор)
# ==========================================================
async def on_operator_claim(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data or ""
    _, _, req_id_str = data.partition(":")
    try:
        req_id = int(req_id_str)
    except ValueError:
        await query.answer("Некорректный ID заявки.", show_alert=True); return

    if not is_admin(query.from_user.id):
        await query.answer("Недостаточно прав.", show_alert=True); return

    req = load_request(req_id)
    if not req:
        await query.answer("Заявка не найдена.", show_alert=True); return

    # Если уже кто-то принял — сообщаем кто и удаляем это сообщение (если можно)
    if req.get("operator_user_id"):
        op_id = req["operator_user_id"]
        try:
            op_member = await context.bot.get_chat(op_id)
            taken_by = getattr(op_member, "username", None)
        except Exception:
            taken_by = None
        who = f"@{taken_by}" if taken_by else f"id {op_id}"
        await query.answer(f"Уже принят оператором ({who})", show_alert=True)
        try:
            await context.bot.delete_message(chat_id=query.from_user.id, message_id=query.message.message_id)
        except Exception:
            pass
        return

    # Помечаем как «принято»
    mark_accepted(req_id, query.from_user.id)

    # Готовим для принявшего полноценную панель
    summary = request_summary_text(req)
    deep_link = _topic_message_link(req.get("thread_message_id") or 0)
    kb_admin = operator_controls_keyboard(req_id, req["status"], deep_link=deep_link)

    # 1) Этому админу — редактируем его «вызов» в карточку с кнопками
    try:
        await query.message.edit_text(summary, parse_mode=ParseMode.HTML, reply_markup=kb_admin)
    except Exception:
        try:
            await context.bot.send_message(chat_id=query.from_user.id, text=summary, parse_mode=ParseMode.HTML, reply_markup=kb_admin)
        except Exception as e:
            logging.exception("send operator panel failed", exc_info=e)

    # 2) Всем остальным админам — удаляем уведомление «Требуется оператор»
    for pair in req.get("admin_message_ids", []):
        aid = pair.get("admin_id")
        mid = pair.get("message_id")
        if not aid or not mid:
            continue
        if aid == query.from_user.id:
            continue
        try:
            await context.bot.delete_message(chat_id=aid, message_id=mid)
        except Exception:
            pass

# ==========================================================
# «НАЗАД»
# ==========================================================
async def on_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data or ""
    _, _, target = data.partition(":")
    logging.info("BACK to -> %s", target)

    targets_map = {
        "task_code":      stage_task_code,
        "vehicle_type":   stage_vehicle_type,
        "vehicle_number": stage_vehicle_number,
        "tasks":          stage_tasks,
    }

    func = targets_map.get(target)
    if not func:
        await query.answer("Неизвестный шаг.", show_alert=True); return

    context.user_data.setdefault("request", {"task_code":"","vehicle_type":"","vehicle_number":"","tasks":[]})
    await func(query.message, context)

# ==========================================================
# ОПЕРАТОР (статус, закрытие, комментарий) + ОТЧЁТ В ТЕМУ
# ==========================================================
async def _refresh_operator_keyboards(context: ContextTypes.DEFAULT_TYPE, req: Dict, query: Optional["telegram.CallbackQuery"] = None):
    deep_link = _topic_message_link(req.get("thread_message_id") or 0)
    kb = operator_controls_keyboard(req["id"], req["status"], deep_link=deep_link)
    if query:
        try: await query.edit_message_reply_markup(reply_markup=kb)
        except Exception as e: logging.debug("edit clicked message kb failed", exc_info=e)

    for pair in req.get("admin_message_ids", []):
        try:
            await context.bot.edit_message_reply_markup(
                chat_id=pair["admin_id"],
                message_id=pair["message_id"],
                reply_markup=kb
            )
        except Exception:
            pass

async def on_operator_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query; await query.answer()
    data = query.data or ""
    logging.info("CALLBACK DATA (op_status): %r", data)
    if not is_admin(query.from_user.id):
        await query.answer("Недостаточно прав.", show_alert=True); return

    parts = data.split(":", 2)
    if len(parts) != 3 or parts[0] != "op_status":
        await query.answer("Некорректные данные кнопки.", show_alert=True); return
    _, req_id_str, status = parts
    try:
        req_id = int(req_id_str)
    except ValueError:
        await query.answer("Некорректный ID заявки.", show_alert=True); return

    set_request_status(req_id, status, query.from_user.id)
    req = load_request(req_id)
    if not req:
        await query.answer("Заявка не найдена.", show_alert=True); return

    await _refresh_operator_keyboards(context, req, query)
    await query.answer("Статус обновлён.")

async def _send_report_to_topic(context: ContextTypes.DEFAULT_TYPE, req: Dict):
    try:
        txt = report_text(req)
        await context.bot.send_message(
            chat_id=THREADS_CHAT_ID,
            message_thread_id=REPORT_THREAD_ID,
            text=txt,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True
        )
    except Exception as e:
        logging.exception("send report to topic failed", exc_info=e)

async def on_operator_close(update: Update, Context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query; await query.answer()
    data = query.data or ""
    logging.info("CALLBACK DATA (op_close): %r", data)
    if not is_admin(query.from_user.id):
        await query.answer("Недостаточно прав.", show_alert=True); return

    prefix, sep, rest = data.partition(":")
    if prefix != "op_close" or not sep:
        await query.answer("Некорректные данные кнопки.", show_alert=True); return
    try:
        req_id = int(rest)
    except ValueError:
        await query.answer("Некорректный ID заявки.", show_alert=True); return

    PENDING_ADMIN_COMMENT[query.from_user.id] = req_id
    _save_state()
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✍️ Написать комментарий", callback_data=f"op_comment_yes:{req_id}")],
        [InlineKeyboardButton("➡️ Продолжить без комментариев", callback_data=f"op_comment_no:{req_id}")],
    ])
    await query.edit_message_reply_markup(reply_markup=None)
    await query.message.reply_text(f"Закрыть заявку. Добавить комментарий?", reply_markup=kb)

async def on_operator_comment_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query; await query.answer()
    data = query.data or ""
    logging.info("CALLBACK DATA (op_comment_*): %r", data)
    if not is_admin(query.from_user.id):
        await query.answer("Недостаточно прав.", show_alert=True); return

    if data.startswith("op_comment_yes:"):
        await query.message.reply_text("Напиши комментарий одним сообщением.")
        return

    if data.startswith("op_comment_no:"):
        req_id = PENDING_ADMIN_COMMENT.get(query.from_user.id)
        if not req_id:
            await query.answer("Не найдена заявка для закрытия.", show_alert=True); return

        set_request_comment(req_id, query.from_user.id, "")
        set_request_status(req_id, "closed", query.from_user.id)
        mark_closed(req_id, query.from_user.id)

        req = load_request(req_id)
        if req:
            await _send_report_to_topic(context, req)

        PENDING_ADMIN_COMMENT.pop(query.from_user.id, None)
        _save_state()
        await query.message.reply_text("Заявка закрыта. Отчёт опубликован в теме.")

# Текст комментария (только ЛС)
async def on_admin_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat and update.effective_chat.type != "private":
        return
    user = update.effective_user
    if not is_admin(user.id):
        return

    req_id = PENDING_ADMIN_COMMENT.get(user.id)
    if not req_id:
        return

    comment = (update.effective_message.text or "").strip()
    set_request_comment(req_id, user.id, comment)
    set_request_status(req_id, "closed", user.id)
    mark_closed(req_id, user.id)

    req = load_request(req_id)
    if req:
        await _send_report_to_topic(context, req)

    PENDING_ADMIN_COMMENT.pop(user.id, None)
    _save_state()
    await update.effective_message.reply_text("Заявка закрыта. Отчёт опубликован в теме.")

# ==========================================================
# ОБРАБОТЧИКИ ДЛЯ КНОПОК ВЫБОРА ТС И ЗАДАЧ
# ==========================================================
async def on_vehicle_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data or ""
    _, _, vtype = data.partition(":")
    if not vtype:
        await query.answer("Некорректные данные кнопки.", show_alert=True); return
    context.user_data.setdefault("request", {})
    context.user_data["request"]["vehicle_type"] = vtype
    await stage_vehicle_number(query.message, context)

async def on_tasks_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data or ""
    logging.info("CALLBACK DATA (tasks): %r", data)

    if data == "tasks_preset":
        context.user_data.setdefault("request", {})
        context.user_data["request"]["tasks"] = PRESET_TASKS.copy()
        await _submit_request_and_notify(query, context)
        return

    if data == "tasks_manual":
        selected = context.user_data.get("request", {}).get("tasks", [])
        await query.message.reply_text(
            "Отметь нужные задачи и нажми «Готово».",
            reply_markup=tasks_keyboard(selected)
        )
        return

    if data == "tasks_cancel":
        context.user_data.pop("request", None); context.user_data["await"] = None
        await query.message.reply_text("Отменено.")
        return

    if data == "tasks_done":
        await _submit_request_and_notify(query, context)
        return

    prefix, sep, rest = data.partition(":")
    if prefix != "task_toggle" or not sep:
        await query.answer("Неизвестная кнопка.", show_alert=False)
        return

    code = rest
    selected = context.user_data.setdefault("request", {}).setdefault("tasks", [])
    if code in selected:
        selected.remove(code)
    else:
        selected.append(code)
    await query.edit_message_reply_markup(reply_markup=tasks_keyboard(selected))

# ==========================================================
# ПРОЧЕЕ/ОШИБКИ
# ==========================================================
async def on_callback_misc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query.data == "noop":
        await query.answer(); return

async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logging.exception("Unhandled error", exc_info=context.error)

# ==========================================================
# MAIN
# ==========================================================
def main():
    async def _post_init(app: Application):
        try:
            await app.bot.delete_webhook(drop_pending_updates=True)
            logging.info("Webhook deleted (if any), pending updates dropped.")
        except Exception as e:
            logging.warning(f"delete_webhook failed: {e}")
        me = await app.bot.get_me()
        logging.info("Logged in as @%s (id=%s)", me.username, me.id)

    # PicklePersistence — полезно для user_data/chat_data/bot_data
    persistence = PicklePersistence(filepath="ptb_persistence.pkl", update_interval=30)

    app = Application.builder().token(BOT_TOKEN).post_init(_post_init).persistence(persistence).build()

    # Команды
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("whoami", cmd_whoami))
    app.add_handler(CommandHandler("id", cmd_id))
    app.add_handler(CommandHandler("ping", cmd_ping))
    app.add_handler(CommandHandler("state", cmd_state))

    # Диагностика (раньше всех)
    app.add_handler(TypeHandler(Update, on_any_update), group=-100)

    # ТЕКСТЫ: только ЛС
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND, on_user_text), group=0)
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND, on_admin_text), group=1)

    # Кнопки
    app.add_handler(CallbackQueryHandler(on_skip_task_code, pattern=r"^skip_task_code$"))
    app.add_handler(CallbackQueryHandler(on_vehicle_type, pattern=r"^vehicle:.+"))
    app.add_handler(CallbackQueryHandler(on_tasks_toggle, pattern=r"^(tasks_preset|tasks_manual|task_toggle:.+|tasks_done|tasks_cancel)$"))
    app.add_handler(CallbackQueryHandler(on_back, pattern=r"^back_to:.+"))

    # «Подключиться» (вызов оператора)
    app.add_handler(CallbackQueryHandler(on_operator_claim, pattern=r"^op_claim:\d+$"))

    # Панель оператора
    app.add_handler(CallbackQueryHandler(on_operator_status, pattern=r"^op_status:\d+:(done|not_done)$"))
    app.add_handler(CallbackQueryHandler(on_operator_close, pattern=r"^op_close:\d+$"))
    app.add_handler(CallbackQueryHandler(on_operator_comment_choice, pattern=r"^op_comment_(yes|no):\d+$"))

    app.add_handler(CallbackQueryHandler(on_callback_misc))

    app.add_error_handler(on_error)
    logging.info("Bot starting (polling)…")
    app.run_polling(allowed_updates=Update.ALL_TYPES, close_loop=False)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[OK] Бот остановлен по Ctrl+C", flush=True)
    except Exception as e:
        import traceback
        print("[FATAL] Uncaught exception:", e, flush=True)
        traceback.print_exc()
        raise
