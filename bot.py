import os
import re
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional
from pathlib import Path
from html import escape as html_escape
import asyncio
from tempfile import NamedTemporaryFile  # <-- для атомарной записи roles.json

# ===== TZ: безопасная работа с часовыми поясами =====
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None
# ====================================================

from dotenv import load_dotenv
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
    # parse_mode по умолчанию ставим в Defaults
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
    Defaults,
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
    if ZoneInfo is not None:
        try:
            return ZoneInfo(name), f"ZoneInfo({name})"
        except Exception:
            pass
    try:
        import tzdata  # noqa: F401
        if ZoneInfo is not None:
            try:
                return ZoneInfo(name), f"ZoneInfo({name}) via tzdata"
            except Exception:
                pass
    except Exception:
        pass
    fixed_map = {"Europe/Moscow": timezone(timedelta(hours=3)), "UTC": timezone.utc}
    if name in fixed_map:
        return fixed_map[name], f"fixed-offset({name})"
    return timezone.utc, "fallback=UTC"

LOCAL_TZ, LOCAL_TZ_SRC = _resolve_local_tz(LOCAL_TZ_NAME)

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

TASK_CHOICES = [
    ("Команда \"Emergency Brake\" с последующим ее отключением", "emergency_brake"),
    ("Команда \"Safe Brake\" с последующим ее отключением", "safe_brake"),
    ("Построение траектории телеоператором", "teleop_path"),
]
PRESET_TASKS = ["emergency_brake", "safe_brake", "teleop_path"]

DRV_RE = re.compile(r"^[A-Za-z]{3}-?\d{3,10}$")
NUM_RE = re.compile(r"^\d{1,6}$")

ENTRY_PROMPT = "Чтобы связаться с телеоператором, перейди в чат с ботом 👇"

# ==========================================================
# ПАМЯТЬ + JSON-персист
# ==========================================================
STATE_PATH = Path(__file__).with_name("bot_state.json")
ROLES_PATH = Path(__file__).with_name("roles.json")  # <-- отдельное хранилище ролей

AUTH_DRIVERS: Dict[int, Dict] = {}
DRIVERS: Dict[int, Dict] = {}
NEXT_REQUEST_ID = 1
REQUESTS: Dict[int, Dict] = {}
PENDING_ADMIN_COMMENT: Dict[int, int] = {}
REQUEST_LOCKS: Dict[int, asyncio.Lock] = {}

# ==== ОТДЕЛЬНОЕ ХРАНИЛИЩЕ РОЛЕЙ ==============================================
class RolesStore:
    """roles.json с атомарной записью, автопереносом из bot_state.json."""
    def __init__(self, path: Path):
        self.path = path
        self._roles: Dict[int, str] = {}

    def load(self):
        # 1) читаем roles.json если есть
        if self.path.exists():
            try:
                data = json.loads(self.path.read_text("utf-8"))
                self._roles = {int(k): v for k, v in data.items()}
                logging.info("Roles loaded from %s (count=%d)", self.path, len(self._roles))
                return
            except Exception as e:
                logging.warning("Failed to load roles.json: %s", e)
        # 2) миграция из старого bot_state.json (поле ROLES)
        try:
            if STATE_PATH.exists():
                st = json.loads(STATE_PATH.read_text("utf-8"))
                old = st.get("ROLES") or {}
                if old:
                    self._roles = {int(k): v for k, v in old.items()}
                    self.save()
                    logging.info("Roles migrated from bot_state.json to roles.json (count=%d)", len(self._roles))
        except Exception as e:
            logging.warning("Failed to migrate roles from bot_state.json: %s", e)

    def save(self):
        # атомарная запись через временный файл
        payload = json.dumps({str(k): v for k, v in self._roles.items()}, ensure_ascii=False, indent=2)
        try:
            with NamedTemporaryFile("w", delete=False, encoding="utf-8", dir=str(self.path.parent)) as tf:
                tf.write(payload)
                tmp = tf.name
            os.replace(tmp, self.path)
        except Exception as e:
            logging.warning("Failed to write roles.json: %s", e)

    def get(self, user_id: int) -> Optional[str]:
        if user_id in ADMIN_IDS:
            return "admin"
        return self._roles.get(user_id)

    def set(self, user_id: int, role: str):
        # админов не пишем — их роль из ADMIN_IDS
        if user_id in ADMIN_IDS:
            self._roles.pop(user_id, None)
        else:
            self._roles[user_id] = role
        self.save()

    def all_operators(self) -> List[int]:
        ops = [uid for uid, r in self._roles.items() if r == "operator"]
        # админов добавляем всегда
        return list(sorted(set(ops + list(ADMIN_IDS))))

ROLES_STORE = RolesStore(ROLES_PATH)
# ============================================================================

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

# ---- дебаунс сохранения состояния ----
class _StateDebouncer:
    def __init__(self, path: Path, interval_ms: int = 250):
        self.path = path
        self.interval = interval_ms / 1000.0
        self._dirty = False
        self._task: Optional[asyncio.Task] = None
    def mark_dirty(self):
        self._dirty = True
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._worker())
    async def _worker(self):
        try:
            await asyncio.sleep(self.interval)
            if not self._dirty:
                return
            self._dirty = False
            data = {
                "DRIVERS": DRIVERS,
                "NEXT_REQUEST_ID": NEXT_REQUEST_ID,
                "REQUESTS": REQUESTS,
                "PENDING_ADMIN_COMMENT": PENDING_ADMIN_COMMENT,
            }
            STATE_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as e:
            logging.warning("Debounced state save failed: %s", e)

_STATE_DEBOUNCER = _StateDebouncer(STATE_PATH)

def _save_state() -> None:
    _STATE_DEBOUNCER.mark_dirty()

_load_state()
ROLES_STORE.load()  # <-- загрузили (или мигрировали) роли отдельно

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

# ---- РОЛИ (через RolesStore) ----
def get_user_role(user_id: int) -> Optional[str]:
    return ROLES_STORE.get(user_id)

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

def is_operator(user_id: int) -> bool:
    return get_user_role(user_id) == "operator"

def is_driver(user_id: int) -> bool:
    return get_user_role(user_id) == "driver"

def is_staff(user_id: int) -> bool:
    return is_admin(user_id) or is_operator(user_id)

def set_user_role(user_id: int, role: str) -> None:
    ROLES_STORE.set(user_id, role)

def all_operator_ids() -> List[int]:
    return ROLES_STORE.all_operators()

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
        "tasks": tasks[:],  # может быть ["custom"]
        "status": "new",
        "operator_user_id": None,
        "operator_comment": "",
        "created_at": now_iso,
        "accepted_at": None,
        "closed_at": None,
        "updated_at": now_iso,
        "admin_message_ids": [],
        "thread_id": None,
        "thread_message_id": None,       # якорь: описание (закреп)
        "thread_wait_message_id": None,  # «Ожидалка»
    }
    _save_state()
    return rid

def save_request_message_ids(request_id: int, admin_msg_ids: Optional[Dict[int, int]] = None, thread_msg_id: Optional[int] = None, thread_wait_message_id: Optional[int] = None):
    req = REQUESTS.get(request_id)
    if not req:
        return
    if admin_msg_ids is not None:
        req["admin_message_ids"] = [{"admin_id": k, "message_id": v} for k, v in admin_msg_ids.items()]
    if thread_msg_id is not None:
        req["thread_message_id"] = thread_msg_id
    if thread_wait_message_id is not None:
        req["thread_wait_message_id"] = thread_wait_message_id
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
    s = (raw or "").strip()
    if not s:
        return "-"
    m = re.fullmatch(r"([A-Za-z]{3})-?(\d{3,10})", s)
    if not m:
        return s[:100]
    return f"{m.group(1).upper()}-{m.group(2)}"

# ==========================================================
# ФОРМАТЫ
# ==========================================================
def tasks_human_readable(codes: List[str]) -> str:
    mapping = {code: text for (text, code) in TASK_CHOICES}
    mapping["custom"] = "Нестандартная задача"
    return "; ".join(mapping.get(c, c) for c in codes) if codes else "—"

def vehicle_bort(vehicle_type: str, vehicle_number: str) -> str:
    number = (vehicle_number or "").strip()
    if not number:
        return "—"
    if vehicle_type == "Kia Ceed":
        return f"kc2-{number}"
    if vehicle_type == "Sitrak":
        return f"st-{number}"
    return f"{vehicle_type} {number}"

def _parse_iso(iso_str: Optional[str]) -> Optional[datetime]:
    if not iso_str:
        return None
    try:
        dt = datetime.fromisoformat(iso_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def _to_local(dt_utc: Optional[datetime]):
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

    task_code = (req.get("task_code") or "").strip() or "-"
    bort = html_escape(vehicle_bort(req.get("vehicle_type",""), req.get("vehicle_number","")))
    tasks_line = html_escape(tasks_human_readable(req.get("tasks", [])))

    return (
        f"<b>Заявка от {dt_local}</b>\n"
        f"Задача в Jira: <b>{html_escape(task_code)}</b>\n"
        f"ВАТС: <b>{bort}</b>\n"
        f"Водитель: {mention}" + (f" (@{username})" if username else "") + "\n"
        f"Задачи: <b>{tasks_line}</b>"
    )

def _esc(s: Optional[str]) -> str:
    return html_escape("" if s is None else str(s), quote=False)

# ====== вычисление времени решения (accepted -> closed) ======
def _resolution_minutes(accepted_iso: Optional[str], closed_iso: Optional[str]) -> Optional[int]:
    if not (accepted_iso and closed_iso):
        return None
    try:
        accepted = datetime.fromisoformat(accepted_iso)
        closed = datetime.fromisoformat(closed_iso)
        delta = closed - accepted
        if delta.total_seconds() < 0:
            return None
        return int(delta.total_seconds() // 60)
    except Exception:
        return None

BULLET_TEXTS: Dict[str, str] = {
    "emergency_brake": 'команда "Emergency Brake" от телеоператора приводит к экстренной остановке со значительным ускорением торможения, после отключения "Emergency Brake" телеоператором, ВАТС продолжает движение;',
    "safe_brake": 'команда "Safe Brake" от телеоператора приводит к плавной остановке, после отключения "Safe Brake" телеоператором, ВАТС продолжает движение;',
    "teleop_path": 'ВАТС проезжает по траектории, построенной телеоператором, останавливается в конце нарисованной дороги и начинает движение после удаления телеоператором нарисованной дороги.',
}

def _punctuate_bullet(text: str, is_last: bool) -> str:
    t = (text or "").strip()
    if is_last:
        t = t.rstrip(";").rstrip(".") + "."
    else:
        t = t.rstrip(".").rstrip(";") + ";"
    return t

# --------- Deeplink helper (предвычисленный внутренний id) ----------
if str(THREADS_CHAT_ID).startswith("-100"):
    _THREADS_INTERNAL_ID = str(THREADS_CHAT_ID)[4:]
else:
    _THREADS_INTERNAL_ID = str(abs(THREADS_CHAT_ID))

def _topic_message_link(msg_id: int) -> Optional[str]:
    if not msg_id:
        return None
    return f"https://t.me/c/{_THREADS_INTERNAL_ID}/{msg_id}"

def _topic_link_for_req(req: Dict) -> Optional[str]:
    return _topic_message_link(req.get("thread_message_id") or 0)

def _build_description(req: Dict) -> str:
    """Формируем тело «Описание» для отчёта, с учётом custom."""
    task_code = (req.get("task_code") or "-").strip()
    tasks = req.get("tasks", []) or []
    comment = (req.get("operator_comment") or "").strip()

    if "custom" in tasks:
        link = _topic_link_for_req(req)
        text = "Нестандартная задача (описание в топике)"
        first_line = f'<a href="{html_escape(link)}">{html_escape(text)}</a>' if link else text
        lines = [first_line]
        if comment:
            lines.append(comment)
        return "\n".join(lines)

    lines = []
    if task_code != "-":
        lines.append(f"Задача {task_code}.")
    checks = [c for c in tasks if BULLET_TEXTS.get(c)]
    if checks:
        lines.append("Выполнили проверку следующих команд:")
        last_idx = len(checks) - 1
        for idx, code in enumerate(checks):
            raw = BULLET_TEXTS[code]
            lines.append(f"- {_punctuate_bullet(raw, is_last=(idx == last_idx))}")
    if comment:
        lines.append(comment)
    return "\n".join(lines)

def report_text(req: Dict) -> str:
    created_iso = req.get("created_at")
    date_line = _fmt_date_from_iso(created_iso)
    time_line = _fmt_hhmm_from_iso(created_iso)
    vts = vehicle_bort(req.get("vehicle_type",""), req.get("vehicle_number",""))
    mins = _resolution_minutes(req.get("accepted_at"), req.get("closed_at"))
    solve = "-" if mins is None else ("<1 мин" if mins < 1 else f"~{mins} мин")
    descr_block = _build_description(req)
    return (
        f"Отчет от <code>{_esc(date_line)}</code>\n"
        f"Время: <code>{_esc(time_line)}</code>\n"
        f"ВАТС: <code>{_esc(vts)}</code>\n"
        f"Описание: {descr_block}\n"
        f"Время решения: <code>{_esc(solve)}</code>"
    )

# ==== Клавиатуры ====
SKIP_TASK_CODE_KB = InlineKeyboardMarkup([[InlineKeyboardButton("Не указывать", callback_data="skip_task_code")]])
VEHICLE_TYPE_KB = InlineKeyboardMarkup([
    [InlineKeyboardButton("Kia Ceed", callback_data="vehicle:Kia Ceed"),
     InlineKeyboardButton("Sitrak",  callback_data="vehicle:Sitrak")],
    [InlineKeyboardButton("⬅️ Назад", callback_data="back_to:task_code")],
])
TASKS_CHOICE_KB = InlineKeyboardMarkup([
    [InlineKeyboardButton("✅ Стандартные проверки", callback_data="tasks_preset")],
    [InlineKeyboardButton("📝 Выбрать вручную", callback_data="tasks_manual")],
    [InlineKeyboardButton("🧩 Нестандартная задача", callback_data="tasks_custom")],
    [InlineKeyboardButton("⬅️ Назад", callback_data="back_to:vehicle_number")],
])

ROLE_PICK_KB = InlineKeyboardMarkup([
    [InlineKeyboardButton("🚗 Водитель", callback_data="set_role:driver")],
    [InlineKeyboardButton("🧑‍💻 Телеоператор", callback_data="set_role:operator")],
])

def back_keyboard(target_stage: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data=f"back_to:{target_stage}")]])

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

def operator_claim_keyboard(request_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🧑‍💻 Подключиться", callback_data=f"op_claim:{request_id}")]])

def _driver_open_url_keyboard(url: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("👨‍💼 Перейти к диалогу с оператором", url=url)]])

# ==========================================================
# ТОПИКИ (FORUM TOPICS)
# ==========================================================
async def ensure_forum_topic(context: ContextTypes.DEFAULT_TYPE, req: Dict) -> Optional[int]:
    if req.get("thread_id"):
        return req["thread_id"]
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
        )
        req["thread_message_id"] = m.message_id  # якорь — описание
        req["updated_at"] = datetime.now(timezone.utc).isoformat()
        _save_state()
        logging.info("Intro posted in thread %s (msg_id=%s)", req["thread_id"], m.message_id)
        return m.message_id
    except Exception as e:
        logging.exception("post_intro_in_topic failed", exc_info=e)
        return None

async def post_waiting_in_topic(context: ContextTypes.DEFAULT_TYPE, req: Dict) -> Optional[int]:
    if not req.get("thread_id"):
        return None
    try:
        m = await context.bot.send_message(
            chat_id=THREADS_CHAT_ID,
            message_thread_id=req["thread_id"],
            text="⏳ Ожидайте подключения оператора",
        )
        req["thread_wait_message_id"] = m.message_id
        req["updated_at"] = datetime.now(timezone.utc).isoformat()
        _save_state()
        logging.info("Waiting message posted in thread %s (msg_id=%s)", req["thread_id"], m.message_id)
        return m.message_id
    except Exception as e:
        logging.exception("post_waiting_in_topic failed", exc_info=e)
        return None

async def _close_forum_topic_if_any(context: ContextTypes.DEFAULT_TYPE, req: Dict):
    try:
        tid = req.get("thread_id")
        if tid:
            await context.bot.close_forum_topic(chat_id=THREADS_CHAT_ID, message_thread_id=tid)
            logging.info("Forum topic closed: thread_id=%s for request #%s", tid, req["id"])
    except Exception as e:
        logging.exception("close_forum_topic failed", exc_info=e)

# ==========================================================
# ЭКРАНЫ (СТАДИИ) + Выбор роли
# ==========================================================
async def _ensure_role_or_ask(update_or_msg, context: ContextTypes.DEFAULT_TYPE) -> Optional[str]:
    """Проверяем роль. Если нет — предлагаем выбрать. Возвращаем роль или None (если спросили)."""
    u = update_or_msg.from_user
    role = get_user_role(u.id)
    if role:
        return role
    # админа не спрашиваем — роль идёт из ADMIN_IDS
    if is_admin(u.id):
        return "admin"
    # Просим выбрать роль
    await context.bot.send_message(
        chat_id=u.id,
        text="Выбери роль для работы с ботом:",
        reply_markup=ROLE_PICK_KB
    )
    return None

async def stage_task_code(message, context: ContextTypes.DEFAULT_TYPE):
    role = await _ensure_role_or_ask(message, context)
    if not role:
        return
    if role == "operator":
        await message.reply_text("У тебя роль телеоператора — создание заявок недоступно.")
        return
    context.user_data.setdefault("request", {"task_code":"","vehicle_type":"","vehicle_number":"","tasks":[]})
    context.user_data["await"] = "task_code"
    await message.reply_text("Привет! Укажи название задачи в Jira (например, drv12345)", reply_markup=SKIP_TASK_CODE_KB)

async def stage_vehicle_type(message, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["await"] = None
    await message.reply_text("Выбери тип ТС", reply_markup=VEHICLE_TYPE_KB)

async def stage_vehicle_number(message, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["await"] = "vehicle_number"
    await message.reply_text("Введи номер борта (только цифры)", reply_markup=back_keyboard("vehicle_type"))

async def stage_tasks(message, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["await"] = None
    await message.reply_text("Что будем проверять?", reply_markup=TASKS_CHOICE_KB)

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
            btn = InlineKeyboardMarkup([[InlineKeyboardButton("Открыть чат с ботом", url=f"https://t.me/{me.username}?start=start")]])
            await update.effective_message.reply_text(ENTRY_PROMPT, reply_markup=btn)
        return

    role = await _ensure_role_or_ask(update.effective_message, context)
    if not role:
        return
    if role == "operator":
        await update.effective_message.reply_text("Роль: телеоператор. Ожидайте вызовы и принимайте заявки из уведомлений.")
        return
    await stage_task_code(update.effective_message, context)

async def cmd_whoami(update: Update, context: ContextTypes.DEFAULT_TYPE):
    role = get_user_role(update.effective_user.id) or "—"
    await update.effective_message.reply_text(f"Твой Telegram ID: <code>{update.effective_user.id}</code>\nРоль: <b>{role}</b>")

async def cmd_setrole(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if is_admin(update.effective_user.id):
        await update.effective_message.reply_text("У тебя роль администратора и её нельзя сменить.")
        return
    await update.effective_message.reply_text("Выбери роль:", reply_markup=ROLE_PICK_KB)

async def cmd_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    thread_id = update.effective_message.message_thread_id
    text = f"ID этого чата: <code>{chat_id}</code>"
    if thread_id:
        text += f"\nID темы: <code>{thread_id}</code>"
    await update.effective_message.reply_text(text)

async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text("pong")

async def cmd_state(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(f"user_data: {dict(context.user_data)}")

# Диагностика: лёгкий лог
async def on_any_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if logging.getLogger().isEnabledFor(logging.DEBUG):
        try:
            d = str(update)
        except Exception:
            d = "<unrepr>"
        logging.debug("UPDATE INBOUND: %s", d)

# ==========================================================
# РОЛИ — обработчик кнопки
# ==========================================================
async def on_set_role(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if is_admin(query.from_user.id):
        set_user_role(query.from_user.id, "admin")
        await query.message.reply_text("Роль: администратор.")
        return
    data = query.data or ""
    _, _, role = data.partition(":")
    if role not in ("driver", "operator"):
        await query.answer("Некорректная роль.", show_alert=True); return
    set_user_role(query.from_user.id, role)
    await query.message.reply_text(f"Роль установлена: <b>{'водитель' if role=='driver' else 'телеоператор'}</b>.")

# ==========================================================
# ДИАЛОГ ВОДИТЕЛЯ
# ==========================================================
async def on_user_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat and update.effective_chat.type != "private":
        return

    role = get_user_role(update.effective_user.id)
    if not role:
        await _ensure_role_or_ask(update.effective_message, context)
        return

    # оператор не может создавать заявки
    if role == "operator":
        return

    # если админ/оператор пишет комментарий — этот обработчик не мешает
    if is_staff(update.effective_user.id) and PENDING_ADMIN_COMMENT.get(update.effective_user.id):
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
                "Формат задачи в Jira: три латинские буквы и 3–10 цифр. \n❗️Попробуйте ещё раз❗️",
                reply_markup=SKIP_TASK_CODE_KB
            )
            return
        context.user_data["request"]["task_code"] = normalize_task_code(msg) if msg else "-"
        await stage_vehicle_type(update.effective_message, context)
        return

    if step == "vehicle_number":
        if not msg or not NUM_RE.match(msg):
            await update.effective_message.reply_text(
                "Номер борта должен содержать только цифры (например, 030). Попробуйте ещё раз!",
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
# САБМИТ ЗАЯВКИ + РАССЫЛКА ОПЕРАТОРАМ/АДМИНАМ
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

    # Создаём тему
    thread_id = await ensure_forum_topic(context, req)

    # 1) Публикуем описание заявки и закрепляем
    thread_msg_id = None
    if thread_id:
        summary_mid = await post_intro_in_topic(context, req, summary)
        thread_msg_id = summary_mid
        if summary_mid:
            try:
                await context.bot.pin_chat_message(chat_id=THREADS_CHAT_ID, message_id=summary_mid, disable_notification=True)
                logging.info("Pinned summary message in thread %s (msg_id=%s)", thread_id, summary_mid)
            except Exception as e:
                logging.exception("pin_chat_message failed", exc_info=e)
        # 2) «Ожидалка»
        await post_waiting_in_topic(context, req)

    deep_link = _topic_message_link(thread_msg_id) if thread_msg_id else None

    # 3) Рассылка операторам и админам (конкурентно)
    admin_msg_ids: Dict[int, int] = {}
    bort_number = vehicle_bort(req.get("vehicle_type",""), req.get("vehicle_number",""))
    call_text = f"🚨 Для <code>{bort_number}</code> требуется оператор!"

    recipients = all_operator_ids()

    async def _send_to(aid: int):
        try:
            m = await context.bot.send_message(
                chat_id=aid,
                text=call_text,
                reply_markup=operator_claim_keyboard(request_id)
            )
            return aid, m.message_id
        except Exception as e:
            logging.exception("send CALL failed (id=%s)", aid, exc_info=e)
            return aid, None

    results = await asyncio.gather(*[_send_to(aid) for aid in recipients])
    for aid, mid in results:
        if mid:
            admin_msg_ids[aid] = mid

    save_request_message_ids(request_id, admin_msg_ids=admin_msg_ids, thread_msg_id=thread_msg_id)

    # 4) Ответ водителю: всегда отправляем summary + кнопку на тему
    await context.bot.send_message(chat_id=query.from_user.id, text="✅ Заявка создана")
    if deep_link:
        await context.bot.send_message(
            chat_id=query.from_user.id,
            text=summary,
            reply_markup=_driver_open_url_keyboard(deep_link)
        )
    else:
        await context.bot.send_message(chat_id=query.from_user.id, text=summary)

    # сброс локального состояния
    context.user_data.pop("request", None)
    context.user_data["await"] = None

# ==========================================================
# ОБРАБОТКА «ПОДКЛЮЧИТЬСЯ»
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

    if not is_staff(query.from_user.id):
        await query.answer("Недостаточно прав.", show_alert=True); return

    req = load_request(req_id)
    if not req:
        await query.answer("Заявка не найдена.", show_alert=True); return

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

    mark_accepted(req_id, query.from_user.id)

    # Обновляем тему: удаляем «ожидалку», пишем «оператор принял…»
    try:
        wait_mid = req.get("thread_wait_message_id")
        if wait_mid:
            try:
                await context.bot.delete_message(chat_id=THREADS_CHAT_ID, message_id=wait_mid)
            except Exception:
                pass
            req["thread_wait_message_id"] = None
        if req.get("thread_id"):
            op_mention = mention_html(query.from_user.id, (query.from_user.full_name or "Оператор"))
            await context.bot.send_message(
                chat_id=THREADS_CHAT_ID,
                message_thread_id=req["thread_id"],
                text=f"✅ Оператор принял заявку: {op_mention}",
            )
            req["updated_at"] = datetime.now(timezone.utc).isoformat()
            _save_state()
    except Exception as e:
        logging.exception("update topic accept message failed", exc_info=e)

    # Панель оператору
    summary = request_summary_text(req)
    deep_link = _topic_message_link(req.get("thread_message_id") or 0)  # якорь = закреплённое описание
    kb_admin = operator_controls_keyboard(req_id, req["status"], deep_link=deep_link)

    try:
        await query.message.edit_text(summary, reply_markup=kb_admin)
    except Exception:
        try:
            await context.bot.send_message(chat_id=query.from_user.id, text=summary, reply_markup=kb_admin)
        except Exception as e:
            logging.exception("send operator panel failed", exc_info=e)

    # Удаляем уведомления у остальных — конкурентно
    others = [
        (pair.get("admin_id"), pair.get("message_id"))
        for pair in req.get("admin_message_ids", [])
        if pair.get("admin_id") and pair.get("message_id") and pair.get("admin_id") != query.from_user.id
    ]
    async def _del(aid, mid):
        try:
            await context.bot.delete_message(chat_id=aid, message_id=mid)
        except Exception:
            pass
    await asyncio.gather(*[_del(aid, mid) for aid, mid in others])

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
# ОПЕРАТОР (статус, закрытие, комментарий) + ОТЧЁТ
# ==========================================================
async def _refresh_operator_keyboards(context: ContextTypes.DEFAULT_TYPE, req: Dict, query: Optional["telegram.CallbackQuery"] = None):
    deep_link = _topic_message_link(req.get("thread_message_id") or 0)
    kb = operator_controls_keyboard(req["id"], req["status"], deep_link=deep_link)
    if query:
        try:
            await query.edit_message_reply_markup(reply_markup=kb)
        except Exception as e:
            logging.debug("edit clicked message kb failed: %s", e)

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
    if not is_staff(query.from_user.id):
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
        )
    except Exception as e:
        logging.exception("send report to topic failed", exc_info=e)

async def on_operator_close(update: Update, Context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query; await query.answer()
    data = query.data or ""
    logging.info("CALLBACK DATA (op_close): %r", data)
    if not is_staff(query.from_user.id):
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
    await query.message.reply_text("Закрыть заявку. Добавить комментарий?", reply_markup=kb)

async def on_operator_comment_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query; await query.answer()
    data = query.data or ""
    logging.info("CALLBACK DATA (op_comment_*): %r", data)
    if not is_staff(query.from_user.id):
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
            await _close_forum_topic_if_any(context, req)

        PENDING_ADMIN_COMMENT.pop(query.from_user.id, None)
        _save_state()
        await query.message.reply_text("Заявка закрыта. Отчёт опубликован в теме.")

async def on_staff_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Текст оператора/админа — это комментарий к закрытию, если он ожидается."""
    if update.effective_chat and update.effective_chat.type != "private":
        return
    user = update.effective_user
    if not is_staff(user.id):
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
        await _close_forum_topic_if_any(context, req)

    PENDING_ADMIN_COMMENT.pop(user.id, None)
    _save_state()
    await update.effective_message.reply_text("Заявка закрыта. Отчёт опубликован в теме.")

# ==========================================================
# ОБРАБОТЧИКИ КНОПОК ТС/ЗАДАЧ
# ==========================================================
async def on_vehicle_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    # только не-операторы (водители или админы) могут продолжать создавать заявки
    if is_operator(query.from_user.id) and not is_admin(query.from_user.id):
        await query.answer("Создание заявок недоступно для телеоператоров.", show_alert=True)
        return
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

    # ограничение для операторов
    if is_operator(query.from_user.id) and not is_admin(query.from_user.id):
        await query.answer("Создание заявок недоступно для телеоператоров.", show_alert=True)
        return

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

    if data == "tasks_custom":
        context.user_data.setdefault("request", {})
        context.user_data["request"]["tasks"] = ["custom"]
        await _submit_request_and_notify(query, context)
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
async def on_set_role_misc(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

    persistence = PicklePersistence(filepath="ptb_persistence.pkl", update_interval=30)
    defaults = Defaults(parse_mode=ParseMode.HTML, disable_web_page_preview=True)

    app = Application.builder().token(BOT_TOKEN).post_init(_post_init).persistence(persistence).defaults(defaults).build()

    # Команды
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("whoami", cmd_whoami))
    app.add_handler(CommandHandler("setrole", cmd_setrole))
    app.add_handler(CommandHandler("id", cmd_id))
    app.add_handler(CommandHandler("ping", cmd_ping))
    app.add_handler(CommandHandler("state", cmd_state))

    # Диагностика
    app.add_handler(TypeHandler(Update, on_any_update), group=-100)

    # ТЕКСТЫ: ЛС для водителя/админа (создание), и текст-ответ на комментарий оператора/админа
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND, on_user_text), group=0)
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND, on_staff_text), group=1)

    # Кнопки: выбор роли
    app.add_handler(CallbackQueryHandler(on_set_role, pattern=r"^set_role:(driver|operator)$"))

    # Кнопки пошагового мастера
    app.add_handler(CallbackQueryHandler(on_skip_task_code, pattern=r"^skip_task_code$"))
    app.add_handler(CallbackQueryHandler(on_vehicle_type, pattern=r"^vehicle:.+"))
    app.add_handler(CallbackQueryHandler(on_tasks_toggle, pattern=r"^(tasks_preset|tasks_manual|tasks_custom|task_toggle:.+|tasks_done|tasks_cancel)$"))
    app.add_handler(CallbackQueryHandler(on_back, pattern=r"^back_to:.+"))

    # «Подключиться»
    app.add_handler(CallbackQueryHandler(on_operator_claim, pattern=r"^op_claim:\d+$"))

    # Панель оператора
    app.add_handler(CallbackQueryHandler(on_operator_status, pattern=r"^op_status:\d+:(done|not_done)$"))
    app.add_handler(CallbackQueryHandler(on_operator_close, pattern=r"^op_close:\d+$"))
    app.add_handler(CallbackQueryHandler(on_operator_comment_choice, pattern=r"^op_comment_(yes|no):\d+$"))

    app.add_handler(CallbackQueryHandler(on_set_role_misc))

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
