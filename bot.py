import os
import re
import logging
from datetime import datetime
from typing import List, Dict, Optional
from pathlib import Path

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

# супергруппа с включёнными ТЕМАМИ (Forum)
THREADS_CHAT_ID = int(os.getenv("THREADS_CHAT_ID", "0") or "0")
# входной топик "Использовать бота" — id темы
ENTRY_THREAD_ID = int(os.getenv("ENTRY_THREAD_ID", "0") or "0")

print(">>> ENV CHECK:",
      "BOT_TOKEN set" if bool(BOT_TOKEN) else "BOT_TOKEN MISSING",
      f"ADMIN_IDS={sorted(ADMIN_IDS)}",
      f"THREADS_CHAT_ID={THREADS_CHAT_ID}",
      f"ENTRY_THREAD_ID={ENTRY_THREAD_ID}",
      sep=" | ", flush=True)

if not BOT_TOKEN:
    raise RuntimeError("Не задан BOT_TOKEN в .env")
if not ADMIN_IDS:
    raise RuntimeError("Не заданы ADMIN_USER_IDS в .env (список ID через запятую)")
if not THREADS_CHAT_ID:
    raise RuntimeError("Не задан THREADS_CHAT_ID — это супергруппа с включёнными Темами (Forum)")
if not ENTRY_THREAD_ID:
    raise RuntimeError("Не задан ENTRY_THREAD_ID — это id темы «Использовать бота» в вашей супергруппе")

VEHICLE_TYPES = ["Kia Ceed", "Sitrak"]

# список пар (текст, код)
TASK_CHOICES = [
    ("Команда \"Emergency Brake\" с последующим ее отключением", "emergency_brake"),
    ("Команда \"Safe Brake\" с последующим ее отключением", "safe_brake"),
    ("Построение траектории телеоператором", "teleop_path"),
]

# Пресет «стандартных проверок»
PRESET_TASKS = ["emergency_brake", "safe_brake"]

# Регулярки (на будущее; сейчас принимаем любой текст)
DRV_RE = re.compile(r"^[A-Za-z]{3}\d{3,10}$")

# ==========================================================
# ПАМЯТЬ В ОЗУ (без БД)
# ==========================================================
DRIVERS: Dict[int, Dict] = {}        # кто видел бота
NEXT_REQUEST_ID = 1
REQUESTS: Dict[int, Dict] = {}
PENDING_ADMIN_COMMENT: Dict[int, int] = {}

BOT_USERNAME = None  # заполним в post_init

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

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

def new_request_id() -> int:
    global NEXT_REQUEST_ID
    rid = NEXT_REQUEST_ID
    NEXT_REQUEST_ID += 1
    return rid

def create_request(driver_user_id: int, task_code: str, vehicle_type: str,
                   vehicle_number: str, tasks: List[str]) -> int:
    rid = new_request_id()
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
        "created_at": datetime.utcnow().isoformat(),
        "updated_at": datetime.utcnow().isoformat(),
        # рассылки
        "admin_message_ids": [],       # список {admin_id, message_id}
        # форумная тема заявки
        "thread_id": None,             # message_thread_id темы
        "thread_message_id": None,     # id первого сообщения в теме
    }
    return rid

def save_request_message_ids(request_id: int, admin_msg_ids: Optional[Dict[int, int]] = None, thread_msg_id: Optional[int] = None):
    req = REQUESTS.get(request_id)
    if not req:
        return
    if admin_msg_ids is not None:
        req["admin_message_ids"] = [{"admin_id": k, "message_id": v} for k, v in admin_msg_ids.items()]
    if thread_msg_id is not None:
        req["thread_message_id"] = thread_msg_id
    req["updated_at"] = datetime.utcnow().isoformat()

def set_request_status(request_id: int, status: str, operator_user_id: Optional[int]):
    req = REQUESTS.get(request_id)
    if not req: return
    req["status"] = status
    if operator_user_id is not None:
        req["operator_user_id"] = operator_user_id
    req["updated_at"] = datetime.utcnow().isoformat()

def set_request_comment(request_id: int, operator_user_id: int, comment: Optional[str]):
    req = REQUESTS.get(request_id)
    if not req: return
    req["operator_comment"] = comment or ""
    req["operator_user_id"] = operator_user_id
    req["updated_at"] = datetime.utcnow().isoformat()

def load_request(request_id: int) -> Optional[Dict]:
    return REQUESTS.get(request_id)

def load_driver(user_id: int) -> Optional[Dict]:
    return DRIVERS.get(user_id)

# ==========================================================
# ФОРМАТЫ
# ==========================================================
def tasks_human_readable(codes: List[str]) -> str:
    mapping = {code: text for (text, code) in TASK_CHOICES}
    return ", ".join(mapping.get(c, c) for c in codes)

def request_summary_text(req: Dict) -> str:
    driver = load_driver(req["driver_user_id"]) or {}
    tasks_codes = req.get("tasks", [])
    driver_name = (driver.get("first_name") or "").strip()
    if driver.get("last_name"):
        driver_name = (driver_name + " " + driver.get("last_name")).strip()
    username = driver.get("username") or ""
    mention = mention_html(driver.get("user_id", 0), driver_name or "Водитель")
    return (
        "<b>Новая заявка</b>\n"
        f"Задача: <code>{req['task_code']}</code>\n"
        f"Номер ТС: <b>{req['vehicle_number']}</b>\n"
        f"Тип ТС: <b>{req['vehicle_type']}</b>\n"
        f"Водитель: {mention}" + (f" (@{username})" if username else "") + "\n"
        f"Задачи: {tasks_human_readable(tasks_codes)}\n"
        f"ID заявки: <code>{req['id']}</code>"
    )

def report_text(req: Dict) -> str:
    driver = load_driver(req["driver_user_id"]) or {}
    tasks_codes = req.get("tasks", [])
    driver_name = (driver.get("first_name") or "").strip()
    if driver.get("last_name"):
        driver_name = (driver_name + " " + driver.get("last_name")).strip()
    uname = driver.get("username") or ""
    dt = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    status_map = {"done":"Выполнена","not_done":"Не выполнена","closed":"Закрыта","new":"Новая"}
    status_h = status_map.get(req["status"], req["status"])
    comment = (req.get("operator_comment") or "").strip() or "—"
    return (
        f"┌──────── ОТЧЁТ ПО ЗАЯВКЕ #{req['id']} ────────\n"
        f"│ Дата: {dt}\n"
        f"│ Задача: {req['task_code']}\n"
        f"│ ТС: {req['vehicle_type']} {req['vehicle_number']}\n"
        f"│ Водитель: {driver_name}" + (f" (@{uname})" if uname else "") + f", id {driver.get('user_id','—')}\n"
        f"│ Задачи: {tasks_human_readable(tasks_codes)}\n"
        f"│ Статус: {status_h}\n"
        f"│ Комментарий оператора: {comment}\n"
        f"└──────────────────────────────────────────────"
    )

def start_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("📞 Вызвать оператора", callback_data="call_operator")]])

def back_keyboard(target_stage: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️ Назад", callback_data=f"back_to:{target_stage}")]
    ])

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

def confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🚀 Отправить заявку", callback_data="submit_request")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back_to:tasks")],
    ])

def operator_controls_keyboard(request_id: int, current_status: str) -> InlineKeyboardMarkup:
    status_line = {"done":"✅ Задача выполнена","not_done":"❌ Задача не выполнена","new":"⏳ Ожидает выполнения","closed":"🔒 Закрыта"}.get(current_status,current_status)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Задача выполнена", callback_data=f"op_status:{request_id}:done")],
        [InlineKeyboardButton("❌ Задача не выполнена", callback_data=f"op_status:{request_id}:not_done")],
        [InlineKeyboardButton("🧾 Закрыть",            callback_data=f"op_close:{request_id}")],
        [InlineKeyboardButton("👤 Связаться с водителем", callback_data=f"op_join:{request_id}")],
        [InlineKeyboardButton(f"ℹ️ Текущий статус: {status_line}", callback_data="noop")],
    ])

# ==========================================================
# ТОПИКИ (FORUM TOPICS)
# ==========================================================
async def ensure_forum_topic(context: ContextTypes.DEFAULT_TYPE, req: Dict) -> Optional[int]:
    if req.get("thread_id"):
        return req["thread_id"]
    title = f"Заявка #{req['id']} — {req.get('vehicle_type','?')} {req.get('vehicle_number','?')}"
    try:
        topic = await context.bot.create_forum_topic(chat_id=THREADS_CHAT_ID, name=title)
        thread_id = topic.message_thread_id
        req["thread_id"] = thread_id
        req["updated_at"] = datetime.utcnow().isoformat()
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
        req["updated_at"] = datetime.utcnow().isoformat()
        logging.info("Intro posted in thread %s (msg_id=%s)", req["thread_id"], m.message_id)
        return m.message_id
    except Exception as e:
        logging.exception("post_intro_in_topic failed", exc_info=e)
        return None

def _public_cid(chat_id: int) -> str:
    s = str(chat_id)
    return s[4:] if s.startswith("-100") else str(abs(chat_id))

def topic_message_url(chat_id: int, message_id: int) -> str:
    # Прямая ссылка на сообщение в теме супергруппы
    return f"https://t.me/c/{_public_cid(chat_id)}/{message_id}"

# ==========================================================
# ЭКРАНЫ (СТАДИИ)
# ==========================================================
async def stage_start(message, context: ContextTypes.DEFAULT_TYPE):
    set_driver_seen(message.from_user)
    context.user_data["await"] = None
    await message.reply_text(
        "Готов к работе — нажми кнопку ниже!",
        reply_markup=start_keyboard(),
        parse_mode=ParseMode.HTML
    )

async def stage_task_code(message, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.setdefault("request", {"task_code":"","vehicle_type":"","vehicle_number":"","tasks":[]})
    context.user_data["await"] = "task_code"
    await message.reply_text(
        "Пожалуйста, укажи номер задачи (в формате DRV-*****)",
        reply_markup=back_keyboard("start")
    )

async def stage_vehicle_type(message, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["await"] = None
    await message.reply_text("Выбери тип ТС:", reply_markup=vehicle_type_keyboard())

async def stage_vehicle_number(message, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["await"] = "vehicle_number"
    await message.reply_text("Введи номер/идентификатора ТС (любой текст).", reply_markup=back_keyboard("vehicle_type"))

async def stage_tasks(message, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["await"] = None
    await message.reply_text("Как задать набор задач?", reply_markup=tasks_choice_keyboard())

async def stage_confirm(message, context: ContextTypes.DEFAULT_TYPE):
    req_local = context.user_data.get("request", {})
    preview = request_summary_text({
        "driver_user_id": message.from_user.id,
        "task_code": req_local.get("task_code", "—"),
        "vehicle_type": req_local.get("vehicle_type", "—"),
        "vehicle_number": req_local.get("vehicle_number", "—"),
        "tasks": req_local.get("tasks", []),
        "id": 0,  # черновик
    })
    await message.reply_text("Проверь данные и отправь заявку:", parse_mode=ParseMode.HTML)
    await message.reply_text(preview, parse_mode=ParseMode.HTML, reply_markup=confirm_keyboard())

# ==========================================================
# КОМАНДЫ
# ==========================================================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Если /start в ЛС — запускаем мастер
    if update.effective_chat and update.effective_chat.type == "private":
        await stage_start(update.effective_message, context)
        return

    # Если /start в нашей супергруппе и именно в "входном" топике — молчим (чтобы не было дублей)
    if (update.effective_chat and update.effective_chat.id == THREADS_CHAT_ID
            and update.effective_message and update.effective_message.message_thread_id == ENTRY_THREAD_ID):
        return

    # Иначе — молчим
    return

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
# ВХОДНОЙ ТОПИК: любое сообщение → кнопка «Открыть чат с ботом»
# ==========================================================
async def on_entry_topic_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    chat = update.effective_chat
    if not msg or not chat:
        return
    if chat.id != THREADS_CHAT_ID:
        return
    if getattr(msg, "message_thread_id", None) != ENTRY_THREAD_ID:
        return
    if msg.from_user and msg.from_user.is_bot:
        return

    me = context.bot_data.get("bot_username") or BOT_USERNAME or ""
    url = f"https://t.me/{me}?start=go" if me else "https://t.me"
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🤖 Открыть чат с ботом", url=url)]])
    await msg.reply_text(
        "Для оформления заявки перейди в личные сообщения с ботом:",
        reply_markup=kb
    )

# ==========================================================
# ДИАЛОГ ВОДИТЕЛЯ (в ЛС)
# ==========================================================
async def on_call_operator(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data["request"] = {"task_code":"","vehicle_type":"","vehicle_number":"","tasks":[]}
    await stage_task_code(query.message, context)

async def on_user_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Работает только в ЛС
    if update.effective_chat and update.effective_chat.type != "private":
        return

    msg_text = (update.effective_message.text or "").strip() if update.effective_message else ""
    logging.info("on_user_text <- %r | await=%r", msg_text, context.user_data.get("await"))

    if "await" not in context.user_data or context.user_data.get("await") is None:
        await update.effective_message.reply_text("Нажми «Вызвать оператора» и следуй шагам 🙂", reply_markup=start_keyboard())
        return

    step = context.user_data.get("await")
    msg = msg_text

    if step == "task_code":
        if not msg:
            await stage_task_code(update.effective_message, context)
            return
        context.user_data["request"]["task_code"] = msg[:100]
        await stage_vehicle_type(update.effective_message, context)
        return

    if step == "vehicle_number":
        if not msg:
            await stage_vehicle_number(update.effective_message, context)
            return
        context.user_data["request"]["vehicle_number"] = msg[:50]
        await stage_tasks(update.effective_message, context)
        return

# выбор типа ТС (кнопки vehicle:...)
async def on_vehicle_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = (query.data or "").strip()
    _, _, vtype = data.partition(":")
    if not vtype:
        await query.answer("Некорректные данные кнопки.", show_alert=True)
        return
    context.user_data.setdefault("request", {"task_code":"","vehicle_type":"","vehicle_number":"","tasks":[]})
    context.user_data["request"]["vehicle_type"] = vtype
    await stage_vehicle_number(query.message, context)

# выбор/подтверждение задач
async def on_tasks_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data or ""
    logging.info("CALLBACK DATA (tasks): %r", data)

    if data == "tasks_preset":
        context.user_data.setdefault("request", {})
        context.user_data["request"]["tasks"] = PRESET_TASKS.copy()
        await stage_confirm(query.message, context)
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
        await stage_confirm(query.message, context)
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

# финальная отправка заявки — показываем кнопку-ссылку «Перейти к диалогу с оператором»
async def on_submit_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()  # закрываем крутилку

    req_local = context.user_data.get("request", {})
    if not (req_local.get("task_code") and req_local.get("vehicle_type") and req_local.get("vehicle_number")):
        await query.answer("Не все поля заполнены.", show_alert=True); return

    request_id = create_request(
        driver_user_id=query.from_user.id,
        task_code=req_local["task_code"],
        vehicle_type=req_local["vehicle_type"],
        vehicle_number=req_local["vehicle_number"],
        tasks=req_local.get("tasks", []),
    )
    req = load_request(request_id)
    summary = request_summary_text(req)

    # Создаём тему и первый пост
    thread_id = await ensure_forum_topic(context, req)
    thread_msg_id = None
    if thread_id:
        thread_msg_id = await post_intro_in_topic(context, req, summary)

    # ЛС всем админам — карточка с управлением и кнопкой «Связаться с водителем»
    kb_admin = operator_controls_keyboard(request_id, req["status"])
    admin_msg_ids: Dict[int, int] = {}
    for admin_id in ADMIN_IDS:
        try:
            m = await context.bot.send_message(chat_id=admin_id, text=summary, parse_mode=ParseMode.HTML, reply_markup=kb_admin)
            admin_msg_ids[admin_id] = m.message_id
        except Exception as e:
            logging.exception("send to ADMIN failed (id=%s)", admin_id, exc_info=e)

    save_request_message_ids(request_id, admin_msg_ids=admin_msg_ids, thread_msg_id=thread_msg_id)

    # Сообщение водителю + КНОПКА "Перейти к диалогу с оператором"
    if thread_msg_id:
        url = topic_message_url(THREADS_CHAT_ID, thread_msg_id)
        await query.message.reply_text(
            "✅ Заявка создана, оператор вызван.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("👉 Перейти к диалогу с оператором", url=url)]
            ])
        )
    else:
        await query.message.reply_text(
            "✅ Заявка создана. Тема ещё готовится — попробуй открыть через несколько секунд: /start"
        )

    # очистка мастера
    context.user_data.pop("request", None); context.user_data["await"] = None

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
        "start":          stage_start,
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
# ОПЕРАТОР (статус, закрытие, комментарий)
# ==========================================================
async def _refresh_operator_keyboards(context: ContextTypes.DEFAULT_TYPE, req: Dict, query: Optional["telegram.CallbackQuery"] = None):
    kb = operator_controls_keyboard(req["id"], req["status"])
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
        except Exception as e:
            logging.debug("edit admin msg kb failed", exc_info=e)

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

async def on_operator_close(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✍️ Написать комментарий", callback_data=f"op_comment_yes:{req_id}")],
        [InlineKeyboardButton("➡️ Продолжить без комментариев", callback_data=f"op_comment_no:{req_id}")],
    ])
    await query.edit_message_reply_markup(reply_markup=None)
    await query.message.reply_text(f"Закрыть заявку #{req_id}. Добавить комментарий?", reply_markup=kb)

async def on_operator_comment_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query; await query.answer()
    data = query.data or ""
    logging.info("CALLBACK DATA (op_comment_*): %r", data)
    if not is_admin(query.from_user.id):
        await query.answer("Недостаточно прав.", show_alert=True); return

    if data.startswith("op_comment_yes:"):
        _, _, rest = data.partition(":")
        try:
            req_id = int(rest)
        except ValueError:
            await query.answer("Некорректный ID заявки.", show_alert=True); return
        PENDING_ADMIN_COMMENT[query.from_user.id] = req_id
        await query.message.reply_text("Напиши комментарий одним сообщением.")
        return

    if data.startswith("op_comment_no:"):
        _, _, rest = data.partition(":")
        try:
            req_id = int(rest)
        except ValueError:
            await query.answer("Некорректный ID заявки.", show_alert=True); return

        set_request_comment(req_id, query.from_user.id, "")
        set_request_status(req_id, "closed", query.from_user.id)

        req = load_request(req_id)
        if req:
            txt = report_text(req)
            await context.bot.send_message(chat_id=query.from_user.id, text=txt)

        await query.message.reply_text("Заявка закрыта без комментария. Отчёт отправлен.")

# Текст комментария (только в ЛС)
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
    req = load_request(req_id)
    if req:
        txt = report_text(req)
        await update.effective_message.reply_text(f"Отчёт сформирован:\n\n{txt}")
    PENDING_ADMIN_COMMENT.pop(user.id, None)

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
        # запомним username для диплинков
        me = await app.bot.get_me()
        global BOT_USERNAME
        BOT_USERNAME = me.username
        app.bot_data["bot_username"] = me.username
        try:
            await app.bot.delete_webhook(drop_pending_updates=True)
            logging.info("Webhook deleted (if any), pending updates dropped.")
        except Exception as e:
            logging.warning(f"delete_webhook failed: {e}")
        logging.info("Logged in as @%s (id=%s)", me.username, me.id)

    app = Application.builder().token(BOT_TOKEN).post_init(_post_init).build()

    # Команды
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("whoami", cmd_whoami))
    app.add_handler(CommandHandler("id", cmd_id))
    app.add_handler(CommandHandler("ping", cmd_ping))
    app.add_handler(CommandHandler("state", cmd_state))

    # Диагностика
    app.add_handler(TypeHandler(Update, on_any_update), group=-100)

    # Сообщения во "входном" топике — ловим все и фильтруем внутри
    app.add_handler(MessageHandler(filters.ALL, on_entry_topic_message), group=-90)

    # ЛС диалог
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND, on_user_text), group=0)
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND, on_admin_text), group=1)

    # Кнопки в ЛС
    app.add_handler(CallbackQueryHandler(on_call_operator, pattern=r"^call_operator$"))
    app.add_handler(CallbackQueryHandler(on_tasks_toggle, pattern=r"^(tasks_preset|tasks_manual|task_toggle:.+|tasks_done|tasks_cancel)$"))
    app.add_handler(CallbackQueryHandler(on_back, pattern=r"^back_to:.+"))
    app.add_handler(CallbackQueryHandler(on_vehicle_type, pattern=r"^vehicle:.+"))
    app.add_handler(CallbackQueryHandler(on_submit_request, pattern=r"^submit_request$"))

    # Оператор
    app.add_handler(CallbackQueryHandler(on_operator_status, pattern=r"^op_status:\d+:(done|not_done)$"))
    app.add_handler(CallbackQueryHandler(on_operator_close, pattern=r"^op_close:\d+$"))
    app.add_handler(CallbackQueryHandler(on_operator_comment_choice, pattern=r"^op_comment_(yes|no):\d+$"))

    # Прочее
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


