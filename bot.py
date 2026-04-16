from __future__ import annotations
import asyncio
import sys

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  
import hashlib
import html
import io
import json
import logging
import math
import os
import re
import time
from collections import OrderedDict
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Awaitable, Callable, Dict, List, Optional, Sequence, Tuple, TypeVar

import aiohttp
from dotenv import load_dotenv
from PIL import Image, ImageDraw, ImageFont

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import (
    RestartingTelegram,
    TelegramBadRequest,
    TelegramNetworkError,
    TelegramRetryAfter,
    TelegramServerError,
)
from aiogram.filters import Command, CommandObject, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BufferedInputFile,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputSticker,
    Message,
)

from sticker_utils import (
    MAX_ANIMATED_STICKER_BYTES,
    build_tgs_preview_tile,
    customize_tgs_template,
    customize_tgs_template_with_secondary_text,
    extract_tgs_layout_info,
    get_font_path,
    get_font_presets,
    get_preview_render_dependency_error,
    get_svg_dependency_error,
    get_text_render_dependency_error,
    render_tgs_preview_image,
    validate_short_name,
    validate_svg_logo,
)

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
DB_PATH = "db.json"
SUPPORT_USERNAME = os.getenv("SUPPORT_USERNAME", "ecronx")
CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME", "emojiicreation")
CHANNEL_URL = os.getenv("CHANNEL_URL", f"https://t.me/{CHANNEL_USERNAME}")
PRIVACY_URL = os.getenv("PRIVACY_URL", "")
TERMS_URL = os.getenv("TERMS_URL", "")

PACK_NAME_MAX_LEN = 64
PACK_NAME_RETRY_LIMIT = 25
BOT_API_RETRY_LIMIT = 6
BOT_API_RETRY_BASE_DELAY = 1.0
BOT_API_RETRY_MAX_DELAY = 8.0
BOT_API_RETRY_AFTER_PAD = 0.5
TEMPLATE_COUNT_CACHE_TTL = 1800
STICKER_SET_CACHE_TTL = TEMPLATE_COUNT_CACHE_TTL
FILE_BYTES_CACHE_TTL = 3600
CUSTOMIZED_TGS_CACHE_TTL = 1800
PREVIEW_IMAGE_CACHE_TTL = 900
SELECTOR_PAGE_SIZE = 12
MULTI_SELECT_LIMIT = 9
DEFAULT_FONT_ID = "montserrat"
FILE_BYTES_CACHE_MAX = 512
LAYOUT_INFO_CACHE_MAX = 512
CUSTOMIZED_TGS_CACHE_MAX = 1024
PREVIEW_IMAGE_CACHE_MAX = 64
PREVIEW_TASK_CONCURRENCY = max(2, min(os.cpu_count() or 4, 8))
PACK_PREPARE_CONCURRENCY = max(4, min(os.cpu_count() or 4, 6))
RAW_API_CONNECTOR_LIMIT = max(12, PACK_PREPARE_CONCURRENCY * 3)
PREVIEW_PNG_COMPRESS_LEVEL = 1
PASSPORT_COLOR_HEX = "#FFFFFF"
PASSPORT_COLOR_LABEL = "Mono"
PASSPORT_PRICE_RUB = 100
CREATION_IS_FREE = True
CUSTOMIZATION_CACHE_VERSION = "logo_bounds_v4"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
log = logging.getLogger("EmojiCreationBot")


@dataclass(frozen=True)
class TemplateDefinition:
    key: str
    number: str
    title: str
    button_icon: str
    short_name: str
    example_url: str
    description: str


@dataclass(frozen=True)
class ColorDefinition:
    key: str
    label: str
    hex_value: str


TEMPLATES: List[TemplateDefinition] = [
    TemplateDefinition(
        key="black_hole",
        number="1",
        title="BLACK HOLE",
        button_icon="eye_off",
        short_name="main_by_emojicreationbot",
        example_url="https://t.me/addemoji/main_by_emojicreationbot",
        description="Полный набор из анимированных шаблонов для ника или бренда.",
    ),
    TemplateDefinition(
        key="color",
        number="2",
        title="COLOR",
        button_icon="brush",
        short_name="main2_by_emojimakers_bot",
        example_url="https://t.me/addemoji/main2_by_emojimakers_bot",
        description="Цветные шаблоны для яркого эмодзи-пака.",
    ),
    TemplateDefinition(
        key="exclusive",
        number="3",
        title="EXCLUSIVE",
        button_icon="gift",
        short_name="main2_by_emojimakers_bot",
        example_url="https://t.me/addemoji/main2_by_emojimakers_bot",
        description="Премиальная подборка шаблонов из эксклюзивной линейки.",
    ),
    TemplateDefinition(
        key="passport",
        number="4",
        title="PASSPORT",
        button_icon="file",
        short_name="main3_by_emojicreationbot",
        example_url="https://t.me/addemoji/main3_by_emojicreationbot",
        description="Мини-набор с паспортным стилем оформления.",
    ),
]
TEMPLATE_BY_KEY = {template.key: template for template in TEMPLATES}

COLOR_PRESETS: List[ColorDefinition] = [
    ColorDefinition("cyber", "Cyber", "#30D5FF"),
    ColorDefinition("blood", "Blood", "#FF3B5C"),
    ColorDefinition("nebula", "Nebula", "#8C62FF"),
    ColorDefinition("lava", "Lava", "#FF7A00"),
    ColorDefinition("forest", "Forest", "#29C970"),
    ColorDefinition("sunset", "Sunset", "#FFB55A"),
    ColorDefinition("ocean", "Ocean", "#4A90FF"),
    ColorDefinition("gold", "Gold", "#F3D16B"),
    ColorDefinition("mono", "Mono", "#F2F2F7"),
]
COLOR_BY_KEY = {color.key: color for color in COLOR_PRESETS}
FONT_PRESETS = get_font_presets()

PREMIUM_EMOJIS: Dict[str, Tuple[str, str]] = {
    "settings": ("5870982283724328568", "⚙️"),
    "profile": ("5870994129244131212", "👤"),
    "users": ("5870772616305839506", "👥"),
    "user_ok": ("5891207662678317861", "👤"),
    "user_x": ("5893192487324880883", "👤"),
    "file": ("5870528606328852614", "📁"),
    "smile": ("5870764288364252592", "🙂"),
    "chart_up": ("5870930636742595124", "📊"),
    "chart": ("5870921681735781843", "📊"),
    "home": ("5873147866364514353", "🏘"),
    "lock": ("6037249452824072506", "🔒"),
    "unlock": ("6037496202990194718", "🔓"),
    "megaphone": ("6039422865189638057", "📣"),
    "check": ("5870633910337015697", "✅"),
    "cross": ("5870657884844462243", "❌"),
    "pencil": ("5870676941614354370", "🖋"),
    "trash": ("5870875489362513438", "🗑"),
    "down": ("5893057118545646106", "📰"),
    "paperclip": ("6039451237743595514", "📎"),
    "link": ("5769289093221454192", "🔗"),
    "info": ("6028435952299413210", "ℹ"),
    "bot": ("6030400221232501136", "🤖"),
    "eye": ("6037397706505195857", "👁"),
    "eye_off": ("6037243349675544634", "👁"),
    "upload": ("5963103826075456248", "⬆"),
    "download": ("6039802767931871481", "⬇"),
    "bell": ("6039486778597970865", "🔔"),
    "gift": ("6032644646587338669", "🎁"),
    "clock": ("5983150113483134607", "⏰"),
    "party": ("6041731551845159060", "🎉"),
    "font": ("5870801517140775623", "🔤"),
    "write": ("5870753782874246579", "✍"),
    "media": ("6035128606563241721", "🖼"),
    "location": ("6042011682497106307", "📍"),
    "wallet": ("5769126056262898415", "👛"),
    "box": ("5884479287171485878", "📦"),
    "cryptobot": ("5260752406890711732", "👾"),
    "calendar": ("5890937706803894250", "📅"),
    "tag": ("5886285355279193209", "🏷"),
    "time_passed": ("5775896410780079073", "🕓"),
    "apps": ("5778672437122045013", "📦"),
    "brush": ("6050679691004612757", "🖌"),
    "text": ("5771851822897566479", "🔡"),
    "resize": ("5778479949572738874", "↔"),
    "money": ("5904462880941545555", "🪙"),
    "money_send": ("5890848474563352982", "🪙"),
    "money_receive": ("5879814368572478751", "🏧"),
    "code": ("5940433880585605708", "💻"),
    "loading": ("5345906554510012647", "🔄"),
}

PASSPORT_BACK_BUTTON_TEXT = "◁ Назад"
PASSPORT_SKIP_BUTTON_TEXT = "⏭️ Пропустить"
PASSPORT_PAY_BUTTON_TEXT = "💳 Оплатить {price}₽"
PASSPORT_TOP_UP_BUTTON_TEXT = "💰 Пополнить баланс"
PASSPORT_CANCEL_BUTTON_TEXT = "❌ Отмена"

FONT_ICON_KEYS: Dict[str, str] = {
    "montserrat": "font",
    "ballet": "write",
    "rubik_glitch": "code",
    "veles": "brush",
}

T = TypeVar("T")

_BOT_USERNAME_CACHE: Optional[str] = None
_TEMPLATE_COUNT_CACHE: Dict[str, Tuple[float, int]] = {}
_TEMPLATE_STICKER_SET_CACHE: "OrderedDict[str, Tuple[float, Any]]" = OrderedDict()
_FILE_BYTES_CACHE: "OrderedDict[str, Tuple[float, bytes]]" = OrderedDict()
_LAYOUT_INFO_CACHE: "OrderedDict[str, Tuple[float, Optional[Tuple[int, int, Tuple[float, float, float, float]]]]]" = OrderedDict()
_CUSTOMIZED_TGS_CACHE: "OrderedDict[Tuple[str, str, str, str, str, str, str], Tuple[float, bytes]]" = OrderedDict()
_PREVIEW_IMAGE_CACHE: "OrderedDict[Tuple[Any, ...], Tuple[float, bytes]]" = OrderedDict()
_INFLIGHT_TEMPLATE_STICKER_SET_TASKS: Dict[str, "asyncio.Task[Any]"] = {}
_INFLIGHT_FILE_BYTES_TASKS: Dict[str, "asyncio.Task[bytes]"] = {}
_INFLIGHT_CUSTOMIZED_TGS_TASKS: Dict[Tuple[str, str, str, str, str, str, str], "asyncio.Task[bytes]"] = {}
_INFLIGHT_PREVIEW_IMAGE_TASKS: Dict[Tuple[Any, ...], "asyncio.Task[bytes]"] = {}
_BOT_USERNAME_TASK: Optional["asyncio.Task[str]"] = None
_RAW_API_SESSION: Optional[aiohttp.ClientSession] = None
_RAW_API_SESSION_LOCK = asyncio.Lock()


class CreateStates(StatesGroup):
    selecting_templates = State()
    waiting_text = State()
    waiting_logo_choice = State()
    waiting_logo_upload = State()
    waiting_color = State()
    waiting_custom_color = State()
    waiting_passport_name = State()
    waiting_passport_username = State()
    waiting_passport_logo = State()
    waiting_passport_preview = State()
    waiting_pack_slug = State()
    processing = State()


class CouponStates(StatesGroup):
    waiting_code = State()


class JsonDatabase:
    def __init__(self, path: str) -> None:
        self.path = path
        self.lock = asyncio.Lock()
        self.data = self._load_sync()

    def _default_data(self) -> Dict[str, Any]:
        return {
            "users": {},
            "stats": [],
        }

    def _load_sync(self) -> Dict[str, Any]:
        if os.path.exists(self.path):
            try:
                with open(self.path, "r", encoding="utf-8") as file:
                    data = json.load(file)
            except (json.JSONDecodeError, OSError):
                data = self._default_data()
        else:
            data = self._default_data()

        data.setdefault("users", {})
        data.setdefault("stats", [])
        return data

    def _save_sync(self) -> None:
        tmp_path = f"{self.path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as file:
            json.dump(self.data, file, ensure_ascii=False, indent=2)
        os.replace(tmp_path, self.path)

    async def _save_locked(self) -> None:
        await asyncio.to_thread(self._save_sync)

    def _user_defaults(self, tg_user_id: int, first_name: str, username: Optional[str]) -> Dict[str, Any]:
        return {
            "id": tg_user_id,
            "first_name": first_name,
            "username": username,
            "registered_at": datetime.now().strftime("%d.%m.%Y"),
            "font_id": DEFAULT_FONT_ID,
            "balance": 0.0,
            "spent": 0.0,
            "created_emoji_count": 0,
            "packs": [],
            "operations": [],
            "referrals": 0,
            "earned": 0.0,
            "referrer_id": None,
        }

    async def ensure_user(
        self,
        tg_user_id: int,
        first_name: str,
        username: Optional[str],
        referrer_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        async with self.lock:
            users = self.data["users"]
            key = str(tg_user_id)
            created = key not in users

            if created:
                users[key] = self._user_defaults(tg_user_id, first_name, username)
                if referrer_id and referrer_id != tg_user_id:
                    ref_key = str(referrer_id)
                    ref_user = users.get(ref_key)
                    if ref_user is not None:
                        users[key]["referrer_id"] = referrer_id
                        ref_user["referrals"] = int(ref_user.get("referrals", 0)) + 1
            else:
                users[key]["first_name"] = first_name
                users[key]["username"] = username

            await self._save_locked()
            return deepcopy(users[key])

    async def get_user(self, tg_user_id: int) -> Dict[str, Any]:
        async with self.lock:
            return deepcopy(self.data["users"].get(str(tg_user_id), {}))

    async def set_font(self, tg_user_id: int, font_id: str) -> Dict[str, Any]:
        async with self.lock:
            user = self.data["users"].get(str(tg_user_id))
            if user is None:
                raise KeyError("User is not registered")
            user["font_id"] = font_id
            await self._save_locked()
            return deepcopy(user)

    async def add_pack(self, tg_user_id: int, pack_entry: Dict[str, Any]) -> Dict[str, Any]:
        async with self.lock:
            user = self.data["users"].get(str(tg_user_id))
            if user is None:
                raise KeyError("User is not registered")

            user["packs"].insert(0, pack_entry)
            user["created_emoji_count"] = int(user.get("created_emoji_count", 0)) + int(pack_entry.get("count", 0))
            user["operations"].insert(0, {
                "type": "emoji_pack",
                "title": pack_entry.get("title"),
                "count": pack_entry.get("count", 0),
                "link": pack_entry.get("link"),
                "created_at": pack_entry.get("created_at"),
            })
            self.data["stats"].append({
                "user_id": tg_user_id,
                "name": pack_entry.get("name"),
                "title": pack_entry.get("title"),
                "link": pack_entry.get("link"),
                "color": pack_entry.get("color_hex"),
                "count": pack_entry.get("count"),
                "type": "emoji",
            })
            await self._save_locked()
            return deepcopy(user)

    async def try_spend(self, tg_user_id: int, amount: float) -> Tuple[bool, Dict[str, Any]]:
        async with self.lock:
            user = self.data["users"].get(str(tg_user_id))
            if user is None:
                raise KeyError("User is not registered")

            balance = float(user.get("balance", 0.0))
            if balance + 1e-9 < amount:
                return False, deepcopy(user)

            user["balance"] = round(balance - amount, 2)
            user["spent"] = round(float(user.get("spent", 0.0)) + amount, 2)
            await self._save_locked()
            return True, deepcopy(user)

    async def refund_spend(self, tg_user_id: int, amount: float) -> Dict[str, Any]:
        async with self.lock:
            user = self.data["users"].get(str(tg_user_id))
            if user is None:
                raise KeyError("User is not registered")

            user["balance"] = round(float(user.get("balance", 0.0)) + amount, 2)
            user["spent"] = round(max(0.0, float(user.get("spent", 0.0)) - amount), 2)
            await self._save_locked()
            return deepcopy(user)


db = JsonDatabase(DB_PATH)
router = Router()


def build_box(lines: Sequence[str]) -> str:
    if not lines:
        return ""
    return "<blockquote>" + "\n".join(str(line) for line in lines) + "</blockquote>"


def format_money(value: float) -> str:
    return f"{value:.2f}₽"


def escape(value: Any) -> str:
    return html.escape(str(value), quote=False)


def tg_emoji(key: str, fallback: Optional[str] = None) -> str:
    emoji_id, default_fallback = PREMIUM_EMOJIS[key]
    fallback_text = str(fallback or default_fallback)
    if any(char in fallback_text for char in "<>&"):
        fallback_text = default_fallback if not any(char in default_fallback for char in "<>&") else "🔹"
    return f'<tg-emoji emoji-id="{emoji_id}">{escape(fallback_text)}</tg-emoji>'


def ikb(
    text: str,
    *,
    icon: Optional[str] = None,
    callback_data: Optional[str] = None,
    url: Optional[str] = None,
) -> InlineKeyboardButton:
    kwargs: Dict[str, Any] = {}
    if callback_data is not None:
        kwargs["callback_data"] = callback_data
    if url is not None:
        kwargs["url"] = url
    if icon is not None:
        kwargs["icon_custom_emoji_id"] = PREMIUM_EMOJIS[icon][0]
    return InlineKeyboardButton(text=text, **kwargs)


def get_font_label(font_id: str) -> str:
    preset = FONT_PRESETS.get(font_id) or FONT_PRESETS.get(DEFAULT_FONT_ID, {})
    return str(preset.get("label") or "Noto Sans Bold")


def parse_referrer(command: Optional[CommandObject]) -> Optional[int]:
    if command is None or not command.args:
        return None
    if not command.args.startswith("ref_"):
        return None
    try:
        return int(command.args.split("_", 1)[1])
    except (TypeError, ValueError):
        return None


def trim_pack_title(title: str) -> str:
    if len(title) <= 64:
        return title
    return title[:61].rstrip() + "..."


def creation_text_summary(count: int) -> str:
    if count == 1:
        return "1 эмодзи"
    return f"{count} эмодзи"


def is_passport_template_key(template_key: Optional[str]) -> bool:
    return str(template_key or "") == "passport"


def _clean_passport_name(raw_value: str) -> str:
    value = re.sub(r"\s+", " ", raw_value.strip())
    return value[:14]


def _clean_passport_username(raw_value: str) -> str:
    value = raw_value.strip()
    if not value:
        return ""
    if value.startswith("@"):
        value = value[1:]
    value = re.sub(r"[^A-Za-z0-9_]", "", value)
    if not value:
        return ""
    return f"@{value[:32]}"


def get_default_passport_name(user: Any) -> str:
    first_name = getattr(user, "first_name", "") or getattr(user, "username", "") or "nickname"
    cleaned = _clean_passport_name(str(first_name))
    return cleaned or "nickname"


def get_default_passport_username(user: Any, fallback_name: str) -> str:
    username = getattr(user, "username", "") or ""
    cleaned = _clean_passport_username(str(username))
    if cleaned:
        return cleaned
    fallback = re.sub(r"[^A-Za-z0-9_]", "", fallback_name)[:32]
    if not fallback:
        fallback = "username"
    return f"@{fallback}"


def format_rubles_short(value: float) -> str:
    if abs(value - round(value)) < 1e-9:
        return f"{int(round(value))}₽"
    return f"{value:.2f}₽"


TEMPLATE_PLAIN_ICONS: Dict[str, str] = {
    "black_hole": "⚫",
    "color": "🎨",
    "exclusive": "⭐",
    "passport": "🛂",
}

COLOR_BUTTON_EMOJIS: Dict[str, str] = {
    "cyber": "🧊",
    "blood": "🩸",
    "nebula": "🌌",
    "lava": "🔥",
    "forest": "🌲",
    "sunset": "🌇",
    "ocean": "🌊",
    "gold": "✨",
    "mono": "⬜",
}

COLOR_SWATCH_EMOJIS: Dict[str, str] = {
    "cyber": "🧊",
    "blood": "🩸",
    "nebula": "🌌",
    "lava": "🔥",
    "forest": "🌲",
    "sunset": "🌇",
    "ocean": "🌊",
    "gold": "✨",
    "mono": "⬜",
    "свой цвет": "🎨",
}


def get_template_plain_icon(template_key: str) -> str:
    return TEMPLATE_PLAIN_ICONS.get(template_key, "✨")


def get_color_button_label(color: ColorDefinition) -> str:
    return f"{COLOR_BUTTON_EMOJIS.get(color.key, '🎨')} {color.label}"


def get_color_swatch(color_label: str) -> str:
    return COLOR_SWATCH_EMOJIS.get(color_label.strip().lower(), "🎨")


def get_text_prompt_target(count: int) -> str:
    if count == 1:
        return "1 эмодзи"
    return f"всех {count} эмодзи"


def get_creation_text_label(text_value: str) -> str:
    value = str(text_value or "").strip()
    return value or "(без текста)"


def calculate_pack_pricing(template_key: str, count: int) -> Dict[str, int]:
    if is_passport_template_key(template_key):
        return {
            "base_per_item": PASSPORT_PRICE_RUB,
            "final_per_item": PASSPORT_PRICE_RUB,
            "base_total": PASSPORT_PRICE_RUB,
            "final_total": PASSPORT_PRICE_RUB,
            "savings": 0,
        }

    safe_count = max(0, int(count))
    base_per_item = 20
    if safe_count >= 50:
        final_per_item = 15
    elif safe_count >= MULTI_SELECT_LIMIT:
        final_per_item = 16
    else:
        final_per_item = 20

    base_total = base_per_item * safe_count
    final_total = final_per_item * safe_count
    return {
        "base_per_item": base_per_item,
        "final_per_item": final_per_item,
        "base_total": base_total,
        "final_total": final_total,
        "savings": max(0, base_total - final_total),
    }


def build_main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [ikb("🎨 Создать эмодзи", callback_data="create:open")],
        [
            ikb("👤 Профиль", callback_data="menu:profile"),
            ikb("💬 Поддержка", callback_data="menu:support"),
        ],
        [ikb("🎁 Розыгрыш", callback_data="menu:giveaway")],
        [ikb("🏳️ Канал", url=CHANNEL_URL)],
    ])


def build_profile_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            ikb("📦 Мой стикерпак", callback_data="profile:packs"),
            ikb("💰 Пополнить баланс", callback_data="profile:balance"),
        ],
        [
            ikb("🎟 Активировать купон", callback_data="profile:coupon"),
            ikb("📊 История операций", callback_data="profile:history"),
        ],
        [ikb("🤝 Партнёрская программа", callback_data="profile:referral")],
        [ikb("🔤 Шрифт", callback_data="profile:font")],
        [ikb("◁ Главное меню", callback_data="menu:main")],
    ])


def build_support_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [ikb("ℹ Инфо", callback_data="menu:info")],
        [ikb("◁ Назад", callback_data="menu:main")],
    ])


def build_info_keyboard() -> InlineKeyboardMarkup:
    privacy_button = ikb(
        "🔒 Политика конфиденциальности",
        url=PRIVACY_URL,
    ) if PRIVACY_URL else ikb(
        "🔒 Политика конфиденциальности",
        callback_data="info:privacy",
    )

    terms_button = ikb(
        "📄 Пользовательское соглашение",
        url=TERMS_URL,
    ) if TERMS_URL else ikb(
        "📄 Пользовательское соглашение",
        callback_data="info:terms",
    )

    return InlineKeyboardMarkup(inline_keyboard=[
        [privacy_button],
        [terms_button],
        [ikb("◁ Назад", callback_data="menu:support")],
    ])


def build_balance_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [ikb("⭐ Telegram Stars", callback_data="balance:stars")],
        [ikb("💎 CryptoBot", callback_data="balance:crypto")],
        [ikb("🏦 СБП", callback_data="balance:sbp")],
        [ikb("◁ Назад", callback_data="menu:profile")],
    ])


def build_coupon_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [ikb("❌ Отмена", callback_data="coupon:cancel")],
    ])


def build_history_back_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [ikb("◁ Назад в профиль", callback_data="menu:profile")],
    ])


def build_referral_back_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [ikb("◁ Назад в профиль", callback_data="menu:profile")],
    ])


def build_fonts_keyboard(current_font: str) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for font_id, meta in FONT_PRESETS.items():
        label = str(meta.get("button") or meta.get("label") or font_id)
        if font_id == current_font:
            label = f"{label} ✅"
        rows.append([ikb(label, callback_data=f"font:set:{font_id}")])
    rows.append([ikb("◁ Назад", callback_data="menu:profile")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_templates_keyboard(counts: Dict[str, int]) -> InlineKeyboardMarkup:
    rows = []
    for template in TEMPLATES:
        count = counts.get(template.key, 0)
        rows.append([
            ikb(
                text=f"{get_template_plain_icon(template.key)} {template.title} ({count} шт.)",
                callback_data=f"create:template:{template.key}",
            )
        ])
    rows.append([ikb("❌ Отмена", callback_data="menu:main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_mode_keyboard(template_key: str, count: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [ikb(f"📦 Весь пак ({count} шт.)", callback_data=f"create:mode:{template_key}:all")],
        [ikb(f"🎯 Несколько (до {MULTI_SELECT_LIMIT})", callback_data=f"create:mode:{template_key}:some")],
        [ikb("✨ Один шаблон", callback_data=f"create:mode:{template_key}:one")],
        [ikb("◁ Назад", callback_data="create:open")],
    ])


def build_selector_keyboard(
    total: int,
    selected: Sequence[int],
    page: int,
    mode: str,
) -> InlineKeyboardMarkup:
    start = page * SELECTOR_PAGE_SIZE
    end = min(total, start + SELECTOR_PAGE_SIZE)
    selected_set = set(selected)
    rows: List[List[InlineKeyboardButton]] = []

    for row_start in range(start, end, 4):
        row: List[InlineKeyboardButton] = []
        for index in range(row_start, min(end, row_start + 4)):
            row.append(
                ikb(
                    text=str(index + 1),
                    callback_data=f"pick:toggle:{index}:{page}",
                    icon="check" if index in selected_set else None,
                )
            )
        rows.append(row)

    nav_row: List[InlineKeyboardButton] = []
    if start > 0:
        nav_row.append(ikb("◁", callback_data=f"pick:page:{page - 1}"))
    if end < total:
        nav_row.append(ikb("▷", callback_data=f"pick:page:{page + 1}"))
    if nav_row:
        rows.append(nav_row)

    done_label = "Готово"
    if mode == "some":
        done_label = f"Готово ({len(selected)}/{MULTI_SELECT_LIMIT})"
    rows.append([ikb(done_label, callback_data="pick:done", icon="check")])
    rows.append([ikb("◁ Назад", callback_data="pick:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_logo_choice_keyboard(has_logo: bool) -> InlineKeyboardMarkup:
    del has_logo
    return InlineKeyboardMarkup(inline_keyboard=[
        [ikb("🖼 Своё лого", callback_data="logo:add")],
        [
            ikb("◁ Назад", callback_data="logo:back"),
            ikb("⏭️ Пропустить", callback_data="logo:skip"),
        ],
    ])


def build_logo_upload_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [ikb("◁ Назад", callback_data="logo_upload:back")],
    ])


def build_passport_step_keyboard(back_callback: str, skip_callback: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            ikb(PASSPORT_BACK_BUTTON_TEXT, callback_data=back_callback),
            ikb(PASSPORT_SKIP_BUTTON_TEXT, callback_data=skip_callback),
        ],
    ])


def build_passport_preview_keyboard(balance: float) -> InlineKeyboardMarkup:
    if CREATION_IS_FREE:
        pay_label = "✅ Создать бесплатно"
    else:
        pay_label = PASSPORT_PAY_BUTTON_TEXT.format(price=PASSPORT_PRICE_RUB)
        if balance + 1e-9 < PASSPORT_PRICE_RUB:
            pay_label = f"{pay_label} (недостаточно средств)"
    return InlineKeyboardMarkup(inline_keyboard=[
        [ikb(pay_label, callback_data="passport_preview:create")],
        [ikb(PASSPORT_TOP_UP_BUTTON_TEXT, callback_data="profile:balance")],
        [ikb(PASSPORT_CANCEL_BUTTON_TEXT, callback_data="passport_preview:cancel")],
    ])


def build_color_keyboard() -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for index in range(0, len(COLOR_PRESETS), 3):
        row = [
            ikb(text=get_color_button_label(color), callback_data=f"color:choose:{color.key}")
            for color in COLOR_PRESETS[index:index + 3]
        ]
        rows.append(row)
    rows.append([
        ikb("🎨 Свой цвет", callback_data="color:custom"),
        ikb("◁ Назад", callback_data="color:back"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_final_preview_keyboard(price: int, balance: float) -> InlineKeyboardMarkup:
    if CREATION_IS_FREE:
        pay_label = "✅ Создать бесплатно"
    else:
        pay_label = f"💳 Оплатить {price}₽"
        if balance + 1e-9 < price:
            pay_label = f"{pay_label} (недостаточно средств)"
    return InlineKeyboardMarkup(inline_keyboard=[
        [ikb(pay_label, callback_data="preview:create")],
        [ikb("💰 Пополнить баланс", callback_data="profile:balance")],
        [ikb("❌ Отмена", callback_data="preview:cancel")],
    ])


def build_pack_slug_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [ikb("◁ Назад", callback_data="slug:back")],
    ])


def build_created_pack_keyboard(pack_link: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [ikb("🔗 Открыть пак", url=pack_link)],
        [ikb("🎨 Создать ещё", callback_data="create:open")],
        [ikb("👤 Профиль", callback_data="menu:profile")],
    ])


def build_user_packs_keyboard(packs: Sequence[Dict[str, Any]]) -> InlineKeyboardMarkup:
    rows = [
        [ikb(f'📦 {str(pack["title"])}', url=pack["link"])]
        for pack in packs[:8]
    ]
    rows.append([ikb("◁ Назад", callback_data="menu:profile")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def render_main_menu_text(user: Dict[str, Any]) -> str:
    name = escape(user.get("first_name") or "друг")
    box = build_box([
        "💭 Создавай уникальные анимированные эмодзи с любым текстом!",
        "",
        f'Примеры: <a href="{TEMPLATES[0].example_url}">ТУТ</a>',
        "",
        "⚡ Как это работает:",
        "① Нажми «Создать эмодзи»",
        "② Выбери шаблон из каталога",
        "③ Введи свой текст (до 20 символов)",
        "④ Выбери цвет и получи превью",
        "⑤ Введи название пака и ссылку на него",
        "⑥ Получи свой эмодзи пак бесплатно!",
    ])
    return (
        f"✨ <b>Emoji Creation Bot</b> ✨\n\n"
        f"👋 Привет, <b>{name}</b>!\n\n"
        f"{box}"
    )


def render_profile_text(user: Dict[str, Any]) -> str:
    username = user.get("username")
    username_text = f'<a href="https://t.me/{escape(username)}">@{escape(username)}</a>' if username else "—"
    box = build_box([
        f'{tg_emoji("code")} ID: {user.get("id", "—")}',
        f'{tg_emoji("profile")} Имя: {escape(user.get("first_name", "—"))}',
        f'{tg_emoji("link")} Username: {username_text}',
        f'{tg_emoji("calendar")} Регистрация: {escape(user.get("registered_at", "—"))}',
        "",
        f'{tg_emoji("wallet")} Баланс: {format_money(float(user.get("balance", 0.0)))}',
        f'{tg_emoji("money_send")} Потрачено: {format_money(float(user.get("spent", 0.0)))}',
        f'{tg_emoji("brush")} Создано эмодзи: {int(user.get("created_emoji_count", 0))}',
    ])
    return (
        f"{tg_emoji('profile')} <b>Твой профиль</b>\n\n"
        f"{box}\n\n"
        "💡 Используй бота для создания уникальных эмодзи!"
    )


def render_support_text() -> str:
    box = build_box([
        "❔ Возникли вопросы?",
        "Мы всегда готовы помочь!",
        "",
        "👥 Контакты:",
        f'• Telegram: <a href="https://t.me/{SUPPORT_USERNAME}">@{SUPPORT_USERNAME}</a>',
        "",
        "📣 Наш tg канал:",
        f'<a href="{CHANNEL_URL}">@{CHANNEL_USERNAME}</a>',
        "",
        "⏰ Время ответа: обычно в течение 10 минут.",
    ])
    return "💬 <b>Поддержка</b>\n\n" + box


def render_info_text() -> str:
    return "👍 <b>Информация</b>\n\n" + build_box(["Ознакомьтесь с документами ниже:"])


def render_create_menu_text() -> str:
    lines = []
    for template in TEMPLATES:
        lines.append(
            f'{template.number}. {get_template_plain_icon(template.key)} {template.title} [ <a href="{template.example_url}">Примеры</a> ]'
        )
    return "🪄 <b>Создание эмодзи</b>\n\n" + "\n".join(lines) + "\n\nВыбери пак:"


def render_template_detail_text(template: TemplateDefinition, count: int) -> str:
    descriptions = {
        "black_hole": "Полный набор из 96 уникальных эмодзи",
        "color": "Яркий набор цветных эмодзи с твоим текстом",
        "exclusive": "Эксклюзивный набор из 9 эмодзи с никнеймом",
        "passport": "Персональный passport эмодзи с ником, username и логотипом",
    }
    return (
        f"{get_template_plain_icon(template.key)} <b>{template.title}</b>\n\n"
        f"{descriptions.get(template.key, template.description)}\n\n"
        "Выбери способ генерации:"
    )


def render_selector_text(template: TemplateDefinition, mode: str, total: int, selected: Sequence[int]) -> str:
    mode_label = "Один шаблон" if mode == "one" else f"Несколько (до {MULTI_SELECT_LIMIT})"
    selected_text = ", ".join(str(index + 1) for index in selected[:24]) or "ничего"
    return (
        f"{tg_emoji('apps')} <b>Выбор шаблонов</b>\n\n"
        f"{tg_emoji(template.button_icon)} Пак: <b>{template.title}</b>\n"
        f"{tg_emoji('tag')} Режим: <b>{mode_label}</b>\n"
        f"{tg_emoji('chart')} Всего в паке: <b>{total}</b>\n"
        f"{tg_emoji('check')} Выбрано: <b>{len(selected)}</b>\n\n"
        f"Выбранные номера: <code>{selected_text}</code>\n\n"
        "Открой «Примеры» и выбери шаблоны по порядку."
    )


def render_text_prompt(template: TemplateDefinition, count: int, font_id: str, current_text: Optional[str]) -> str:
    del template, font_id, current_text
    return (
        f"{tg_emoji('pencil')} <b>Отправь текст для {get_text_prompt_target(count)} (до 20 символов)</b>\n\n"
        "<i>Можно пропустить этап и сделать эмодзи только с SVG логотипом</i>"
    )


def render_logo_choice_text(font_id: str, text_value: str, has_logo: bool) -> str:
    status_text = "загружено" if has_logo else "не добавлено"
    return (
        f"{tg_emoji('media')} <b>Своё лого</b>\n\n"
        + build_box([
            f'{tg_emoji("text")} Текст: {escape(get_creation_text_label(text_value))}',
            f'{tg_emoji("font")} Шрифт: {escape(get_font_label(font_id))}',
            f"{tg_emoji('media')} Лого: {status_text}",
        ])
    )


def render_logo_upload_text() -> str:
    box = build_box([
        f"{tg_emoji('paperclip')} Отправь SVG-файл как документ",
        f"{tg_emoji('check')} Подходят: чёткие векторные контуры",
        f"{tg_emoji('cross')} Не подходят: PNG/JPG/WEBP, фото, 3D",
        "",
        f"{tg_emoji('brush')} Цвет совпадёт с цветом текста",
    ])
    return f"{tg_emoji('upload')} <b>Логотип</b>\n\n" + box


def render_passport_name_prompt(current_name: Optional[str]) -> str:
    del current_name
    return "\n".join([
        f"{tg_emoji('pencil')} <b>PASSPORT — Шаг 1/3</b>",
        "",
        "Введи никнейм (до 14 символов):",
        "",
        "<i>Это текст, который будет внутри эмодзи</i>",
    ])


def render_passport_username_prompt(current_username: Optional[str]) -> str:
    del current_username
    return "\n".join([
        f"{tg_emoji('pencil')} <b>PASSPORT — Шаг 2/3</b>",
        "",
        f"Введи <a href=\"https://t.me/username\">@username</a> (до 32 символов):",
        "",
        "<i>Например: @nickname</i>",
    ])


def render_passport_logo_prompt(has_logo: bool) -> str:
    del has_logo
    return "\n".join([
        f"{tg_emoji('upload')} <b>PASSPORT — Шаг 3/3</b>",
        "",
        "Отправь SVG логотип как документ:",
        "",
        "<i>Это изображение заменит лого на паспорте</i>",
    ])


def render_passport_final_preview_caption(
    text_value: str,
    username_value: str,
    balance: float,
) -> str:
    price_text = "<b>Бесплатно</b>" if CREATION_IS_FREE else f"<b>{PASSPORT_PRICE_RUB}₽</b>"
    return (
        "✨ <b>Финальный превью</b>\n\n"
        f"Текст: {escape(text_value)}\n"
        f"Username: {escape(username_value)}\n\n"
        f"💰 Стоимость: {price_text}\n"
        f"💳 Ваш баланс: <b>{format_rubles_short(balance)}</b>"
    )


def render_color_caption(count: int, text_value: str, font_id: str, has_logo: bool) -> str:
    del font_id, has_logo
    return (
        "🎨 <b>Выбери цветовую схему</b>\n\n"
        f"Этот цвет будет применён ко всем <b>{count}</b> шаблонам.\n\n"
        f"Текст: <code>{escape(get_creation_text_label(text_value))}</code>"
    )


def render_final_preview_caption(
    template: TemplateDefinition,
    count: int,
    text_value: str,
    color_label: str,
    color_hex: str,
    font_id: str,
    has_logo: bool,
    balance: float,
) -> str:
    del font_id, has_logo
    pricing = calculate_pack_pricing(template.key, count)
    color_text = (
        f"{get_color_swatch(color_label)} <code>{escape(color_hex)}</code>"
        if color_label.strip().lower() == "свой цвет"
        else get_color_swatch(color_label)
    )
    if CREATION_IS_FREE:
        pricing_lines = (
            "💰 Стоимость: <b>Бесплатно</b>\n"
            f"💳 Ваш баланс: <b>{format_rubles_short(balance)}</b>"
        )
    else:
        pricing_lines = (
            f"💰 Стоимость: <s>{pricing['base_total']}₽</s> → <b>{pricing['final_total']}₽</b> ({count} шт.)\n"
            f"🪙 Цена за 1 эмодзи: <s>{pricing['base_per_item']}₽</s> → <b>{pricing['final_per_item']}₽</b>\n"
            f"🎉 Экономия: <b>{pricing['savings']}₽</b>\n"
            f"💳 Ваш баланс: <b>{format_rubles_short(balance)}</b>"
        )
    return (
        "✨ <b>Финальный превью</b>\n\n"
        f"Текст: {escape(get_creation_text_label(text_value))}\n"
        f"Цвет: {color_text}\n\n"
        f"{pricing_lines}"
    )


def render_pack_slug_text(bot_username: str) -> str:
    return (
        f"{tg_emoji('link')} <b>Имя ссылки для пака</b>\n\n"
        "Отправь короткое имя ссылки только из <code>a-z</code>, <code>0-9</code> и <code>_</code>.\n\n"
        "Пример: <code>samkert_blackhole</code>\n"
        "Итоговая ссылка будет такой:\n"
        f"<code>https://t.me/addemoji/&lt;имя&gt;_by_{escape(bot_username.lower())}</code>"
    )


def render_balance_text() -> str:
    return "💳 <b>Пополнение баланса</b>\n\nВыберите удобный способ оплаты:"


def render_coupon_text() -> str:
    return "🎟 <b>Активация купона</b>\n\n" + build_box([
        "Отправьте код купона в чат.",
        "",
        "Формат: XXXXX-XXXXX",
    ])


def render_history_text(operations: Sequence[Dict[str, Any]]) -> str:
    if not operations:
        return (
            "📊 <b>История операций</b>\n\n"
            + build_box(["У вас пока нет операций.", "", "Пополните баланс или активируйте купон!"])
        )

    lines = ["📊 <b>История операций</b>"]
    for entry in operations[:8]:
        lines.append(
            f"• <b>{escape(entry.get('title', 'Emoji Pack'))}</b>\n"
            f"  📦 {entry.get('count', 0)} шт. | 📅 {escape(entry.get('created_at', '—'))}\n"
            f"  🔗 <a href=\"{entry.get('link', '#')}\">Открыть пак</a>"
        )
    return "\n\n".join(lines)


def render_referral_text(user: Dict[str, Any], bot_username: str) -> str:
    referral_link = f"https://t.me/{bot_username}?start=ref_{user.get('id')}"
    box = build_box([
        f'{tg_emoji("paperclip")} Твоя ссылка:',
        referral_link,
        "",
        f'👥 Приглашено: {int(user.get("referrals", 0))} чел.',
        f'💸 Заработано: {format_money(float(user.get("earned", 0.0)))}',
    ])
    return (
        "🤝 <b>Партнёрская программа</b>\n\n"
        f"{box}\n\n"
        "🎁 <b>Как получить 10₽ за друга:</b>\n"
        "— друг переходит по твоей ссылке и отправляет <code>/start</code>\n"
        "— система проверяет подписку на обязательный канал\n"
        "— после проверки начисляется бонус\n\n"
        "🦋 Плюс 10% с каждого пополнения твоих приглашённых навсегда."
    )


def render_fonts_text(current_font: str) -> str:
    return (
        "🔤 <b>Выбор шрифта</b>\n\n"
        f"Текущий шрифт: <b>{escape(get_font_label(current_font))}</b>\n\n"
        "Выбранный шрифт будет использоваться для всех новых эмодзи."
    )


async def safe_delete_message(bot: Bot, chat_id: int, message_id: int) -> None:
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except TelegramBadRequest:
        pass


async def safe_delete_user_message(message: Message) -> None:
    try:
        await message.delete()
    except TelegramBadRequest:
        pass


async def safe_edit_message(message: Message, text: str, **kwargs: Any) -> None:
    try:
        await message.edit_text(text, **kwargs)
    except TelegramBadRequest as error:
        if "message is not modified" not in str(error):
            raise


def _cache_get(cache: "OrderedDict[Any, Tuple[float, T]]", key: Any, ttl: float) -> Optional[T]:
    cached = cache.get(key)
    if cached is None:
        return None
    stored_at, value = cached
    if time.monotonic() - stored_at >= ttl:
        cache.pop(key, None)
        return None
    cache.move_to_end(key)
    return value


def _cache_put(cache: "OrderedDict[Any, Tuple[float, T]]", key: Any, value: T, max_items: int) -> T:
    cache[key] = (time.monotonic(), value)
    cache.move_to_end(key)
    while len(cache) > max_items:
        cache.popitem(last=False)
    return value


async def _run_singleflight(
    inflight_tasks: Dict[Any, "asyncio.Task[T]"],
    key: Any,
    factory: Callable[[], Awaitable[T]],
) -> T:
    existing = inflight_tasks.get(key)
    if existing is not None:
        return await asyncio.shield(existing)

    task: "asyncio.Task[T]" = asyncio.create_task(factory())
    inflight_tasks[key] = task
    try:
        return await asyncio.shield(task)
    finally:
        if inflight_tasks.get(key) is task and task.done():
            inflight_tasks.pop(key, None)


def _logo_cache_key(logo_svg: Optional[bytes]) -> str:
    if not logo_svg:
        return ""
    return hashlib.sha1(logo_svg).hexdigest()[:16]


async def _get_raw_api_session() -> aiohttp.ClientSession:
    global _RAW_API_SESSION
    if _RAW_API_SESSION is not None and not _RAW_API_SESSION.closed:
        return _RAW_API_SESSION

    async with _RAW_API_SESSION_LOCK:
        if _RAW_API_SESSION is not None and not _RAW_API_SESSION.closed:
            return _RAW_API_SESSION
        connector = aiohttp.TCPConnector(limit=RAW_API_CONNECTOR_LIMIT)
        _RAW_API_SESSION = aiohttp.ClientSession(
            connector=connector,
            timeout=aiohttp.ClientTimeout(total=120),
        )
        return _RAW_API_SESSION


async def _close_raw_api_session() -> None:
    global _RAW_API_SESSION
    if _RAW_API_SESSION is not None and not _RAW_API_SESSION.closed:
        await _RAW_API_SESSION.close()
    _RAW_API_SESSION = None


async def _get_template_sticker_set(bot: Bot, short_name: str) -> Any:
    cached = _cache_get(_TEMPLATE_STICKER_SET_CACHE, short_name, STICKER_SET_CACHE_TTL)
    if cached is not None:
        return cached

    async def load_sticker_set() -> Any:
        sticker_set = await _call_bot_api("get_sticker_set", bot.get_sticker_set, short_name)
        return _cache_put(_TEMPLATE_STICKER_SET_CACHE, short_name, sticker_set, len(TEMPLATES) + 4)

    return await _run_singleflight(_INFLIGHT_TEMPLATE_STICKER_SET_TASKS, short_name, load_sticker_set)


def _get_layout_info_cached(
    file_id: str,
    raw_bytes: bytes,
) -> Optional[Tuple[int, int, Tuple[float, float, float, float]]]:
    cached = _cache_get(_LAYOUT_INFO_CACHE, file_id, FILE_BYTES_CACHE_TTL)
    if cached is not None:
        return cached
    layout_info = extract_tgs_layout_info(raw_bytes)
    return _cache_put(_LAYOUT_INFO_CACHE, file_id, layout_info, LAYOUT_INFO_CACHE_MAX)


async def _get_customized_tgs_cached(
    file_id: str,
    raw_bytes: bytes,
    text: str,
    hex_color: str,
    font_id: str,
    logo_svg: Optional[bytes],
    secondary_text: Optional[str] = None,
) -> bytes:
    cache_key = (
        CUSTOMIZATION_CACHE_VERSION,
        file_id,
        text,
        secondary_text or "",
        hex_color.upper(),
        font_id,
        _logo_cache_key(logo_svg),
    )
    cached = _cache_get(_CUSTOMIZED_TGS_CACHE, cache_key, CUSTOMIZED_TGS_CACHE_TTL)
    if cached is not None:
        return cached

    async def customize() -> bytes:
        if secondary_text is not None:
            customized = await asyncio.to_thread(
                customize_tgs_template_with_secondary_text,
                raw_bytes,
                text,
                secondary_text,
                hex_color,
                font_id,
                logo_svg,
                False,
            )
        else:
            customized = await asyncio.to_thread(
                customize_tgs_template,
                raw_bytes,
                text,
                hex_color,
                font_id,
                logo_svg,
                False,
            )
        return _cache_put(_CUSTOMIZED_TGS_CACHE, cache_key, customized, CUSTOMIZED_TGS_CACHE_MAX)

    return await _run_singleflight(_INFLIGHT_CUSTOMIZED_TGS_TASKS, cache_key, customize)


def is_expired_callback_error(error: TelegramBadRequest) -> bool:
    text = str(error).lower()
    return (
        "query is too old" in text
        or "query id is invalid" in text
        or "query is already answered" in text
    )


async def safe_answer_callback(query: CallbackQuery) -> None:
    try:
        await query.answer()
    except TelegramBadRequest as error:
        if not is_expired_callback_error(error):
            raise


async def send_text_screen(
    source: Message | CallbackQuery,
    text: str,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
    *,
    edit: bool = False,
    ack: bool = True,
) -> Message:
    if isinstance(source, CallbackQuery):
        if ack:
            await safe_answer_callback(source)
        if edit and source.message and source.message.text is not None:
            try:
                return await source.message.edit_text(
                    text,
                    reply_markup=reply_markup,
                    disable_web_page_preview=True,
                )
            except TelegramBadRequest:
                pass
        return await source.message.answer(
            text,
            reply_markup=reply_markup,
            disable_web_page_preview=True,
        )
    return await source.answer(
        text,
        reply_markup=reply_markup,
        disable_web_page_preview=True,
    )


async def send_photo_screen(
    source: Message | CallbackQuery,
    image_bytes: bytes,
    caption: str,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
    *,
    ack: bool = True,
) -> Message:
    photo = BufferedInputFile(image_bytes, filename="preview.png")
    if isinstance(source, CallbackQuery):
        if ack:
            await safe_answer_callback(source)
        return await source.message.answer_photo(
            photo=photo,
            caption=caption,
            reply_markup=reply_markup,
        )
    return await source.answer_photo(photo=photo, caption=caption, reply_markup=reply_markup)


async def get_bot_username(bot: Bot) -> str:
    global _BOT_USERNAME_CACHE
    global _BOT_USERNAME_TASK
    if _BOT_USERNAME_CACHE is not None:
        return _BOT_USERNAME_CACHE

    if _BOT_USERNAME_TASK is None or _BOT_USERNAME_TASK.done():
        async def load_username() -> str:
            me = await bot.get_me()
            return me.username or "bot"

        _BOT_USERNAME_TASK = asyncio.create_task(load_username())

    try:
        _BOT_USERNAME_CACHE = await asyncio.shield(_BOT_USERNAME_TASK)
        return _BOT_USERNAME_CACHE
    finally:
        if _BOT_USERNAME_TASK is not None and _BOT_USERNAME_TASK.done():
            _BOT_USERNAME_TASK = None


async def get_template_count(bot: Bot, short_name: str) -> int:
    now = time.monotonic()
    cached = _TEMPLATE_COUNT_CACHE.get(short_name)
    if cached and now - cached[0] < TEMPLATE_COUNT_CACHE_TTL:
        return cached[1]

    sticker_set = await _get_template_sticker_set(bot, short_name)
    count = len(sticker_set.stickers)
    _TEMPLATE_COUNT_CACHE[short_name] = (now, count)
    return count


def build_grid_image(
    tiles: Sequence[Image.Image],
    columns: int,
    *,
    gap: int = 16,
    padding: int = 18,
    background: Tuple[int, int, int, int] = (18, 18, 22, 255),
) -> bytes:
    if not tiles:
        blank = Image.new("RGBA", (640, 640), background)
        buffer = io.BytesIO()
        blank.save(buffer, format="PNG", compress_level=PREVIEW_PNG_COMPRESS_LEVEL)
        return buffer.getvalue()

    tile_width, tile_height = tiles[0].size
    rows = (len(tiles) + columns - 1) // columns
    width = padding * 2 + columns * tile_width + gap * max(columns - 1, 0)
    height = padding * 2 + rows * tile_height + gap * max(rows - 1, 0)
    image = Image.new("RGBA", (width, height), background)

    for index, tile in enumerate(tiles):
        row = index // columns
        col = index % columns
        x = padding + col * (tile_width + gap)
        y = padding + row * (tile_height + gap)
        image.alpha_composite(tile, (x, y))

    buffer = io.BytesIO()
    image.save(buffer, format="PNG", compress_level=PREVIEW_PNG_COMPRESS_LEVEL)
    return buffer.getvalue()


async def build_color_preview_image(
    bot: Bot,
    template_key: str,
    sticker: Any,
    text_value: str,
    font_id: str,
    logo_svg: Optional[bytes],
) -> bytes:
    cache_key = (
        CUSTOMIZATION_CACHE_VERSION,
        "color_preview",
        template_key,
        str(sticker.file_id),
        text_value,
        font_id,
        _logo_cache_key(logo_svg),
    )
    cached = _cache_get(_PREVIEW_IMAGE_CACHE, cache_key, PREVIEW_IMAGE_CACHE_TTL)
    if cached is not None:
        return cached

    async def render_preview() -> bytes:
        raw_bytes = await _download_file(bot, sticker.file_id)
        layout_info = _get_layout_info_cached(str(sticker.file_id), raw_bytes)
        preview_bounds_override = get_preview_bounds_override(template_key)
        semaphore = asyncio.Semaphore(PREVIEW_TASK_CONCURRENCY)

        async def build_tile(color: ColorDefinition) -> Image.Image:
            async with semaphore:
                customized = await _get_customized_tgs_cached(
                    str(sticker.file_id),
                    raw_bytes,
                    text_value,
                    color.hex_value,
                    font_id,
                    logo_svg,
                )
                return await asyncio.to_thread(
                    build_tgs_preview_tile,
                    customized,
                    text_value,
                    color.hex_value,
                    font_id,
                    layout_info,
                    logo_svg,
                    preview_bounds_override,
                    190,
                )

        tiles = list(await asyncio.gather(*(build_tile(color) for color in COLOR_PRESETS)))
        image_bytes = await asyncio.to_thread(build_grid_image, tiles, 3)
        return _cache_put(_PREVIEW_IMAGE_CACHE, cache_key, image_bytes, PREVIEW_IMAGE_CACHE_MAX)

    return await _run_singleflight(_INFLIGHT_PREVIEW_IMAGE_TASKS, cache_key, render_preview)


async def build_final_preview_image(
    bot: Bot,
    template_key: str,
    stickers: Sequence[Any],
    text_value: str,
    color_hex: str,
    font_id: str,
    logo_svg: Optional[bytes],
) -> bytes:
    count = len(stickers)
    if count <= 1:
        columns = 1
        tile_size = 640
    else:
        columns = max(2, int(math.ceil(math.sqrt(count))))
        if count <= 9:
            tile_size = 220
        elif count <= 25:
            tile_size = 180
        elif count <= 49:
            tile_size = 144
        elif count <= 81:
            tile_size = 120
        else:
            tile_size = 106

    cache_key = (
        CUSTOMIZATION_CACHE_VERSION,
        "final_preview",
        template_key,
        tuple(str(getattr(sticker, "file_id", "")) for sticker in stickers),
        text_value,
        color_hex.upper(),
        font_id,
        _logo_cache_key(logo_svg),
    )
    cached = _cache_get(_PREVIEW_IMAGE_CACHE, cache_key, PREVIEW_IMAGE_CACHE_TTL)
    if cached is not None:
        return cached

    async def render_preview() -> bytes:
        preview_bounds_override = get_preview_bounds_override(template_key)
        semaphore = asyncio.Semaphore(PREVIEW_TASK_CONCURRENCY)

        async def build_tile(sticker: Any) -> Image.Image:
            async with semaphore:
                file_id = str(sticker.file_id)
                raw_bytes = await _download_file(bot, file_id)
                layout_info = _get_layout_info_cached(file_id, raw_bytes)
                customized = await _get_customized_tgs_cached(
                    file_id,
                    raw_bytes,
                    text_value,
                    color_hex,
                    font_id,
                    logo_svg,
                )
                return await asyncio.to_thread(
                    build_tgs_preview_tile,
                    customized,
                    text_value,
                    color_hex,
                    font_id,
                    layout_info,
                    logo_svg,
                    preview_bounds_override,
                    tile_size,
                )

        tiles = list(await asyncio.gather(*(build_tile(sticker) for sticker in stickers)))
        image_bytes = await asyncio.to_thread(
            lambda: build_grid_image(tiles, columns=columns, gap=12, padding=16)
        )
        return _cache_put(_PREVIEW_IMAGE_CACHE, cache_key, image_bytes, PREVIEW_IMAGE_CACHE_MAX)

    return await _run_singleflight(_INFLIGHT_PREVIEW_IMAGE_TASKS, cache_key, render_preview)


def progress_bar(done: int, total: int) -> str:
    if total <= 0:
        return "░░░░░░░░░░ 0/0"
    filled = max(1, int(round(done / total * 10))) if done else 0
    bar = "█" * filled + "░" * (10 - filled)
    return f"{bar} {done}/{total}"


def get_preview_bounds_override(
    template_key: str,
    image_size: Tuple[int, int] = (512, 512),
) -> Optional[Tuple[float, float, float, float]]:
    width, height = image_size
    if template_key in {"color", "exclusive"}:
        return (
            width * 0.18,
            height * 0.40,
            width * 0.82,
            height * 0.60,
        )
    if template_key == "passport":
        return (
            width * 0.46,
            height * 0.20,
            width * 0.82,
            height * 0.40,
        )
    return None


async def get_selected_stickers(bot: Bot, data: Dict[str, Any]) -> List[Any]:
    template = TEMPLATE_BY_KEY[data["template_key"]]
    sticker_set = await _get_template_sticker_set(bot, template.short_name)
    stickers = sticker_set.stickers

    mode = data.get("mode")
    if mode == "all":
        return list(stickers)

    indexes = sorted(set(int(index) for index in data.get("selected_indices", [])))
    return [stickers[index] for index in indexes if 0 <= index < len(stickers)]


def get_selected_count(data: Dict[str, Any]) -> int:
    if data.get("mode") == "all":
        return int(data.get("template_count", 0))
    return len(data.get("selected_indices", []))


def render_prepare_failure_text(template_key: str, errors: Sequence[Exception]) -> str:
    hint = (
        "Попробуйте короче текст, короче username или более простой SVG-логотип."
        if is_passport_template_key(template_key)
        else "Попробуйте короче текст, другой цвет или более простой SVG-логотип."
    )
    if not errors:
        return f"{tg_emoji('cross')} Не удалось подготовить ни одного эмодзи.\n{hint}"

    raw_reason = next((str(error).strip() for error in errors if str(error).strip()), "")
    reason_lower = raw_reason.lower()
    if "too big after customization" in reason_lower:
        reason = "Итоговый animated emoji превысил лимит Telegram 64 KB."
    elif "svg" in reason_lower and ("сложн" in reason_lower or "complex" in reason_lower):
        reason = "SVG-логотип оказался слишком сложным для animated emoji."
    else:
        reason = raw_reason

    if not reason:
        return f"{tg_emoji('cross')} Не удалось подготовить ни одного эмодзи.\n{hint}"
    return (
        f"{tg_emoji('cross')} Не удалось подготовить ни одного эмодзи.\n"
        f"{hint}\n\nПричина: <code>{escape(reason)}</code>"
    )


async def open_main_menu(source: Message | CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if isinstance(source, CallbackQuery):
        await safe_answer_callback(source)
    await state.clear()
    user = await db.ensure_user(
        tg_user_id=source.from_user.id,
        first_name=source.from_user.first_name,
        username=source.from_user.username,
    )
    await send_text_screen(
        source,
        render_main_menu_text(user),
        build_main_menu_keyboard(),
        edit=isinstance(source, CallbackQuery),
        ack=False,
    )


async def open_profile(source: Message | CallbackQuery) -> None:
    if isinstance(source, CallbackQuery):
        await safe_answer_callback(source)
    user = await db.get_user(source.from_user.id)
    await send_text_screen(
        source,
        render_profile_text(user),
        build_profile_keyboard(),
        edit=isinstance(source, CallbackQuery),
        ack=False,
    )


async def open_create_menu(source: Message | CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if isinstance(source, CallbackQuery):
        await safe_answer_callback(source)
    dep_error = get_text_render_dependency_error()
    if dep_error:
        message = (
            f"{tg_emoji('cross')} Генератор сейчас недоступен: отсутствует зависимость <code>fonttools</code>.\n"
            "Установите зависимости командой <code>pip install -r requirements.txt</code>."
        )
        await send_text_screen(
            source,
            message,
            build_main_menu_keyboard(),
            edit=isinstance(source, CallbackQuery),
            ack=False,
        )
        return

    unique_short_names = list(dict.fromkeys(template.short_name for template in TEMPLATES))
    count_values = await asyncio.gather(*(get_template_count(bot, short_name) for short_name in unique_short_names))
    counts_by_short_name = dict(zip(unique_short_names, count_values))

    counts = {template.key: counts_by_short_name[template.short_name] for template in TEMPLATES}
    await state.clear()
    await send_text_screen(
        source,
        render_create_menu_text(),
        build_templates_keyboard(counts),
        edit=isinstance(source, CallbackQuery),
        ack=False,
    )


async def open_logo_choice(source: Message | CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    await state.set_state(CreateStates.waiting_logo_choice)
    await send_text_screen(
        source,
        render_logo_choice_text(
            font_id=str(data["font_id"]),
            text_value=str(data["user_text"]),
            has_logo=bool(data.get("logo_svg")),
        ),
        build_logo_choice_keyboard(bool(data.get("logo_svg"))),
    )


async def open_text_prompt(source: Message | CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    template = TEMPLATE_BY_KEY[data["template_key"]]
    await state.set_state(CreateStates.waiting_text)
    await send_text_screen(
        source,
        render_text_prompt(
            template=template,
            count=get_selected_count(data),
            font_id=str(data["font_id"]),
            current_text=data.get("user_text"),
        ),
        InlineKeyboardMarkup(inline_keyboard=[
            [
                ikb("◁ Назад", callback_data="text:back"),
                ikb("⏭️ Пропустить", callback_data="text:skip"),
            ],
        ]),
    )


async def open_passport_name_prompt(
    source: Message | CallbackQuery,
    state: FSMContext,
    *,
    ack: bool = True,
) -> None:
    data = await state.get_data()
    await state.set_state(CreateStates.waiting_passport_name)
    await send_text_screen(
        source,
        render_passport_name_prompt(data.get("user_text")),
        build_passport_step_keyboard("passport_name:back", "passport_name:skip"),
        edit=isinstance(source, CallbackQuery),
        ack=ack,
    )


async def open_passport_username_prompt(source: Message | CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    await state.set_state(CreateStates.waiting_passport_username)
    await send_text_screen(
        source,
        render_passport_username_prompt(data.get("passport_username")),
        build_passport_step_keyboard("passport_username:back", "passport_username:skip"),
        edit=isinstance(source, CallbackQuery),
    )


async def open_passport_logo_prompt(source: Message | CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    await state.set_state(CreateStates.waiting_passport_logo)
    await send_text_screen(
        source,
        render_passport_logo_prompt(bool(data.get("logo_svg"))),
        build_passport_step_keyboard("passport_logo:back", "passport_logo:skip"),
        edit=isinstance(source, CallbackQuery),
    )


async def build_passport_preview_image(bot: Bot, data: Dict[str, Any]) -> bytes:
    stickers = await get_selected_stickers(bot, data)
    if not stickers:
        raise ValueError("Passport template sticker was not loaded.")

    sticker = stickers[0]
    file_id = str(sticker.file_id)
    raw_bytes = await _download_file(bot, file_id)
    customized = await _get_customized_tgs_cached(
        file_id=file_id,
        raw_bytes=raw_bytes,
        text=str(data["user_text"]),
        hex_color=PASSPORT_COLOR_HEX,
        font_id=str(data["font_id"]),
        logo_svg=data.get("logo_svg"),
        secondary_text=str(data["passport_username"]),
    )
    image = await asyncio.to_thread(
        render_tgs_preview_image,
        customized,
        None,
        PASSPORT_COLOR_HEX,
        str(data["font_id"]),
        None,
        None,
        None,
        False,
        (768, 768),
    )

    buffer = io.BytesIO()
    image.save(buffer, format="PNG", compress_level=PREVIEW_PNG_COMPRESS_LEVEL)
    return buffer.getvalue()


async def open_passport_final_preview(
    source: Message | CallbackQuery,
    state: FSMContext,
    bot: Bot,
) -> None:
    if isinstance(source, CallbackQuery):
        await safe_answer_callback(source)
    data = await state.get_data()
    preview_dep_error = get_preview_render_dependency_error()
    if preview_dep_error:
        await send_text_screen(
            source,
            f"{tg_emoji('cross')} Генератор preview сейчас недоступен: отсутствует <code>rlottie-python</code>.",
            build_main_menu_keyboard(),
            edit=isinstance(source, CallbackQuery),
            ack=False,
        )
        await state.clear()
        return
    user = await db.get_user(source.from_user.id)
    balance = float(user.get("balance", 0.0))

    status = await send_text_screen(
        source,
        f"{tg_emoji('loading')} Генерирую passport preview...",
        ack=False,
    )
    try:
        preview_image = await build_passport_preview_image(bot, data)
    except Exception as error:
        await safe_delete_message(status.bot, status.chat.id, status.message_id)
        await send_text_screen(
            source,
            f"{tg_emoji('cross')} Не удалось собрать passport preview:\n<code>{escape(error)}</code>",
            build_passport_step_keyboard("passport_logo:back", "passport_logo:skip"),
            edit=isinstance(source, CallbackQuery),
            ack=False,
        )
        return
    finally:
        await safe_delete_message(status.bot, status.chat.id, status.message_id)

    await state.update_data(
        selected_color_hex=PASSPORT_COLOR_HEX,
        selected_color_label=PASSPORT_COLOR_LABEL,
    )
    await state.set_state(CreateStates.waiting_passport_preview)
    await send_photo_screen(
        source,
        preview_image,
        render_passport_final_preview_caption(
            text_value=str(data["user_text"]),
            username_value=str(data["passport_username"]),
            balance=balance,
        ),
        build_passport_preview_keyboard(balance),
        ack=False,
    )


async def open_color_preview(source: Message | CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if isinstance(source, CallbackQuery):
        await safe_answer_callback(source)
    data = await state.get_data()
    preview_dep_error = get_preview_render_dependency_error()
    if preview_dep_error:
        await send_text_screen(
            source,
            f"{tg_emoji('cross')} Генератор превью сейчас недоступен: отсутствует <code>rlottie-python</code>.\n"
            "Установите зависимости командой <code>pip install -r requirements.txt</code>.",
            build_main_menu_keyboard(),
            edit=isinstance(source, CallbackQuery),
            ack=False,
        )
        await state.clear()
        return

    stickers = await get_selected_stickers(bot, data)
    if not stickers:
        await send_text_screen(source, f"{tg_emoji('cross')} Не удалось загрузить выбранные шаблоны.", build_main_menu_keyboard(), ack=False)
        await state.clear()
        return

    await state.set_state(CreateStates.waiting_color)
    status = await send_text_screen(
        source,
        f"{tg_emoji('loading')} Генерирую превью цветов...\n\nПодождите ~3-5 секунд.",
        ack=False,
    )
    try:
        preview_image = await build_color_preview_image(
            bot=bot,
            template_key=str(data["template_key"]),
            sticker=stickers[0],
            text_value=str(data["user_text"]),
            font_id=str(data["font_id"]),
            logo_svg=data.get("logo_svg"),
        )
    finally:
        await safe_delete_message(status.bot, status.chat.id, status.message_id)

    await send_photo_screen(
        source,
        preview_image,
        render_color_caption(
            count=len(stickers),
            text_value=str(data["user_text"]),
            font_id=str(data["font_id"]),
            has_logo=bool(data.get("logo_svg")),
        ),
        build_color_keyboard(),
        ack=False,
    )


async def open_final_preview(
    source: Message | CallbackQuery,
    state: FSMContext,
    bot: Bot,
    color_hex: str,
    color_label: str,
) -> None:
    if isinstance(source, CallbackQuery):
        await safe_answer_callback(source)
    data = await state.get_data()
    stickers = await get_selected_stickers(bot, data)
    if not stickers:
        await send_text_screen(source, f"{tg_emoji('cross')} Не удалось загрузить шаблоны для превью.", build_main_menu_keyboard(), ack=False)
        await state.clear()
        return

    template = TEMPLATE_BY_KEY[data["template_key"]]
    user = await db.get_user(source.from_user.id)
    balance = float(user.get("balance", 0.0))
    pricing = calculate_pack_pricing(template.key, len(stickers))
    status = await send_text_screen(
        source,
        f"{tg_emoji('loading')} Генерация превью...\n\n{progress_bar(min(len(stickers), 16), max(len(stickers), 1))}",
        ack=False,
    )
    try:
        preview_image = await build_final_preview_image(
            bot=bot,
            template_key=template.key,
            stickers=stickers,
            text_value=str(data["user_text"]),
            color_hex=color_hex,
            font_id=str(data["font_id"]),
            logo_svg=data.get("logo_svg"),
        )
    finally:
        await safe_delete_message(status.bot, status.chat.id, status.message_id)

    await state.update_data(selected_color_hex=color_hex, selected_color_label=color_label)
    await state.set_state(CreateStates.waiting_color)
    await send_photo_screen(
        source,
        preview_image,
        render_final_preview_caption(
            template=template,
            count=len(stickers),
            text_value=str(data["user_text"]),
            color_label=color_label,
            color_hex=color_hex,
            font_id=str(data["font_id"]),
            has_logo=bool(data.get("logo_svg")),
            balance=balance,
        ),
        build_final_preview_keyboard(pricing["final_total"], balance),
        ack=False,
    )


async def _prepare_input_sticker(
    bot: Bot,
    user_id: int,
    sticker: Any,
    index: int,
    text_value: str,
    color_hex: str,
    font_id: str,
    logo_svg: Optional[bytes],
    secondary_text: Optional[str] = None,
) -> Tuple[int, Optional[InputSticker], Optional[Exception]]:
    try:
        file_id = str(sticker.file_id)
        raw_bytes = await _download_file(bot, file_id)
        customized = await _get_customized_tgs_cached(
            file_id,
            raw_bytes,
            text_value,
            color_hex,
            font_id,
            logo_svg,
            secondary_text,
        )
        if len(customized) > MAX_ANIMATED_STICKER_BYTES:
            raise ValueError(
                f"animated sticker is too big after customization: {len(customized)} bytes > {MAX_ANIMATED_STICKER_BYTES}"
            )
        input_sticker = await _upload_animated_emoji(
            user_id=user_id,
            data=customized,
            filename=f"emoji_{index}.tgs",
            emoji=str(getattr(sticker, "emoji", None) or "✨"),
        )
        return index, input_sticker, None
    except Exception as error:
        return index, None, error


async def create_pack_from_state(
    bot: Bot,
    message: Message,
    state: FSMContext,
    pack_base_name: str,
) -> None:
    data = await state.get_data()
    stickers = await get_selected_stickers(bot, data)
    if not stickers:
        await message.answer(f"{tg_emoji('cross')} Не удалось загрузить шаблоны для создания пака.")
        await state.clear()
        return

    template = TEMPLATE_BY_KEY[data["template_key"]]
    color_hex = str(data["selected_color_hex"])
    color_label = str(data["selected_color_label"])
    text_value = str(data["user_text"])
    font_id = str(data["font_id"])
    logo_svg = data.get("logo_svg")
    secondary_text = str(data.get("passport_username") or "") if is_passport_template_key(template.key) else None
    bot_username = await get_bot_username(bot)
    pricing = calculate_pack_pricing(template.key, len(stickers))
    charge_amount = 0.0 if CREATION_IS_FREE else float(pricing["final_total"])
    charged_amount = 0.0

    if charge_amount > 0.0:
        spend_ok, _ = await db.try_spend(message.from_user.id, charge_amount)
        if not spend_ok:
            await message.answer("Недостаточно средств для создания эмодзи-пака.")
            if is_passport_template_key(template.key):
                await open_passport_final_preview(message, state, bot)
            else:
                await open_final_preview(message, state, bot, color_hex, color_label)
            return
        charged_amount = charge_amount

    status = await message.answer(
        f"{tg_emoji('loading')} Создаю эмодзи-пак...\n\nПодготовка шаблонов."
    )
    await state.set_state(CreateStates.processing)

    prepared_by_index: Dict[int, InputSticker] = {}
    skipped: List[int] = []
    prepare_errors: List[Exception] = []
    prepare_semaphore = asyncio.Semaphore(PACK_PREPARE_CONCURRENCY)

    async def prepare_with_limit(index: int, sticker: Any) -> Tuple[int, Optional[InputSticker], Optional[Exception]]:
        async with prepare_semaphore:
            return await _prepare_input_sticker(
                bot=bot,
                user_id=message.from_user.id,
                sticker=sticker,
                index=index,
                text_value=text_value,
                color_hex=color_hex,
                font_id=font_id,
                logo_svg=logo_svg,
                secondary_text=secondary_text,
            )

    tasks = [
        asyncio.create_task(prepare_with_limit(index, sticker))
        for index, sticker in enumerate(stickers, start=1)
    ]
    completed = 0
    progress_step = max(1, len(stickers) // 8)

    for future in asyncio.as_completed(tasks):
        index, input_sticker, error = await future
        completed += 1
        if input_sticker is None:
            skipped.append(index)
            if error is not None:
                prepare_errors.append(error)
                log.warning("template sticker %d/%d failed: %s", index, len(stickers), error)
        else:
            prepared_by_index[index] = input_sticker

        if completed == len(stickers) or completed % progress_step == 0:
            await safe_edit_message(
                status,
                f"{tg_emoji('loading')} Создаю эмодзи-пак...\n\n{progress_bar(completed, len(stickers))}",
            )

    prepared_stickers: List[InputSticker] = [prepared_by_index[index] for index in sorted(prepared_by_index)]

    for index, sticker in []:
        try:
            raw_bytes = await _download_file(bot, sticker.file_id)
            customized = customize_tgs_template(
                tgs_bytes=raw_bytes,
                text=text_value,
                hex_color=color_hex,
                font_id=font_id,
                logo_svg=logo_svg,
            )
            input_sticker = await _upload_animated_emoji(
                user_id=message.from_user.id,
                data=customized,
                filename=f"emoji_{index}.tgs",
                emoji=str(getattr(sticker, "emoji", None) or "✨"),
            )
            if input_sticker is None:
                skipped.append(index)
                continue
            prepared_stickers.append(input_sticker)
        except Exception as error:
            skipped.append(index)
            log.warning("template sticker %d/%d failed: %s", index, len(stickers), error)

        if index == len(stickers) or index % max(1, len(stickers) // 8) == 0:
            await safe_edit_message(
                status,
                f"{tg_emoji('loading')} Создаю эмодзи-пак...\n\n{progress_bar(index, len(stickers))}",
            )

    if not prepared_stickers:
        if charged_amount > 0.0:
            await db.refund_spend(message.from_user.id, charged_amount)
        await safe_edit_message(
            status,
            render_prepare_failure_text(template.key, prepare_errors),
        )
        await state.clear()
        return

    pack_title = trim_pack_title(f"{template.title} • {get_creation_text_label(text_value)}")
    try:
        actual_pack_name = await _create_pack_with_retry(
            bot=bot,
            user_id=message.from_user.id,
            base_name=pack_base_name,
            bot_username=bot_username,
            pack_title=pack_title,
            stickers=prepared_stickers,
        )
    except Exception as error:
        if charged_amount > 0.0:
            await db.refund_spend(message.from_user.id, charged_amount)
        await safe_edit_message(status, f"{tg_emoji('cross')} Ошибка создания пака:\n<code>{escape(error)}</code>")
        await state.clear()
        return

    pack_link = f"https://t.me/addemoji/{actual_pack_name}"
    created_at = datetime.now().strftime("%d.%m.%Y %H:%M")
    title_text = get_creation_text_label(text_value)
    await db.add_pack(
        message.from_user.id,
        {
            "name": actual_pack_name,
            "title": f"{template.title} • {title_text}",
            "template_key": template.key,
            "color_label": color_label,
            "color_hex": color_hex,
            "text": text_value,
            "count": len(prepared_stickers),
            "link": pack_link,
            "created_at": created_at,
        },
    )

    result_lines = [
        f"{tg_emoji('check')} <b>Пак готов!</b>",
        "",
        f"{tg_emoji(template.button_icon)} Пак: <b>{template.title}</b>",
        f"{tg_emoji('text')} Текст: <code>{escape(title_text)}</code>",
        f"{tg_emoji('brush')} Цвет: <b>{escape(color_label)}</b> <code>{escape(color_hex)}</code>",
        f"{tg_emoji('smile')} Эмодзи: <b>{len(prepared_stickers)}</b>",
        f"{tg_emoji('link')} Ссылка: <a href=\"{pack_link}\">{pack_link}</a>",
    ]
    if skipped:
        result_lines.extend(["", f"{tg_emoji('info')} Пропущено шаблонов: <b>{len(skipped)}</b>."])

    await safe_edit_message(
        status,
        "\n".join(result_lines),
        reply_markup=build_created_pack_keyboard(pack_link),
    )
    await state.clear()


async def _call_bot_api(
    label: str,
    operation: Callable[..., Awaitable[T]],
    *args: Any,
    max_retries: int = BOT_API_RETRY_LIMIT,
    **kwargs: Any,
) -> T:
    attempt = 0
    while True:
        try:
            return await operation(*args, **kwargs)
        except TelegramRetryAfter as error:
            if attempt >= max_retries:
                raise
            delay = max(float(getattr(error, "retry_after", 1.0)), 1.0) + BOT_API_RETRY_AFTER_PAD
            attempt += 1
            log.warning("%s flood control, retry in %.1fs (%d/%d)", label, delay, attempt, max_retries)
            await asyncio.sleep(delay)
        except (TelegramNetworkError, TelegramServerError, RestartingTelegram) as error:
            if attempt >= max_retries:
                raise
            delay = min(BOT_API_RETRY_BASE_DELAY * (2 ** attempt), BOT_API_RETRY_MAX_DELAY)
            attempt += 1
            log.warning(
                "%s transient Telegram error, retry in %.1fs (%d/%d): %s",
                label,
                delay,
                attempt,
                max_retries,
                error,
            )
            await asyncio.sleep(delay)


async def _download_file(bot: Bot, file_id: str) -> bytes:
    cached = _cache_get(_FILE_BYTES_CACHE, file_id, FILE_BYTES_CACHE_TTL)
    if cached is not None:
        return cached

    async def load_file() -> bytes:
        telegram_file = await _call_bot_api("get_file", bot.get_file, file_id)
        buffer: io.BytesIO = await _call_bot_api("download_file", bot.download_file, telegram_file.file_path)
        data = buffer.read()
        return _cache_put(_FILE_BYTES_CACHE, file_id, data, FILE_BYTES_CACHE_MAX)

    return await _run_singleflight(_INFLIGHT_FILE_BYTES_TASKS, file_id, load_file)


async def _download_thumbnail(bot: Bot, sticker: Any) -> bytes:
    thumbnail = getattr(sticker, "thumbnail", None)
    if thumbnail is None:
        image = Image.new("RGBA", (512, 512), (0, 0, 0, 0))
        buffer = io.BytesIO()
        image.save(buffer, format="PNG")
        return buffer.getvalue()
    return await _download_file(bot, thumbnail.file_id)


def _guess_upload_content_type(input_file: BufferedInputFile) -> str:
    filename = (input_file.filename or "").lower()
    if filename.endswith(".tgs"):
        return "application/x-tgsticker"
    if filename.endswith(".webp"):
        return "image/webp"
    if filename.endswith(".webm"):
        return "video/webm"
    return "application/octet-stream"


async def _raw_bot_api_request(
    method_name: str,
    fields: Dict[str, Any],
    files: Optional[List[Tuple[str, BufferedInputFile]]] = None,
    max_retries: int = BOT_API_RETRY_LIMIT,
) -> Any:
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method_name}"
    attempt = 0

    while True:
        form = aiohttp.FormData()
        for key, value in fields.items():
            if value is None:
                continue
            if isinstance(value, (dict, list)):
                form.add_field(key, json.dumps(value, ensure_ascii=False))
            else:
                form.add_field(key, str(value))

        for field_name, input_file in files or []:
            form.add_field(
                field_name,
                input_file.data,
                filename=input_file.filename,
                content_type=_guess_upload_content_type(input_file),
            )

        try:
            session = await _get_raw_api_session()
            async with session.post(url, data=form) as response:
                payload = await response.json(content_type=None)
        except (aiohttp.ClientError, asyncio.TimeoutError) as error:
            if attempt >= max_retries:
                raise RuntimeError(f"Telegram HTTP request failed on {method_name}: {error}") from error
            delay = min(BOT_API_RETRY_BASE_DELAY * (2 ** attempt), BOT_API_RETRY_MAX_DELAY)
            attempt += 1
            await asyncio.sleep(delay)
            continue

        if payload.get("ok"):
            return payload.get("result")

        description = payload.get("description", "Unknown Telegram error")
        retry_after = ((payload.get("parameters") or {}).get("retry_after"))
        if retry_after is not None and attempt < max_retries:
            delay = max(float(retry_after), 1.0) + BOT_API_RETRY_AFTER_PAD
            attempt += 1
            await asyncio.sleep(delay)
            continue

        raise RuntimeError(f"Telegram server says: {description}")


def _input_sticker_to_payload(sticker: InputSticker) -> Dict[str, Any]:
    if not isinstance(sticker.sticker, str):
        raise RuntimeError("Animated emoji sticker payload must use uploaded file_id")
    return {
        "sticker": sticker.sticker,
        "format": sticker.format,
        "emoji_list": sticker.emoji_list,
    }


async def _upload_animated_emoji(
    user_id: int,
    data: bytes,
    filename: str,
    emoji: str,
) -> Optional[InputSticker]:
    if len(data) > MAX_ANIMATED_STICKER_BYTES:
        log.warning("animated emoji skipped because payload is too large: %d bytes", len(data))
        return None

    uploaded = await _raw_bot_api_request(
        "uploadStickerFile",
        fields={"user_id": user_id, "sticker_format": "animated"},
        files=[("sticker", BufferedInputFile(data, filename=filename))],
    )
    file_id = uploaded.get("file_id") if isinstance(uploaded, dict) else None
    if not file_id:
        raise RuntimeError("uploadStickerFile did not return file_id")

    return InputSticker(sticker=file_id, emoji_list=[emoji or "✨"], format="animated")


async def _create_pack(
    user_id: int,
    pack_name: str,
    pack_title: str,
    stickers: Sequence[InputSticker],
) -> None:
    create_payload = [_input_sticker_to_payload(sticker) for sticker in stickers[:50]]
    await _raw_bot_api_request(
        "createNewStickerSet",
        fields={
            "user_id": user_id,
            "name": pack_name,
            "title": pack_title,
            "stickers": create_payload,
            "sticker_type": "custom_emoji",
        },
    )

    for sticker in stickers[50:]:
        await _raw_bot_api_request(
            "addStickerToSet",
            fields={
                "user_id": user_id,
                "name": pack_name,
                "sticker": _input_sticker_to_payload(sticker),
            },
        )


def normalize_pack_base_name(raw_value: str, bot_username: str) -> str:
    value = raw_value.strip().lower()
    value = re.sub(r"^(?:https?://)?t\.me/addemoji/", "", value)
    value = re.sub(rf"_by_{re.escape(bot_username.lower())}$", "", value)
    value = re.sub(r"[^a-z0-9_]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value


def _build_pack_name(base_name: str, bot_username: str, attempt: int = 0) -> str:
    bot_suffix = f"_by_{bot_username.lower()}"
    serial_suffix = "" if attempt == 0 else f"_{attempt + 1}"
    max_base_length = PACK_NAME_MAX_LEN - len(bot_suffix) - len(serial_suffix)
    trimmed = normalize_pack_base_name(base_name, bot_username)
    if trimmed and not trimmed[0].isalpha():
        trimmed = f"pack_{trimmed}"
    trimmed = trimmed[:max_base_length].strip("_") if max_base_length > 0 else ""
    trimmed = re.sub(r"_+", "_", trimmed).strip("_")
    if not trimmed or not trimmed[0].isalpha():
        trimmed = ("pack"[:max_base_length] if max_base_length > 0 else "") or "p"
    pack_name = f"{trimmed}{serial_suffix}{bot_suffix}"
    if not validate_short_name(pack_name):
        raise RuntimeError(f"Generated invalid sticker set name: {pack_name}")
    return pack_name


def _is_pack_name_occupied_error(error: Exception) -> bool:
    text = str(error).lower()
    return (
        "sticker set name is already occupied" in text
        or "short name is already taken" in text
        or "name is already occupied" in text
    )


def _is_pack_missing_error(error: Exception) -> bool:
    text = str(error).lower()
    return "stickerset_invalid" in text or "sticker set not found" in text


async def _pack_name_exists(bot: Bot, pack_name: str) -> bool:
    try:
        await _call_bot_api("get_sticker_set", bot.get_sticker_set, pack_name)
        return True
    except TelegramBadRequest as error:
        if _is_pack_missing_error(error):
            return False
        raise


async def _create_pack_with_retry(
    bot: Bot,
    user_id: int,
    base_name: str,
    bot_username: str,
    pack_title: str,
    stickers: Sequence[InputSticker],
) -> str:
    last_error: Optional[Exception] = None
    for attempt in range(PACK_NAME_RETRY_LIMIT):
        pack_name = _build_pack_name(base_name, bot_username, attempt)
        if await _pack_name_exists(bot, pack_name):
            continue
        try:
            await _create_pack(user_id, pack_name, pack_title, stickers)
            return pack_name
        except Exception as error:
            if _is_pack_name_occupied_error(error):
                last_error = error
                continue
            raise
    if last_error is not None:
        raise last_error
    raise RuntimeError("Unable to allocate a free emoji pack name")


@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext, bot: Bot, command: CommandObject) -> None:
    referrer_id = parse_referrer(command)
    await state.clear()
    await db.ensure_user(
        tg_user_id=message.from_user.id,
        first_name=message.from_user.first_name,
        username=message.from_user.username,
        referrer_id=referrer_id,
    )
    await open_main_menu(message, state, bot)


@router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext, bot: Bot) -> None:
    await state.clear()
    await open_main_menu(message, state, bot)


@router.callback_query(F.data == "menu:main")
async def cb_main_menu(call: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    await open_main_menu(call, state, bot)


@router.callback_query(F.data == "menu:profile")
async def cb_profile(call: CallbackQuery) -> None:
    await open_profile(call)


@router.callback_query(F.data == "menu:support")
async def cb_support(call: CallbackQuery) -> None:
    await send_text_screen(call, render_support_text(), build_support_keyboard(), edit=True)


@router.callback_query(F.data == "menu:info")
async def cb_info(call: CallbackQuery) -> None:
    await send_text_screen(call, render_info_text(), build_info_keyboard(), edit=True)


@router.callback_query(F.data == "menu:giveaway")
async def cb_giveaway(call: CallbackQuery) -> None:
    await call.answer("🎁 Розыгрышей пока нет. Следите за обновлениями!", show_alert=True)


@router.callback_query(F.data == "profile:packs")
async def cb_profile_packs(call: CallbackQuery) -> None:
    user = await db.get_user(call.from_user.id)
    packs = user.get("packs", [])
    if not packs:
        await call.answer(
            "📦 У тебя пока нет стикерпаков\n\nСоздай свой первый стикер через главное меню!\nПак создастся автоматически.",
            show_alert=True,
        )
        return
    await send_text_screen(
        call,
        f"{tg_emoji('box')} <b>Мои эмодзи-паки</b>\n\nВыбери пак для открытия:",
        build_user_packs_keyboard(packs),
    )


@router.callback_query(F.data == "profile:balance")
async def cb_profile_balance(call: CallbackQuery) -> None:
    await send_text_screen(call, render_balance_text(), build_balance_keyboard())


@router.callback_query(F.data.in_({"balance:stars", "balance:crypto", "balance:sbp"}))
async def cb_balance_methods(call: CallbackQuery) -> None:
    await call.answer("В этой версии бота пополнение отключено. Доступно только создание эмодзи.", show_alert=True)


@router.callback_query(F.data == "profile:coupon")
async def cb_profile_coupon(call: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(CouponStates.waiting_code)
    await send_text_screen(call, render_coupon_text(), build_coupon_keyboard())


@router.callback_query(F.data == "coupon:cancel")
async def cb_coupon_cancel(call: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await open_profile(call)


@router.message(StateFilter(CouponStates.waiting_code), F.text)
async def msg_coupon_code(message: Message, state: FSMContext, bot: Bot) -> None:
    await safe_delete_user_message(message)
    await state.clear()
    info = await message.answer(
        f"{tg_emoji('tag')} Купоны в этой версии отключены. Основной сценарий бота теперь только создание эмодзи."
    )
    await asyncio.sleep(1.0)
    await safe_delete_message(bot, info.chat.id, info.message_id)
    await open_profile(message)


@router.callback_query(F.data == "profile:history")
async def cb_profile_history(call: CallbackQuery) -> None:
    user = await db.get_user(call.from_user.id)
    await send_text_screen(call, render_history_text(user.get("operations", [])), build_history_back_keyboard())


@router.callback_query(F.data == "profile:referral")
async def cb_profile_referral(call: CallbackQuery, bot: Bot) -> None:
    await safe_answer_callback(call)
    user = await db.get_user(call.from_user.id)
    await send_text_screen(
        call,
        render_referral_text(user, await get_bot_username(bot)),
        build_referral_back_keyboard(),
        ack=False,
    )


@router.callback_query(F.data == "profile:font")
async def cb_profile_font(call: CallbackQuery) -> None:
    user = await db.get_user(call.from_user.id)
    await send_text_screen(
        call,
        render_fonts_text(str(user.get("font_id", DEFAULT_FONT_ID))),
        build_fonts_keyboard(str(user.get("font_id", DEFAULT_FONT_ID))),
    )


@router.callback_query(F.data.startswith("font:set:"))
async def cb_font_select(call: CallbackQuery) -> None:
    font_id = call.data.split(":")[-1]
    if font_id not in FONT_PRESETS:
        await call.answer("Неизвестный шрифт.", show_alert=True)
        return
    user = await db.set_font(call.from_user.id, font_id)
    await send_text_screen(
        call,
        render_fonts_text(str(user.get("font_id", DEFAULT_FONT_ID))),
        build_fonts_keyboard(str(user.get("font_id", DEFAULT_FONT_ID))),
        edit=True,
    )


@router.callback_query(F.data.in_({"info:privacy", "info:terms"}))
async def cb_missing_info_doc(call: CallbackQuery) -> None:
    await call.answer("Ссылка на документ пока не настроена.", show_alert=True)


@router.callback_query(F.data == "create:open")
async def cb_create_open(call: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    await open_create_menu(call, state, bot)


@router.callback_query(F.data.startswith("create:template:"))
async def cb_create_template(call: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    template_key = call.data.split(":")[-1]
    template = TEMPLATE_BY_KEY.get(template_key)
    if template is None:
        await call.answer("Неизвестный шаблон.", show_alert=True)
        return

    await safe_answer_callback(call)
    count = await get_template_count(bot, template.short_name)
    user = await db.get_user(call.from_user.id)
    await state.update_data(
        template_key=template.key,
        template_count=count,
        font_id=DEFAULT_FONT_ID if is_passport_template_key(template.key) else str(user.get("font_id", DEFAULT_FONT_ID)),
        mode=None,
        selected_indices=[],
        user_text=None,
        passport_username=None,
        logo_svg=None,
        selected_color_hex=None,
        selected_color_label=None,
    )
    if is_passport_template_key(template.key):
        await state.update_data(
            mode="all",
            selected_color_hex=PASSPORT_COLOR_HEX,
            selected_color_label=PASSPORT_COLOR_LABEL,
        )
        await open_passport_name_prompt(call, state, ack=False)
        return

    await state.set_state(CreateStates.waiting_text)
    await send_text_screen(
        call,
        render_template_detail_text(template, count),
        build_mode_keyboard(template.key, count),
        ack=False,
    )


@router.callback_query(F.data.startswith("create:mode:"))
async def cb_create_mode(call: CallbackQuery, state: FSMContext) -> None:
    _, _, template_key, mode = call.data.split(":")
    data = await state.get_data()
    template = TEMPLATE_BY_KEY.get(template_key)
    if template is None:
        await call.answer("Неизвестный шаблон.", show_alert=True)
        return

    template_count = int(data.get("template_count", 0))
    await state.update_data(template_key=template_key, mode=mode)

    if mode == "all":
        await state.update_data(selected_indices=[])
        await open_text_prompt(call, state)
        return

    await state.set_state(CreateStates.selecting_templates)
    await state.update_data(selected_indices=[], selector_page=0)
    await send_text_screen(
        call,
        render_selector_text(template, mode, template_count, []),
        build_selector_keyboard(template_count, [], 0, mode),
    )


@router.callback_query(StateFilter(CreateStates.selecting_templates), F.data.startswith("pick:page:"))
async def cb_pick_page(call: CallbackQuery, state: FSMContext) -> None:
    page = int(call.data.split(":")[-1])
    data = await state.get_data()
    template = TEMPLATE_BY_KEY[data["template_key"]]
    selected = list(data.get("selected_indices", []))
    mode = str(data.get("mode"))
    total = int(data.get("template_count", 0))
    await state.update_data(selector_page=page)
    await send_text_screen(
        call,
        render_selector_text(template, mode, total, selected),
        build_selector_keyboard(total, selected, page, mode),
        edit=True,
    )


@router.callback_query(StateFilter(CreateStates.selecting_templates), F.data.startswith("pick:toggle:"))
async def cb_pick_toggle(call: CallbackQuery, state: FSMContext) -> None:
    _, _, raw_index, raw_page = call.data.split(":")
    index = int(raw_index)
    page = int(raw_page)
    data = await state.get_data()
    selected = list(data.get("selected_indices", []))
    mode = str(data.get("mode"))
    total = int(data.get("template_count", 0))
    template = TEMPLATE_BY_KEY[data["template_key"]]

    if mode == "one":
        selected = [index]
    else:
        if index in selected:
            selected.remove(index)
        else:
            if len(selected) >= MULTI_SELECT_LIMIT:
                await call.answer(f"Можно выбрать максимум {MULTI_SELECT_LIMIT} шаблонов.", show_alert=True)
                return
            selected.append(index)

    selected.sort()
    await state.update_data(selected_indices=selected, selector_page=page)
    await send_text_screen(
        call,
        render_selector_text(template, mode, total, selected),
        build_selector_keyboard(total, selected, page, mode),
        edit=True,
    )


@router.callback_query(StateFilter(CreateStates.selecting_templates), F.data == "pick:done")
async def cb_pick_done(call: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    selected = list(data.get("selected_indices", []))
    if not selected:
        await call.answer("Сначала выбери хотя бы один шаблон.", show_alert=True)
        return
    await open_text_prompt(call, state)


@router.callback_query(StateFilter(CreateStates.selecting_templates), F.data == "pick:back")
async def cb_pick_back(call: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    template = TEMPLATE_BY_KEY[data["template_key"]]
    template_count = int(data.get("template_count", 0))
    await send_text_screen(call, render_template_detail_text(template, template_count), build_mode_keyboard(template.key, template_count))


@router.callback_query(StateFilter(CreateStates.waiting_passport_name), F.data == "passport_name:back")
async def cb_passport_name_back(call: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    await open_create_menu(call, state, bot)


@router.callback_query(StateFilter(CreateStates.waiting_passport_name), F.data == "passport_name:skip")
async def cb_passport_name_skip(call: CallbackQuery, state: FSMContext) -> None:
    passport_name = get_default_passport_name(call.from_user)
    await state.update_data(user_text=passport_name)
    await open_passport_username_prompt(call, state)


@router.message(StateFilter(CreateStates.waiting_passport_name), F.text)
async def msg_passport_name(message: Message, state: FSMContext) -> None:
    raw_name = re.sub(r"\s+", " ", message.text.strip())
    passport_name = _clean_passport_name(raw_name)
    if not passport_name:
        await message.answer(f"{tg_emoji('cross')} Никнейм не может быть пустым.")
        return
    if len(raw_name) > 14:
        await message.answer(f"{tg_emoji('cross')} Максимум 14 символов.")
        return

    await state.update_data(user_text=passport_name)
    await safe_delete_user_message(message)
    await open_passport_username_prompt(message, state)


@router.message(StateFilter(CreateStates.waiting_passport_name))
async def msg_passport_name_invalid(message: Message) -> None:
    await message.answer(f"{tg_emoji('cross')} Отправь никнейм обычным текстом.")


@router.callback_query(StateFilter(CreateStates.waiting_passport_username), F.data == "passport_username:back")
async def cb_passport_username_back(call: CallbackQuery, state: FSMContext) -> None:
    await open_passport_name_prompt(call, state)


@router.callback_query(StateFilter(CreateStates.waiting_passport_username), F.data == "passport_username:skip")
async def cb_passport_username_skip(call: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    passport_name = str(data.get("user_text") or get_default_passport_name(call.from_user))
    await state.update_data(passport_username=get_default_passport_username(call.from_user, passport_name))
    await open_passport_logo_prompt(call, state)


@router.message(StateFilter(CreateStates.waiting_passport_username), F.text)
async def msg_passport_username(message: Message, state: FSMContext) -> None:
    raw_username = message.text.strip()
    raw_username_body = raw_username[1:] if raw_username.startswith("@") else raw_username
    normalized_body = re.sub(r"[^A-Za-z0-9_]", "", raw_username_body)
    passport_username = _clean_passport_username(raw_username)
    if not passport_username:
        await message.answer(f"{tg_emoji('cross')} Username должен содержать только латиницу, цифры и _.")
        return
    if len(normalized_body) > 32:
        await message.answer(f"{tg_emoji('cross')} Максимум 32 символа без @.")
        return

    await state.update_data(passport_username=passport_username)
    await safe_delete_user_message(message)
    await open_passport_logo_prompt(message, state)


@router.message(StateFilter(CreateStates.waiting_passport_username))
async def msg_passport_username_invalid(message: Message) -> None:
    await message.answer(f"{tg_emoji('cross')} Отправь username обычным текстом.")


@router.callback_query(StateFilter(CreateStates.waiting_passport_logo), F.data == "passport_logo:back")
async def cb_passport_logo_back(call: CallbackQuery, state: FSMContext) -> None:
    await open_passport_username_prompt(call, state)


@router.callback_query(StateFilter(CreateStates.waiting_passport_logo), F.data == "passport_logo:skip")
async def cb_passport_logo_skip(call: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    await open_passport_final_preview(call, state, bot)


@router.message(StateFilter(CreateStates.waiting_passport_logo), F.document)
async def msg_passport_logo_upload(message: Message, state: FSMContext, bot: Bot) -> None:
    document = message.document
    name = (document.file_name or "").lower()
    mime = (document.mime_type or "").lower()
    if not name.endswith(".svg") and mime != "image/svg+xml":
        await message.answer(f"{tg_emoji('cross')} Нужен именно SVG-файл, отправленный как документ.")
        return

    svg_bytes = await _download_file(bot, document.file_id)
    try:
        validate_svg_logo(svg_bytes)
    except Exception as error:
        await message.answer(f"{tg_emoji('cross')} SVG не подошёл:\n<code>{escape(error)}</code>")
        return

    await state.update_data(logo_svg=svg_bytes)
    await safe_delete_user_message(message)
    await open_passport_final_preview(message, state, bot)


@router.message(StateFilter(CreateStates.waiting_passport_logo))
async def msg_passport_logo_invalid(message: Message) -> None:
    await message.answer(f"{tg_emoji('cross')} Отправь SVG-файл как документ или нажми «Пропустить».")


@router.callback_query(StateFilter(CreateStates.waiting_text), F.data == "text:back")
async def cb_text_back(call: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    data = await state.get_data()
    template = TEMPLATE_BY_KEY.get(str(data.get("template_key")))
    if template is None:
        await open_create_menu(call, state, bot)
        return
    template_count = int(data.get("template_count", 0))
    await send_text_screen(call, render_template_detail_text(template, template_count), build_mode_keyboard(template.key, template_count))


@router.callback_query(StateFilter(CreateStates.waiting_text), F.data == "text:skip")
async def cb_text_skip(call: CallbackQuery, state: FSMContext) -> None:
    await state.update_data(user_text="")
    await open_logo_choice(call, state)


@router.message(StateFilter(CreateStates.waiting_text), F.text)
async def msg_creation_text(message: Message, state: FSMContext) -> None:
    text_value = message.text.strip()
    if not text_value:
        await message.answer(f"{tg_emoji('cross')} Текст не может быть пустым.")
        return
    if len(text_value) > 20:
        await message.answer(f"{tg_emoji('cross')} Максимум 20 символов.")
        return

    await state.update_data(user_text=text_value)
    await safe_delete_user_message(message)
    await open_logo_choice(message, state)


@router.message(StateFilter(CreateStates.waiting_text))
async def msg_creation_text_invalid(message: Message) -> None:
    await message.answer(f"{tg_emoji('cross')} Отправь текст сообщением. Максимум 20 символов.")


@router.callback_query(StateFilter(CreateStates.waiting_logo_choice), F.data == "logo:add")
async def cb_logo_add(call: CallbackQuery, state: FSMContext) -> None:
    dep_error = get_svg_dependency_error()
    if dep_error:
        await call.answer("SVG-парсер не установлен. Выполните pip install -r requirements.txt.", show_alert=True)
        return
    await state.set_state(CreateStates.waiting_logo_upload)
    await send_text_screen(call, render_logo_upload_text(), build_logo_upload_keyboard())


@router.callback_query(StateFilter(CreateStates.waiting_logo_upload), F.data == "logo_upload:back")
async def cb_logo_upload_back(call: CallbackQuery, state: FSMContext) -> None:
    await open_logo_choice(call, state)


@router.message(StateFilter(CreateStates.waiting_logo_upload), F.document)
async def msg_logo_upload(message: Message, state: FSMContext, bot: Bot) -> None:
    document = message.document
    name = (document.file_name or "").lower()
    mime = (document.mime_type or "").lower()
    if not name.endswith(".svg") and mime != "image/svg+xml":
        await message.answer(f"{tg_emoji('cross')} Нужен именно SVG-файл, отправленный как документ.")
        return

    svg_bytes = await _download_file(bot, document.file_id)
    try:
        validate_svg_logo(svg_bytes)
    except Exception as error:
        await message.answer(f"{tg_emoji('cross')} SVG не подошёл:\n<code>{escape(error)}</code>")
        return

    await state.update_data(logo_svg=svg_bytes)
    await safe_delete_user_message(message)
    await message.answer(f"{tg_emoji('check')} SVG-логотип загружен.")
    await open_color_preview(message, state, bot)


@router.message(StateFilter(CreateStates.waiting_logo_upload))
async def msg_logo_upload_invalid(message: Message) -> None:
    await message.answer(f"{tg_emoji('cross')} Отправь SVG-файл именно как документ.")


@router.callback_query(StateFilter(CreateStates.waiting_logo_choice), F.data == "logo:skip")
async def cb_logo_skip(call: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    data = await state.get_data()
    if not str(data.get("user_text") or "").strip() and not data.get("logo_svg"):
        await call.answer("Нужен текст или SVG логотип.", show_alert=True)
        return
    await open_color_preview(call, state, bot)


@router.callback_query(StateFilter(CreateStates.waiting_logo_choice), F.data == "logo:back")
async def cb_logo_back(call: CallbackQuery, state: FSMContext) -> None:
    await open_text_prompt(call, state)


@router.callback_query(StateFilter(CreateStates.waiting_color), F.data == "color:back")
async def cb_color_back(call: CallbackQuery, state: FSMContext) -> None:
    await open_logo_choice(call, state)


@router.callback_query(StateFilter(CreateStates.waiting_color), F.data == "color:custom")
async def cb_color_custom(call: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(CreateStates.waiting_custom_color)
    await send_text_screen(
        call,
        f"{tg_emoji('brush')} Отправь HEX-цвет в формате <code>#RRGGBB</code>, например <code>#FF6A00</code>.",
        InlineKeyboardMarkup(inline_keyboard=[
            [ikb("◁ Назад", callback_data="custom_color:back")],
        ]),
    )


@router.callback_query(StateFilter(CreateStates.waiting_custom_color), F.data == "custom_color:back")
async def cb_custom_color_back(call: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    await open_color_preview(call, state, bot)


@router.message(StateFilter(CreateStates.waiting_custom_color), F.text)
async def msg_custom_color(message: Message, state: FSMContext, bot: Bot) -> None:
    raw_value = message.text.strip()
    hex_value = raw_value if raw_value.startswith("#") else f"#{raw_value}"
    if not len(hex_value) == 7 or any(char not in "0123456789abcdefABCDEF#" for char in hex_value):
        await message.answer(f"{tg_emoji('cross')} Неверный формат. Используй HEX вроде <code>#A23BFF</code>.")
        return
    await safe_delete_user_message(message)
    await open_final_preview(message, state, bot, hex_value.upper(), "Свой цвет")


@router.message(StateFilter(CreateStates.waiting_custom_color))
async def msg_custom_color_invalid(message: Message) -> None:
    await message.answer(f"{tg_emoji('cross')} Отправь HEX-цвет обычным текстом, например <code>#FF6A00</code>.")


@router.callback_query(StateFilter(CreateStates.waiting_color), F.data.startswith("color:choose:"))
async def cb_color_choose(call: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    color_key = call.data.split(":")[-1]
    color = COLOR_BY_KEY.get(color_key)
    if color is None:
        await call.answer("Неизвестный цвет.", show_alert=True)
        return
    await open_final_preview(call, state, bot, color.hex_value, color.label)


@router.callback_query(StateFilter(CreateStates.waiting_color), F.data == "preview:colors")
async def cb_preview_colors(call: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    await open_color_preview(call, state, bot)


@router.callback_query(StateFilter(CreateStates.waiting_color), F.data == "preview:back")
async def cb_preview_back(call: CallbackQuery, state: FSMContext) -> None:
    await open_logo_choice(call, state)


@router.callback_query(StateFilter(CreateStates.waiting_color), F.data == "preview:create")
async def cb_preview_create(call: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    data = await state.get_data()
    if not data.get("selected_color_hex"):
        await call.answer("Сначала выбери цвет и дождись финального превью.", show_alert=True)
        return
    if not CREATION_IS_FREE:
        price = calculate_pack_pricing(str(data.get("template_key") or ""), get_selected_count(data))["final_total"]
        user = await db.get_user(call.from_user.id)
        if float(user.get("balance", 0.0)) + 1e-9 < price:
            await call.answer("Недостаточно средств. Пополните баланс.", show_alert=True)
            return
    await state.set_state(CreateStates.waiting_pack_slug)
    await safe_answer_callback(call)
    await send_text_screen(
        call,
        render_pack_slug_text(await get_bot_username(bot)),
        build_pack_slug_keyboard(),
        ack=False,
    )


@router.callback_query(StateFilter(CreateStates.waiting_color), F.data == "preview:cancel")
async def cb_preview_cancel(call: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    await open_main_menu(call, state, bot)


@router.callback_query(StateFilter(CreateStates.waiting_passport_preview), F.data == "passport_preview:create")
async def cb_passport_preview_create(call: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if not CREATION_IS_FREE:
        user = await db.get_user(call.from_user.id)
        if float(user.get("balance", 0.0)) + 1e-9 < PASSPORT_PRICE_RUB:
            await call.answer("Недостаточно средств. Пополните баланс.", show_alert=True)
            return
    await state.set_state(CreateStates.waiting_pack_slug)
    await safe_answer_callback(call)
    await send_text_screen(
        call,
        render_pack_slug_text(await get_bot_username(bot)),
        build_pack_slug_keyboard(),
        ack=False,
    )


@router.callback_query(StateFilter(CreateStates.waiting_passport_preview), F.data == "passport_preview:cancel")
async def cb_passport_preview_cancel(call: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    await open_main_menu(call, state, bot)


@router.callback_query(StateFilter(CreateStates.waiting_pack_slug), F.data == "slug:back")
async def cb_slug_back(call: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    data = await state.get_data()
    if is_passport_template_key(data.get("template_key")):
        await open_passport_final_preview(call, state, bot)
        return
    await open_final_preview(call, state, bot, str(data["selected_color_hex"]), str(data["selected_color_label"]))


@router.message(StateFilter(CreateStates.waiting_pack_slug), F.text)
async def msg_pack_slug(message: Message, state: FSMContext, bot: Bot) -> None:
    bot_username = await get_bot_username(bot)
    pack_base_name = normalize_pack_base_name(message.text, bot_username)
    if not pack_base_name:
        await message.answer(f"{tg_emoji('cross')} Допустимы только <code>a-z</code>, <code>0-9</code> и <code>_</code>.")
        return
    await safe_delete_user_message(message)
    if not validate_short_name(_build_pack_name(pack_base_name, bot_username)):
        await message.answer(
            f"{tg_emoji('cross')} Имя пака не подходит для Telegram. "
            "Используй только <code>a-z</code>, <code>0-9</code> и <code>_</code>, "
            "а имя должно начинаться с буквы."
        )
        return
    await create_pack_from_state(bot, message, state, pack_base_name)


@router.message(StateFilter(CreateStates.waiting_pack_slug))
async def msg_pack_slug_invalid(message: Message) -> None:
    await message.answer(f"{tg_emoji('cross')} Отправь короткое имя ссылки обычным текстом.")


@router.message(StateFilter(CreateStates.processing))
async def msg_processing_lock(message: Message) -> None:
    await message.answer(f"{tg_emoji('loading')} Идёт обработка. Подожди, пока бот соберёт эмодзи-пак.")


@router.callback_query(StateFilter(CreateStates.processing))
async def cb_processing_lock(call: CallbackQuery) -> None:
    await call.answer("Идёт обработка. Подожди завершения генерации.", show_alert=True)


@router.message(StateFilter(CreateStates.waiting_passport_preview))
async def msg_passport_preview_lock(message: Message) -> None:
    await message.answer("Используй кнопки под passport preview.")


@router.message()
async def fallback_message(message: Message, state: FSMContext, bot: Bot) -> None:
    current_state = await state.get_state()
    if current_state in {
        CreateStates.waiting_logo_upload.state,
        CreateStates.waiting_custom_color.state,
        CreateStates.waiting_text.state,
        CreateStates.waiting_passport_name.state,
        CreateStates.waiting_passport_username.state,
        CreateStates.waiting_passport_logo.state,
        CreateStates.waiting_passport_preview.state,
        CreateStates.waiting_pack_slug.state,
        CouponStates.waiting_code.state,
    }:
        return
    await open_main_menu(message, state, bot)


async def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN не задан в .env")

    # ПРОКСИ - вставьте сюда рабочий прокси
    PROXY_URL = None  # замените None на строку с прокси, например:
    # PROXY_URL = "socks5://184.82.129.84:1080"
    # PROXY_URL = "http://username:password@proxy.com:8080"

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
        proxy=PROXY_URL,  # добавить эту строку
    )
    dispatcher = Dispatcher(storage=MemoryStorage())
    dispatcher.include_router(router)

    log.info("EmojiCreationBot starting...")
    try:
        await dispatcher.start_polling(bot, allowed_updates=["message", "callback_query"])
    finally:
        await _close_raw_api_session()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
