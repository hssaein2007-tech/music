import os
import re
import html
import math
import time
import uuid
import json
import random
import inspect
import asyncio
from io import BytesIO
from pathlib import Path
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from difflib import SequenceMatcher
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple, Any

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None

from aiohttp import web
from pyrogram import Client, filters, idle
from pyrogram.enums import ChatMemberStatus, ChatType
from pyrogram.types import (
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
    ChatMemberUpdated,
    ReplyParameters,
    LinkPreviewOptions,
)
from pyrogram.errors import UserAlreadyParticipant, UserNotParticipant, InviteRequestSent

from yt_dlp import YoutubeDL
from PIL import Image, ImageDraw, ImageFont
from pytgcalls import PyTgCalls
from pytgcalls.types import MediaStream, AudioQuality, VideoQuality


# =========================
# Config
# =========================
def _env_int(name: str, default: int) -> int:
    try:
        return int(str(os.environ.get(name, str(default))).strip() or default)
    except Exception:
        return default


def _resolve_data_dir() -> str:
    explicit = (os.environ.get("DATA_DIR", "") or "").strip()
    if explicit:
        return explicit
    railway_mount = (os.environ.get("RAILWAY_VOLUME_MOUNT_PATH", "") or "").strip()
    if railway_mount:
        return railway_mount
    if os.path.isdir("/data"):
        return "/data"
    return "./data"


DATA_DIR = _resolve_data_dir()


def _default_data_path(filename: str) -> str:
    return str(Path(DATA_DIR) / filename)


API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
ASSISTANT_SESSION = os.environ.get("ASSISTANT_SESSION", "")
OWNER_ID = int(os.environ.get("OWNER_ID", "0"))

CHANNEL_LINK = os.environ.get("CHANNEL_LINK", "")
SOURCE_LINK = os.environ.get("SOURCE_LINK", "")
BOT_IMAGE = os.environ.get("BOT_IMAGE", "")
NOW_PLAYING_TEMPLATE = (os.environ.get("NOW_PLAYING_TEMPLATE", "https://i.postimg.cc/sD8Lg1Hs/IMG-2817.jpg") or "https://i.postimg.cc/sD8Lg1Hs/IMG-2817.jpg").strip()
NOW_PLAYING_CHANNEL_NAME = (os.environ.get("NOW_PLAYING_CHANNEL_NAME", "") or "").strip()
NOW_PLAYING_SOURCE_NAME = (os.environ.get("NOW_PLAYING_SOURCE_NAME", "") or "").strip()
NOW_PLAYING_CARDS_DIR = (os.environ.get("NOW_PLAYING_CARDS_DIR", _default_data_path("nowplaying_cards")) or _default_data_path("nowplaying_cards")).strip()
BOT_USERNAME = ""
ASSISTANT_ID = 0
BOT_ID = 0

ATHAN_CHECK_INTERVAL = 20
ATHAN_PIN_SECONDS = 3600
ATHAN_HTTP_TIMEOUT = 20
ATHAN_TIMEZONE = (os.environ.get("ATHAN_TIMEZONE", "Asia/Baghdad") or "Asia/Baghdad").strip()
ATHAN_LOCATION_LABEL = (os.environ.get("ATHAN_LOCATION_LABEL", "العراق/كربلاء") or "العراق/كربلاء").strip()
ATHAN_STATE_FILE = (os.environ.get("ATHAN_STATE_FILE", _default_data_path("athan_state.json")) or _default_data_path("athan_state.json")).strip()
PRAYER_TIME_API_URL = (os.environ.get("PRAYER_TIME_API_URL", "https://hq.alkafeel.net/Api/init/init.php?timezone=Asia/Baghdad&long=44.0249&lati=32.6160&v=jsonPrayerTimes") or "https://hq.alkafeel.net/Api/init/init.php?timezone=Asia/Baghdad&long=44.0249&lati=32.6160&v=jsonPrayerTimes").strip()
ATHAN_FAJR_IMAGE = (os.environ.get("ATHAN_FAJR_IMAGE", "") or "").strip()
ATHAN_DHUHR_IMAGE = (os.environ.get("ATHAN_DHUHR_IMAGE", "") or "").strip()
ATHAN_MAGHRIB_IMAGE = (os.environ.get("ATHAN_MAGHRIB_IMAGE", "") or "").strip()
ASSISTANT_JOIN_TIMEOUT = 12
ASSISTANT_MEMBER_CACHE_TTL = 120
HEALTHCHECK_HOST = (os.environ.get("HEALTHCHECK_HOST", "0.0.0.0") or "0.0.0.0").strip()
HEALTHCHECK_PORT = max(1, _env_int("PORT", _env_int("HEALTHCHECK_PORT", 8080)))
WATCHDOG_INTERVAL = max(15, _env_int("WATCHDOG_INTERVAL", 45))
TELEGRAM_PROBE_INTERVAL = max(WATCHDOG_INTERVAL, _env_int("TELEGRAM_PROBE_INTERVAL", 180))
TELEGRAM_PROBE_TIMEOUT = max(10, _env_int("TELEGRAM_PROBE_TIMEOUT", 25))
TELEGRAM_PROBE_FAILURE_LIMIT = max(1, _env_int("TELEGRAM_PROBE_FAILURE_LIMIT", 3))
UNHEALTHY_EXIT_AFTER = max(0, _env_int("UNHEALTHY_EXIT_AFTER", 420))
STREAM_URL_REFRESH_AFTER = max(1800, _env_int("STREAM_URL_REFRESH_AFTER", 14400))
STATE_IDLE_TTL = max(3600, _env_int("STATE_IDLE_TTL", 43200))
STUCK_TRACK_GRACE = max(15, _env_int("STUCK_TRACK_GRACE", 45))

YTDLP_CACHE_TTL = _env_int("YTDLP_CACHE_TTL", 1800)
YTDLP_RESOLVE_TIMEOUT = _env_int("YTDLP_RESOLVE_TIMEOUT", 14)
YTDLP_DOWNLOAD_TIMEOUT = _env_int("YTDLP_DOWNLOAD_TIMEOUT", 180)
ATHAN_CHECK_INTERVAL = max(5, _env_int("ATHAN_CHECK_INTERVAL", ATHAN_CHECK_INTERVAL))
ATHAN_PIN_SECONDS = max(0, _env_int("ATHAN_PIN_SECONDS", ATHAN_PIN_SECONDS))
ATHAN_HTTP_TIMEOUT = max(5, _env_int("ATHAN_HTTP_TIMEOUT", ATHAN_HTTP_TIMEOUT))
ASSISTANT_JOIN_TIMEOUT = max(5, _env_int("ASSISTANT_JOIN_TIMEOUT", ASSISTANT_JOIN_TIMEOUT))
ASSISTANT_MEMBER_CACHE_TTL = max(15, _env_int("ASSISTANT_MEMBER_CACHE_TTL", ASSISTANT_MEMBER_CACHE_TTL))
YTDLP_PLAYER_CLIENT = (os.environ.get("YTDLP_PLAYER_CLIENT", "default") or "default").strip()
YTDLP_USER_AGENT = (
    os.environ.get(
        "YTDLP_USER_AGENT",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
    )
    or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
).strip()


# =========================
# Extra Features Config
# =========================
GROUP_STATS_FILE = (os.environ.get("GROUP_STATS_FILE", _default_data_path("group_stats.json")) or _default_data_path("group_stats.json")).strip()
USER_PLAYLISTS_FILE = (os.environ.get("USER_PLAYLISTS_FILE", _default_data_path("user_playlists.json")) or _default_data_path("user_playlists.json")).strip()
CACHE_INDEX_FILE = (os.environ.get("CACHE_INDEX_FILE", _default_data_path("cache_index.json")) or _default_data_path("cache_index.json")).strip()
CACHE_DIR = (os.environ.get("CACHE_DIR", _default_data_path("cache")) or _default_data_path("cache")).strip()
RECORDER_DIR = (os.environ.get("RECORDER_DIR", _default_data_path("records")) or _default_data_path("records")).strip()
CACHE_HOT_LIMIT = max(10, _env_int("CACHE_HOT_LIMIT", 50))
CACHE_MIN_REQUESTS = max(2, _env_int("CACHE_MIN_REQUESTS", 3))
MAX_PLAYLIST_ITEMS = max(10, _env_int("MAX_PLAYLIST_ITEMS", 200))

GROUP_STATS: Dict[str, Any] = {}
USER_PLAYLISTS: Dict[str, Any] = {}
CACHE_INDEX: Dict[str, Any] = {}
CACHE_DOWNLOAD_TASKS: Dict[str, asyncio.Task] = {}

AUDIO_FILTER_PRESETS: Dict[str, Dict[str, str]] = {
    "normal": {"label_ar": "عادي", "label_en": "Normal", "af": ""},
    "slowed": {"label_ar": "تبطيء", "label_en": "Slowed & Reverb", "af": "atempo=0.85,aecho=0.8:0.88:60:0.25"},
    "nightcore": {"label_ar": "تسريع", "label_en": "Nightcore", "af": "asetrate=48000*1.15,aresample=48000,atempo=1.0"},
    "bassboost": {"label_ar": "تضخيم", "label_en": "Bassboost", "af": "bass=g=8,volume=1.05"},
}


if not (API_ID and API_HASH and BOT_TOKEN and ASSISTANT_SESSION):
    raise RuntimeError("Missing env vars. Set: API_ID, API_HASH, BOT_TOKEN, ASSISTANT_SESSION")


# =========================
# Dictionaries (Languages)
# =========================
users_lang: Dict[int, str] = {}

LANG = {
    "ar": {
        "start_text": (
            "• <b>اهلا بك في بوت الميوزك</b>\n\n"
            "• <b>بوت خاص لتشغيل الأغاني الصوتية والمرئية</b>\n"
            "• <b>قم بإضافة البوت إلى مجموعتك أو قناتك</b>\n"
            "• <b>سيتم تفعيل البوت وانضمام المساعد تلقائياً</b>"
        ),
        "help_main": (
            "≡ <b>مرحباً</b> ، {name} .\n\n"
            "≡ <b>انا بوت لتشغيل الاغاني في\nالمكالمات</b>\n"
            "≡ <b>يمكنني التشغيل في مجموعه او\nقناة.</b>\n"
            "≡ <b>فقط اضفني وارفعني مشرف.</b>"
        ),
        "help_members": (
            "👤 <b>أوامر الاعضاء :</b>\n\n"
            "» <b>ايقاف مؤقت</b> أو <b>اوكف</b> - ايقاف التشغيل موقتأ\n"
            "» <b>استكمال</b> أو <b>كمل</b> - لاستكمال التشغيل\n"
            "» <b>تخطي</b> - لتخطي التشغيل الحالي\n"
            "» <b>ايقاف</b> أو <b>اسكت</b> - لايقاف التشغيل الحالي \n"
            "» <b>تكرار</b> أو <b>كررها</b> - لتكرار التشغيل الحالي"
        ),
        "help_playback": (
            "🎵 <b>أوامر التشغيل :</b>\n\n"
            "» <b>شغل</b> أو <b>تشغيل</b> - لتشغيل الموسيقى\n"
            "» <b>فيد</b> أو <b>فيديو</b> - لتشغيل مقطع فيديو\n"
            "» <b>تحويل لفيديو</b> - لتحويل التشغيل الحالي إلى فيديو بدون إعادة تشغيل من البداية\n"
            "» <b>رجوع للصوت</b> - لإطفاء الفيديو والرجوع للصوت فقط بدون إعادة تشغيل من البداية\n"
            "» <b>عشوائي</b> - لتشغيل اغنيه عشوائية من الطابور\n"
            "» <b>بحث</b> - للبحث عن نتائج في اليوتيوب\n"
            "» <b>يوت</b> أو <b>تحميل</b> - لتنزيل ملف صوتي\n"
            "» <b>تحميل فيديو</b> أو <b>vdown</b> - لتحميل مقطع فيديو\n"
            "» <b>تقديم</b> + عدد الثواني - لتقديم الأغنية\n"
            "» <b>رجوع</b> + عدد الثواني - لترجيع الأغنية\n"
            "» <b>قران</b> - جلب قائمة بالقرآن الكريم\n"
            "» <b>اغاني</b> - جلب قائمة الاغاني والفنانين\n"
            "» <b>تفعيل الاذان</b> - تفعيل تنبيهات الصلاة في المحادثه\n"
            "» <b>تعطيل الاذان</b> - تعطيل تنبيهات الصلاة في المحادثه\n"
            "» <b>معلومات الاذان</b> - لعرض تقرير عن حالة وأوقات الأذان\n"
            "» <b>تيست الاذان</b> + فجر/ظهر/مغرب - اختبار صورة وكليشة الاذان فوراً\n"
            "» <b>بنج</b> - عرض سرعة الاستجابة\n"
            "» <b>سورس</b> - لعرض معلومات السورس"
        ),
        "btn_cmds": "طريقة الاستخدام",
        "btn_bot_channel": "قناة البوت",
        "btn_add_group": "اضفني لمجموعتك",
        "btn_dev": "المطور",
        "btn_langs": "لغات البوت",
        "btn_mem_cmds": "أوامر الاعضاء 👤",
        "btn_play_cmds": "أوامر التشغيل 🎵",
        "btn_settings": "إعدادات التحكم ⚙️",
        "btn_back": "العودة 🔙",
        "btn_owner": "المالك",
        "btn_add_me": "〔 aDD Me To Your Groups 〕",
        "btn_end": "↪️ END",
        "btn_resume": "🎙️ RESUME",
        "btn_pause": "🔇 PAUSE",
        "song_name": "••• Song Name :",
        "duration_time": "••• Duration Time :",
        "requested_by": "••• Requested By :",
        "download_done": "••• تم التحميل بنجاح : ≍",
        "download_vid_done": "••• تم تحميل الفيديو بنجاح : ≍",
        "searching": "🎧 جاري البحث…",
        "searching_vid": "🎬 جاري البحث وتحميل الفيديو…",
        "added_queue": "✅ انضافت للطابور:",
        "added_queue_vid": "✅ انضاف للطابور (فيديو):",
        "need_vc": "لازم يكون <b>فويس شات شغّال</b> بالمجموعة حتى أدخل وأشغّل.",
        "skipped": "⏭️ تم التخطي من قبل {name}.",
        "stopped_by": "تم الأنهاء من قبل {name}",
        "paused_by": "تم الإيقاف من قبل {name}",
        "resumed_by": "تم الاستكمال من قبل {name}",
        "nothing_playing": "ماكو شي شغّال هسه.",
        "nothing_playing_linked": "عيني ماكو شيء شغال حاليا…‼️",
        "welcome_group": (
            "<b>= : شكراً لإضافة البوت لمجموعتكم</b>\n"
            "<b>= : الاسم : {title}</b>\n"
            "<b>= : قم بترقية البوت مشرف</b>\n"
            "<b>= : سيتم التفعيل تلقائي</b>\n"
            "<b>= : ثم قوم بتشغيل ما تريده</b>"
        ),
        "joining_ass": "⏳ جاري انضمام حساب المساعد للمجموعة...",
        "need_play_name": "عيني أكتب شغل + أسم الأغنية لتشغيل الاغنية بالمكالمة…♥️🧨",
        "need_video_name": "عيني أكتب فيديو + أسم الأغنية لتشغيل فيديو بالمكالمة…💎💧",
        "need_download_name": "عيني أكتب يوت + أسم الأغنية لتنزيل ملف صوتي…▶️💡",
        "assistant_returned": "تم رجوع حساب المساعد للعمل…✅",
        "assistant_kicked_by": "تم الطرد من قبل: {name}…⚠️🚫",
        "deleted_msg": "تم حذف الرسالة ✅",
        "del_denied": "بس المشرف أو اللي شغّل يگدر يحذفها ❌",
        "need_seconds": "اكتب عدد الثواني بعد الأمر.",
        "invalid_seconds": "عيني اكتب عدد ثواني صحيح.",
        "seek_forward_done": "تم تقديم التشغيل إلى {time} ✅",
        "seek_backward_done": "تم ترجيع التشغيل إلى {time} ✅",
        "seek_failed": "ماكدر أقدّم/أرجّع هسه.",
        "lang_changed": "تم تغيير اللغة ✅",
        "switch_to_video_done": "تم تحويل التشغيل الحالي إلى فيديو بدون الرجوع للبداية ✅",
        "switch_to_audio_done": "تم الرجوع إلى الصوت فقط بدون الرجوع للبداية ✅",
        "switch_failed": "ماكدر أحول التشغيل الحالي بهالشكل هسه.",
    },
    "en": {
        "start_text": (
            "• <b>Welcome to the Music Bot</b>\n\n"
            "• <b>A bot to play audio and video songs</b>\n"
            "• <b>Add the bot to your group or channel</b>\n"
            "• <b>It will activate & join automatically</b>"
        ),
        "help_main": (
            "≡ <b>Welcome</b>, {name} .\n\n"
            "≡ <b>I am a bot to play music in\nVoice Chats.</b>\n"
            "≡ <b>I can play in groups or\nchannels.</b>\n"
            "≡ <b>Just add me and promote to Admin.</b>"
        ),
        "help_members": (
            "👤 <b>Member Commands:</b>\n\n"
            "» <b>pause</b> - Pause current playback\n"
            "» <b>resume</b> - Resume playback\n"
            "» <b>skip</b> - Skip current track\n"
            "» <b>stop</b> - Stop playback and leave\n"
            "» <b>loop</b> - Loop current track"
        ),
        "help_playback": (
            "🎵 <b>Playback Commands:</b>\n\n"
            "» <b>play</b> - Play audio track\n"
            "» <b>vplay</b> - Play video track\n"
            "» <b>to video</b> - Switch current playback to video without restarting from beginning\n"
            "» <b>to audio</b> - Return from video to audio only without restarting from beginning\n"
            "» <b>shuffle</b> - Shuffle the queue\n"
            "» <b>search</b> - Search on YouTube\n"
            "» <b>down</b> or <b>yout</b> - Download audio\n"
            "» <b>vdown</b> - Download video\n"
            "» <b>forward</b> + seconds - Seek forward\n"
            "» <b>back</b> + seconds - Seek backward\n"
            "» <b>quran</b> - Get Quran list\n"
            "» <b>songs</b> - Get songs list\n"
            "» <b>athan on</b> / <b>athan off</b> - Control prayer alerts\n"
            "» <b>athan info</b> - Show prayer alert status and times\n"
            "» <b>test athan</b> + fajr/dhuhr/maghrib - Send a prayer test now\n"
            "» <b>ping</b> - Check bot latency\n"
            "» <b>source</b> - Show source info"
        ),
        "btn_cmds": "How to Use",
        "btn_bot_channel": "Bot Channel",
        "btn_add_group": "Add Bot to Group",
        "btn_dev": "Developer",
        "btn_langs": "Languages",
        "btn_mem_cmds": "Member Cmds 👤",
        "btn_play_cmds": "Playback Cmds 🎵",
        "btn_settings": "Settings ⚙️",
        "btn_back": "Back 🔙",
        "btn_owner": "Owner",
        "btn_add_me": "〔 aDD Me To Your Groups 〕",
        "btn_end": "↪️ END",
        "btn_resume": "🎙️ RESUME",
        "btn_pause": "🔇 PAUSE",
        "song_name": "••• Song Name :",
        "duration_time": "••• Duration Time :",
        "requested_by": "••• Requested By :",
        "download_done": "••• Downloaded Successfully : ≍",
        "download_vid_done": "••• Video Downloaded Successfully : ≍",
        "searching": "🎧 Searching...",
        "searching_vid": "🎬 Searching & Downloading Video...",
        "added_queue": "✅ Added to queue:",
        "added_queue_vid": "✅ Added to queue (Video):",
        "need_vc": "<b>Voice Chat must be active</b> in the group for me to play.",
        "skipped": "⏭️ Skipped by {name}.",
        "stopped_by": "Stopped by {name}",
        "paused_by": "Paused by {name}",
        "resumed_by": "Resumed by {name}",
        "nothing_playing": "Nothing is playing right now.",
        "nothing_playing_linked": "Nothing is playing right now…‼️",
        "welcome_group": (
            "<b>= : Thanks for adding the bot</b>\n"
            "<b>= : Name : {title}</b>\n"
            "<b>= : Promote the bot to Admin</b>\n"
            "<b>= : It will activate automatically</b>\n"
            "<b>= : Then play whatever you want</b>"
        ),
        "joining_ass": "⏳ Assistant is joining the group...",
        "need_play_name": "Type play + song name to play audio in voice chat…♥️🧨",
        "need_video_name": "Type vplay + song name to play video in voice chat…💎💧",
        "need_download_name": "Type down + song name to download audio…▶️💡",
        "assistant_returned": "Assistant account returned to work…✅",
        "assistant_kicked_by": "Assistant account was removed by: {name}…⚠️🚫",
        "deleted_msg": "Message deleted ✅",
        "del_denied": "Only admins or the requester can delete it ❌",
        "need_seconds": "Type the number of seconds after the command.",
        "invalid_seconds": "Please send a valid number of seconds.",
        "seek_forward_done": "Playback moved forward to {time} ✅",
        "seek_backward_done": "Playback moved backward to {time} ✅",
        "seek_failed": "I can't seek right now.",
        "lang_changed": "Language changed ✅",
        "switch_to_video_done": "Current playback switched to video without restarting ✅",
        "switch_to_audio_done": "Returned to audio only without restarting ✅",
        "switch_failed": "I can't switch the current playback right now.",
    },
}


def get_lang(user_id: int) -> str:
    return users_lang.get(user_id, "ar")


def _t(user_id: int, key: str) -> str:
    lang = get_lang(user_id)
    return LANG[lang].get(key, LANG["ar"][key])


BOT_REPLIES = [
    "وياك عيني بوت ميٰوزكہٰ 𝐏𝐑𝐍𝐒 💙💤",
    "لتغلط اسمي بوت ميٰوزكہٰ 𝐏𝐑𝐍𝐒 😤🗣",
]


def now_ts() -> int:
    return int(time.time())


async def maybe_await(x):
    if inspect.isawaitable(x):
        return await x
    return x


async def call_first(obj, names: List[str], *args, **kwargs):
    last_err = None
    for name in names:
        if hasattr(obj, name):
            fn = getattr(obj, name)
            try:
                return await maybe_await(fn(*args, **kwargs))
            except Exception as e:
                last_err = e
    if last_err:
        raise last_err
    raise AttributeError(f"None of methods exist: {names}")


def hms(seconds: int) -> str:
    seconds = max(0, int(seconds))
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h:02d}:{m:02d}:{s:02d}"
    return f"{m:02d}:{s:02d}"


def progress_bar(elapsed: int, total: int, width: int = 15) -> str:
    total = max(1, int(total))
    elapsed = max(0, min(int(elapsed), total))
    ratio = elapsed / total
    pos = int(ratio * (width - 1))
    bar = ["━"] * width
    bar[pos] = "●"
    return "".join(bar)


def safe_html(s: str) -> str:
    return html.escape(s or "", quote=False)


def mention_user(user) -> str:
    if not user:
        return "<b>مستخدم</b>"
    name = user.first_name or user.username or "User"
    return f"<a href='tg://user?id={user.id}'>{safe_html(name)}</a>"


def get_bot_add_url() -> str:
    return f"https://t.me/{BOT_USERNAME}?startgroup=true" if BOT_USERNAME else "https://t.me/"


def full_link_text(text: str) -> str:
    return f"<a href='{get_bot_add_url()}'>{safe_html(text)}</a>"


def parse_int_seconds(s: str) -> Optional[int]:
    try:
        val = int(float((s or "").strip()))
        return max(0, val)
    except Exception:
        return None


def normalize_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


ARABIC_NORMALIZE_MAP = str.maketrans({
    "أ": "ا",
    "إ": "ا",
    "آ": "ا",
    "ة": "ه",
    "ى": "ي",
    "ؤ": "و",
    "ئ": "ي",
    "ٱ": "ا",
})


def normalize_search_text(text: str) -> str:
    text = normalize_spaces(text).casefold().translate(ARABIC_NORMALIZE_MAP)
    text = re.sub(r"[^\w\s\u0600-\u06FF]", " ", text)
    return normalize_spaces(text)


URL_RE = re.compile(r"^(?:https?://|www\.)", re.I)
KNOWN_MEDIA_DOMAINS = (
    "youtube.com", "youtu.be", "music.youtube.com", "soundcloud.com", "spotify.com",
    "tiktok.com", "facebook.com", "fb.watch", "instagram.com", "x.com",
    "twitter.com", "twitch.tv", "kick.com", "vm.tiktok.com",
)


def looks_like_url(q: str) -> bool:
    q = (q or "").strip()
    if not q:
        return False
    if URL_RE.match(q) or "://" in q:
        return True
    q_cf = q.casefold()
    return any(domain in q_cf for domain in KNOWN_MEDIA_DOMAINS)


async def is_admin(client: Client, chat_id: int, user_id: int) -> bool:
    if chat_id == user_id:
        return True
    if OWNER_ID and user_id == OWNER_ID:
        return True
    try:
        member = await client.get_chat_member(chat_id, user_id)
        return member.status in [ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER]
    except Exception:
        return False


COMMAND_ALIASES: List[Tuple[str, str]] = [
    ("ايقاف التسجيل", "record_stop"),
    ("stop recording", "record_stop"),
    ("شغل قائمتي", "play_my_playlist"),
    ("play my playlist", "play_my_playlist"),
    ("مسح قائمتي", "clear_my_playlist"),
    ("clear my playlist", "clear_my_playlist"),
    ("فلتر عادي", "filter_off"),
    ("filter off", "filter_off"),
    ("كروس off", "crossfade_off"),
    ("crossfade off", "crossfade_off"),
    ("تحميل فيديو", "vdown"),
    ("ايقاف مؤقت", "pause"),
    ("تحويل لفيديو", "switch_video"),
    ("رجوع للصوت", "switch_audio"),
    ("شغل تيك", "social_video"),
    ("شغل ريلز", "social_video"),
    ("tiktok", "social_video"),
    ("reels", "social_video"),
    ("تيك", "social_video"),
    ("ريلز", "social_video"),
    ("تسجيل", "record_start"),
    ("record", "record_start"),
    ("توب", "stats"),
    ("احصائيات", "stats"),
    ("leaderboard", "stats"),
    ("stats", "stats"),
    ("قائمتي", "my_playlist"),
    ("my playlist", "my_playlist"),
    ("حفظ", "save_playlist"),
    ("save", "save_playlist"),
    ("تبطيء", "filter_slowed"),
    ("slowed", "filter_slowed"),
    ("تسريع", "filter_nightcore"),
    ("nightcore", "filter_nightcore"),
    ("تضخيم", "filter_bass"),
    ("bassboost", "filter_bass"),
    ("كروس", "crossfade"),
    ("crossfade", "crossfade"),
    ("كاش", "cache_info"),
    ("cache", "cache_info"),
    ("عشوائي", "shuffle"),
    ("shuffle", "shuffle"),
    ("تحميل فيديو", "vdown"),
    ("ايقاف مؤقت", "pause"),
    ("تحويل لفيديو", "switch_video"),
    ("رجوع للصوت", "switch_audio"),
    ("to video", "switch_video"),
    ("to audio", "switch_audio"),
    ("تشغيل", "play"),
    ("شغل", "play"),
    ("play", "play"),
    ("فيد", "vplay"),
    ("فيديو", "vplay"),
    ("vplay", "vplay"),
    ("بحث", "search"),
    ("search", "search"),
    ("يوت", "down"),
    ("تنزيل", "down"),
    ("تحميل", "down"),
    ("down", "down"),
    ("yout", "down"),
    ("vdown", "vdown"),
    ("تقديم", "forward"),
    ("forward", "forward"),
    ("رجوع", "back"),
    ("back", "back"),
    ("تخطي", "skip"),
    ("skip", "skip"),
    ("اسكُت", "stop"),
    ("اسكت", "stop"),
    ("ايقاف", "stop"),
    ("انهاء", "stop"),
    ("stop", "stop"),
    ("أوكف", "pause"),
    ("اوكف", "pause"),
    ("اوگف", "pause"),
    ("اوْكف", "pause"),
    ("pause", "pause"),
    ("استكمال", "resume"),
    ("كمل", "resume"),
    ("resume", "resume"),
    ("تكرار", "loop"),
    ("كررها", "loop"),
    ("loop", "loop"),
    ("بنج", "ping"),
    ("ping", "ping"),
    ("سورس", "source"),
    ("source", "source"),
    ("قران", "quran"),
    ("quran", "quran"),
    ("اغاني", "songs"),
    ("songs", "songs"),
    ("معلومات الاذان", "athan_info"),
    ("athan info", "athan_info"),
    ("تيست الاذان", "athan_test"),
    ("اختبار الاذان", "athan_test"),
    ("اذان تيست", "athan_test"),
    ("test athan", "athan_test"),
    ("athan test", "athan_test"),
    ("تفعيل الاذان", "athan_on"),
    ("تعطيل الاذان", "athan_off"),
    ("الاذان", "athan_toggle"),
    ("اذان", "athan_toggle"),
    ("athan on", "athan_on"),
    ("athan off", "athan_off"),
    ("athan", "athan_toggle"),
    ("الاوامر", "help"),
    ("أوامر", "help"),
    ("مساعدة", "help"),
    ("help", "help"),
    ("cmds", "help"),
]
COMMAND_ALIASES.sort(key=lambda x: len(x[0]), reverse=True)


def match_command(text: str) -> Tuple[str, str]:
    original = normalize_spaces(text)
    lowered = original.casefold()
    for alias, canonical in COMMAND_ALIASES:
        alias_cf = alias.casefold()
        if lowered == alias_cf:
            return canonical, ""
        if lowered.startswith(alias_cf + " "):
            return canonical, original[len(alias):].strip()
    return "", ""


LOCAL_TZ = ZoneInfo(ATHAN_TIMEZONE) if ZoneInfo else timezone(timedelta(hours=3))
PRAYER_TIME_RE = re.compile(r"(\d{1,2}:\d{2})")
ATHAN_PRAYERS = {
    "Fajr": {
        "name_ar": "الفجر",
        "image": lambda: ATHAN_FAJR_IMAGE or BOT_IMAGE,
        "aliases": {"fajr", "الفجر", "فجر"},
    },
    "Dhuhr": {
        "name_ar": "الظهر",
        "image": lambda: ATHAN_DHUHR_IMAGE or BOT_IMAGE,
        "aliases": {"dhuhr", "duhr", "الظهر", "ظهر"},
    },
    "Maghrib": {
        "name_ar": "المغرب",
        "image": lambda: ATHAN_MAGHRIB_IMAGE or BOT_IMAGE,
        "aliases": {"maghrib", "المغرب", "مغرب"},
    },
}
ATHAN_STATE: Dict[str, Any] = {"enabled_chats": [], "sent": {}, "pending_unpins": {}}
ATHAN_TIMINGS_CACHE: Dict[str, Dict[str, str]] = {}
ATHAN_BACKGROUND_TASK: Optional[asyncio.Task] = None
ASSISTANT_JOIN_LOCKS: Dict[int, asyncio.Lock] = {}
ASSISTANT_MEMBER_CACHE: Dict[int, Tuple[int, bool]] = {}
HEALTHCHECK_RUNNER: Optional[web.AppRunner] = None
HEALTHCHECK_READY = False
HEALTHCHECK_STARTED_AT = int(time.time())
WATCHDOG_TASK: Optional[asyncio.Task] = None
WATCHDOG_LAST_TICK = 0
TELEGRAM_LAST_OK_TS = 0
TELEGRAM_CONSECUTIVE_FAILURES = 0
UNHEALTHY_SINCE = 0
HEALTH_LAST_ERROR = ""
INLINE_QUERY_CACHE: Dict[str, Tuple[int, str]] = {}


def local_now() -> datetime:
    return datetime.now(LOCAL_TZ)


def runtime_health_snapshot() -> Dict[str, Any]:
    now = now_ts()
    watchdog_alive = bool(WATCHDOG_TASK and not WATCHDOG_TASK.done())
    watchdog_recent = bool(WATCHDOG_LAST_TICK and (now - WATCHDOG_LAST_TICK) <= max(WATCHDOG_INTERVAL * 3, 90))
    athan_alive = bool(ATHAN_BACKGROUND_TASK and not ATHAN_BACKGROUND_TASK.done())
    telegram_recent = bool(TELEGRAM_LAST_OK_TS and (now - TELEGRAM_LAST_OK_TS) <= (TELEGRAM_PROBE_INTERVAL * max(TELEGRAM_PROBE_FAILURE_LIMIT, 1)) + TELEGRAM_PROBE_TIMEOUT + 30)
    ok = bool(HEALTHCHECK_READY and watchdog_alive and watchdog_recent and athan_alive and telegram_recent and TELEGRAM_CONSECUTIVE_FAILURES < TELEGRAM_PROBE_FAILURE_LIMIT)
    return {
        "ok": ok,
        "service": "telegram-music-bot",
        "uptime_seconds": max(0, int(time.time()) - HEALTHCHECK_STARTED_AT),
        "bot_ready": bool(HEALTHCHECK_READY),
        "assistant_started": bool(ASSISTANT_ID),
        "timezone": ATHAN_TIMEZONE,
        "data_dir": DATA_DIR,
        "watchdog_alive": watchdog_alive,
        "watchdog_recent": watchdog_recent,
        "telegram_last_ok": TELEGRAM_LAST_OK_TS,
        "telegram_failures": TELEGRAM_CONSECUTIVE_FAILURES,
        "athan_task_alive": athan_alive,
        "last_error": HEALTH_LAST_ERROR,
        "unhealthy_since": UNHEALTHY_SINCE or None,
    }


async def _health_index(_: web.Request) -> web.Response:
    payload = runtime_health_snapshot()
    return web.json_response(payload, status=200 if payload.get("ok") else 503)


async def _readiness_index(_: web.Request) -> web.Response:
    payload = runtime_health_snapshot()
    return web.json_response(payload, status=200 if payload.get("ok") else 503)


async def start_healthcheck_server():
    global HEALTHCHECK_RUNNER
    app = web.Application()
    app.router.add_get("/", _health_index)
    app.router.add_get("/healthz", _health_index)
    app.router.add_get("/readyz", _readiness_index)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, HEALTHCHECK_HOST, HEALTHCHECK_PORT)
    await site.start()
    HEALTHCHECK_RUNNER = runner
    print(f"Healthcheck server started on {HEALTHCHECK_HOST}:{HEALTHCHECK_PORT}")


async def stop_healthcheck_server():
    global HEALTHCHECK_RUNNER
    if HEALTHCHECK_RUNNER is not None:
        await HEALTHCHECK_RUNNER.cleanup()
        HEALTHCHECK_RUNNER = None


def _safe_int_chat_id(value: Any) -> Optional[int]:
    try:
        return int(str(value).strip())
    except Exception:
        return None


def _load_json_file(path: str, default: dict) -> dict:
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        print(f"Failed to load JSON {path}: {e}")
    return dict(default)


def _save_json_file(path: str, payload: dict):
    try:
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        tmp_path = f"{path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, path)
    except Exception as e:
        print(f"Failed to save JSON {path}: {e}")


def cleanup_old_athan_state():
    sent = ATHAN_STATE.setdefault("sent", {})
    today = local_now().date()
    keep_dates = {(today - timedelta(days=2)).isoformat(), (today - timedelta(days=1)).isoformat(), today.isoformat()}
    for chat_key in list(sent.keys()):
        chat_sent = sent.get(chat_key) or {}
        for day_key in list(chat_sent.keys()):
            if day_key not in keep_dates:
                chat_sent.pop(day_key, None)
        if not chat_sent:
            sent.pop(chat_key, None)


def load_athan_state():
    global ATHAN_STATE
    ATHAN_STATE = _load_json_file(ATHAN_STATE_FILE, {"enabled_chats": [], "sent": {}, "pending_unpins": {}})
    ATHAN_STATE.setdefault("enabled_chats", [])
    ATHAN_STATE.setdefault("sent", {})
    ATHAN_STATE.setdefault("pending_unpins", {})
    cleanup_old_athan_state()
    enabled_ids: List[int] = []
    for raw_chat_id in ATHAN_STATE.get("enabled_chats", []):
        chat_id = _safe_int_chat_id(raw_chat_id)
        if chat_id is None:
            continue
        enabled_ids.append(chat_id)
        get_state(chat_id).athan_enabled = True
    ATHAN_STATE["enabled_chats"] = enabled_ids
    _save_json_file(ATHAN_STATE_FILE, ATHAN_STATE)


def _prune_cache_dict(cache: dict):
    expired = []
    current = now_ts()
    for key, item in list(cache.items()):
        try:
            ts = int(item[0])
        except Exception:
            expired.append(key)
            continue
        if current - ts > CACHE_TTL:
            expired.append(key)
    for key in expired:
        cache.pop(key, None)


def prune_runtime_state():
    current = now_ts()
    _prune_cache_dict(SEARCH_CACHE)
    _prune_cache_dict(RESOLVE_CACHE)

    for key, value in list(INLINE_QUERY_CACHE.items()):
        try:
            ts = int(value[0])
        except Exception:
            INLINE_QUERY_CACHE.pop(key, None)
            continue
        if current - ts > 86400:
            INLINE_QUERY_CACHE.pop(key, None)

    for chat_id, cached in list(ASSISTANT_MEMBER_CACHE.items()):
        try:
            ts = int(cached[0])
        except Exception:
            ASSISTANT_MEMBER_CACHE.pop(chat_id, None)
            continue
        if current - ts > ASSISTANT_MEMBER_CACHE_TTL:
            ASSISTANT_MEMBER_CACHE.pop(chat_id, None)

    for chat_id, st in list(states.items()):
        idle_for = current - int(getattr(st, "last_activity", 0) or 0)
        has_running_task = bool(st.progress_task and not st.progress_task.done()) or bool(st.athan_unpin_task and not st.athan_unpin_task.done())
        if st.playing and st.queue and not has_running_task:
            reset_progress_task(chat_id)
        if (not st.queue) and (not st.playing) and (not st.recording) and (not has_running_task) and idle_for > STATE_IDLE_TTL:
            states.pop(chat_id, None)
            ASSISTANT_JOIN_LOCKS.pop(chat_id, None)


async def probe_telegram_runtime():
    global TELEGRAM_LAST_OK_TS, TELEGRAM_CONSECUTIVE_FAILURES, HEALTH_LAST_ERROR
    try:
        await asyncio.wait_for(bot.get_me(), timeout=TELEGRAM_PROBE_TIMEOUT)
        await asyncio.wait_for(assistant.get_me(), timeout=TELEGRAM_PROBE_TIMEOUT)
        TELEGRAM_LAST_OK_TS = now_ts()
        TELEGRAM_CONSECUTIVE_FAILURES = 0
        if HEALTH_LAST_ERROR.startswith("telegram_probe:"):
            HEALTH_LAST_ERROR = ""
        return True
    except asyncio.CancelledError:
        raise
    except Exception as e:
        TELEGRAM_CONSECUTIVE_FAILURES += 1
        HEALTH_LAST_ERROR = f"telegram_probe: {e}"
        print(f"telegram runtime probe failed: {e}")
        return False


async def watchdog_loop():
    global WATCHDOG_LAST_TICK, UNHEALTHY_SINCE, HEALTH_LAST_ERROR
    while True:
        WATCHDOG_LAST_TICK = now_ts()
        try:
            prune_runtime_state()
            if not ATHAN_BACKGROUND_TASK or ATHAN_BACKGROUND_TASK.done():
                if not HEALTH_LAST_ERROR.startswith("athan_task:"):
                    HEALTH_LAST_ERROR = "athan_task: background task stopped"
            for chat_id, st in list(states.items()):
                if st.playing and st.queue and st.current_index < len(st.queue):
                    track = st.queue[st.current_index]
                    elapsed = get_track_elapsed(st, track)
                    if track.duration > 0 and elapsed >= track.duration + STUCK_TRACK_GRACE:
                        await next_track(chat_id)
            if (not TELEGRAM_LAST_OK_TS) or (now_ts() - TELEGRAM_LAST_OK_TS >= TELEGRAM_PROBE_INTERVAL):
                await probe_telegram_runtime()
            snapshot = runtime_health_snapshot()
            if snapshot.get("ok"):
                UNHEALTHY_SINCE = 0
            else:
                if not UNHEALTHY_SINCE:
                    UNHEALTHY_SINCE = now_ts()
                if UNHEALTHY_EXIT_AFTER > 0 and (now_ts() - UNHEALTHY_SINCE) >= UNHEALTHY_EXIT_AFTER:
                    print("Runtime unhealthy for too long, exiting for supervisor restart")
                    os._exit(1)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            HEALTH_LAST_ERROR = f"watchdog: {e}"
            print(f"watchdog error: {e}")
        await asyncio.sleep(WATCHDOG_INTERVAL)


def save_athan_state():
    cleanup_old_athan_state()
    _save_json_file(ATHAN_STATE_FILE, ATHAN_STATE)


def set_athan_enabled(chat_id: int, enabled: bool):
    enabled_chats = {int(x) for x in ATHAN_STATE.get("enabled_chats", []) if _safe_int_chat_id(x) is not None}
    if enabled:
        enabled_chats.add(int(chat_id))
    else:
        enabled_chats.discard(int(chat_id))
    ATHAN_STATE["enabled_chats"] = sorted(enabled_chats)
    get_state(chat_id).athan_enabled = enabled
    save_athan_state()


def athan_is_enabled(chat_id: int) -> bool:
    return int(chat_id) in {int(x) for x in ATHAN_STATE.get("enabled_chats", []) if _safe_int_chat_id(x) is not None}


def was_athan_sent(chat_id: int, prayer_key: str, day_key: str) -> bool:
    chat_sent = (ATHAN_STATE.get("sent", {}) or {}).get(str(chat_id), {})
    return prayer_key in (chat_sent.get(day_key) or [])


def mark_athan_sent(chat_id: int, prayer_key: str, day_key: str):
    sent = ATHAN_STATE.setdefault("sent", {})
    chat_sent = sent.setdefault(str(chat_id), {})
    prayers = set(chat_sent.get(day_key) or [])
    prayers.add(prayer_key)
    chat_sent[day_key] = sorted(prayers)
    save_athan_state()


def set_pending_unpin(chat_id: int, message_id: int, unpin_at_ts: int):
    pending = ATHAN_STATE.setdefault("pending_unpins", {})
    pending[str(chat_id)] = {"message_id": int(message_id), "unpin_at": int(unpin_at_ts)}
    save_athan_state()


def clear_pending_unpin(chat_id: int, message_id: Optional[int] = None):
    pending = ATHAN_STATE.setdefault("pending_unpins", {})
    current = pending.get(str(chat_id))
    if not current:
        return
    if message_id is not None and int(current.get("message_id", 0)) != int(message_id):
        return
    pending.pop(str(chat_id), None)
    save_athan_state()


def get_athan_caption(prayer_key: str) -> str:
    prayer_name = ATHAN_PRAYERS.get(prayer_key, {}).get("name_ar", prayer_key)
    return (
        "-------------------------------------------------\n"
        f"حان الأن موعد صلاة ( {prayer_name} )\n"
        f"بتوقيت : ( {ATHAN_LOCATION_LABEL} ) 🌹\n\n"
        "{ وَأَقِيمُوا الصَّلَاةَ وَآتُوا الزَّكَاةَ وَارْكَعُوا مَعَ الرَّاكِعِينَ }\n\n"
        "الأمراء | 𝔞𝔩 𝔭𝔯𝔧𝔫𝔠𝔢𝔰\n"
        "-------------------------------------------------"
    )


def parse_prayer_key(raw: str) -> str:
    raw_norm = normalize_search_text(raw)
    if not raw_norm:
        return ""
    for prayer_key, info in ATHAN_PRAYERS.items():
        aliases = {normalize_search_text(x) for x in info.get("aliases", set())}
        if raw_norm in aliases:
            return prayer_key
    return ""


def choose_default_test_prayer() -> str:
    now = local_now().strftime("%H:%M")
    preferred = ["Fajr", "Dhuhr", "Maghrib"]
    timings = ATHAN_TIMINGS_CACHE.get(local_now().date().isoformat(), {})
    for prayer_key in preferred:
        prayer_time = timings.get(prayer_key, "")
        if prayer_time and prayer_time >= now:
            return prayer_key
    return "Maghrib"


def build_prayer_api_url() -> str:
    return PRAYER_TIME_API_URL


def normalize_prayer_clock(value: str) -> str:
    match = PRAYER_TIME_RE.search(str(value or ""))
    if not match:
        return ""
    hh, mm = match.group(1).split(":", 1)
    return f"{int(hh):02d}:{mm}"


def _fetch_today_prayer_times_sync() -> Dict[str, str]:
    url = build_prayer_api_url()
    if not url:
        return {}
    try:
        req = Request(url, headers={"User-Agent": YTDLP_USER_AGENT or "Mozilla/5.0"})
        with urlopen(req, timeout=ATHAN_HTTP_TIMEOUT) as resp:
            payload = json.loads(resp.read().decode("utf-8", errors="replace"))
        
        def _extract_times(obj):
            res = {}
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if isinstance(v, str) and re.match(r'^\d{1,2}:\d{2}', str(v).strip()):
                        res[str(k).lower()] = str(v).strip()
                    elif isinstance(v, (dict, list)):
                        res.update(_extract_times(v))
            elif isinstance(obj, list):
                for item in obj:
                    res.update(_extract_times(item))
            return res

        flat_times = _extract_times(payload)
        result: Dict[str, str] = {}
        
        # 1. استخراج الفجر (موقع الكفيل يكتبه fajir)
        fajr_val = flat_times.get("fajr") or flat_times.get("fajir")
        if fajr_val:
            hh, mm = fajr_val.split(":", 1)
            result["Fajr"] = f"{int(hh):02d}:{mm}"
            
        # 2. استخراج الظهر (موقع الكفيل يكتبه doher)
        dhuhr_val = flat_times.get("dhuhr") or flat_times.get("zhuhr") or flat_times.get("doher")
        if dhuhr_val:
            hh, mm = dhuhr_val.split(":", 1)
            h_int = int(hh)
            if h_int < 11:  # في حال كان الظهر 1:00 نحوله الى 13:00
                h_int += 12
            result["Dhuhr"] = f"{h_int:02d}:{mm}"
            
        # 3. استخراج المغرب (تحويل صيغة 12 ساعة الى 24 ساعة)
        maghrib_val = flat_times.get("maghrib")
        if maghrib_val:
            hh, mm = maghrib_val.split(":", 1)
            h_int = int(hh)
            if h_int < 12: # تحويل وقت المغرب إلى صيغة 24 ساعة حصراً
                h_int += 12
            result["Maghrib"] = f"{h_int:02d}:{mm}"
            
        return result
    except Exception as e:
        print(f"Prayer API fetch failed: {e}")
        return {}


async def get_today_prayer_times(force_refresh: bool = False) -> Dict[str, str]:
    day_key = local_now().date().isoformat()
    if not force_refresh and ATHAN_TIMINGS_CACHE.get(day_key):
        return ATHAN_TIMINGS_CACHE[day_key]
    timings = await asyncio.to_thread(_fetch_today_prayer_times_sync)
    if timings:
        ATHAN_TIMINGS_CACHE.clear()
        ATHAN_TIMINGS_CACHE[day_key] = timings
    return timings


def get_assistant_join_lock(chat_id: int) -> asyncio.Lock:
    lock = ASSISTANT_JOIN_LOCKS.get(chat_id)
    if lock is None:
        lock = asyncio.Lock()
        ASSISTANT_JOIN_LOCKS[chat_id] = lock
    return lock


def get_assistant_cache(chat_id: int) -> Optional[bool]:
    cached = ASSISTANT_MEMBER_CACHE.get(chat_id)
    if not cached:
        return None
    ts, value = cached
    if now_ts() - ts > ASSISTANT_MEMBER_CACHE_TTL:
        ASSISTANT_MEMBER_CACHE.pop(chat_id, None)
        return None
    return value


def set_assistant_cache(chat_id: int, value: bool):
    ASSISTANT_MEMBER_CACHE[chat_id] = (now_ts(), bool(value))


async def is_assistant_participant(chat_id: int) -> bool:
    cached = get_assistant_cache(chat_id)
    if cached is not None:
        return cached
    ass_me = await assistant.get_me()
    try:
        await bot.get_chat_member(chat_id, ass_me.id)
        set_assistant_cache(chat_id, True)
        return True
    except UserNotParticipant:
        set_assistant_cache(chat_id, False)
        return False
    except Exception:
        return False


async def unpin_athan_message_later(chat_id: int, message_id: int, delay_seconds: int):
    st = get_state(chat_id)
    try:
        await asyncio.sleep(max(0, int(delay_seconds)))
        try:
            await bot.unpin_chat_message(chat_id, message_id)
        except Exception:
            pass
        if st.athan_pin_message_id == message_id:
            st.athan_pin_message_id = None
        clear_pending_unpin(chat_id, message_id)
    finally:
        if st.athan_unpin_task and st.athan_unpin_task.done():
            st.athan_unpin_task = None


def schedule_athan_unpin(chat_id: int, message_id: int, delay_seconds: int):
    st = get_state(chat_id)
    if st.athan_unpin_task and not st.athan_unpin_task.done():
        st.athan_unpin_task.cancel()
    st.athan_pin_message_id = message_id
    unpin_at_ts = now_ts() + max(0, int(delay_seconds))
    set_pending_unpin(chat_id, message_id, unpin_at_ts)
    st.athan_unpin_task = asyncio.create_task(unpin_athan_message_later(chat_id, message_id, delay_seconds))


async def restore_pending_unpins():
    pending = dict(ATHAN_STATE.get("pending_unpins", {}) or {})
    for chat_key, payload in pending.items():
        chat_id = _safe_int_chat_id(chat_key)
        if chat_id is None:
            continue
        message_id = _safe_int_chat_id((payload or {}).get("message_id"))
        unpin_at = _safe_int_chat_id((payload or {}).get("unpin_at"))
        if not message_id or not unpin_at:
            clear_pending_unpin(chat_id)
            continue
        delay_seconds = max(0, unpin_at - now_ts())
        schedule_athan_unpin(chat_id, message_id, delay_seconds)


async def send_athan_alert(chat_id: int, prayer_key: str, triggered_by: Optional[int] = None, is_test: bool = False) -> Optional[int]:
    st = get_state(chat_id)
    photo = ATHAN_PRAYERS.get(prayer_key, {}).get("image", lambda: BOT_IMAGE)() or BOT_IMAGE
    caption = get_athan_caption(prayer_key)
    sent = None
    try:
        sent = await bot.send_photo(chat_id, photo=photo, caption=caption)
    except Exception:
        try:
            sent = await bot.send_message(chat_id, caption, link_preview_options=LinkPreviewOptions(is_disabled=True))
        except Exception as e:
            print(f"Failed to send athan alert to {chat_id}: {e}")
            return None
    if sent and ATHAN_PIN_SECONDS > 0 and getattr(sent.chat, "type", None) != ChatType.PRIVATE:
        try:
            if st.athan_pin_message_id and st.athan_pin_message_id != sent.id:
                try:
                    await bot.unpin_chat_message(chat_id, st.athan_pin_message_id)
                except Exception:
                    pass
            await bot.pin_chat_message(chat_id, sent.id, disable_notification=True)
            schedule_athan_unpin(chat_id, sent.id, ATHAN_PIN_SECONDS)
        except Exception as e:
            print(f"Failed to pin athan message in {chat_id}: {e}")
    return sent.id if sent else None


async def athan_loop():
    while True:
        try:
            now = local_now()
            timings = await get_today_prayer_times(force_refresh=not bool(ATHAN_TIMINGS_CACHE.get(now.date().isoformat())))
            if timings:
                minute_now = now.strftime("%H:%M")
                day_key = now.date().isoformat()
                for chat_id in list(ATHAN_STATE.get("enabled_chats", []) or []):
                    if not athan_is_enabled(int(chat_id)):
                        continue
                    for prayer_key in ["Fajr", "Dhuhr", "Maghrib"]:
                        prayer_time = timings.get(prayer_key, "")
                        if prayer_time and prayer_time == minute_now and not was_athan_sent(int(chat_id), prayer_key, day_key):
                            sent_id = await send_athan_alert(int(chat_id), prayer_key)
                            if sent_id:
                                mark_athan_sent(int(chat_id), prayer_key, day_key)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"Athan loop error: {e}")
        await asyncio.sleep(ATHAN_CHECK_INTERVAL)


async def warmup_assistant_join(chat_id: int, user_id: int):
    try:
        await ensure_assistant_joined(chat_id, user_id)
    except Exception:
        pass


async def ensure_assistant_joined(chat_id: int, user_id: int) -> bool:
    try:
        chat = await bot.get_chat(chat_id)
        if chat.type == ChatType.PRIVATE:
            return True
        if await is_assistant_participant(chat_id):
            return True

        lock = get_assistant_join_lock(chat_id)
        async with lock:
            if await is_assistant_participant(chat_id):
                return True

            join_targets: List[str] = []
            if chat.username:
                join_targets.append(chat.username)
            try:
                exported = await bot.export_chat_invite_link(chat_id)
                if exported:
                    join_targets.append(exported)
            except Exception:
                pass
            try:
                invite_link = await bot.create_chat_invite_link(chat_id, name=f"assistant-{now_ts()}")
                if invite_link and getattr(invite_link, "invite_link", None):
                    join_targets.append(invite_link.invite_link)
            except Exception:
                pass

            deduped_targets: List[str] = []
            seen_targets = set()
            for target in join_targets:
                if target and target not in seen_targets:
                    deduped_targets.append(target)
                    seen_targets.add(target)

            last_error = None
            for target in deduped_targets:
                try:
                    await asyncio.wait_for(assistant.join_chat(target), timeout=ASSISTANT_JOIN_TIMEOUT)
                    set_assistant_cache(chat_id, True)
                    return True
                except UserAlreadyParticipant:
                    set_assistant_cache(chat_id, True)
                    return True
                except InviteRequestSent:
                    last_error = InviteRequestSent("Assistant join request sent")
                    continue
                except Exception as e:
                    last_error = e
                    continue

            joined_after_attempt = await is_assistant_participant(chat_id)
            if joined_after_attempt:
                return True
            if last_error:
                print(f"Failed to join assistant: {last_error}")
            return False
    except Exception as e:
        print(f"Error in ensure_assistant_joined: {e}")
        return False


async def restore_assistant_after_kick(chat_id: int, actor=None) -> bool:
    global ASSISTANT_ID
    try:
        if not ASSISTANT_ID:
            ass_me = await assistant.get_me()
            ASSISTANT_ID = ass_me.id
        try:
            await bot.unban_chat_member(chat_id, ASSISTANT_ID)
        except Exception:
            pass

        joined = await ensure_assistant_joined(chat_id, actor.id if actor else 0)
        if joined:
            actor_id = actor.id if actor else 0
            text = (
                f"{_t(actor_id, 'assistant_returned')}\n"
                f"{_t(actor_id, 'assistant_kicked_by').format(name=mention_user(actor))}"
            )
            try:
                await bot.send_message(chat_id, text, link_preview_options=LinkPreviewOptions(is_disabled=True))
            except Exception as e:
                print(f"Failed to send assistant restore message: {e}")
            return True
        return False
    except Exception as e:
        print(f"restore_assistant_after_kick error: {e}")
        return False


CACHE_TTL = max(60, YTDLP_CACHE_TTL)
SEARCH_CACHE: Dict[Tuple[str, bool, int], Tuple[int, List[dict]]] = {}
RESOLVE_CACHE: Dict[Tuple[str, bool], Tuple[int, dict]] = {}


def _split_clients(value: str, default: List[str]) -> List[str]:
    items = [x.strip() for x in (value or "").split(",") if x.strip()]
    return items or list(default)


def _build_extractor_args(player_clients: Optional[List[str]] = None) -> Dict[str, Any]:
    clients = player_clients or _split_clients(YTDLP_PLAYER_CLIENT, ["default"])
    youtube_args: Dict[str, Any] = {
        "player_client": clients,
        "skip": ["translated_subs"],
    }
    return {
        "youtube": youtube_args,
        "youtubepot-wpc": {
            "browser_path": ["/usr/bin/chromium"]
        }
    }


def make_ydl_opts(
    *,
    player_clients: Optional[List[str]] = None,
    socket_timeout: Optional[int] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    opts: Dict[str, Any] = {
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "geo_bypass": True,
        "source_address": "0.0.0.0",
        "cachedir": False,
        "concurrent_fragment_downloads": 4,
        "extractor_retries": 1,
        "retries": 1,
        "fragment_retries": 1,
        "file_access_retries": 1,
        "ignoreerrors": False,
        "socket_timeout": socket_timeout or YTDLP_RESOLVE_TIMEOUT,
        "http_headers": {"User-Agent": YTDLP_USER_AGENT},
        "extractor_args": _build_extractor_args(player_clients=player_clients),
    }
    if extra:
        opts.update(extra)
    return opts


def ydl_profiles() -> List[Dict[str, Any]]:
    primary_clients = _split_clients(YTDLP_PLAYER_CLIENT, ["default"])
    fallback_clients = ["default", "web_safari"]
    lean_clients = ["tv", "default"]
    return [
        {"player_clients": primary_clients},
        {"player_clients": fallback_clients},
        {"player_clients": lean_clients},
    ]


def _cache_get(cache: dict, key):
    item = cache.get(key)
    if not item:
        return None
    ts, data = item
    if now_ts() - ts > CACHE_TTL:
        cache.pop(key, None)
        return None
    return data


def _cache_set(cache: dict, key, data):
    cache[key] = (now_ts(), data)
    return data


async def fast_search_candidates(query: str, want_video: bool = False, limit: int = 8) -> List[dict]:
    query = normalize_spaces(query)
    cache_key = (normalize_search_text(query), want_video, limit)
    cached = _cache_get(SEARCH_CACHE, cache_key)
    if cached is not None:
        return cached

    def _run():
        last_error = None
        for profile in ydl_profiles():
            try:
                opts = make_ydl_opts(
                    player_clients=profile["player_clients"],
                    socket_timeout=YTDLP_RESOLVE_TIMEOUT,
                    extra={"extract_flat": True, "skip_download": True, "lazy_playlist": True},
                )
                with YoutubeDL(opts) as ydl:
                    info = ydl.extract_info(f"ytsearch{limit}:{query}", download=False)
                    return [e for e in (info.get("entries") or []) if e]
            except Exception as e:
                last_error = e
                continue
        if last_error:
            raise last_error
        return []

    try:
        results = await asyncio.to_thread(_run)
    except Exception as e:
        print(f"fast_search_candidates error: {e}")
        results = []
    return _cache_set(SEARCH_CACHE, cache_key, results)


def _candidate_ref(entry: dict) -> str:
    if entry.get("webpage_url"):
        return entry["webpage_url"]
    if entry.get("url") and str(entry.get("url", "")).startswith("http"):
        return entry["url"]
    if entry.get("id"):
        return f"https://www.youtube.com/watch?v={entry['id']}"
    return entry.get("url") or ""


def score_search_entry(query: str, entry: dict) -> float:
    q = normalize_search_text(query)
    title = normalize_search_text(entry.get("title") or "")
    if not q or not title:
        return 0.0
    ratio = SequenceMatcher(None, q, title).ratio()
    q_words = [w for w in q.split() if w]
    contains_bonus = 0.22 if q in title else 0.0
    word_bonus = sum(0.045 for w in q_words if w in title)
    all_words_bonus = 0.14 if q_words and all(w in title for w in q_words) else 0.0
    prefix_bonus = 0.08 if title.startswith(q) else 0.0
    short_bonus = 0.03 if len(title) <= 90 else 0.0
    view_count = int(entry.get("view_count") or 0)
    views_bonus = min(math.log10(view_count + 1) / 15.0, 0.12) if view_count > 0 else 0.0
    return ratio + contains_bonus + word_bonus + all_words_bonus + prefix_bonus + short_bonus + views_bonus


async def resolve_media(query: str, want_video: bool = False) -> dict:
    query = normalize_spaces(query)
    cache_key = (query.casefold(), want_video)
    cached = _cache_get(RESOLVE_CACHE, cache_key)
    if cached is not None:
        return cached

    async def _extract_detail(target: str) -> dict:
        def _run():
            fmt = "best[ext=mp4]/best" if want_video else "bestaudio[ext=m4a]/bestaudio/best"
            last_error = None
            for profile in ydl_profiles():
                try:
                    opts = make_ydl_opts(
                        player_clients=profile["player_clients"],
                            socket_timeout=YTDLP_RESOLVE_TIMEOUT,
                        extra={"format": fmt, "extract_flat": False, "skip_download": True},
                    )
                    with YoutubeDL(opts) as ydl:
                        info = ydl.extract_info(target, download=False)
                        if info and "entries" in info:
                            info = next((e for e in info["entries"] if e), None)
                        if info:
                            return info or {}
                except Exception as e:
                    last_error = e
                    continue
            if last_error:
                raise last_error
            return {}
        return await asyncio.to_thread(_run)

    try:
        if looks_like_url(query):
            info = await _extract_detail(query)
            if not info.get("url") and info.get("webpage_url") and info.get("webpage_url") != query:
                info = await _extract_detail(info["webpage_url"])
            if info:
                info["query"] = query
                info.setdefault("webpage_url", query)
                return _cache_set(RESOLVE_CACHE, cache_key, info)

        candidates = await fast_search_candidates(query, want_video=want_video, limit=1)
        if not candidates:
            return {}
        best = candidates[0]
        target = _candidate_ref(best) or query
        info = await _extract_detail(target)
        if not info:
            return {}
        if not info.get("title"):
            info["title"] = best.get("title") or "Unknown"
        info.setdefault("duration", best.get("duration") or 0)
        info.setdefault("webpage_url", target)
        info["query"] = query
        return _cache_set(RESOLVE_CACHE, cache_key, info)
    except Exception as e:
        print(f"resolve_media error: {e}")
        return {}


async def ytdlp_extract(query: str, want_video: bool = False, search_only: bool = False) -> dict:
    if search_only:
        if looks_like_url(query):
            info = await resolve_media(query, want_video=want_video)
            return {"entries": [info] if info else []}
        entries = await fast_search_candidates(query, want_video=want_video, limit=5)
        return {"entries": entries[:5]}
    return await resolve_media(query, want_video=want_video)


async def ytdlp_download(query: str, kind: str) -> Tuple[str, dict, str]:
    target = normalize_spaces(query)
    want_video = kind == "video"
    if not looks_like_url(target):
        resolved = await resolve_media(target, want_video=want_video)
        target = resolved.get("webpage_url") or resolved.get("original_url") or resolved.get("query") or target

    def _run():
        outdir = "/tmp"
        os.makedirs(outdir, exist_ok=True)
        uid = str(uuid.uuid4())
        base_extra = {
            "outtmpl": f"{outdir}/{uid}.%(ext)s",
            "writethumbnail": True,
            "postprocessors": [{"key": "FFmpegThumbnailsConvertor", "format": "jpg"}],
            "socket_timeout": YTDLP_DOWNLOAD_TIMEOUT,
        }
        if kind == "audio":
            base_extra.update({
                "format": "bestaudio[ext=m4a]/bestaudio/best",
            })
        else:
            base_extra.update({
                "format": "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
                "merge_output_format": "mp4",
            })

        q = target if looks_like_url(target) else f"ytsearch1:{target}"
        last_error = None
        for profile in ydl_profiles():
            try:
                opts = make_ydl_opts(
                    player_clients=profile["player_clients"],
                    socket_timeout=YTDLP_DOWNLOAD_TIMEOUT,
                    extra=base_extra,
                )
                with YoutubeDL(opts) as ydl:
                    info = ydl.extract_info(q, download=True)
                    if info and "entries" in info:
                        info = next((e for e in info["entries"] if e), None)
                    path = info.get("_filename") if info else None
                    if not path:
                        for ext in ("mp3", "m4a", "mp4", "webm", "mkv"):
                            p = f"{outdir}/{uid}.{ext}"
                            if os.path.exists(p):
                                path = p
                                break
                    thumb_path = None
                    for ext in ("jpg", "jpeg", "webp", "png"):
                        tp = f"{outdir}/{uid}.{ext}"
                        if os.path.exists(tp):
                            thumb_path = tp
                            break
                    if path:
                        return (path or "", info or {}, thumb_path or "")
            except Exception as e:
                last_error = e
                continue
        if last_error:
            raise last_error
        return ("", {}, "")
    return await asyncio.to_thread(_run)


@dataclass
class Track:
    title: str
    source: str
    stream_url: str
    duration: int
    requester_id: int
    request_msg_id: int = 0
    thumbnail: Optional[str] = None
    kind: str = "audio"
    seek_offset: int = 0
    query: str = ""
    webpage_url: str = ""
    uploader: str = ""
    view_count: int = 0
    channel_name: str = ""
    card_path: str = ""
    cached_path: str = ""
    resolved_at: int = 0

@dataclass
class ChatState:
    queue: List[Track] = field(default_factory=list)
    current_index: int = 0
    playing: bool = False
    paused: bool = False
    started_at: int = 0
    paused_at: int = 0
    now_msg: Optional[Tuple[int, int]] = None
    progress_task: Optional[asyncio.Task] = None
    loop: bool = False
    admins_only: bool = False
    athan_enabled: bool = False
    athan_pin_message_id: Optional[int] = None
    athan_unpin_task: Optional[asyncio.Task] = None
    audio_filter: str = "normal"
    crossfade_seconds: int = 0
    recording: bool = False
    recording_path: str = ""
    recording_mode: str = ""
    recording_started_at: int = 0
    recording_proc: Any = None
    last_activity: int = field(default_factory=now_ts)


bot = Client("bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
assistant = Client("assistant", api_id=API_ID, api_hash=API_HASH, session_string=ASSISTANT_SESSION)
tgcalls = PyTgCalls(assistant)

states: Dict[int, ChatState] = {}


def get_state(chat_id: int) -> ChatState:
    if chat_id not in states:
        states[chat_id] = ChatState()
    states[chat_id].last_activity = now_ts()
    return states[chat_id]


def current_week_key() -> str:
    iso = local_now().isocalendar()
    return f"{iso.year}-W{int(iso.week):02d}"


def sanitize_filename(name: str, fallback: str = "file") -> str:
    cleaned = re.sub(r'[^\w\-\u0600-\u06FF\. ]+', '_', (name or '').strip(), flags=re.UNICODE)
    cleaned = normalize_spaces(cleaned).replace(' ', '_').strip('._')
    return cleaned[:100] or fallback


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def load_extra_feature_state():
    global GROUP_STATS, USER_PLAYLISTS, CACHE_INDEX
    GROUP_STATS = _load_json_file(GROUP_STATS_FILE, {})
    USER_PLAYLISTS = _load_json_file(USER_PLAYLISTS_FILE, {})
    CACHE_INDEX = _load_json_file(CACHE_INDEX_FILE, {})
    ensure_dir(CACHE_DIR)
    ensure_dir(RECORDER_DIR)
    ensure_dir(NOW_PLAYING_CARDS_DIR)


def save_group_stats():
    _save_json_file(GROUP_STATS_FILE, GROUP_STATS)


def save_user_playlists():
    _save_json_file(USER_PLAYLISTS_FILE, USER_PLAYLISTS)


def save_cache_index():
    _save_json_file(CACHE_INDEX_FILE, CACHE_INDEX)


def get_help_text(section: str, user_id: int) -> str:
    if get_lang(user_id) == "en":
        if section == "members":
            return (
                "👤 <b>Member Commands:</b>\n\n"
                "» <b>pause</b> / <b>resume</b> / <b>skip</b> / <b>stop</b> / <b>loop</b>\n"
                "» <b>save</b> - Save current song to your playlist\n"
                "» <b>my playlist</b> - Show your saved songs\n"
                "» <b>play my playlist</b> - Queue your saved songs\n"
                "» <b>clear my playlist</b> - Clear your personal playlist\n"
                "» <b>leaderboard</b> / <b>stats</b> - Weekly group statistics\n"
                "» <b>slowed</b> / <b>nightcore</b> / <b>bassboost</b> / <b>filter off</b> - Live audio filters"
            )
        if section == "playback2":
            return (
                "🎵 <b>Playback Commands 2/2:</b>\n\n"
                "» <b>quran</b> / <b>songs</b>\n"
                "» <b>record</b> - Start call recording\n"
                "» <b>stop recording</b> - Stop recording and send file\n"
                "» <b>crossfade</b> + seconds / <b>crossfade off</b> - Smooth transitions\n"
                "» <b>athan on/off</b> / <b>athan info</b> / <b>test athan</b>\n"
                "» <b>ping</b> / <b>source</b>"
            )
        return (
            "🎵 <b>Playback Commands 1/2:</b>\n\n"
            "» <b>play</b> - Play audio\n"
            "» <b>vplay</b> - Play video\n"
            "» <b>tiktok</b> / <b>reels</b> - Play TikTok or Reels link instantly\n"
            "» <b>to video</b> / <b>to audio</b> - Switch current mode\n"
            "» <b>shuffle</b> - Shuffle the queue\n"
            "» <b>search</b> - Search YouTube\n"
            "» <b>down</b> / <b>yout</b> - Download audio\n"
            "» <b>vdown</b> - Download video\n"
            "» <b>forward</b> + seconds / <b>back</b> + seconds\n"
            "» <b>cache</b> - Local cache status\n"
            "» <b>slowed</b> / <b>nightcore</b> / <b>bassboost</b> - Professional audio filters"
        )

    if section == "members":
        return (
            "👤 <b>أوامر الاعضاء :</b>\n\n"
            "» <b>ايقاف مؤقت</b> أو <b>اوكف</b> - ايقاف التشغيل مؤقتاً\n"
            "» <b>استكمال</b> أو <b>كمل</b> - استكمال التشغيل\n"
            "» <b>تخطي</b> - تخطي الحالي\n"
            "» <b>ايقاف</b> أو <b>اسكت</b> - ايقاف التشغيل\n"
            "» <b>تكرار</b> أو <b>كررها</b> - تكرار الحالي\n"
            "» <b>حفظ</b> - حفظ الأغنية الحالية بقائمتك\n"
            "» <b>قائمتي</b> - عرض قائمتك الشخصية\n"
            "» <b>شغل قائمتي</b> - تشغيل كل المحفوظات\n"
            "» <b>مسح قائمتي</b> - حذف قائمتك الشخصية\n"
            "» <b>توب</b> أو <b>احصائيات</b> - احصائيات الجروب لهذا الأسبوع\n"
            "» <b>تبطيء</b> / <b>تسريع</b> / <b>تضخيم</b> / <b>فلتر عادي</b> - فلاتر صوت مباشرة"
        )
    if section == "playback2":
        return (
            "🎵 <b>أوامر التشغيل 2/2 :</b>\n\n"
            "» <b>قران</b> - قائمة القرآن\n"
            "» <b>اغاني</b> - قائمة الأغاني\n"
            "» <b>تسجيل</b> - بدء تسجيل المكالمة\n"
            "» <b>ايقاف التسجيل</b> - ايقاف التسجيل وإرسال الملف\n"
            "» <b>كروس</b> + ثواني / <b>كروس off</b> - تلاشي بين الأغاني\n"
            "» <b>تفعيل الاذان</b> / <b>تعطيل الاذان</b> / <b>معلومات الاذان</b> / <b>تيست الاذان</b>\n"
            "» <b>بنج</b> - سرعة الاستجابة\n"
            "» <b>سورس</b> - معلومات السورس"
        )
    return (
        "🎵 <b>أوامر التشغيل 1/2 :</b>\n\n"
        "» <b>شغل</b> أو <b>تشغيل</b> - تشغيل موسيقى\n"
        "» <b>فيد</b> أو <b>فيديو</b> - تشغيل فيديو\n"
        "» <b>تيك</b> أو <b>ريلز</b> - تشغيل رابط تيك توك أو ريلز فوراً\n"
        "» <b>تحويل لفيديو</b> - تحويل الحالي لفيديو\n"
        "» <b>رجوع للصوت</b> - رجوع للصوت فقط\n"
        "» <b>عشوائي</b> - خلط الطابور\n"
        "» <b>بحث</b> - بحث يوتيوب\n"
        "» <b>يوت</b> أو <b>تحميل</b> - تنزيل صوت\n"
        "» <b>تحميل فيديو</b> أو <b>vdown</b> - تنزيل فيديو\n"
        "» <b>تقديم</b> + ثواني / <b>رجوع</b> + ثواني\n"
        "» <b>كاش</b> - معلومات الكاش المحلي\n"
        "» <b>تبطيء</b> / <b>تسريع</b> / <b>تضخيم</b> - فلاتر صوت احترافية"
    )


def track_stats_key(track: Track) -> str:
    return normalize_search_text(track.webpage_url or track.query or track.title)[:180] or f"track:{sanitize_filename(track.title)}"


def track_reference(track: Track) -> str:
    return (track.webpage_url or track.query or track.source or '').strip()


def get_stats_week(chat_id: int) -> Dict[str, Any]:
    chat_bucket = GROUP_STATS.setdefault(str(chat_id), {})
    weeks = chat_bucket.setdefault("weeks", {})
    return weeks.setdefault(current_week_key(), {"users": {}, "songs": {}, "requests": 0, "plays": 0})


def record_group_request(chat_id: int, track: Track):
    week = get_stats_week(chat_id)
    user_key = str(track.requester_id)
    week["users"][user_key] = int(week["users"].get(user_key, 0)) + 1
    song_key = track_stats_key(track)
    song = week["songs"].setdefault(song_key, {"title": track.title, "requested": 0, "played": 0})
    song["title"] = track.title
    song["requested"] = int(song.get("requested", 0)) + 1
    week["requests"] = int(week.get("requests", 0)) + 1
    save_group_stats()


def record_group_play(chat_id: int, track: Track):
    week = get_stats_week(chat_id)
    song_key = track_stats_key(track)
    song = week["songs"].setdefault(song_key, {"title": track.title, "requested": 0, "played": 0})
    song["title"] = track.title
    song["played"] = int(song.get("played", 0)) + 1
    week["plays"] = int(week.get("plays", 0)) + 1
    save_group_stats()


async def get_group_leaderboard_text(chat_id: int, user_id: int) -> str:
    week = get_stats_week(chat_id)
    top_user_id = 0
    top_user_count = 0
    for raw_uid, count in (week.get("users") or {}).items():
        if int(count or 0) > top_user_count:
            top_user_id = int(raw_uid)
            top_user_count = int(count or 0)
    top_song = None
    for song in (week.get("songs") or {}).values():
        if not top_song or int(song.get("played", 0)) > int(top_song.get("played", 0)):
            top_song = song

    if get_lang(user_id) == "en":
        parts = [f"🏆 <b>Weekly Leaderboard</b> — <code>{current_week_key()}</code>"]
        if top_user_id:
            try:
                user = await bot.get_users(top_user_id)
                user_txt = mention_user(user)
            except Exception:
                user_txt = f"<code>{top_user_id}</code>"
            parts.append(f"👤 Most requests: {user_txt} — <b>{top_user_count}</b>")
        else:
            parts.append("👤 Most requests: No data yet")
        if top_song:
            parts.append(f"🎵 Most played song: <b>{safe_html(top_song.get('title', 'Unknown'))}</b> — <b>{int(top_song.get('played', 0))}</b>")
        else:
            parts.append("🎵 Most played song: No data yet")
        parts.append(f"📦 Total requests: <b>{int(week.get('requests', 0))}</b>")
        return "\n".join(parts)

    parts = [f"🏆 <b>إحصائيات هذا الأسبوع</b> — <code>{current_week_key()}</code>"]
    if top_user_id:
        try:
            user = await bot.get_users(top_user_id)
            user_txt = mention_user(user)
        except Exception:
            user_txt = f"<code>{top_user_id}</code>"
        parts.append(f"👤 أكثر شخص طلب أغاني: {user_txt} — <b>{top_user_count}</b>")
    else:
        parts.append("👤 أكثر شخص طلب أغاني: لا توجد بيانات بعد")
    if top_song:
        parts.append(f"🎵 أكثر أغنية تم تشغيلها: <b>{safe_html(top_song.get('title', 'غير معروف'))}</b> — <b>{int(top_song.get('played', 0))}</b>")
    else:
        parts.append("🎵 أكثر أغنية تم تشغيلها: لا توجد بيانات بعد")
    parts.append(f"📦 مجموع الطلبات: <b>{int(week.get('requests', 0))}</b>")
    return "\n".join(parts)


def get_user_playlist(user_id: int) -> List[dict]:
    return list((USER_PLAYLISTS.get(str(user_id), {}) or {}).get("tracks", []) or [])


def save_user_playlist_track(user_id: int, track: Track) -> Tuple[bool, str]:
    ref = track_reference(track)
    if not ref or is_local_track(track):
        return False, "local"
    root = USER_PLAYLISTS.setdefault(str(user_id), {"tracks": []})
    tracks = root.setdefault("tracks", [])
    if any((item.get("ref") or "") == ref for item in tracks):
        return False, "exists"
    if len(tracks) >= MAX_PLAYLIST_ITEMS:
        return False, "full"
    tracks.append({
        "title": track.title,
        "ref": ref,
        "kind": track.kind,
        "saved_at": now_ts(),
    })
    save_user_playlists()
    return True, ref


def clear_user_playlist_items(user_id: int) -> int:
    tracks = get_user_playlist(user_id)
    USER_PLAYLISTS[str(user_id)] = {"tracks": []}
    save_user_playlists()
    return len(tracks)


def cache_lookup(query: str) -> Optional[dict]:
    qn = normalize_search_text(query)
    for item in (CACHE_INDEX.get("items") or {}).values():
        path = item.get("path") or ""
        refs = {normalize_search_text(item.get("ref") or ""), normalize_search_text(item.get("query") or ""), normalize_search_text(item.get("title") or "")}
        if qn in refs and path and os.path.exists(path):
            return item
    return None


def cache_stats_text(user_id: int) -> str:
    items = list((CACHE_INDEX.get("items") or {}).values())
    valid = [x for x in items if x.get("path") and os.path.exists(x.get("path"))]
    total_size = 0
    for item in valid:
        try:
            total_size += os.path.getsize(item["path"])
        except Exception:
            pass
    if get_lang(user_id) == "en":
        return f"⚡ <b>Local Cache</b>\nFiles: <b>{len(valid)}</b>\nApprox size: <b>{round(total_size / 1024 / 1024, 2)} MB</b>"
    return f"⚡ <b>الكاش المحلي</b>\nعدد الملفات: <b>{len(valid)}</b>\nالحجم التقريبي: <b>{round(total_size / 1024 / 1024, 2)} MB</b>"


async def prefetch_cache(ref: str, title: str = ""):
    ref = normalize_spaces(ref)
    if not ref or ref in CACHE_DOWNLOAD_TASKS:
        return

    async def _job():
        try:
            item = cache_lookup(ref)
            if item:
                return
            path, info, _ = await ytdlp_download(ref, "audio")
            if not path or not os.path.exists(path):
                return
            ensure_dir(CACHE_DIR)
            ext = os.path.splitext(path)[1] or ".m4a"
            final_name = sanitize_filename(info.get("title") or title or ref, fallback="cache") + "_" + uuid.uuid4().hex[:8] + ext
            final_path = str(Path(CACHE_DIR) / final_name)
            os.replace(path, final_path)
            items = CACHE_INDEX.setdefault("items", {})
            items[uuid.uuid4().hex] = {
                "ref": ref,
                "query": ref,
                "title": info.get("title") or title or ref,
                "path": final_path,
                "uploader": info.get("uploader") or "",
                "channel_name": info.get("channel") or info.get("uploader") or "",
                "view_count": int(info.get("view_count") or 0),
                "created_at": now_ts(),
            }
            while len(items) > CACHE_HOT_LIMIT:
                oldest_key = sorted(items.items(), key=lambda kv: int((kv[1] or {}).get("created_at", 0)))[0][0]
                old = items.pop(oldest_key, None) or {}
                old_path = old.get("path") or ""
                if old_path and os.path.exists(old_path):
                    try:
                        os.remove(old_path)
                    except Exception:
                        pass
            save_cache_index()
        except Exception as e:
            print(f"prefetch_cache error: {e}")
        finally:
            CACHE_DOWNLOAD_TASKS.pop(ref, None)

    CACHE_DOWNLOAD_TASKS[ref] = asyncio.create_task(_job())


async def touch_cache_heat(track: Track):
    ref = track_reference(track)
    if not ref or track.kind != "audio":
        return
    heat = CACHE_INDEX.setdefault("heat", {})
    count = int(heat.get(ref, 0)) + 1
    heat[ref] = count
    save_cache_index()
    if count >= CACHE_MIN_REQUESTS:
        await prefetch_cache(ref, track.title)


async def enqueue_track(chat_id: int, track: Track):
    st = get_state(chat_id)
    st.queue.append(track)
    try:
        record_group_request(chat_id, track)
    except Exception as e:
        print(f"record_group_request error: {e}")
    try:
        await touch_cache_heat(track)
    except Exception as e:
        print(f"touch_cache_heat error: {e}")


async def ffmpeg_run(args: List[str]) -> bool:
    try:
        proc = await asyncio.create_subprocess_exec(*args, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
        await proc.communicate()
        return proc.returncode == 0
    except Exception as e:
        print(f"ffmpeg_run error: {e}")
        return False


def get_current_track(chat_id: int) -> Optional[Track]:
    st = get_state(chat_id)
    if not st.queue or st.current_index >= len(st.queue):
        return None
    return st.queue[st.current_index]


def join_audio_filters(filters_list: List[str]) -> str:
    parts = [x.strip() for x in filters_list if x and x.strip()]
    return ",".join(parts)


def build_ffmpeg_parameters(chat_id: int, track: Track) -> Optional[str]:
    st = get_state(chat_id)
    args: List[str] = []
    if int(track.seek_offset or 0) > 0:
        args.extend(["-ss", str(int(track.seek_offset))])
    audio_filters = []
    preset = AUDIO_FILTER_PRESETS.get(st.audio_filter or "normal") or AUDIO_FILTER_PRESETS["normal"]
    if preset.get("af"):
        audio_filters.append(preset["af"])
    if st.crossfade_seconds > 0 and track.duration > max(st.crossfade_seconds + 1, 2):
        fade_d = min(st.crossfade_seconds, max(track.duration - 1, 1))
        fade_start = max(track.duration - fade_d, 0)
        audio_filters.append(f"afade=t=in:st=0:d={min(1, fade_d)}")
        audio_filters.append(f"afade=t=out:st={fade_start}:d={fade_d}")
    af = join_audio_filters(audio_filters)
    if af:
        args.extend(["-af", af])
    return " ".join(args) if args else None


async def apply_live_audio_filter(chat_id: int, filter_key: str):
    st = get_state(chat_id)
    st.audio_filter = filter_key if filter_key in AUDIO_FILTER_PRESETS else "normal"
    track = get_current_track(chat_id)
    if not track:
        return
    elapsed = get_track_elapsed(st, track)
    track.seek_offset = elapsed
    await play_or_change_stream(chat_id, track)
    st.playing = True
    st.paused = False
    st.paused_at = 0
    st.started_at = now_ts() - elapsed
    reset_progress_task(chat_id)


async def start_playlist_queue(chat_id: int, user_id: int, msg_id: int) -> Tuple[int, int]:
    playlist = get_user_playlist(user_id)
    added = 0
    failed = 0
    for item in playlist:
        tr = await build_track_from_query(item.get("ref") or "", want_video=(item.get("kind") == "video"), requester_id=user_id, request_msg_id=msg_id)
        if tr:
            await enqueue_track(chat_id, tr)
            added += 1
        else:
            failed += 1
    return added, failed


async def start_call_recording(chat_id: int) -> Tuple[bool, str]:
    st = get_state(chat_id)
    if st.recording:
        return False, st.recording_path
    ensure_dir(RECORDER_DIR)
    out_path = str(Path(RECORDER_DIR) / f"record_{chat_id}_{now_ts()}.mp3")
    native_ok = False
    try:
        await call_first(tgcalls, ["start_recording", "record", "start_output", "start_recording_to_file"], chat_id, out_path)
        native_ok = True
        st.recording_mode = "native"
    except Exception as e:
        print(f"native recording unavailable: {e}")
    if not native_ok:
        current = get_current_track(chat_id)
        if not current:
            return False, ""
        ref = current.stream_url
        elapsed = get_track_elapsed(st, current)
        duration_left = max((current.duration - elapsed), 0) if current.duration else 3600
        try:
            proc = await asyncio.create_subprocess_exec(
                "ffmpeg", "-y", "-ss", str(elapsed), "-i", ref,
                "-t", str(max(duration_left, 60)), "-vn", "-c:a", "libmp3lame", "-b:a", "192k", out_path,
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
            )
            st.recording_proc = proc
            st.recording_mode = "outbound"
        except Exception as e:
            print(f"fallback recording error: {e}")
            return False, ""
    st.recording = True
    st.recording_path = out_path
    st.recording_started_at = now_ts()
    return True, out_path


async def stop_call_recording(chat_id: int) -> str:
    st = get_state(chat_id)
    if not st.recording:
        return ""
    try:
        if st.recording_mode == "native":
            try:
                await call_first(tgcalls, ["stop_recording", "record_stop", "stop_output", "stop_recording_to_file"], chat_id)
            except Exception as e:
                print(f"stop native recording error: {e}")
        elif st.recording_proc is not None:
            try:
                st.recording_proc.terminate()
                await st.recording_proc.communicate()
            except Exception:
                pass
    finally:
        st.recording = False
        st.recording_mode = ""
        st.recording_proc = None
    return st.recording_path


def get_track_elapsed(st: ChatState, track: Track) -> int:
    if not st.started_at:
        return int(track.seek_offset or 0)
    anchor = st.paused_at if st.paused and st.paused_at else now_ts()
    return max(0, int(anchor - st.started_at))


def is_local_track(track: Track) -> bool:
    return track.source in ("telegram", "local")


def is_cache_track(track: Track) -> bool:
    return track.source == "cache" or bool(track.cached_path)


def is_remote_stream_track(track: Track) -> bool:
    return not is_local_track(track) and not is_cache_track(track)


def track_stream_refresh_needed(track: Track) -> bool:
    return bool(is_remote_stream_track(track) and (not track.resolved_at or (now_ts() - int(track.resolved_at or 0)) >= STREAM_URL_REFRESH_AFTER))


async def refresh_track_stream(track: Track) -> bool:
    if not is_remote_stream_track(track):
        track.resolved_at = now_ts()
        return True
    base_query = track.webpage_url or track.query or track.source
    if not base_query:
        return False
    info = await resolve_media(base_query, want_video=(track.kind == "video"))
    if not info or not info.get("url"):
        return False
    track.stream_url = info["url"]
    track.duration = int(info.get("duration") or track.duration or 0)
    track.title = info.get("title") or track.title
    track.webpage_url = info.get("webpage_url") or track.webpage_url or base_query
    track.uploader = info.get("uploader") or track.uploader
    track.view_count = int(info.get("view_count") or track.view_count or 0)
    track.channel_name = info.get("channel") or info.get("uploader") or track.channel_name
    track.resolved_at = now_ts()
    return True


def cleanup_local_track(track: Optional[Track]):
    try:
        if track and is_local_track(track) and track.stream_url and os.path.exists(track.stream_url):
            os.remove(track.stream_url)
    except Exception:
        pass
    try:
        if track and track.card_path and os.path.exists(track.card_path):
            os.remove(track.card_path)
    except Exception:
        pass


def cleanup_queue_files(queue: List[Track]):
    for track in queue:
        cleanup_local_track(track)


def reset_progress_task(chat_id: int):
    st = get_state(chat_id)
    if st.progress_task and not st.progress_task.done():
        st.progress_task.cancel()
    st.progress_task = asyncio.create_task(update_progress_loop(chat_id))


def build_stream(chat_id: int, track: Track):
    ffmpeg_params = build_ffmpeg_parameters(chat_id, track)
    if track.kind == "video":
        return MediaStream(
            track.stream_url,
            audio_parameters=AudioQuality.HIGH,
            video_parameters=VideoQuality.HD_720p,
            ffmpeg_parameters=ffmpeg_params,
        )
    return MediaStream(track.stream_url, audio_parameters=AudioQuality.HIGH, ffmpeg_parameters=ffmpeg_params)


async def force_leave_call(chat_id: int):
    st = get_state(chat_id)
    cleanup_queue_files(st.queue)
    st.queue.clear()
    st.current_index = 0
    st.playing = False
    st.paused = False
    st.paused_at = 0
    st.loop = False
    st.now_msg = None
    st.audio_filter = "normal"
    if st.recording:
        try:
            await stop_call_recording(chat_id)
        except Exception:
            pass
    if st.progress_task and not st.progress_task.done():
        st.progress_task.cancel()
    try:
        await call_first(tgcalls, ["pause_stream", "stop_stream", "stop"], chat_id)
    except Exception:
        pass
    try:
        await call_first(tgcalls, ["leave_call", "leave_group_call", "leave"], chat_id)
    except Exception:
        pass


def _display_name_from_link(link: str, fallback: str) -> str:
    link = (link or "").strip()
    if not link:
        return fallback
    try:
        parsed = urlparse(link)
        path = (parsed.path or "").strip("/")
        if parsed.netloc and parsed.netloc.endswith("t.me") and path:
            return f"@{path.split('/')[0]}"
        if path:
            return path.split('/')[-1] or fallback
        if parsed.netloc:
            return parsed.netloc.replace('www.', '')
    except Exception:
        pass
    return fallback


def _pick_font(size: int, bold: bool = False):
    candidates = [
        '/usr/share/fonts/truetype/noto/NotoSerifDisplay-Bold.ttf' if bold else '/usr/share/fonts/truetype/noto/NotoSerifDisplay-Regular.ttf',
        '/usr/share/fonts/truetype/ebgaramond/EBGaramond12-Bold.ttf' if bold else '/usr/share/fonts/truetype/ebgaramond/EBGaramond12-Regular.ttf',
        '/usr/share/fonts/truetype/dejavu/DejaVuSerif-Bold.ttf' if bold else '/usr/share/fonts/truetype/dejavu/DejaVuSerif.ttf',
    ]
    for path in candidates:
        if os.path.exists(path):
            return ImageFont.truetype(path, size=size)
    return ImageFont.load_default()


def _fit_text(draw: ImageDraw.ImageDraw, text: str, font, max_width: int) -> str:
    value = normalize_spaces(str(text or ''))
    if not value:
        return ''
    if draw.textlength(value, font=font) <= max_width:
        return value
    ellipsis = '...'
    while value and draw.textlength(value + ellipsis, font=font) > max_width:
        value = value[:-1]
    return (value + ellipsis) if value else ellipsis


def _draw_gold_text(draw: ImageDraw.ImageDraw, xy: Tuple[int, int], text: str, font, fill=(235, 201, 109), shadow=(0, 0, 0, 220)):
    x, y = xy
    for dx, dy in ((3, 3), (2, 2), (1, 1)):
        draw.text((x + dx, y + dy), text, font=font, fill=shadow)
    draw.text((x, y), text, font=font, fill=fill)


def _format_views(value: int) -> str:
    try:
        value = int(value or 0)
    except Exception:
        value = 0
    return f"{value:,}" if value > 0 else 'Unknown'


def _duration_label(track: Track) -> str:
    total = hms(track.duration) if int(track.duration or 0) > 0 else 'Live'
    return f"00:00 / {total}" if total != 'Live' else total


def build_nowplaying_card(track: Track) -> str:
    template_ref = NOW_PLAYING_TEMPLATE or BOT_IMAGE
    if not template_ref:
        return ''

    try:
        if template_ref.startswith('http://') or template_ref.startswith('https://'):
            req = Request(template_ref, headers={'User-Agent': YTDLP_USER_AGENT})
            with urlopen(req, timeout=20) as resp:
                raw = resp.read()
            base = Image.open(BytesIO(raw)).convert('RGBA')
        else:
            base = Image.open(template_ref).convert('RGBA')
    except Exception as e:
        print(f"build_nowplaying_card template error: {e}")
        return ''

    try:
        base = base.resize((1280, 720))
        overlay = Image.new('RGBA', base.size, (0, 0, 0, 0))
        od = ImageDraw.Draw(overlay)

        # تنظيف مساحة النصوص المتغيرة فقط حتى تبقى الصورة الأساسية نفسها
        od.rounded_rectangle((602, 450, 1208, 650), radius=18, fill=(4, 4, 6, 252))
        composed = Image.alpha_composite(base, overlay)
        draw = ImageDraw.Draw(composed)

        info_font = _pick_font(28, bold=False)
        channel = NOW_PLAYING_CHANNEL_NAME or _display_name_from_link(CHANNEL_LINK, track.channel_name or track.uploader or 'Channel')
        source = NOW_PLAYING_SOURCE_NAME or _display_name_from_link(SOURCE_LINK, _display_name_from_link(track.webpage_url, track.source or 'Source'))

        lines = [
            f"Views :  {_format_views(track.view_count)}",
            f"Duration :  {_duration_label(track)}",
            f"Channel :  {channel}",
            f"Source :  {source}",
        ]
        y = 476
        max_width = 540
        for line in lines:
            fitted = _fit_text(draw, line, info_font, max_width)
            _draw_gold_text(draw, (632, y), fitted, info_font)
            y += 40

        ensure_dir(NOW_PLAYING_CARDS_DIR)
        out_path = str(Path(NOW_PLAYING_CARDS_DIR) / f"np_{uuid.uuid4().hex}.jpg")
        composed.convert('RGB').save(out_path, format='JPEG', quality=95, optimize=True)
        return out_path
    except Exception as e:
        print(f"build_nowplaying_card render error: {e}")
        return ''


async def get_nowplaying_caption(track: Track, user_id: int) -> str:
    try:
        user = await bot.get_users(track.requester_id)
    except Exception:
        user = None
    return (
        f"{_t(user_id, 'song_name')} {safe_html(track.title)}\n"
        f"{_t(user_id, 'duration_time')} {hms(track.duration)}\n"
        f"{_t(user_id, 'requested_by')} {mention_user(user)}"
    )


def get_nowplaying_markup(chat_id: int, track: Track, user_id: int) -> InlineKeyboardMarkup:
    st = get_state(chat_id)
    elapsed = get_track_elapsed(st, track)
    bar = progress_bar(elapsed, track.duration, width=15)
    progress_str = f"⏱ {hms(elapsed)}  {bar}  {hms(track.duration)}"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(progress_str, callback_data="noop")],
        [
            InlineKeyboardButton(_t(user_id, "btn_end"), callback_data=f"ctl:stop:{chat_id}"),
            InlineKeyboardButton(_t(user_id, "btn_resume"), callback_data=f"ctl:resume:{chat_id}"),
            InlineKeyboardButton(_t(user_id, "btn_pause"), callback_data=f"ctl:pause:{chat_id}"),
        ],
        [InlineKeyboardButton("ᴄʜᴀɴɴᴇʟ", url=CHANNEL_LINK)],
        [InlineKeyboardButton("⦗ 𝐀𝐝𝐝 𝐌𝐞 𝐓𝐨 𝐘𝐨𝐮𝐫 𝐆𝐫𝐨𝐮𝐩 ⦘", url=get_bot_add_url())],
        [InlineKeyboardButton("🗑", callback_data=f"ctl:delnp:{chat_id}")],
    ])


async def send_nowplaying(chat_id: int, track: Track):
    st = get_state(chat_id)
    caption = await get_nowplaying_caption(track, track.requester_id)
    markup = get_nowplaying_markup(chat_id, track, track.requester_id)
    rep_params = ReplyParameters(message_id=track.request_msg_id) if track.request_msg_id else None
    card_path = await asyncio.to_thread(build_nowplaying_card, track)
    if card_path:
        track.card_path = card_path
    photo_ref = card_path or track.thumbnail or BOT_IMAGE
    try:
        sent = await bot.send_photo(
            chat_id,
            photo=photo_ref,
            caption=caption,
            reply_markup=markup,
            reply_parameters=rep_params,
        )
    except Exception:
        sent = await bot.send_message(
            chat_id,
            caption,
            reply_markup=markup,
            link_preview_options=LinkPreviewOptions(is_disabled=True),
            reply_parameters=rep_params,
        )
    st.now_msg = (chat_id, sent.id)


async def update_progress_loop(chat_id: int):
    st = get_state(chat_id)
    while True:
        await asyncio.sleep(6)
        if not st.playing or st.paused:
            return
        try:
            if st.current_index < len(st.queue):
                track = st.queue[st.current_index]
                elapsed = get_track_elapsed(st, track)
                if track.duration > 0 and elapsed >= track.duration + 2:
                    asyncio.create_task(next_track(chat_id))
                    return
                if st.now_msg:
                    c_id, m_id = st.now_msg
                    try:
                        await bot.edit_message_reply_markup(c_id, m_id, reply_markup=get_nowplaying_markup(chat_id, track, track.requester_id))
                    except Exception:
                        pass
        except Exception:
            continue


async def play_or_change_stream(chat_id: int, track: Track):
    stream = build_stream(chat_id, track)
    try:
        return await call_first(tgcalls, ["change_stream"], chat_id, stream)
    except Exception:
        return await call_first(tgcalls, ["play"], chat_id, stream)


async def pause_stream(chat_id: int):
    return await call_first(tgcalls, ["pause_stream", "pause"], chat_id)


async def resume_stream(chat_id: int):
    return await call_first(tgcalls, ["resume_stream", "resume"], chat_id)


async def resume_current_track(chat_id: int):
    st = get_state(chat_id)
    await resume_stream(chat_id)
    if st.paused_at:
        st.started_at += max(0, now_ts() - st.paused_at)
    st.paused = False
    st.paused_at = 0
    reset_progress_task(chat_id)


async def seek_current_track(chat_id: int, seconds_delta: int, backward: bool = False) -> int:
    st = get_state(chat_id)
    if not st.queue or st.current_index >= len(st.queue):
        raise RuntimeError("No track to seek")
    track = st.queue[st.current_index]
    current_elapsed = get_track_elapsed(st, track)
    target = max(0, current_elapsed - seconds_delta) if backward else current_elapsed + seconds_delta
    if track.duration > 0:
        target = min(target, max(track.duration - 1, 0))
    track.seek_offset = target
    await play_or_change_stream(chat_id, track)
    st.playing = True
    st.paused = False
    st.paused_at = 0
    st.started_at = now_ts() - target
    reset_progress_task(chat_id)
    if st.now_msg:
        try:
            c_id, m_id = st.now_msg
            await bot.edit_message_reply_markup(c_id, m_id, reply_markup=get_nowplaying_markup(chat_id, track, track.requester_id))
        except Exception:
            pass
    return target


async def switch_current_media_mode(chat_id: int, to_video: bool):
    st = get_state(chat_id)
    if not st.queue or st.current_index >= len(st.queue):
        raise RuntimeError("No track to switch")
    track = st.queue[st.current_index]
    elapsed = get_track_elapsed(st, track)
    target_seek = max(0, min(elapsed, max((track.duration or 0) - 1, 0)))
    if to_video and track.kind == "video":
        return
    if (not to_video) and track.kind == "audio":
        return

    if is_local_track(track):
        if to_video and track.kind == "audio":
            raise RuntimeError("Local audio cannot become video")
        track.kind = "video" if to_video else "audio"
    else:
        base_query = track.webpage_url or track.query or track.source
        info = await resolve_media(base_query, want_video=to_video)
        if not info or not info.get("url"):
            raise RuntimeError("Switch resolve failed")
        track.kind = "video" if to_video else "audio"
        track.stream_url = info["url"]
        track.duration = int(info.get("duration") or track.duration or 0)
        track.title = info.get("title") or track.title
        track.webpage_url = info.get("webpage_url") or track.webpage_url or base_query

    track.seek_offset = target_seek
    await play_or_change_stream(chat_id, track)
    st.playing = True
    st.paused = False
    st.paused_at = 0
    st.started_at = now_ts() - target_seek
    reset_progress_task(chat_id)
    if st.now_msg:
        try:
            c_id, m_id = st.now_msg
            await bot.edit_message_reply_markup(c_id, m_id, reply_markup=get_nowplaying_markup(chat_id, track, track.requester_id))
        except Exception:
            pass


async def build_track_from_query(query: str, want_video: bool, requester_id: int, request_msg_id: int) -> Optional[Track]:
    if not want_video:
        cached = cache_lookup(query)
        if cached:
            return Track(
                title=cached.get("title") or "Cached Audio",
                source="cache",
                stream_url=cached.get("path") or "",
                duration=int(cached.get("duration") or 0),
                requester_id=requester_id,
                request_msg_id=request_msg_id,
                thumbnail=BOT_IMAGE,
                kind="audio",
                query=query,
                webpage_url=cached.get("ref") or query,
                uploader=cached.get("uploader") or "",
                view_count=int(cached.get("view_count") or 0),
                channel_name=cached.get("channel_name") or cached.get("uploader") or "",
                cached_path=cached.get("path") or "",
                resolved_at=now_ts(),
            )
    info = await resolve_media(query, want_video=want_video)
    if info and info.get("url"):
        return Track(
            title=info.get("title") or "Unknown",
            source=query,
            stream_url=info["url"],
            duration=int(info.get("duration") or 0),
            requester_id=requester_id,
            request_msg_id=request_msg_id,
            thumbnail=BOT_IMAGE,
            kind="video" if want_video else "audio",
            query=query,
            webpage_url=info.get("webpage_url") or (query if looks_like_url(query) else ""),
            uploader=info.get("uploader") or "",
            view_count=int(info.get("view_count") or 0),
            channel_name=info.get("channel") or info.get("uploader") or "",
            resolved_at=now_ts(),
        )
    if looks_like_url(query):
        path, info, _ = await ytdlp_download(query, "video" if want_video else "audio")
        if path and os.path.exists(path):
            return Track(
                title=info.get("title") or "Unknown",
                source="local",
                stream_url=path,
                duration=int(info.get("duration") or 0),
                requester_id=requester_id,
                request_msg_id=request_msg_id,
                thumbnail=BOT_IMAGE,
                kind="video" if want_video else "audio",
                query=query,
                webpage_url=info.get("webpage_url") or query,
                uploader=info.get("uploader") or "",
                resolved_at=now_ts(),
            )
    return None


async def start_track(chat_id: int, user_id: int):
    st = get_state(chat_id)
    if not st.queue:
        st.playing = False
        st.paused = False
        st.paused_at = 0
        return
    joined = await ensure_assistant_joined(chat_id, user_id)
    if not joined:
        try:
            err_msg = "❌ Could not add the assistant account. Please add it manually and ensure the bot is an admin with 'add users' rights." if get_lang(user_id) == "en" else "❌ لم أتمكن من إضافة حساب المساعد، يرجى إضافته يدوياً وتأكد أن البوت مشرف ولديه صلاحية إضافة مستخدمين."
            await bot.send_message(chat_id, err_msg)
        except Exception:
            pass
        bad_index = min(st.current_index, len(st.queue) - 1)
        bad_track = st.queue.pop(bad_index)
        cleanup_local_track(bad_track)
        st.current_index = max(0, min(st.current_index, len(st.queue) - 1)) if st.queue else 0
        if st.queue:
            await start_track(chat_id, st.queue[st.current_index].requester_id)
        else:
            await force_leave_call(chat_id)
        return

    track = st.queue[st.current_index]
    if track_stream_refresh_needed(track):
        try:
            await refresh_track_stream(track)
        except Exception as e:
            print(f"pre-play refresh failed for {chat_id}: {e}")
    try:
        await play_or_change_stream(chat_id, track)
        st.playing = True
        st.paused = False
        st.paused_at = 0
        st.started_at = now_ts() - int(track.seek_offset or 0)
        record_group_play(chat_id, track)
        await send_nowplaying(chat_id, track)
        reset_progress_task(chat_id)
    except Exception as first_error:
        retried_ok = False
        if is_remote_stream_track(track):
            try:
                if await refresh_track_stream(track):
                    await play_or_change_stream(chat_id, track)
                    retried_ok = True
            except Exception as retry_error:
                print(f"play retry failed for {chat_id}: {retry_error}")
        if retried_ok:
            st.playing = True
            st.paused = False
            st.paused_at = 0
            st.started_at = now_ts() - int(track.seek_offset or 0)
            record_group_play(chat_id, track)
            await send_nowplaying(chat_id, track)
            reset_progress_task(chat_id)
            return
        print(f"start_track failed in {chat_id}: {first_error}")
        try:
            fail_msg = "❌ Failed to start this track, moving to the next one automatically." if get_lang(user_id) == "en" else "❌ تعذر تشغيل هذا المقطع، راح أنتقل تلقائياً للي بعده."
            await bot.send_message(chat_id, fail_msg)
        except Exception:
            pass
        bad_track = st.queue.pop(st.current_index)
        cleanup_local_track(bad_track)
        st.playing = False
        st.paused = False
        st.paused_at = 0
        if st.queue:
            st.current_index = max(0, min(st.current_index, len(st.queue) - 1))
            await start_track(chat_id, st.queue[st.current_index].requester_id)
        else:
            await force_leave_call(chat_id)


async def next_track(chat_id: int):
    st = get_state(chat_id)
    if not st.queue:
        return
    prev_index = st.current_index
    if prev_index < len(st.queue):
        st.queue[prev_index].seek_offset = 0
    if st.loop:
        st.current_index = prev_index
    else:
        st.current_index += 1
    if st.current_index >= len(st.queue):
        if not st.loop and prev_index < len(st.queue):
            cleanup_local_track(st.queue[prev_index])
        await force_leave_call(chat_id)
        return
    if not st.loop and prev_index < len(st.queue):
        cleanup_local_track(st.queue[prev_index])
    user_id = st.queue[st.current_index].requester_id
    await start_track(chat_id, user_id)


if hasattr(tgcalls, "on_stream_end"):
    @tgcalls.on_stream_end()
    async def _on_stream_end(_, update: Any):
        chat_id = getattr(update, "chat_id", None)
        if not chat_id:
            return
        try:
            await next_track(chat_id)
        except Exception:
            pass

if hasattr(tgcalls, "on_playout_ended"):
    @tgcalls.on_playout_ended()
    async def _on_playout_ended(_, update: Any):
        chat_id = getattr(update, "chat_id", None)
        if not chat_id:
            return
        try:
            await next_track(chat_id)
        except Exception:
            pass


@bot.on_chat_member_updated()
async def on_chat_member_update(client: Client, update: ChatMemberUpdated):
    global ASSISTANT_ID
    try:
        if update.chat.type == ChatType.PRIVATE:
            return
        if not ASSISTANT_ID:
            ass_me = await assistant.get_me()
            ASSISTANT_ID = ass_me.id
        old_member = getattr(update, "old_chat_member", None)
        new_member = getattr(update, "new_chat_member", None)
        target_user = None
        if new_member and getattr(new_member, "user", None):
            target_user = new_member.user
        elif old_member and getattr(old_member, "user", None):
            target_user = old_member.user
        if not target_user or target_user.id != ASSISTANT_ID:
            return
        old_status = getattr(old_member, "status", None)
        new_status = getattr(new_member, "status", None)
        removed_statuses = [ChatMemberStatus.BANNED, ChatMemberStatus.LEFT]
        valid_old_statuses = [ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER, ChatMemberStatus.RESTRICTED]
        if new_status not in removed_statuses or old_status not in valid_old_statuses:
            return
        actor = getattr(update, "from_user", None)
        if actor and actor.id == ASSISTANT_ID:
            return
        await restore_assistant_after_kick(update.chat.id, actor)
    except Exception as e:
        print(f"on_chat_member_update error: {e}")


async def build_start_markup(client: Client, user_id: int):
    me = await client.get_me()
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(_t(user_id, "btn_cmds"), callback_data=f"start:commands:{user_id}")],
        [InlineKeyboardButton(_t(user_id, "btn_bot_channel"), url=CHANNEL_LINK), InlineKeyboardButton(_t(user_id, "btn_add_group"), url=f"https://t.me/{me.username}?startgroup=true")],
        [InlineKeyboardButton("𝐒𝐨𝐮𝐫𝐜𝐞 𝐏𝐫𝐢𝐧𝐜𝐞𝐬™", url=SOURCE_LINK)],
        [InlineKeyboardButton(_t(user_id, "btn_dev"), user_id=OWNER_ID if OWNER_ID else 123456789)],
        [InlineKeyboardButton(_t(user_id, "btn_langs"), callback_data=f"start:langs:{user_id}")],
    ])


def build_help_markup(menu_type="main", chat_id=0, user_id=0):
    st = get_state(chat_id)
    next_txt = "Next ⏭" if get_lang(user_id) == "en" else "التالي ⏭"
    prev_txt = "⏮ Prev" if get_lang(user_id) == "en" else "⏮ السابق"
    if menu_type == "main":
        return InlineKeyboardMarkup([
            [InlineKeyboardButton(_t(user_id, "btn_mem_cmds"), callback_data=f"menu:members:{chat_id}:{user_id}"), InlineKeyboardButton(_t(user_id, "btn_play_cmds"), callback_data=f"menu:playback:{chat_id}:{user_id}")],
            [InlineKeyboardButton(_t(user_id, "btn_settings"), callback_data=f"menu:settings:{chat_id}:{user_id}")],
            [InlineKeyboardButton(_t(user_id, "btn_owner"), user_id=OWNER_ID if OWNER_ID else 123456789), InlineKeyboardButton(_t(user_id, "btn_add_me"), url=get_bot_add_url())],
        ])
    if menu_type == "members":
        return InlineKeyboardMarkup([[InlineKeyboardButton(_t(user_id, "btn_back"), callback_data=f"menu:main:{chat_id}:{user_id}")]])
    if menu_type == "playback":
        return InlineKeyboardMarkup([
            [InlineKeyboardButton(next_txt, callback_data=f"menu:playback2:{chat_id}:{user_id}")],
            [InlineKeyboardButton(_t(user_id, "btn_back"), callback_data=f"menu:main:{chat_id}:{user_id}")],
        ])
    if menu_type == "playback2":
        return InlineKeyboardMarkup([
            [InlineKeyboardButton(prev_txt, callback_data=f"menu:playback:{chat_id}:{user_id}")],
            [InlineKeyboardButton(_t(user_id, "btn_back"), callback_data=f"menu:main:{chat_id}:{user_id}")],
        ])
    if menu_type == "settings":
        s_txt = "المشرفين فقط 🚷" if st.admins_only else "الكل 👥"
        if get_lang(user_id) == "en":
            s_txt = "Admins Only 🚷" if st.admins_only else "Everyone 👥"
        return InlineKeyboardMarkup([
            [InlineKeyboardButton(f"{_t(user_id, 'btn_settings').split(' ')[0]}: {s_txt}", callback_data=f"menu:toggle_lock:{chat_id}:{user_id}")],
            [InlineKeyboardButton(_t(user_id, "btn_back"), callback_data=f"menu:main:{chat_id}:{user_id}")],
        ])
    return InlineKeyboardMarkup([])


@bot.on_message(filters.command("start") & filters.private)
async def start_private_cmd(client: Client, m: Message):
    user_id = m.from_user.id
    await m.reply_photo(photo=BOT_IMAGE, caption=_t(user_id, "start_text"), reply_markup=await build_start_markup(client, user_id))


@bot.on_message(filters.new_chat_members)
async def welcome_bot(client: Client, message: Message):
    me = await client.get_me()
    user_id = message.from_user.id if message.from_user else 0
    for member in message.new_chat_members:
        if member.id == me.id:
            cap = _t(user_id, "welcome_group").format(title=safe_html(message.chat.title))
            markup = InlineKeyboardMarkup([
                [InlineKeyboardButton("〔 CHANEEL 〕", url=CHANNEL_LINK)],
                [InlineKeyboardButton(_t(user_id, "btn_owner"), user_id=OWNER_ID if OWNER_ID else 123456789)],
                [InlineKeyboardButton(_t(user_id, "btn_add_me"), url=f"https://t.me/{me.username}?startgroup=true")],
            ])
            try:
                await client.send_photo(message.chat.id, photo=BOT_IMAGE, caption=cap, reply_markup=markup)
            except Exception:
                pass
            break


async def send_help(chat_id: int, reply_to_message_id: Optional[int] = None, user=None):
    user_id = user.id if user else 0
    caption = _t(user_id, "help_main").format(name=mention_user(user))
    markup = build_help_markup("main", chat_id, user_id)
    rep_params = ReplyParameters(message_id=reply_to_message_id) if reply_to_message_id else None
    try:
        await bot.send_photo(chat_id, photo=BOT_IMAGE, caption=caption, reply_markup=markup, reply_parameters=rep_params)
    except Exception:
        await bot.send_message(chat_id, caption, reply_markup=markup, link_preview_options=LinkPreviewOptions(is_disabled=True), reply_parameters=rep_params)


@bot.on_callback_query()
async def on_callback(client: Client, cq: CallbackQuery):
    data = cq.data or ""
    user_id = cq.from_user.id
    if data == "noop":
        return await cq.answer()

    if data.startswith("pcb:"):
        parts = data.split(":")
        req_user_id = int(parts[1])
        action = parts[2]
        
        if user_id != req_user_id:
            return await cq.answer("❌ هذا الأمر لا يخصك." if get_lang(user_id) != "en" else "❌ This command is not for you.", show_alert=True)
        
        query = ""
        if action == "q1": query = "سورة البقرة ماهر المعيقلي"
        elif action == "q2": query = "سورة الكهف عبدالباسط"
        elif action == "q3": query = "سورة يوسف ياسر الدوسري"
        elif action == "s1": query = "اغاني عراقية"
        elif action == "s2": query = "اغاني حزينة"
        elif action == "s3": query = "محمد عبدالجبار"
        elif action == "s4": query = "محمود التركي"
        else:
            cached = INLINE_QUERY_CACHE.get(action)
            if cached:
                query = cached[1]
            else:
                return await cq.answer("انتهت صلاحية الزر ❌" if get_lang(user_id) != "en" else "Button expired ❌", show_alert=True)

        await cq.answer("جاري التشغيل... ⚡" if get_lang(user_id) != "en" else "Starting... ⚡")
        
        chat_id = cq.message.chat.id
        st = get_state(chat_id)
        
        wait_text = "Starting, please wait a moment…⚡" if get_lang(user_id) == "en" else "جاري التشغيل انتظر لحظة…⚡"
        try:
            wait_msg = await cq.message.edit_text(wait_text)
        except Exception:
            wait_msg = cq.message

        join_task = asyncio.create_task(warmup_assistant_join(chat_id, user_id))
        tr = await build_track_from_query(query, want_video=False, requester_id=user_id, request_msg_id=cq.message.id)
        
        if not tr:
            try:
                await wait_msg.edit_text("Not found." if get_lang(user_id) == "en" else "ما لگيتها.")
            except Exception:
                pass
            return
            
        await enqueue_track(chat_id, tr)
        if not st.playing:
            try:
                run_text = "Starting now…🚦" if get_lang(user_id) == "en" else "راح يشتغل هسه…🚦"
                await wait_msg.edit_text(run_text)
                await asyncio.sleep(0.8)
            except Exception:
                pass
            await start_track(chat_id, user_id)
            try:
                if wait_msg.text in ["Starting now…🚦", "راح يشتغل هسه…🚦"]:
                    await wait_msg.delete()
            except Exception:
                pass
        else:
            try:
                await wait_msg.edit_text(f"{_t(user_id, 'added_queue')} <b>{safe_html(tr.title)}</b>")
            except Exception:
                pass
        return

    if data.startswith("start:"):
        parts = data.split(":")
        action = parts[1]
        if action == "setlang":
            lang = parts[2] if len(parts) > 2 else "ar"
            req_user_id = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0
        else:
            req_user_id = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
        if req_user_id and user_id != req_user_id:
            return await cq.answer("❌ هذا الأمر لا يخصك." if get_lang(user_id) != "en" else "❌ This command is not for you.", show_alert=True)
        if action == "main":
            await cq.message.edit_caption(_t(user_id, "start_text"), reply_markup=await build_start_markup(client, user_id))
        elif action == "commands":
            text = get_help_text("members", user_id)
            next_txt = "🎵 1/2" if get_lang(user_id) == "en" else "🎵 1/2"
            next2_txt = "🎛 2/2" if get_lang(user_id) == "en" else "🎛 2/2"
            markup = InlineKeyboardMarkup([[InlineKeyboardButton(next_txt, callback_data=f"start:play1:{user_id}"), InlineKeyboardButton(next2_txt, callback_data=f"start:play2:{user_id}")], [InlineKeyboardButton(_t(user_id, "btn_back"), callback_data=f"start:main:{user_id}")]])
            await cq.message.edit_caption(text, reply_markup=markup)
        elif action == "play1":
            markup = InlineKeyboardMarkup([[InlineKeyboardButton("👤", callback_data=f"start:commands:{user_id}"), InlineKeyboardButton("🎛 2/2", callback_data=f"start:play2:{user_id}")], [InlineKeyboardButton(_t(user_id, "btn_back"), callback_data=f"start:main:{user_id}")]])
            await cq.message.edit_caption(get_help_text("playback", user_id), reply_markup=markup)
        elif action == "play2":
            markup = InlineKeyboardMarkup([[InlineKeyboardButton("👤", callback_data=f"start:commands:{user_id}"), InlineKeyboardButton("🎵 1/2", callback_data=f"start:play1:{user_id}")], [InlineKeyboardButton(_t(user_id, "btn_back"), callback_data=f"start:main:{user_id}")]])
            await cq.message.edit_caption(get_help_text("playback2", user_id), reply_markup=markup)
        elif action == "langs":
            text = "🌍 <b>اختر لغة البوت (Choose Language):</b>"
            markup = InlineKeyboardMarkup([
                [InlineKeyboardButton("العربية 🇮🇶", callback_data=f"start:setlang:ar:{user_id}"), InlineKeyboardButton("English 🇺🇸", callback_data=f"start:setlang:en:{user_id}")],
                [InlineKeyboardButton(_t(user_id, "btn_back"), callback_data=f"start:main:{user_id}")],
            ])
            await cq.message.edit_caption(text, reply_markup=markup)
        elif action == "setlang":
            users_lang[user_id] = lang
            await cq.answer(_t(user_id, "lang_changed"), show_alert=True)
            await cq.message.edit_caption(_t(user_id, "start_text"), reply_markup=await build_start_markup(client, user_id))
        return

    if data.startswith("menu:"):
        parts = data.split(":")
        action = parts[1]
        chat_id = int(parts[2]) if len(parts) > 2 else cq.message.chat.id
        req_user_id = int(parts[3]) if len(parts) > 3 else 0
        st = get_state(chat_id)
        if req_user_id and user_id != req_user_id:
            return await cq.answer("❌ هذه القائمة تخص الشخص الذي طلبها فقط." if get_lang(user_id) != "en" else "❌ This menu is only for the requester.", show_alert=True)
        if action == "main":
            await cq.message.edit_caption(_t(user_id, "help_main").format(name=mention_user(cq.from_user)), reply_markup=build_help_markup("main", chat_id, user_id))
        elif action == "members":
            await cq.message.edit_caption(get_help_text("members", user_id), reply_markup=build_help_markup("members", chat_id, user_id))
        elif action == "playback":
            await cq.message.edit_caption(get_help_text("playback", user_id), reply_markup=build_help_markup("playback", chat_id, user_id))
        elif action == "playback2":
            await cq.message.edit_caption(get_help_text("playback2", user_id), reply_markup=build_help_markup("playback2", chat_id, user_id))
        elif action == "settings":
            if not await is_admin(client, chat_id, user_id):
                return await cq.answer("Admins only ❌" if get_lang(user_id) == "en" else "للمشرفين فقط ❌", show_alert=True)
            txt = "⚙️ <b>Settings:</b>\nControl who can use the bot." if get_lang(user_id) == "en" else "⚙️ <b>إعدادات التحكم:</b>\nحدد من يمكنه الاستخدام."
            await cq.message.edit_caption(txt, reply_markup=build_help_markup("settings", chat_id, user_id))
        elif action == "toggle_lock":
            if not await is_admin(client, chat_id, user_id):
                return await cq.answer("Admins only ❌" if get_lang(user_id) == "en" else "للمشرفين فقط ❌", show_alert=True)
            st.admins_only = not st.admins_only
            await cq.message.edit_reply_markup(reply_markup=build_help_markup("settings", chat_id, user_id))
            await cq.answer("Changed ✅" if get_lang(user_id) == "en" else "تم التغيير ✅")
        return

    if data.startswith("ctl:"):
        _, action, chat_id_s = data.split(":", 2)
        chat_id = int(chat_id_s)
        st = get_state(chat_id)
        name = mention_user(cq.from_user)
        if action != "delnp" and st.admins_only and not await is_admin(client, chat_id, user_id):
            return await cq.answer("Locked ❌" if get_lang(user_id) == "en" else "التحكم مقفول ❌", show_alert=True)
        try:
            if action == "pause":
                if st.playing and not st.paused:
                    await pause_stream(chat_id)
                    st.paused = True
                    st.paused_at = now_ts()
                    await cq.answer("Paused ✅" if get_lang(user_id) == "en" else "⏸️ اوكفت ✅")
                    await bot.send_message(chat_id, _t(user_id, "paused_by").format(name=name), link_preview_options=LinkPreviewOptions(is_disabled=True))
                else:
                    await cq.answer("Nothing to pause" if get_lang(user_id) == "en" else "ماكو شي شغّال ✅")
            elif action == "resume":
                if st.queue:
                    if st.playing and st.paused:
                        await resume_current_track(chat_id)
                        await cq.answer("Resumed ✅" if get_lang(user_id) == "en" else "▶️ كملت ✅")
                        await bot.send_message(chat_id, _t(user_id, "resumed_by").format(name=name), link_preview_options=LinkPreviewOptions(is_disabled=True))
                    elif not st.playing:
                        await start_track(chat_id, user_id)
                        await cq.answer("Started ✅" if get_lang(user_id) == "en" else "▶️ اشتغلت ✅")
                    else:
                        await cq.answer("Already playing" if get_lang(user_id) == "en" else "شغّال هسه ✅")
            elif action == "stop":
                if not st.queue and not st.playing:
                    await cq.answer(_t(user_id, "nothing_playing"), show_alert=True)
                else:
                    await force_leave_call(chat_id)
                    await cq.answer("Stopped ✅" if get_lang(user_id) == "en" else "🛑 طلعت ✅")
                    await bot.send_message(chat_id, _t(user_id, "stopped_by").format(name=name), link_preview_options=LinkPreviewOptions(is_disabled=True))
            elif action == "delnp":
                allowed = False
                if await is_admin(client, chat_id, user_id):
                    allowed = True
                elif st.queue and st.current_index < len(st.queue) and st.queue[st.current_index].requester_id == user_id:
                    allowed = True
                elif OWNER_ID and user_id == OWNER_ID:
                    allowed = True
                if not allowed:
                    return await cq.answer(_t(user_id, "del_denied"), show_alert=True)
                try:
                    await cq.message.delete()
                except Exception:
                    pass
                if st.now_msg and st.now_msg[1] == cq.message.id:
                    st.now_msg = None
                await cq.answer(_t(user_id, "deleted_msg"))
        except Exception:
            await cq.answer("Error" if get_lang(user_id) == "en" else "صار خطأ")


@bot.on_message(filters.text)
async def on_text(client: Client, m: Message):
    if getattr(m, "edit_date", None):
        return
    text = normalize_spaces(m.text or "")
    if not text or text.startswith("/start"):
        return
    chat_id = m.chat.id
    uid = m.from_user.id if m.from_user else 0
    st = get_state(chat_id)
    name = mention_user(m.from_user)

    if text == "بوت":
        try:
            await m.reply_text(f"<a href='{get_bot_add_url()}'>{safe_html(random.choice(BOT_REPLIES))}</a>", link_preview_options=LinkPreviewOptions(is_disabled=True), quote=True)
        except Exception:
            pass
        return

    if st.admins_only and m.chat.type != ChatType.PRIVATE and not await is_admin(client, chat_id, uid):
        return

    cmd, arg = match_command(text)
    if not cmd:
        return

    if cmd == "help":
        return await send_help(chat_id, reply_to_message_id=m.id, user=m.from_user)


    if cmd == "stats":
        return await m.reply_text(await get_group_leaderboard_text(chat_id, uid), quote=True, link_preview_options=LinkPreviewOptions(is_disabled=True))

    if cmd == "save_playlist":
        current = get_current_track(chat_id)
        if not current:
            msg_txt = "Nothing is playing right now." if get_lang(uid) == "en" else "ماكو شي شغال حتى أحفظه."
            return await m.reply_text(msg_txt, quote=True)
        ok, reason = save_user_playlist_track(uid, current)
        if ok:
            msg_txt = f"✅ Saved: <b>{safe_html(current.title)}</b>" if get_lang(uid) == "en" else f"✅ انحفظت بقائمتك: <b>{safe_html(current.title)}</b>"
        elif reason == "exists":
            msg_txt = "Already saved in your playlist." if get_lang(uid) == "en" else "موجودة أصلاً بقائمتك."
        elif reason == "full":
            msg_txt = "Your playlist is full." if get_lang(uid) == "en" else "قائمتك ممتلئة."
        else:
            msg_txt = "This track can't be saved because it is local/temporary." if get_lang(uid) == "en" else "ما أگدر أحفظ هذا المقطع لأنه محلي/مؤقت."
        return await m.reply_text(msg_txt, quote=True)

    if cmd == "my_playlist":
        playlist = get_user_playlist(uid)
        if not playlist:
            msg_txt = "Your playlist is empty." if get_lang(uid) == "en" else "قائمتك فارغة."
            return await m.reply_text(msg_txt, quote=True)
        lines = ["🎧 <b>Your Playlist</b>" if get_lang(uid) == "en" else "🎧 <b>قائمتك الشخصية</b>"]
        for i, item in enumerate(playlist[:20], 1):
            lines.append(f"{i}. <b>{safe_html(item.get('title') or 'Unknown')}</b>")
        if len(playlist) > 20:
            lines.append(f"… +{len(playlist) - 20}")
        tail = "\n\nUse <b>play my playlist</b> to queue all songs." if get_lang(uid) == "en" else "\n\nاكتب <b>شغل قائمتي</b> حتى يضيفهن للطابور."
        return await m.reply_text("\n".join(lines) + tail, quote=True)

    if cmd == "play_my_playlist":
        playlist = get_user_playlist(uid)
        if not playlist:
            msg_txt = "Your playlist is empty." if get_lang(uid) == "en" else "قائمتك فارغة."
            return await m.reply_text(msg_txt, quote=True)
        status = await m.reply_text("Loading playlist…" if get_lang(uid) == "en" else "جاري تحميل قائمتك…", quote=True)
        added, failed = await start_playlist_queue(chat_id, uid, m.id)
        if added and not st.playing:
            await start_track(chat_id, uid)
        msg_txt = f"✅ Added <b>{added}</b> tracks. Failed: <b>{failed}</b>" if get_lang(uid) == "en" else f"✅ أضفت <b>{added}</b> عنصر من قائمتك. الفشل: <b>{failed}</b>"
        return await status.edit_text(msg_txt)

    if cmd == "clear_my_playlist":
        count = clear_user_playlist_items(uid)
        msg_txt = f"🗑 Cleared <b>{count}</b> saved tracks." if get_lang(uid) == "en" else f"🗑 تم حذف <b>{count}</b> عنصر من قائمتك."
        return await m.reply_text(msg_txt, quote=True)

    if cmd == "cache_info":
        return await m.reply_text(cache_stats_text(uid), quote=True)

    if cmd in {"filter_slowed", "filter_nightcore", "filter_bass", "filter_off"}:
        mapping = {
            "filter_slowed": "slowed",
            "filter_nightcore": "nightcore",
            "filter_bass": "bassboost",
            "filter_off": "normal",
        }
        if not get_current_track(chat_id):
            return await m.reply_text(_t(uid, "nothing_playing"), quote=True)
        try:
            await apply_live_audio_filter(chat_id, mapping[cmd])
            preset = AUDIO_FILTER_PRESETS.get(get_state(chat_id).audio_filter, AUDIO_FILTER_PRESETS["normal"])
            msg_txt = f"🎛 Filter: <b>{safe_html(preset['label_en'])}</b>" if get_lang(uid) == "en" else f"🎛 الفلتر الحالي: <b>{safe_html(preset['label_ar'])}</b>"
            return await m.reply_text(msg_txt, quote=True)
        except Exception:
            msg_txt = "Couldn't apply the filter right now." if get_lang(uid) == "en" else "ماكدر أطبّق الفلتر هسه."
            return await m.reply_text(msg_txt, quote=True)

    if cmd == "crossfade_off":
        st.crossfade_seconds = 0
        msg_txt = "🎚 Crossfade disabled." if get_lang(uid) == "en" else "🎚 تم تعطيل الكروس فيد."
        return await m.reply_text(msg_txt, quote=True)

    if cmd == "crossfade":
        sec = parse_int_seconds(arg or "")
        if sec is None or sec <= 0:
            msg_txt = "Use: crossfade + seconds" if get_lang(uid) == "en" else "اكتب: كروس + عدد الثواني"
            return await m.reply_text(msg_txt, quote=True)
        st.crossfade_seconds = min(sec, 10)
        msg_txt = f"🎚 Crossfade set to <b>{st.crossfade_seconds}</b>s" if get_lang(uid) == "en" else f"🎚 تم ضبط الكروس فيد على <b>{st.crossfade_seconds}</b> ثانية"
        return await m.reply_text(msg_txt, quote=True)

    if cmd == "record_start":
        ok, out_path = await start_call_recording(chat_id)
        if ok:
            txt = "⏺ Recording started." if get_lang(uid) == "en" else "⏺ بدء تسجيل المكالمة."
        else:
            txt = "Couldn't start recording right now." if get_lang(uid) == "en" else "ماكدر أبدأ التسجيل هسه."
        return await m.reply_text(txt, quote=True)

    if cmd == "record_stop":
        out_path = await stop_call_recording(chat_id)
        if not out_path or not os.path.exists(out_path):
            txt = "No recording file was produced." if get_lang(uid) == "en" else "ما انشأ ملف تسجيل."
            return await m.reply_text(txt, quote=True)
        note = "Recorded file" if get_lang(uid) == "en" else "ملف التسجيل"
        await bot.send_audio(chat_id, audio=out_path, caption=f"⏺ {note}", reply_parameters=ReplyParameters(message_id=m.id))
        return


    if cmd == "social_video":
        cmd = "vplay"

    if cmd == "shuffle":
        if len(st.queue) <= max(st.current_index + 1, 1):
            txt = "Queue is too small to shuffle." if get_lang(uid) == "en" else "الطابور قصير وما يحتاج خلط."
            return await m.reply_text(txt, quote=True)
        head = st.queue[:st.current_index + 1]
        tail = st.queue[st.current_index + 1:]
        random.shuffle(tail)
        st.queue = head + tail
        txt = "🔀 Queue shuffled." if get_lang(uid) == "en" else "🔀 انخلط الطابور."
        return await m.reply_text(txt, quote=True)

    if cmd == "ping":
        start = time.time()
        msg = await m.reply_text("⏳...", quote=True)
        end = time.time()
        await msg.edit_text(f"🚀 **Ping:** `{round((end - start) * 1000)}ms`")
        return

    if cmd == "source":
        cap = "<b>🎶 Source</b>\nBest bot for VC." if get_lang(uid) == "en" else "<b>🎶 سورس الأمراء</b>\nأفضل سورس لتشغيل الصوتيات والمرئيات."
        markup = InlineKeyboardMarkup([[InlineKeyboardButton("𝐒𝐨𝐮𝐫𝐜𝐞 𝐏𝐫𝐢𝐧𝐜𝐞𝐬™", url=SOURCE_LINK)]])
        return await m.reply_photo(photo=BOT_IMAGE, caption=cap, reply_markup=markup, quote=True)

    if cmd == "quran":
        cap = "نتائج البحث ادناه 👇🏻…" if get_lang(uid) != "en" else "Search results below 👇🏻..."
        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("سورة البقرة ماهر المعيقلي", callback_data=f"pcb:{uid}:q1")],
            [InlineKeyboardButton("سورة الكهف عبدالباسط", callback_data=f"pcb:{uid}:q2")],
            [InlineKeyboardButton("سورة يوسف ياسر الدوسري", callback_data=f"pcb:{uid}:q3")]
        ])
        return await m.reply_text(cap, reply_markup=markup, quote=True)

    if cmd == "songs":
        cap = "نتائج البحث ادناه 👇🏻…" if get_lang(uid) != "en" else "Search results below 👇🏻..."
        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("اغاني عراقية", callback_data=f"pcb:{uid}:s1"),
             InlineKeyboardButton("اغاني حزينة", callback_data=f"pcb:{uid}:s2")],
            [InlineKeyboardButton("محمد عبدالجبار", callback_data=f"pcb:{uid}:s3"),
             InlineKeyboardButton("محمود التركي", callback_data=f"pcb:{uid}:s4")]
        ])
        return await m.reply_text(cap, reply_markup=markup, quote=True)

    if cmd == "athan_info":
        now = local_now()
        timings = await get_today_prayer_times()
        is_on = athan_is_enabled(chat_id)
        
        if get_lang(uid) == "en":
            msg = f"🕌 **Athan System Report** 🕌\n\n"
            msg += f"⏱ **Current Time (Karbala):** `{now.strftime('%H:%M:%S')}`\n"
            msg += f"✅ **Athan Status here:** `{'Enabled 🟢' if is_on else 'Disabled 🔴'}`\n\n"
            msg += f"📅 **Fetched Prayer Times:**\n"
            msg += f"🌅 Fajr: `{timings.get('Fajr', 'N/A ❌')}`\n"
            msg += f"☀️ Dhuhr: `{timings.get('Dhuhr', 'N/A ❌')}`\n"
            msg += f"🌙 Maghrib: `{timings.get('Maghrib', 'N/A ❌')}`\n"
        else:
            msg = f"🕌 **تقرير نظام الأذان** 🕌\n\n"
            msg += f"⏱ **وقت البوت الحالي (بتوقيت كربلاء):** `{now.strftime('%H:%M:%S')}`\n"
            msg += f"✅ **حالة الأذان بهذا الجروب:** `{'مفعل 🟢' if is_on else 'معطل 🔴'}`\n\n"
            msg += f"📅 **أوقات الصلاة اللي سحبها البوت اليوم:**\n"
            msg += f"🌅 الفجر: `{timings.get('Fajr', 'غير متوفر ❌')}`\n"
            msg += f"☀️ الظهر: `{timings.get('Dhuhr', 'غير متوفر ❌')}`\n"
            msg += f"🌙 المغرب: `{timings.get('Maghrib', 'غير متوفر ❌')}`\n"
            
        return await m.reply_text(msg, quote=True)

    if cmd == "play":
        if m.reply_to_message and (m.reply_to_message.audio or m.reply_to_message.voice or m.reply_to_message.video or m.reply_to_message.document):
            join_task = asyncio.create_task(warmup_assistant_join(chat_id, uid))
            wait_text = "Starting, please wait a moment…⚡" if get_lang(uid) == "en" else "جاري التشغيل انتظر لحظة…⚡"
            wait_msg = await m.reply_text(wait_text, quote=True)
            file_path = await m.reply_to_message.download()
            if m.reply_to_message.audio:
                duration = m.reply_to_message.audio.duration
                title_def = "Audio File" if get_lang(uid) == "en" else "ملف صوتي"
                title = m.reply_to_message.audio.title or m.reply_to_message.audio.file_name or title_def
            elif m.reply_to_message.voice:
                duration = m.reply_to_message.voice.duration
                title = "Voice Note" if get_lang(uid) == "en" else "بصمة صوتية"
            elif m.reply_to_message.video:
                duration = m.reply_to_message.video.duration
                title_def = "Video Clip" if get_lang(uid) == "en" else "مقطع فيديو"
                title = m.reply_to_message.video.file_name or title_def
            else:
                duration = 0
                title = "File" if get_lang(uid) == "en" else "ملف"
            tr = Track(title=title, source="telegram", stream_url=file_path, duration=duration or 0, requester_id=uid, request_msg_id=m.id, thumbnail=BOT_IMAGE, kind="audio")
            await enqueue_track(chat_id, tr)
            if not st.playing:
                try:
                    run_text = "Starting now…🚦" if get_lang(uid) == "en" else "راح يشتغل هسه…🚦"
                    await wait_msg.edit_text(run_text)
                    await asyncio.sleep(0.8)
                except Exception:
                    pass
                await start_track(chat_id, uid)
            else:
                await m.reply_text(f"{_t(uid, 'added_queue')} <b>{safe_html(tr.title)}</b>", quote=True)
            try:
                await wait_msg.delete()
            except Exception:
                pass
            return
        if not arg:
            return await m.reply_text(full_link_text(_t(uid, "need_play_name")), link_preview_options=LinkPreviewOptions(is_disabled=True), quote=True)
        join_task = asyncio.create_task(warmup_assistant_join(chat_id, uid))
        wait_text = "Starting, please wait a moment…⚡" if get_lang(uid) == "en" else "جاري التشغيل انتظر لحظة…⚡"
        wait_msg = await m.reply_text(wait_text, quote=True)
        tr = await build_track_from_query(arg, want_video=False, requester_id=uid, request_msg_id=m.id)
        if not tr:
            await wait_msg.delete()
            return await m.reply_text("Not found." if get_lang(uid) == "en" else "ما لگيتها.", quote=True)
        await enqueue_track(chat_id, tr)
        if not st.playing:
            try:
                run_text = "Starting now…🚦" if get_lang(uid) == "en" else "راح يشتغل هسه…🚦"
                await wait_msg.edit_text(run_text)
                await asyncio.sleep(0.8)
            except Exception:
                pass
            await start_track(chat_id, uid)
        else:
            await m.reply_text(f"{_t(uid, 'added_queue')} <b>{safe_html(tr.title)}</b>", quote=True)
        try:
            await wait_msg.delete()
        except Exception:
            pass
        return

    if cmd == "vplay":
        if m.reply_to_message and (m.reply_to_message.video or m.reply_to_message.document):
            join_task = asyncio.create_task(warmup_assistant_join(chat_id, uid))
            wait_text = "Starting, please wait a moment…⚡" if get_lang(uid) == "en" else "جاري التشغيل انتظر لحظة…⚡"
            wait_msg = await m.reply_text(wait_text, quote=True)
            file_path = await m.reply_to_message.download()
            if m.reply_to_message.video:
                duration = m.reply_to_message.video.duration
                title_def = "Video Clip" if get_lang(uid) == "en" else "مقطع فيديو"
                title = m.reply_to_message.video.file_name or title_def
            else:
                duration = 0
                title = "Video File" if get_lang(uid) == "en" else "ملف فيديو"
            tr = Track(title=title, source="telegram", stream_url=file_path, duration=duration or 0, requester_id=uid, request_msg_id=m.id, thumbnail=BOT_IMAGE, kind="video")
            await enqueue_track(chat_id, tr)
            if not st.playing:
                try:
                    run_text = "Starting now…🚦" if get_lang(uid) == "en" else "راح يشتغل هسه…🚦"
                    await wait_msg.edit_text(run_text)
                    await asyncio.sleep(0.8)
                except Exception:
                    pass
                await start_track(chat_id, uid)
            else:
                await m.reply_text(f"{_t(uid, 'added_queue_vid')} <b>{safe_html(tr.title)}</b>", quote=True)
            try:
                await wait_msg.delete()
            except Exception:
                pass
            return
        if not arg:
            return await m.reply_text(full_link_text(_t(uid, "need_video_name")), link_preview_options=LinkPreviewOptions(is_disabled=True), quote=True)
        join_task = asyncio.create_task(warmup_assistant_join(chat_id, uid))
        wait_text = "Starting, please wait a moment…⚡" if get_lang(uid) == "en" else "جاري التشغيل انتظر لحظة…⚡"
        wait_msg = await m.reply_text(wait_text, quote=True)
        tr = await build_track_from_query(arg, want_video=True, requester_id=uid, request_msg_id=m.id)
        if not tr:
            await wait_msg.delete()
            return await m.reply_text("Not found." if get_lang(uid) == "en" else "ما لگيتها.", quote=True)
        await enqueue_track(chat_id, tr)
        if not st.playing:
            try:
                run_text = "Starting now…🚦" if get_lang(uid) == "en" else "راح يشتغل هسه…🚦"
                await wait_msg.edit_text(run_text)
                await asyncio.sleep(0.8)
            except Exception:
                pass
            await start_track(chat_id, uid)
        else:
            await m.reply_text(f"{_t(uid, 'added_queue_vid')} <b>{safe_html(tr.title)}</b>", quote=True)
        try:
            await wait_msg.delete()
        except Exception:
            pass
        return

    if cmd == "search":
        if not arg:
            err_msg = "Type: <b>search</b> + the word you want" if get_lang(uid) == "en" else "اكتب: <b>بحث</b> + الكلمة اللي تريدها"
            return await m.reply_text(err_msg, quote=True)
        wait_txt = "🔍 Searching..." if get_lang(uid) == "en" else "🔍 جاري البحث..."
        wait = await m.reply_text(wait_txt, quote=True)
        results = await ytdlp_extract(arg, search_only=True)
        
        if not results or "entries" not in results or not results["entries"]:
            no_res = "No results found." if get_lang(uid) == "en" else "ما لقيت نتائج."
            await wait.edit_text(no_res)
            return
            
        buttons = []
        un_name = "Unknown" if get_lang(uid) == "en" else "بدون اسم"
        for entry in results["entries"][:5]:
            title = entry.get('title', un_name)
            url_or_id = entry.get('webpage_url') or entry.get('url') or entry.get('id')
            if not url_or_id:
                continue
            
            # Generate a short ID for the callback data to stay within Telegram limits
            short_id = uuid.uuid4().hex[:8]
            INLINE_QUERY_CACHE[short_id] = (now_ts(), url_or_id)
            buttons.append([InlineKeyboardButton(title, callback_data=f"pcb:{uid}:{short_id}")])
            
        if not buttons:
            no_res = "No results found." if get_lang(uid) == "en" else "ما لقيت نتائج."
            await wait.edit_text(no_res)
            return

        cap = "نتائج البحث ادناه 👇🏻…" if get_lang(uid) != "en" else "Search results below 👇🏻..."
        markup = InlineKeyboardMarkup(buttons)
        await wait.edit_text(cap, reply_markup=markup)
        
        # Cleanup cache if it grows too big (keep entries for max 24 hours)
        if len(INLINE_QUERY_CACHE) > 1000:
            current_time = now_ts()
            keys_to_del = [k for k, v in INLINE_QUERY_CACHE.items() if current_time - v[0] > 86400]
            for k in keys_to_del:
                INLINE_QUERY_CACHE.pop(k, None)
        return

    if cmd == "down":
        if not arg:
            return await m.reply_text(full_link_text(_t(uid, "need_download_name")), link_preview_options=LinkPreviewOptions(is_disabled=True), quote=True)
        status_txt = "Downloading, please wait a moment…🎵" if get_lang(uid) == "en" else "جاري التحميل انتظر لحظة…🎵"
        status_msg = await m.reply_text(status_txt, quote=True)
        path, info, thumb_path = await ytdlp_download(arg, "audio")
        if not path or not os.path.exists(path):
            return await status_msg.edit_text("Failed." if get_lang(uid) == "en" else "فشل التحميل.")
        dl_markup = InlineKeyboardMarkup([[InlineKeyboardButton("Channel" if get_lang(uid) == "en" else "ᴄʜᴀɴɴᴇʟ", url=CHANNEL_LINK)]])
        try:
            cap_by = f"••• Downloaded By : {name}" if get_lang(uid) == "en" else f"••• تم التحميل من قبل : {name}"
            await bot.send_audio(chat_id, audio=path, caption=cap_by, title=info.get("title", "Audio"), performer=info.get("uploader", "Unknown"), thumb=thumb_path if thumb_path and os.path.exists(thumb_path) else None, reply_parameters=ReplyParameters(message_id=m.id), reply_markup=dl_markup)
        finally:
            try:
                os.remove(path)
            except Exception:
                pass
            if thumb_path and os.path.exists(thumb_path):
                try:
                    os.remove(thumb_path)
                except Exception:
                    pass
            try:
                await status_msg.delete()
            except Exception:
                pass
        return

    if cmd == "vdown":
        if not arg:
            return await m.reply_text(full_link_text(_t(uid, "need_video_name")), link_preview_options=LinkPreviewOptions(is_disabled=True), quote=True)
        status_msg = await m.reply_text(_t(uid, "searching_vid"), quote=True)
        path, info, thumb_path = await ytdlp_download(arg, "video")
        if not path or not os.path.exists(path):
            return await status_msg.edit_text("Failed." if get_lang(uid) == "en" else "فشل التحميل.")
        dl_markup = InlineKeyboardMarkup([[InlineKeyboardButton("Channel" if get_lang(uid) == "en" else "ᴄʜᴀɴɴᴇʟ", url=CHANNEL_LINK)]])
        try:
            cap_by = f"••• Downloaded By : {name}" if get_lang(uid) == "en" else f"••• تم التحميل من قبل : {name}"
            await bot.send_video(chat_id, video=path, caption=f"{_t(uid, 'download_vid_done')} {safe_html(info.get('title', ''))}\n{cap_by}", thumb=thumb_path if thumb_path and os.path.exists(thumb_path) else None, reply_parameters=ReplyParameters(message_id=m.id), reply_markup=dl_markup)
        finally:
            try:
                os.remove(path)
            except Exception:
                pass
            if thumb_path and os.path.exists(thumb_path):
                try:
                    os.remove(thumb_path)
                except Exception:
                    pass
            try:
                await status_msg.delete()
            except Exception:
                pass
        return

    if cmd == "switch_video":
        if not st.playing or st.current_index >= len(st.queue):
            return await m.reply_text(full_link_text(_t(uid, "nothing_playing_linked")), link_preview_options=LinkPreviewOptions(is_disabled=True), quote=True)
        try:
            await switch_current_media_mode(chat_id, True)
            await m.reply_text(_t(uid, "switch_to_video_done"), quote=True)
        except Exception:
            await m.reply_text(_t(uid, "switch_failed"), quote=True)
        return

    if cmd == "switch_audio":
        if not st.playing or st.current_index >= len(st.queue):
            return await m.reply_text(full_link_text(_t(uid, "nothing_playing_linked")), link_preview_options=LinkPreviewOptions(is_disabled=True), quote=True)
        try:
            await switch_current_media_mode(chat_id, False)
            await m.reply_text(_t(uid, "switch_to_audio_done"), quote=True)
        except Exception:
            await m.reply_text(_t(uid, "switch_failed"), quote=True)
        return

    if cmd == "forward":
        if not arg:
            return await m.reply_text(_t(uid, "need_seconds"), quote=True)
        sec = parse_int_seconds(arg)
        if sec is None:
            return await m.reply_text(_t(uid, "invalid_seconds"), quote=True)
        if not st.playing or st.current_index >= len(st.queue):
            return await m.reply_text(_t(uid, "nothing_playing"), quote=True)
        try:
            target = await seek_current_track(chat_id, sec, backward=False)
            await m.reply_text(_t(uid, "seek_forward_done").format(time=hms(target)), quote=True)
        except Exception:
            await m.reply_text(_t(uid, "seek_failed"), quote=True)
        return

    if cmd == "back":
        if not arg:
            return await m.reply_text(_t(uid, "need_seconds"), quote=True)
        sec = parse_int_seconds(arg)
        if sec is None:
            return await m.reply_text(_t(uid, "invalid_seconds"), quote=True)
        if not st.playing or st.current_index >= len(st.queue):
            return await m.reply_text(_t(uid, "nothing_playing"), quote=True)
        try:
            target = await seek_current_track(chat_id, sec, backward=True)
            await m.reply_text(_t(uid, "seek_backward_done").format(time=hms(target)), quote=True)
        except Exception:
            await m.reply_text(_t(uid, "seek_failed"), quote=True)
        return

    if cmd == "skip":
        try:
            st.loop = False
            await next_track(chat_id)
            await m.reply_text(_t(uid, "skipped").format(name=name), link_preview_options=LinkPreviewOptions(is_disabled=True), quote=True)
        except Exception:
            pass
        return

    if cmd == "stop":
        if not st.playing and not st.queue:
            return await m.reply_text(full_link_text(_t(uid, "nothing_playing_linked")), link_preview_options=LinkPreviewOptions(is_disabled=True), quote=True)
        await force_leave_call(chat_id)
        await m.reply_text(_t(uid, "stopped_by").format(name=name), link_preview_options=LinkPreviewOptions(is_disabled=True), quote=True)
        return

    if cmd == "pause":
        if st.playing and not st.paused:
            try:
                await pause_stream(chat_id)
                st.paused = True
                st.paused_at = now_ts()
                await m.reply_text(_t(uid, "paused_by").format(name=name), link_preview_options=LinkPreviewOptions(is_disabled=True), quote=True)
            except Exception:
                pass
        else:
            await m.reply_text(_t(uid, "nothing_playing"), quote=True)
        return

    if cmd == "resume":
        try:
            if st.playing and st.paused:
                await resume_current_track(chat_id)
                await m.reply_text(_t(uid, "resumed_by").format(name=name), link_preview_options=LinkPreviewOptions(is_disabled=True), quote=True)
            elif not st.playing and st.queue:
                await start_track(chat_id, uid)
                await m.reply_text(_t(uid, "resumed_by").format(name=name), link_preview_options=LinkPreviewOptions(is_disabled=True), quote=True)
            else:
                await m.reply_text(_t(uid, "nothing_playing"), quote=True)
        except Exception:
            pass
        return

    if cmd in {"athan_on", "athan_off", "athan_toggle", "athan_test"}:
        if m.chat.type != ChatType.PRIVATE and not await is_admin(client, chat_id, uid):
            err_adm = "Admins only ❌" if get_lang(uid) == "en" else "للمشرفين فقط ❌"
            return await m.reply_text(err_adm, quote=True)

        if cmd == "athan_test":
            prayer_key = parse_prayer_key(arg) if arg else ""
            if not prayer_key:
                timings = await get_today_prayer_times()
                if timings:
                    prayer_key = choose_default_test_prayer()
                else:
                    prayer_key = "Fajr"
            sent_id = await send_athan_alert(chat_id, prayer_key, triggered_by=uid, is_test=True)
            if sent_id:
                prayer_name = ATHAN_PRAYERS.get(prayer_key, {}).get("name_ar", prayer_key)
                if get_lang(uid) == "en":
                    await m.reply_text(f"Athan test sent for {prayer_key} prayer ✅", quote=True)
                else:
                    await m.reply_text(f"تم إرسال تيست الأذان لصلاة {prayer_name} ✅", quote=True)
            else:
                err_test = "Failed to send Athan test. Ensure I have photo and pin rights. ❌" if get_lang(uid) == "en" else "فشل إرسال تيست الأذان. تأكد من صلاحية إرسال الصور والتثبيت. ❌"
                await m.reply_text(err_test, quote=True)
            return

        current = athan_is_enabled(chat_id)
        if cmd == "athan_on":
            target = True
        elif cmd == "athan_off":
            target = False
        else:
            target = not current
        set_athan_enabled(chat_id, target)
        await get_today_prayer_times(force_refresh=True)
        
        msg_on = "Athan alerts have been ENABLED in this chat ✅" if get_lang(uid) == "en" else "تم تفعيل تنبيهات الأذان في هذه المحادثة ✅"
        msg_off = "Athan alerts have been DISABLED in this chat ✅" if get_lang(uid) == "en" else "تم تعطيل تنبيهات الأذان في هذه المحادثة ✅"
        await m.reply_text(msg_on if target else msg_off, quote=True)
        return

    if cmd == "loop":
        if not st.playing:
            return await m.reply_text(_t(uid, "nothing_playing"), quote=True)
        st.loop = not st.loop
        msg_text = "Loop: ON 🔂" if st.loop else "Loop: OFF ❌"
        if get_lang(uid) != "en":
            msg_text = f"التكرار: {'مفعل 🔂' if st.loop else 'معطل ❌'}"
        await m.reply_text(msg_text, quote=True)
        return


# =========================
# قسم الإذاعة (للمالك حصراً - للخاص فقط) + متتبع المحادثات
# =========================
BCAST_USERS = {}
CHATS_DB = (os.environ.get("CHATS_DB_FILE", _default_data_path("chats_db.json")) or _default_data_path("chats_db.json")).strip()

def get_bcast_chats():
    if os.path.exists(CHATS_DB):
        try:
            with open(CHATS_DB, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"private": [], "groups": [], "channels": []}

def save_bcast_chat(chat_id: int, chat_type: ChatType):
    db = get_bcast_chats()
    c_type = None
    if chat_type == ChatType.PRIVATE:
        c_type = "private"
    elif chat_type in [ChatType.GROUP, ChatType.SUPERGROUP]:
        c_type = "groups"
    elif chat_type == ChatType.CHANNEL:
        c_type = "channels"
        
    if c_type and chat_id not in db[c_type]:
        db[c_type].append(chat_id)
        try:
            _save_json_file(CHATS_DB, db)
        except Exception:
            pass

# متتبع صامت يعمل بالخلفية بدون التأثير على الكود الأصلي
@bot.on_message(group=100)
async def silent_chat_tracker(client: Client, m: Message):
    if m.chat:
        save_bcast_chat(m.chat.id, m.chat.type)

@bot.on_message(filters.private & filters.user(OWNER_ID) & filters.regex(r"^(اذاعة|/broadcast)$"), group=1)
async def bcast_cmd(client: Client, m: Message):
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("اذاعة للخاص 👤", callback_data="bcast:private")],
        [InlineKeyboardButton("اذاعة للقروبات 👥", callback_data="bcast:groups")],
        [InlineKeyboardButton("اذاعة للقنوات 📢", callback_data="bcast:channels")],
        [InlineKeyboardButton("اذاعة للكل 🌍", callback_data="bcast:all")],
        [InlineKeyboardButton("إلغاء ❌", callback_data="bcast:cancel")]
    ])
    await m.reply_text("📣 **قسم الإذاعة**\n\nاختر نوع الإذاعة من الأزرار الشفافة أدناه:", reply_markup=markup, quote=True)

@bot.on_callback_query(filters.regex(r"^bcast:"), group=1)
async def bcast_cb(client: Client, cq: CallbackQuery):
    if cq.from_user.id != OWNER_ID:
        return await cq.answer("هذا الأمر مخصص للمالك فقط ❌", show_alert=True)
    
    action = cq.data.split(":")[1]
    if action == "cancel":
        BCAST_USERS.pop(OWNER_ID, None)
        return await cq.message.edit_text("تم إلغاء الإذاعة ✅")
    
    BCAST_USERS[OWNER_ID] = action
    db = get_bcast_chats()
    
    # حساب عدد المحادثات المسجلة
    total = 0
    if action == "private": total = len(db["private"])
    elif action == "groups": total = len(db["groups"])
    elif action == "channels": total = len(db["channels"])
    elif action == "all": total = len(db["private"]) + len(db["groups"]) + len(db["channels"])
    
    await cq.message.edit_text(f"✅ حسناً، أرسل الآن الرسالة التي تريد إذاعتها.\n\n📊 **المسجل في قاعدة البيانات:** `{total}` محادثة.\n\nأو أرسل كلمة **الغاء** لإلغاء الأمر.")

@bot.on_message(filters.private & filters.user(OWNER_ID), group=2)
async def bcast_receiver(client: Client, m: Message):
    if OWNER_ID not in BCAST_USERS:
        return
    
    if m.text and (m.text == "/cancel" or m.text == "الغاء"):
        BCAST_USERS.pop(OWNER_ID, None)
        return await m.reply_text("تم إلغاء الإذاعة ✅", quote=True)
        
    if m.text and re.match(r"^(اذاعة|/broadcast)$", m.text):
        return
    
    bcast_type = BCAST_USERS.pop(OWNER_ID)
    msg = await m.reply_text("⏳ جاري الإذاعة... يرجى الانتظار", quote=True)
    
    db = get_bcast_chats()
    targets = []
    if bcast_type in ["private", "all"]: targets.extend(db.get("private", []))
    if bcast_type in ["groups", "all"]: targets.extend(db.get("groups", []))
    if bcast_type in ["channels", "all"]: targets.extend(db.get("channels", []))
    
    success = 0
    failed = 0
    
    for chat_id in targets:
        try:
            await m.copy(chat_id)
            success += 1
            await asyncio.sleep(0.05)
        except Exception:
            failed += 1
        
    await msg.edit_text(f"📣 **اكتملت الإذاعة بنجاح!**\n\n✅ نجح إرسالها إلى: `{success}`\n❌ فشل إرسالها إلى: `{failed}`\n\n*(ملاحظة: البوت سيذيع للمحادثات التي سجلها في قاعدة بياناته الجديدة فقط)*")


async def main():
    global BOT_USERNAME, ASSISTANT_ID, BOT_ID, ATHAN_BACKGROUND_TASK, HEALTHCHECK_READY, WATCHDOG_TASK, TELEGRAM_LAST_OK_TS, TELEGRAM_CONSECUTIVE_FAILURES, HEALTH_LAST_ERROR
    await start_healthcheck_server()
    load_athan_state()
    load_extra_feature_state()
    try:
        await bot.start()
        me = await bot.get_me()
        BOT_USERNAME = me.username
        BOT_ID = me.id
        print(f"Bot started as @{me.username}")
        await assistant.start()
        ass_me = await assistant.get_me()
        ASSISTANT_ID = ass_me.id
        await maybe_await(tgcalls.start())
        await restore_pending_unpins()
        ATHAN_BACKGROUND_TASK = asyncio.create_task(athan_loop())
        TELEGRAM_LAST_OK_TS = now_ts()
        TELEGRAM_CONSECUTIVE_FAILURES = 0
        HEALTH_LAST_ERROR = ""
        WATCHDOG_TASK = asyncio.create_task(watchdog_loop())
        HEALTHCHECK_READY = True
        await idle()
    finally:
        HEALTHCHECK_READY = False
        try:
            if WATCHDOG_TASK and not WATCHDOG_TASK.done():
                WATCHDOG_TASK.cancel()
                try:
                    await WATCHDOG_TASK
                except asyncio.CancelledError:
                    pass
        except Exception:
            pass
        try:
            if ATHAN_BACKGROUND_TASK and not ATHAN_BACKGROUND_TASK.done():
                ATHAN_BACKGROUND_TASK.cancel()
                try:
                    await ATHAN_BACKGROUND_TASK
                except asyncio.CancelledError:
                    pass
        except Exception:
            pass
        try:
            await maybe_await(tgcalls.stop())
        except Exception:
            pass
        try:
            await assistant.stop()
        except Exception:
            pass
        try:
            await bot.stop()
        except Exception:
            pass
        try:
            await stop_healthcheck_server()
        except Exception:
            pass


if __name__ == "__main__":
    loop = getattr(bot, "loop", None) or asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main())
