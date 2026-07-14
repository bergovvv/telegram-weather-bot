"""Weather Bot — aiogram 3. Погода, избранное, графики, геолокация, inline-режим."""

import asyncio
import logging
import os
import time
from collections import Counter
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from html import escape

import aiohttp
import aiosqlite
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    BotCommand,
    CallbackQuery,
    InlineQuery,
    InlineQueryResultArticle,
    InlineQueryResultsButton,
    InputTextMessageContent,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")
WEATHER_API_KEY = os.getenv("WEATHER_API_KEY")
BASE_URL = "https://api.openweathermap.org/data/2.5"
DB_PATH = "weather.db"
CACHE_TTL = 600  # секунд

POPULAR_CITIES = ["Москва", "Санкт-Петербург", "Барселона", "Дубай", "Тбилиси", "Ереван"]

WEATHER_EMOJI = {
    "Clear": "☀️", "Clouds": "☁️", "Rain": "🌧", "Drizzle": "🌦", "Thunderstorm": "⛈",
    "Snow": "❄️", "Mist": "🌫", "Fog": "🌫", "Haze": "🌫", "Smoke": "🌫", "Dust": "🌪",
}

SPARK = "▁▂▃▄▅▆▇█"
WIND_ARROWS = ["⬇️", "↙️", "⬅️", "↖️", "⬆️", "↗️", "➡️", "↘️"]
WEEKDAYS = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]


# ═══════════════════════════ БАЗА ДАННЫХ ═══════════════════════════

async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS favorites (
                user_id INTEGER NOT NULL,
                city    TEXT    NOT NULL,
                PRIMARY KEY (user_id, city)
            )
        """)
        await db.commit()


async def get_favorites(user_id: int) -> list[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT city FROM favorites WHERE user_id = ? ORDER BY city", (user_id,)
        )
        return [row[0] for row in await cursor.fetchall()]


async def toggle_favorite(user_id: int, city: str) -> bool:
    """True — город добавлен, False — удалён."""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT 1 FROM favorites WHERE user_id = ? AND city = ?", (user_id, city)
        )
        if await cursor.fetchone():
            await db.execute(
                "DELETE FROM favorites WHERE user_id = ? AND city = ?", (user_id, city)
            )
            await db.commit()
            return False
        await db.execute("INSERT INTO favorites (user_id, city) VALUES (?, ?)", (user_id, city))
        await db.commit()
        return True


# ═══════════════════════════ СЕРВИС ПОГОДЫ ═══════════════════════════

class CityNotFound(Exception):
    """Город не найден."""


class WeatherServiceError(Exception):
    """API недоступен или вернул ошибку."""


@dataclass
class CurrentWeather:
    city: str
    country: str
    description: str
    emoji: str
    temp: float
    feels_like: float
    temp_min: float
    temp_max: float
    humidity: int
    pressure: int
    wind_speed: float
    wind_deg: int
    clouds: int
    visibility: int
    sunrise: datetime
    sunset: datetime


@dataclass
class HourlyItem:
    time: datetime
    temp: float
    emoji: str
    description: str
    pop: float  # вероятность осадков, 0..1


@dataclass
class DailyItem:
    date: datetime
    temp_min: float
    temp_max: float
    emoji: str
    description: str


class WeatherService:
    """Работа с OpenWeatherMap + кэш: не дёргаем API одинаковыми запросами."""

    def __init__(self, api_key: str) -> None:
        self._api_key = api_key
        self._session: aiohttp.ClientSession | None = None
        self._cache: dict[str, tuple[float, dict]] = {}

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def _request(self, endpoint: str, query: dict) -> dict:
        cache_key = f"{endpoint}:{sorted(query.items())}"
        cached = self._cache.get(cache_key)
        if cached and time.monotonic() - cached[0] < CACHE_TTL:
            logger.info("Из кэша: %s", cache_key)
            return cached[1]

        session = await self._get_session()
        params = {**query, "appid": self._api_key, "units": "metric", "lang": "ru"}
        try:
            async with session.get(f"{BASE_URL}/{endpoint}", params=params) as response:
                if response.status == 404:
                    raise CityNotFound(str(query.get("q", "?")))
                if response.status == 401:
                    raise WeatherServiceError("Неверный API-ключ OpenWeatherMap")
                if response.status == 429:
                    raise WeatherServiceError("Превышен лимит запросов к API")
                if response.status != 200:
                    raise WeatherServiceError(f"API вернул статус {response.status}")
                data = await response.json()
        except aiohttp.ClientError as exc:
            raise WeatherServiceError(f"Сеть недоступна: {exc}") from exc

        self._cache[cache_key] = (time.monotonic(), data)
        return data

    @staticmethod
    def _query(city: str | None, coords: tuple[float, float] | None) -> dict:
        if coords:
            return {"lat": coords[0], "lon": coords[1]}
        return {"q": city}

    async def get_current(
        self, city: str | None = None, coords: tuple[float, float] | None = None
    ) -> CurrentWeather:
        data = await self._request("weather", self._query(city, coords))
        tz = timezone(timedelta(seconds=data.get("timezone", 0)))
        weather = data["weather"][0]
        main = data["main"]

        return CurrentWeather(
            city=data["name"],
            country=data["sys"].get("country", ""),
            description=weather["description"].capitalize(),
            emoji=WEATHER_EMOJI.get(weather["main"], "🌡"),
            temp=main["temp"],
            feels_like=main["feels_like"],
            temp_min=main["temp_min"],
            temp_max=main["temp_max"],
            humidity=main["humidity"],
            pressure=round(main["pressure"] * 0.750062),  # гПа → мм рт. ст.
            wind_speed=data["wind"]["speed"],
            wind_deg=data["wind"].get("deg", 0),
            clouds=data["clouds"]["all"],
            visibility=data.get("visibility", 10000) // 1000,
            sunrise=datetime.fromtimestamp(data["sys"]["sunrise"], tz),
            sunset=datetime.fromtimestamp(data["sys"]["sunset"], tz),
        )

    async def get_hourly(self, city: str, limit: int = 8) -> list[HourlyItem]:
        data = await self._request("forecast", {"q": city})
        tz = timezone(timedelta(seconds=data["city"].get("timezone", 0)))
        items = []
        for entry in data["list"][:limit]:
            weather = entry["weather"][0]
            items.append(HourlyItem(
                time=datetime.fromtimestamp(entry["dt"], tz),
                temp=entry["main"]["temp"],
                emoji=WEATHER_EMOJI.get(weather["main"], "🌡"),
                description=weather["description"].capitalize(),
                pop=entry.get("pop", 0),
            ))
        return items

    async def get_daily(self, city: str) -> list[DailyItem]:
        """API отдаёт шаг в 3 часа — схлопываем в дни сами."""
        data = await self._request("forecast", {"q": city})
        tz = timezone(timedelta(seconds=data["city"].get("timezone", 0)))

        by_day: dict = {}
        for entry in data["list"]:
            moment = datetime.fromtimestamp(entry["dt"], tz)
            bucket = by_day.setdefault(
                moment.date(), {"temps": [], "conditions": [], "descriptions": []}
            )
            bucket["temps"].append(entry["main"]["temp"])
            bucket["conditions"].append(entry["weather"][0]["main"])
            bucket["descriptions"].append(entry["weather"][0]["description"])

        result = []
        for day, bucket in sorted(by_day.items())[:5]:
            top_condition = Counter(bucket["conditions"]).most_common(1)[0][0]
            top_description = Counter(bucket["descriptions"]).most_common(1)[0][0]
            result.append(DailyItem(
                date=datetime.combine(day, datetime.min.time()),
                temp_min=min(bucket["temps"]),
                temp_max=max(bucket["temps"]),
                emoji=WEATHER_EMOJI.get(top_condition, "🌡"),
                description=top_description.capitalize(),
            ))
        return result


# ═══════════════════════════ ОФОРМЛЕНИЕ ═══════════════════════════

def wind_arrow(degrees: int) -> str:
    return WIND_ARROWS[round(degrees / 45) % 8]


def sparkline(values: list[float]) -> str:
    """Мини-график температуры символами: дёшево, а выглядит дорого."""
    low, high = min(values), max(values)
    span = high - low or 1
    return "".join(
        SPARK[min(int((v - low) / span * (len(SPARK) - 1)), len(SPARK) - 1)] for v in values
    )


def daylight_bar(sunrise: datetime, sunset: datetime, now: datetime) -> str:
    """Где мы сейчас между рассветом и закатом."""
    total = (sunset - sunrise).total_seconds()
    if total <= 0:
        return ""
    progress = (now - sunrise).total_seconds() / total
    if not 0 <= progress <= 1:
        return "🌙 " + "─" * 12 + " ночь"
    bar = ["─"] * 12
    bar[min(int(progress * 12), 11)] = "☀️"
    return "🌅" + "".join(bar) + "🌇"


def format_current(w: CurrentWeather, is_favorite: bool) -> str:
    now = datetime.now(w.sunrise.tzinfo)
    star = "⭐️ " if is_favorite else ""
    return (
        f"{w.emoji} <b>{star}{escape(w.city)}, {w.country}</b>\n"
        f"<i>{escape(w.description)}</i>\n\n"
        f"<blockquote>"
        f"🌡 <b>{w.temp:+.1f}°C</b>  ·  ощущается {w.feels_like:+.1f}°C\n"
        f"📉 от {w.temp_min:+.0f}°  до  {w.temp_max:+.0f}°"
        f"</blockquote>\n"
        f"<pre>"
        f"Ветер      {wind_arrow(w.wind_deg)} {w.wind_speed:.1f} м/с\n"
        f"Влажность  💧 {w.humidity}%\n"
        f"Давление   🔻 {w.pressure} мм\n"
        f"Облачность ☁️ {w.clouds}%\n"
        f"Видимость  👁 {w.visibility} км"
        f"</pre>\n"
        f"{daylight_bar(w.sunrise, w.sunset, now)}\n"
        f"<code>{w.sunrise:%H:%M}</code> рассвет · закат <code>{w.sunset:%H:%M}</code>\n\n"
        f"<i>Обновлено в {now:%H:%M} по местному времени</i>"
    )


def format_hourly(city: str, items: list[HourlyItem]) -> str:
    temps = [i.temp for i in items]
    lines = [
        f"📊 <b>Ближайшие сутки — {escape(city)}</b>\n",
        f"<code>{sparkline(temps)}</code>",
        f"<i>от {min(temps):+.0f}° до {max(temps):+.0f}°</i>\n",
    ]
    for item in items:
        rain = f"  ☔️{item.pop * 100:.0f}%" if item.pop >= 0.2 else ""
        lines.append(
            f"<code>{item.time:%H:%M}</code> {item.emoji} <b>{item.temp:+3.0f}°</b>"
            f"  {escape(item.description)}{rain}"
        )
    return "\n".join(lines)


def format_daily(city: str, items: list[DailyItem]) -> str:
    lines = [f"📅 <b>Прогноз на 5 дней — {escape(city)}</b>\n"]
    for item in items:
        weekday = WEEKDAYS[item.date.weekday()]
        lines.append(
            f"<b>{weekday}, {item.date:%d.%m}</b>  {item.emoji}\n"
            f"<blockquote>{item.temp_max:+.0f}° / {item.temp_min:+.0f}°  ·  "
            f"{escape(item.description)}</blockquote>"
        )
    return "\n".join(lines)


# ═══════════════════════════ КЛАВИАТУРЫ ═══════════════════════════

def main_reply_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[
            KeyboardButton(text="📍 Погода рядом", request_location=True),
            KeyboardButton(text="⭐️ Избранное"),
        ]],
        resize_keyboard=True,
        input_field_placeholder="Напиши город…",
    )


def weather_kb(city: str, is_favorite: bool):
    builder = InlineKeyboardBuilder()
    builder.button(text="📊 На сутки", callback_data=f"hourly:{city}")
    builder.button(text="📅 На 5 дней", callback_data=f"daily:{city}")
    builder.button(
        text="💔 Убрать из избранного" if is_favorite else "⭐️ В избранное",
        callback_data=f"fav:{city}",
    )
    builder.button(text="🔄 Обновить", callback_data=f"city:{city}")
    builder.adjust(2, 1, 1)
    return builder.as_markup()


def back_kb(city: str):
    builder = InlineKeyboardBuilder()
    builder.button(text="⬅️ К погоде", callback_data=f"city:{city}")
    return builder.as_markup()


def cities_kb(cities: list[str], columns: int = 2):
    builder = InlineKeyboardBuilder()
    for city in cities:
        builder.button(text=city, callback_data=f"city:{city}")
    builder.adjust(columns)
    return builder.as_markup()


# ═══════════════════════════ ХЕНДЛЕРЫ ═══════════════════════════

router = Router()


@router.message(CommandStart())
async def cmd_start(message: Message) -> None:
    await message.answer(
        f"👋 Привет, {escape(message.from_user.first_name)}!\n\n"
        "Я показываю погоду в любой точке мира.\n\n"
        "<blockquote>"
        "✍️ Напиши <b>город</b> — покажу погоду сейчас\n"
        "📍 Жми <b>«Погода рядом»</b> — определю по геолокации\n"
        "⭐️ Копи <b>избранные города</b> — доступ в один тап\n"
        "🔍 Пиши <code>@weatherbergov_bot Москва</code> в любом чате"
        "</blockquote>",
        reply_markup=main_reply_kb(),
    )
    await message.answer("Популярные города:", reply_markup=cities_kb(POPULAR_CITIES))


@router.message(Command("help"))
async def cmd_help(message: Message) -> None:
    await message.answer(
        "<b>Возможности</b>\n\n"
        "<blockquote>"
        "• Погода по названию города или по геолокации\n"
        "• Почасовой прогноз на сутки с графиком\n"
        "• Прогноз на 5 дней\n"
        "• Избранные города\n"
        "• Inline-режим в любом чате"
        "</blockquote>\n"
        "<b>Команды</b>\n"
        "/start — меню\n"
        "/favorites — избранное\n"
        "/about — о проекте\n\n"
        "<i>Данные: OpenWeatherMap</i>"
    )


@router.message(Command("about"))
async def cmd_about(message: Message) -> None:
    await message.answer(
        "🛠 <b>О проекте</b>\n\n"
        "<blockquote>"
        "Python 3.11 · aiogram 3.x · aiohttp · SQLite\n"
        "Асинхронный клиент API, кэширование ответов, "
        "хранение избранного, inline-режим"
        "</blockquote>\n"
        "Разработка ботов под заказ — пиши в личные сообщения."
    )


async def send_weather(
    message: Message,
    user_id: int,
    service: WeatherService,
    city: str | None = None,
    coords: tuple[float, float] | None = None,
) -> None:
    try:
        weather = await service.get_current(city=city, coords=coords)
    except CityNotFound:
        await message.answer(
            f"Не нашёл город «{escape(city or '')}» 🤔\n\n"
            "<blockquote>Проверь написание или попробуй на английском — "
            "например, <code>Barcelona</code></blockquote>"
        )
        return
    except WeatherServiceError as exc:
        logger.error("Сервис погоды: %s", exc)
        await message.answer("⚠️ Сервис погоды недоступен. Попробуй через минуту.")
        return

    favorites = await get_favorites(user_id)
    is_favorite = weather.city in favorites
    await message.answer(
        format_current(weather, is_favorite),
        reply_markup=weather_kb(weather.city, is_favorite),
    )


@router.message(F.location)
async def handle_location(message: Message, weather_service: WeatherService) -> None:
    await send_weather(
        message,
        message.from_user.id,
        weather_service,
        coords=(message.location.latitude, message.location.longitude),
    )


@router.message(Command("favorites"))
@router.message(F.text == "⭐️ Избранное")
async def cmd_favorites(message: Message) -> None:
    favorites = await get_favorites(message.from_user.id)
    if not favorites:
        await message.answer(
            "⭐️ Избранное пусто.\n\n"
            "<blockquote>Открой погоду любого города и нажми "
            "<b>«В избранное»</b> — он появится здесь.</blockquote>"
        )
        return
    await message.answer("⭐️ <b>Твои города</b>", reply_markup=cities_kb(favorites))


@router.message(F.text & ~F.text.startswith("/"))
async def handle_city_text(message: Message, weather_service: WeatherService) -> None:
    city = message.text.strip()
    if len(city) > 60:
        await message.answer("Слишком длинное название 🙂 Напиши просто город.")
        return
    await send_weather(message, message.from_user.id, weather_service, city=city)


@router.callback_query(F.data.startswith("city:"))
async def cb_city(callback: CallbackQuery, weather_service: WeatherService) -> None:
    city = callback.data.split(":", 1)[1]
    await callback.answer()
    await send_weather(callback.message, callback.from_user.id, weather_service, city=city)


@router.callback_query(F.data.startswith("fav:"))
async def cb_favorite(callback: CallbackQuery) -> None:
    city = callback.data.split(":", 1)[1]
    added = await toggle_favorite(callback.from_user.id, city)
    await callback.answer("⭐️ Добавлено в избранное" if added else "💔 Убрано из избранного")
    with suppress(Exception):  # разметка могла не измениться — Telegram ругнётся, нам всё равно
        await callback.message.edit_reply_markup(reply_markup=weather_kb(city, added))


@router.callback_query(F.data.startswith("hourly:"))
async def cb_hourly(callback: CallbackQuery, weather_service: WeatherService) -> None:
    city = callback.data.split(":", 1)[1]
    await callback.answer()
    try:
        items = await weather_service.get_hourly(city)
    except (CityNotFound, WeatherServiceError) as exc:
        logger.error("Прогноз: %s", exc)
        await callback.message.answer("⚠️ Не удалось получить прогноз.")
        return
    await callback.message.answer(format_hourly(city, items), reply_markup=back_kb(city))


@router.callback_query(F.data.startswith("daily:"))
async def cb_daily(callback: CallbackQuery, weather_service: WeatherService) -> None:
    city = callback.data.split(":", 1)[1]
    await callback.answer()
    try:
        items = await weather_service.get_daily(city)
    except (CityNotFound, WeatherServiceError) as exc:
        logger.error("Прогноз: %s", exc)
        await callback.message.answer("⚠️ Не удалось получить прогноз.")
        return
    await callback.message.answer(format_daily(city, items), reply_markup=back_kb(city))


# ═══════════════════════════ INLINE-РЕЖИМ ═══════════════════════════

@router.inline_query()
async def inline_weather(query: InlineQuery, weather_service: WeatherService) -> None:
    """Работает в любом чате: @weatherbergov_bot Москва"""
    city = query.query.strip()
    if not city:
        await query.answer(
            [],
            cache_time=10,
            button=InlineQueryResultsButton(text="Напиши название города…", start_parameter="start"),
        )
        return

    try:
        weather = await weather_service.get_current(city=city)
    except (CityNotFound, WeatherServiceError):
        await query.answer(
            [],
            cache_time=10,
            button=InlineQueryResultsButton(text="Город не найден", start_parameter="start"),
        )
        return

    result = InlineQueryResultArticle(
        id=f"w:{weather.city}",
        title=f"{weather.emoji} {weather.city} — {weather.temp:+.0f}°C",
        description=f"{weather.description} · ощущается {weather.feels_like:+.0f}°C",
        input_message_content=InputTextMessageContent(
            message_text=format_current(weather, is_favorite=False),
            parse_mode=ParseMode.HTML,
        ),
    )
    await query.answer([result], cache_time=300, is_personal=False)


# ═══════════════════════════ ЗАПУСК ═══════════════════════════

async def set_commands(bot: Bot) -> None:
    await bot.set_my_commands([
        BotCommand(command="start", description="🏠 Главное меню"),
        BotCommand(command="favorites", description="⭐️ Избранные города"),
        BotCommand(command="help", description="❓ Помощь"),
        BotCommand(command="about", description="🛠 О проекте"),
    ])


async def main() -> None:
    missing = [n for n, v in (("BOT_TOKEN", BOT_TOKEN), ("WEATHER_API_KEY", WEATHER_API_KEY)) if not v]
    if missing:
        raise RuntimeError("Не заданы переменные: " + ", ".join(missing) + ". Добавь их в Secrets.")

    await init_db()

    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    weather_service = WeatherService(api_key=WEATHER_API_KEY)

    dp = Dispatcher(weather_service=weather_service)
    dp.include_router(router)

    await set_commands(bot)
    await bot.delete_webhook(drop_pending_updates=True)

    logger.info("Бот запущен")
    try:
        await dp.start_polling(bot)
    finally:
        await weather_service.close()
        await bot.session.close()
        logger.info("Бот остановлен")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Выход")