from datetime import datetime, timedelta
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import ChatForwardsRestricted
import asyncio
from utils.misc import modules_help, prefix

DEFAULT_TIME_ZONE_OFFSET = 5
MORNING_TIME = "08:00 AM"
NIGHT_TIME = "11:10 PM"

GREETINGS = {
    "morning": [
        "Good morning! 🌞",
        "Good Morning, love ❤️",
        "Morning, and love you",
        "Morning 🌄",
        "Have a good day, love",
    ],
    "night": [
        "Good night! 🌙",
        "Sweet dreams! 💤",
        "Rest well, love",
        "Good night and love you",
        "Take rest, GN love ❤️",
    ],
}


def local_to_utc(local_time: datetime) -> datetime:
    """Converts local time to UTC based on the default time zone offset."""
    return local_time - timedelta(hours=DEFAULT_TIME_ZONE_OFFSET)


def parse_days_and_time(args: list) -> tuple[int, datetime]:
    """
    Parses the number of days and time from the command arguments.
    """
    days = int(args[1])
    local_time = datetime.strptime(args[2], "%I:%M %p")
    now = datetime.now()
    scheduled_time = now.replace(
        hour=local_time.hour, minute=local_time.minute, second=0, microsecond=0
    )
    return days, scheduled_time


async def schedule_greetings(
    client: Client, chat_id: int, messages: list, start_time: datetime, days: int, thread_id: int = None
):
    """Schedules greetings messages in the chat or a specific topic."""
    for day in range(days):
        schedule_date = start_time + timedelta(days=day)
        message_text = messages[day % len(messages)]
        try:
            await client.send_message(
                chat_id=chat_id,
                text=message_text,
                schedule_date=schedule_date,
                message_thread_id=thread_id
            )
        except ChatForwardsRestricted:
            await client.send_message(
                chat_id,
                "<code>Scheduling failed: Restricted copy/forwards in this chat.</code>",
                message_thread_id=thread_id,
            )
            return


async def send_status_and_delete(message: Message, text: str):
    """Edits the message with the given text and deletes it after 5 seconds."""
    status_message = await message.edit(text)
    await asyncio.sleep(1)
    await status_message.delete()


async def handle_schedule_command(client: Client, message: Message, greeting_type: str):
    """Handles the scheduling of specific greeting types."""
    try:
        args = message.text.split(maxsplit=2)
        if len(args) < 3:
            raise ValueError

        days, scheduled_time = parse_days_and_time(args)
        start_time_utc = local_to_utc(scheduled_time)
        if start_time_utc < datetime.utcnow():
            start_time_utc += timedelta(days=1)

        await schedule_greetings(
            client, 
            message.chat.id, 
            GREETINGS[greeting_type], 
            start_time_utc, 
            days, 
            thread_id=message.message_thread_id
        )

        formatted_time = scheduled_time.strftime("%I:%M %p")
        await send_status_and_delete(
            message,
            f"<code>Scheduled {greeting_type} greetings for {days} days starting at {formatted_time} (UTC+{DEFAULT_TIME_ZONE_OFFSET}).</code>",
        )
    except ValueError:
        await send_status_and_delete(
            message,
            f"<b>Usage:</b>\n<code>{prefix}{greeting_type} [days] [HH:MM AM/PM]</code>",
        )


@Client.on_message(filters.command("greet", prefix) & filters.me)
async def schedule_greet(client: Client, message: Message):
    """Schedules both morning and night greetings for a given number of days."""
    try:
        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            raise ValueError

        days = int(args[1])
        now = datetime.now()

        morning_time = datetime.strptime(MORNING_TIME, "%I:%M %p").replace(
            year=now.year, month=now.month, day=now.day
        )
        night_time = datetime.strptime(NIGHT_TIME, "%I:%M %p").replace(
            year=now.year, month=now.month, day=now.day
        )

        morning_time_utc = local_to_utc(morning_time)
        night_time_utc = local_to_utc(night_time)

        if morning_time_utc < datetime.utcnow():
            morning_time_utc += timedelta(days=1)
        if night_time_utc < datetime.utcnow():
            night_time_utc += timedelta(days=1)

        await schedule_greetings(
            client, message.chat.id, GREETINGS["morning"], morning_time_utc, days, thread_id=message.message_thread_id
        )
        await schedule_greetings(
            client, message.chat.id, GREETINGS["night"], night_time_utc, days, thread_id=message.message_thread_id
        )

        await send_status_and_delete(
            message,
            f"<code>Scheduled morning and night greetings for {days} days starting at {MORNING_TIME} and {NIGHT_TIME} (UTC+{DEFAULT_TIME_ZONE_OFFSET}).</code>",
        )
    except ValueError:
        await send_status_and_delete(
            message, f"<b>Usage:</b>\n<code>{prefix}greet [days]</code>"
        )


@Client.on_message(filters.command("morning", prefix) & filters.me)
async def schedule_morning(client: Client, message: Message):
    """Schedules morning greetings."""
    await handle_schedule_command(client, message, "morning")


@Client.on_message(filters.command("night", prefix) & filters.me)
async def schedule_night(client: Client, message: Message):
    """Schedules night greetings."""
    await handle_schedule_command(client, message, "night")


modules_help["greetings"] = {
    "morning <days> <HH:MM AM/PM>": "Schedules morning greetings for the specified days at the given time.",
    "night <days> <HH:MM AM/PM>": "Schedules night greetings for the specified days at the given time.",
    "greet <days>": "Schedules both morning and night greetings for the specified days at fixed times.",
    "\n<b>Default time zone offset:</b>": f"UTC+{DEFAULT_TIME_ZONE_OFFSET}",
}
