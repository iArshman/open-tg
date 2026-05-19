import asyncio
import os
import random
from pyrogram import Client, filters, enums
from pyrogram.types import Message
from utils.db import db
from utils.misc import modules_help, prefix
import time
import datetime
import pytz

# Database & settings

collection = "custom.followup"

# Load persistent data from the database.
global_followup_settings = db.get(collection, "followup_settings_private") or {}
followup_users = db.get(collection, "followup_users") or {}

# Default global settings (timer in milliseconds)
DEFAULT_SETTINGS = {"enabled": False, "message": "hi", "timer": 86400}
if not global_followup_settings:
    global_followup_settings = DEFAULT_SETTINGS.copy()
    db.set(collection, "followup_settings_private", global_followup_settings)

# In-memory dictionary to store active timers for users.
private_followup_timers = {}

# ------------------ Private Follow-Up Handler ------------------

async def private_followup_handler(client, user_id, delay, followup_message, message=None):
    """
    Waits for the specified delay (in milliseconds) then sends the follow-up message.
    After sending, updates the user's record to mark status as "inactive" and stores the current timestamp.
    If 'message' is provided, its chat id is used.
    """
    try:
        await asyncio.sleep(delay)
        # Check if follow-up is still enabled globally.
        if not global_followup_settings.get("enabled", True):
            return

        # Use the chat id from the original message if available.
        chat_id = message.chat.id if message is not None else int(user_id)
        await client.send_message(chat_id=chat_id, text=followup_message)
        
        # Update the user's record: mark as inactive and store current time.
        followup_users[user_id] = {
            "timestamp": time.time(),  # Numeric timestamp
            "status": "inactive"
        }
        db.set(collection, "followup_users", followup_users)
        private_followup_timers.pop(user_id, None)
    except asyncio.CancelledError:
        return
    except Exception as e:
        await client.send_message("me", f"Error in private follow-up handler: {str(e)}")

# ------------------ Update Private Follow-Up Timer ------------------

async def update_private_followup_timer(client, user_id, message):
    """
    For the given user, update their record with the current timestamp and mark status as "active".
    Then, cancel any existing timer and start a new timer using the global settings.
    """
    if not global_followup_settings.get("enabled", True):
        return

    # Cancel any existing timer.
    if user_id in private_followup_timers:
        private_followup_timers[user_id].cancel()
    
    # Update the user's record: set current time and status "active".
    followup_users[user_id] = {
        "timestamp": time.time(),
        "status": "active"
    }
    db.set(collection, "followup_users", followup_users)
    
    timer_value = global_followup_settings.get("timer", DEFAULT_SETTINGS["timer"])  
    private_followup_timers[user_id] = asyncio.create_task(
        private_followup_handler(client, user_id, timer_value, global_followup_settings.get("message", "hi"), message)
    )

# ------------------ Private Follow-Up Command ------------------
@Client.on_message(filters.command("fp", prefix) & filters.me)
async def private_followup_command(client: Client, message: Message):
    """
    Controls the private follow-up feature using global settings.
    Usage:
      /fp on           → Enable follow-up.
      /fp off          → Disable follow-up.
      /fp message [text] → Set follow-up message.
      /fp timer [seconds] → Set follow-up timer (in seconds).
    """
    try:
        parts = message.text.strip().split()
        if len(parts) < 2:
            await message.edit_text(f"Usage: {prefix}fp on/off/message/timer")
            return

        subcommand = parts[1].lower()
        if subcommand == "on":
            global_followup_settings["enabled"] = True
            await message.edit_text("✅ Follow-up enabled for all private chats.")
        elif subcommand == "off":
            global_followup_settings["enabled"] = False
            # Cancel any active timers.
            for uid in list(private_followup_timers.keys()):
                private_followup_timers[uid].cancel()
            await message.edit_text("🚫 Follow-up disabled for all private chats.")
        elif subcommand == "message":
            if len(parts) < 3:
                await message.edit_text("Usage: /fp message [text]")
                return
            new_msg = " ".join(parts[2:])
            global_followup_settings["message"] = new_msg
            await message.edit_text(f"✉️ Follow-up message set to: {new_msg}")
        elif subcommand == "timer":
            if len(parts) < 3:
                await message.edit_text("Usage: /fp timer [seconds]")
                return
            try:
                timer_value = int(parts[2]) 
                global_followup_settings["timer"] = timer_value
                await message.edit_text(f"⏳ Follow-up timer set to: {parts[2]} seconds")
            except ValueError:
                await message.edit_text("❌ Invalid timer value. It must be an integer.")
                return
        else:
            await message.edit_text("❌ Invalid subcommand. Use on/off/message/timer.")
            return

        # Persist the updated global settings.
        db.set(collection, "followup_settings_private", global_followup_settings)
        await asyncio.sleep(1)
        await message.delete()
    except Exception as e:
        await client.send_message("me", f"❌ Error in private follow-up command: {str(e)}")

# ------------------ Private Follow-Up Update Handler ------------------

@Client.on_message(filters.private & ~filters.me & ~filters.bot, group=10)
async def followup_update_handler(client: Client, message: Message):
    """
    When a private message is received, update the user's record (timestamp and status "active")
    and start (or reset) the follow-up timer using the global settings.
    """
    try:
        user_id = str(message.from_user.id)
        followup_users[user_id] = {
            "timestamp": time.time(),
            "status": "active"
        }
        db.set(collection, "followup_users", followup_users)
        if global_followup_settings.get("enabled", True):
            await update_private_followup_timer(client, user_id, message)
    except Exception as e:
        await client.send_message("me", f"❌ Error in follow-up update handler: {str(e)}")

# ------------------ Restore Timers on Startup ------------------
async def restore_private_followup_timers(client):
    """
    On startup, for each user with status "active", calculate the remaining time based on their stored timestamp
    and restore their timer. If the timer has already expired, send the follow-up immediately.
    """
    current_time = time.time()
    timer_duration = global_followup_settings.get("timer", DEFAULT_SETTINGS["timer"])
    for user_id, data in followup_users.items():
        if isinstance(data, dict) and data.get("status") == "active":
            elapsed = (current_time - data.get("timestamp", current_time)) * 1000  # in milliseconds
            remaining = max(0, timer_duration - elapsed)
            if remaining > 0:
                asyncio.create_task(private_followup_handler(client, user_id, remaining, global_followup_settings.get("message", "hi"), None))
            else:
                asyncio.create_task(private_followup_handler(client, user_id, 1000, global_followup_settings.get("message", "hi"), None))

# ------------------ Restore Command ------------------
@Client.on_message(filters.command("fprestore", prefix) & filters.me)
async def restore_followup_command(client: Client, message: Message):
    """
    Command to manually restore follow-up timers for active users.
    Usage: /restorefp
    """
    try:
        await restore_private_followup_timers(client)
        await message.edit_text("✅ Private follow-up timers restored.")
    except Exception as e:
        await client.send_message("me", f"❌ Error in restore-followup command: {str(e)}")


# ------------------ reminder ------------------
# ------------------ Persistent Data for Private Reminders ------------------

# Load persistent user follow-up records.
followup_users = db.get(collection, "followup_users") or {}

# Load persistent reminder settings.
private_reminder_settings = db.get(collection, "private_reminder_settings")
DEFAULT_REMINDER_SETTINGS = {
    "reminder_message": "What's up",
    "reminder_threshold": 172800  # 
}

if private_reminder_settings is None:
    private_reminder_settings = DEFAULT_REMINDER_SETTINGS.copy()
    db.set(collection, "private_reminder_settings", private_reminder_settings)

# ------------------ Command: fpr reminder ------------------

@Client.on_message(filters.command("fpreminder", prefix) & filters.me)
async def send_reminder(client: Client, message: Message):
    """
    Sends a reminder message to all inactive users (private chats) who have been inactive for at least the threshold.
    Usage: /fpreminder
    """
    try:
        threshold = private_reminder_settings.get("reminder_threshold", DEFAULT_REMINDER_SETTINGS["reminder_threshold"])
        reminder_text = private_reminder_settings.get("reminder_message", DEFAULT_REMINDER_SETTINGS["reminder_message"])
        count = 0
        delay_between = 15  # seconds delay between messages to avoid flooding
        current_time = time.time()
        
        # Notify that reminder sending is starting
        await message.edit_text(f"⏳ Sending reminders to inactive users (threshold: {threshold} seconds)...")
        
        for user_id, data in followup_users.items():
            if isinstance(data, dict) and data.get("status") == "inactive":
                last_timestamp = data.get("timestamp", current_time)
                if current_time - last_timestamp >= threshold:
                    await client.send_message(chat_id=int(user_id), text=reminder_text)
                    count += 1
                    await asyncio.sleep(delay_between)
        await message.edit_text(f"✅ Sent reminders to {count} inactive user(s) (threshold: {threshold} seconds).")
    except Exception as e:
        await client.send_message("me", f"❌ Error in tfp reminder command: {str(e)}")

# ------------------ Combined Command: fpr ------------------


@Client.on_message(filters.command("fpr", prefix) & filters.me)
async def private_reminder_settings_handler(client: Client, message: Message):
    """
    Combined command for private reminder settings.
    
    Usage:
      /fpr message <new reminder message>
          → Sets the private reminder message.
      /fpr time <new threshold in seconds>
         or /fpr seconds <new threshold in seconds>
          → Sets the inactivity threshold.
      /fpr reset
          → Resets the private reminder settings from the database.
    """
    try:
        parts = message.text.split(maxsplit=2)
        if len(parts) < 2:
            await message.edit_text("Usage: /fpr message|time|seconds|reset [value]")
            return
        
        subcommand = parts[1].lower()
        
        if subcommand == "message":
            if len(parts) < 3:
                await message.edit_text("Usage: /fpr message <new reminder message>")
                return
            new_msg = parts[2].strip()
            # Optional: remove a leading "message" word if present.
            if new_msg.lower().startswith("message"):
                new_msg = new_msg[len("message"):].strip()
            private_reminder_settings["reminder_message"] = new_msg
            db.set(collection, "private_reminder_settings", private_reminder_settings)
            await message.edit_text(f"✅ Private reminder message updated to: {new_msg}")
        
        elif subcommand in ["time", "seconds"]:
            if len(parts) < 3:
                await message.edit_text("Usage: /fpr time <new threshold in seconds>")
                return
            try:
                new_threshold = int(parts[2])
                private_reminder_settings["reminder_threshold"] = new_threshold
                db.set(collection, "private_reminder_settings", private_reminder_settings)
                await message.edit_text(f"✅ Private reminder threshold updated to: {new_threshold} seconds")
            except ValueError:
                await message.edit_text("❌ Invalid threshold value. It must be an integer.")
        
        elif subcommand == "reset":
            new_settings = db.get(collection, "private_reminder_settings")
            if new_settings is None:
                new_settings = DEFAULT_REMINDER_SETTINGS.copy()
                db.set(collection, "private_reminder_settings", new_settings)
            private_reminder_settings.clear()
            private_reminder_settings.update(new_settings)
            await message.edit_text("✅ Private reminder settings reset from the database.")
        
        else:
            await message.edit_text(
                "Usage:\n"
                "/fpr message <new reminder message>\n"
                "/fpr time <new threshold in seconds>\n"
                "/fpr reset"
            )
    
    except Exception as e:
        await client.send_message("me", f"❌ Error in private reminder settings handler: {str(e)}")
        await message.edit_text("❌ Something went wrong in the private reminder settings handler.")
# ------------------ Command: fp inactive ------------------

@Client.on_message(filters.command("fpinactive", prefix) & filters.me)
async def fetch_inactive_list(client: Client, message: Message):
    """
    Displays the list of inactive users (private follow-up) who have been inactive for at least the threshold.
    Usage: /tfp inactive
    """
    try:
        threshold = private_reminder_settings.get("reminder_threshold", DEFAULT_REMINDER_SETTINGS["reminder_threshold"])
        current_time = time.time()
        inactive_users = []
        for user_id, data in followup_users.items():
            if isinstance(data, dict) and data.get("status") == "inactive":
                last_timestamp = data.get("timestamp", current_time)
                if current_time - last_timestamp >= threshold:
                    inactive_users.append(user_id)
        if inactive_users:
            inactive_text = "\n".join([f"User ID: {user}" for user in inactive_users])
            await message.edit_text(f"⚠️ Inactive users (inactive > {threshold} sec):\n{inactive_text}")
        else:
            await message.edit_text(f"✅ No inactive users found (threshold: {threshold} sec).")
    except Exception as e:
        await client.send_message("me", f"❌ Error fetching inactive users: {str(e)}")
        await message.edit_text("❌ Something went wrong while fetching inactive users.")
## End Follow Up

modules_help["followup"] = {
    "fp on": "Enable follow-up messages for all users.",
    "fp off": "Disable follow-up messages for all users.",
    "fp message <text>": "Set the follow-up message for all private chats.",
    "fp timer <seconds>": "Set the follow-up delay (in seconds) for all private chats.",
    "fprestore": "Restore follow-up timer after reboot for all users.",
    "fpinactive": "View inactive users.",
    "fpreminder": "Send reminder messages to inactive private chat users (if they've been inactive for the threshold).",
    "fpr message <text>": "Set the private follow-up reminder message.",
    "fpr seconds <seconds>": "Set the private follow-up reminder threshold (in seconds).",
}
