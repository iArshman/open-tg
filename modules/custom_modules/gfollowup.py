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

collection = "custom.group_followup"


# Load persistent data
group_followup_settings = db.get(collection, "group_followup_settings") or {}
group_followup_topics = db.get(collection, "followup_groups") or {}

# Default settings for groups.
DEFAULT_GROUP_SETTINGS = {"enabled": False, "message": "hi", "timer": 86400}  # timer in seconds
# If no settings exist at all, initialize with default (global defaults).
if not group_followup_settings:
    group_followup_settings = DEFAULT_GROUP_SETTINGS.copy()
    db.set(collection, "group_followup_settings", group_followup_settings)

# In-memory dictionary to store active timers for topics.
group_followup_timers = {}

# ------------------ Group Follow-Up Handler ------------------
async def group_followup_handler(client, group_id, topic_id, message=None, delay=None):
    """
    Waits for the given delay (in seconds) then sends the follow-up message in the topic.
    After sending, updates the persistent record for that topic to mark status as "inactive".
    If message is provided, its chat id and thread id are used.
    """
    try:
        # Use provided delay if given; otherwise, use the timer value from settings.
        if delay is None:
            delay = group_followup_settings.get(group_id, DEFAULT_GROUP_SETTINGS).get("timer", DEFAULT_GROUP_SETTINGS["timer"])
        await asyncio.sleep(delay)
        settings = group_followup_settings.get(group_id, DEFAULT_GROUP_SETTINGS)
        if not settings.get("enabled", False):
            return

        if message:
            chat_id = message.chat.id
            thread_id = message.message_thread_id
        else:
            chat_id = int(group_id)
            thread_id = None

        await client.send_message(
            chat_id=chat_id,
            text=settings["message"],
            message_thread_id=thread_id
        )
        # Update persistent record: mark this topic as inactive and update timestamp.
        if group_id in group_followup_topics and topic_id in group_followup_topics[group_id]:
            group_followup_topics[group_id][topic_id]["timestamp"] = time.time()
            group_followup_topics[group_id][topic_id]["status"] = "inactive"
            db.set(collection, "followup_groups", group_followup_topics)
        group_followup_timers.pop(topic_id, None)
    except asyncio.CancelledError:
        return
    except Exception as e:
        await client.send_message("me", f"[LOG] Error in group_followup_handler: {str(e)}")

# ------------------ Update Group Follow-Up Timer ------------------
async def update_group_followup_timer(client, group_id, topic_id, message):
    """
    For the given group and topic, update persistent storage with the current timestamp and mark status as "active".
    Then, cancel any existing timer for the topic and start a new timer using the group's timer setting.
    """
    settings = group_followup_settings.get(group_id, DEFAULT_GROUP_SETTINGS)
    if not settings.get("enabled", False):
        return
    if topic_id in group_followup_timers:
        group_followup_timers[topic_id].cancel()
    if group_id not in group_followup_topics:
        group_followup_topics[group_id] = {}
    group_followup_topics[group_id][topic_id] = {"timestamp": time.time(), "status": "active"}
    db.set(collection, "followup_groups", group_followup_topics)
    delay = settings.get("timer", DEFAULT_GROUP_SETTINGS["timer"])
    group_followup_timers[topic_id] = asyncio.create_task(
        group_followup_handler(client, group_id, topic_id, message, delay)
    )

# ------------------ Group Follow-Up Message Handler ------------------
@Client.on_message(filters.text & filters.group & ~filters.me, group=11)
async def group_followup_message_handler(client: Client, message: Message):
    """
    Resets the follow-up timer on every new text message in the group.
    Each topic (group_id:thread_id) gets its own timer.
    """
    group_id = str(message.chat.id)
    topic_id = f"{group_id}:{message.message_thread_id}"
    settings = group_followup_settings.get(group_id, DEFAULT_GROUP_SETTINGS)
    if not settings.get("enabled", False):
        return
    await update_group_followup_timer(client, group_id, topic_id, message)

# ------------------ Group Follow-Up Command ------------------
@Client.on_message(filters.command("gfp", prefix) & filters.me)
async def group_followup_command(client: Client, message: Message):
    """
    Command to control group follow-ups for the group.
    Usage: /gfp on/off/message/timer [value]
    """
    try:
        group_id = str(message.chat.id)
        parts = message.text.strip().split()
        if len(parts) < 2:
            await message.edit_text(f"Usage: {prefix}gfp on/off/message/timer")
            return

        subcommand = parts[1].lower()
        # Get current settings for this group; if not set, use a copy of defaults.
        settings = group_followup_settings.get(group_id, DEFAULT_GROUP_SETTINGS.copy())

        if subcommand == "on":
            settings["enabled"] = True
            # Cancel all active timers for topics in this group.
            for topic in list(group_followup_timers.keys()):
                if topic.startswith(group_id):
                    group_followup_timers[topic].cancel()
                    group_followup_timers.pop(topic, None)
            await message.edit_text("✅ Follow-up enabled for this group.")
        elif subcommand == "off":
            settings["enabled"] = False
            # Cancel all active timers for topics in this group.
            for topic in list(group_followup_timers.keys()):
                if topic.startswith(group_id):
                    group_followup_timers[topic].cancel()
                    group_followup_timers.pop(topic, None)
            await message.edit_text("🚫 Follow-up disabled for this group.")
        elif subcommand == "message":
            if len(parts) < 3:
                await message.edit_text("Usage: /gfp message <follow-up message>")
                return
            new_msg = " ".join(parts[2:])
            settings["message"] = new_msg
            await message.edit_text(f"✉️ Follow-up message set to: {new_msg}")
        elif subcommand == "timer":
            if len(parts) < 3:
                await message.edit_text("Usage: /gfp timer <time in seconds>")
                return
            try:
                new_timer = int(parts[2])
                settings["timer"] = new_timer
                await message.edit_text(f"⏳ Follow-up timer set to: {new_timer} seconds")
            except ValueError:
                await message.edit_text("Invalid timer value. It must be an integer.")
                return
        else:
            await message.edit_text("Invalid subcommand. Use on, off, message, or timer.")
            return

        # Update the settings for this group and save to the database.
        group_followup_settings[group_id] = settings
        db.set(collection, "group_followup_settings", group_followup_settings)
        await asyncio.sleep(1)
        await message.delete()
        # Optional: Send a debug log.
        await client.send_message("me", f"[LOG] Updated group settings for {group_id}: {settings}")
    except Exception as e:
        await client.send_message("me", f"Error in Group follow-up command: {str(e)}")

# ------------------ Restore Group Follow-Up Timers on Startup ------------------
async def restore_group_followup_timers(client):
    """
    On startup, fetch the group follow-up topics stored in the database.
    For each group and topic with status "active", calculate the remaining time (in seconds)
    based on the stored timestamp and restore its timer using the group's timer setting.
    If the timer has already expired, send the follow-up immediately.
    """
    for group_id, topics in group_followup_topics.items():
        for topic_id, data in topics.items():
            if isinstance(data, dict) and data.get("status") == "active":
                elapsed = time.time() - data.get("timestamp", time.time())
                timer_duration = group_followup_settings.get(group_id, DEFAULT_GROUP_SETTINGS).get("timer", DEFAULT_GROUP_SETTINGS["timer"])
                remaining = max(0, timer_duration - elapsed)
                asyncio.create_task(group_followup_handler(client, group_id, topic_id, None, remaining))

@Client.on_message(filters.command("gfprestore", prefix) & filters.me)
async def restore_group_followup_command(client: Client, message: Message):
    """
    Command to manually restore group follow-up timers.
    Usage: /restoregfp
    """
    try:
        await restore_group_followup_timers(client)
        await message.edit_text("✅ Group follow-up timers restored.")
    except Exception as e:
        await client.send_message("me", f"Error in restore group follow-up command: {str(e)}")


# ------------------ Reminder Settings ------------------

# Load persistent data from the database
group_reminder_settings = db.get(collection, "group_reminder_settings") or {}

# Default reminder settings for groups
DEFAULT_REMINDER_SETTINGS = {
    "reminder_message": "What's up!",
    "reminder_threshold": 172800 
}

# If no settings exist, initialize with default (global defaults).
if not group_reminder_settings:
    group_reminder_settings = {}
    db.set(collection, "group_reminder_settings", group_reminder_settings)

# ------------------- Command to Fetch Inactive Topics -------------------

@Client.on_message(filters.command("gfpinactive", prefix) & filters.me)
async def fetch_inactive_topics(client: Client, message: Message):
    """
    Command to fetch and display the list of inactive topics that have exceeded the inactivity threshold.
    Usage: /fpinactive
    """
    try:
        group_id = str(message.chat.id)  # Get the group ID
        
        # Fetch the group's inactivity threshold from the database
        group_settings = group_reminder_settings.get(group_id, DEFAULT_REMINDER_SETTINGS.copy())
        threshold = group_settings.get("reminder_threshold", DEFAULT_REMINDER_SETTINGS["reminder_threshold"])  # Default inactivity threshold (1 hour)
        
        current_time = time.time()
        inactive_topics = []
        
        # Fetch topics for the group from the database
        group_topics = db.get(collection, "followup_groups") or {}
        topics = group_topics.get(group_id, {})

        for topic_id, data in topics.items():
            if isinstance(data, dict) and data.get("status") == "inactive":
                last_timestamp = data.get("timestamp", current_time)
                # Check if the topic has been inactive for longer than the group's threshold
                if current_time - last_timestamp >= threshold:
                    inactive_topics.append(topic_id)

        # Show the list of inactive topics in the same message instead of sending it to saved messages
        if inactive_topics:
            inactive_topics_text = "\n".join([f"Topic ID: {topic}" for topic in inactive_topics])
            await message.edit_text(f"⚠️ Inactive topics (inactive for more than {threshold} seconds) in group {group_id}:\n{inactive_topics_text}")
        else:
            await message.edit_text(f"✅ No inactive topics found in group {group_id} for the given threshold of {threshold} seconds.")
    
    except Exception as e:
        await client.send_message("me", f"❌ Error in fetching inactive topics for group {group_id}: {str(e)}")
        await message.edit_text("❌ Something went wrong while fetching inactive topics.")

# ------------------ Send Reminders ------------------

@Client.on_message(filters.command("gfpreminder", prefix) & filters.me)
async def group_send_reminder(client: Client, message: Message):
    """
    Sends reminder messages to topics that have been inactive for at least the group's reminder threshold.
    Usage: /gfpreminder
    """
    try:
        group_id = str(message.chat.id)  # Get the group ID
        
        # Fetch the group's reminder settings from the database
        group_settings = group_reminder_settings.get(group_id, DEFAULT_REMINDER_SETTINGS.copy())
        threshold = group_settings.get("reminder_threshold", DEFAULT_REMINDER_SETTINGS["reminder_threshold"])
        reminder_text = group_settings.get("reminder_message", DEFAULT_REMINDER_SETTINGS["reminder_message"])
        
        # Notify in the chat that reminders are starting
        await message.edit_text(f"⏳ Starting to send reminders for inactive topics (threshold: {threshold} seconds)...")
        
        count = 0
        delay_between = 30 # seconds delay to avoid flooding
        current_time = time.time()

        # Fetch topics for the group from the database
        group_topics = db.get(collection, "followup_groups") or {}
        topics = group_topics.get(group_id, {})

        if not topics:
            await message.edit_text("⚠️ No topics found in this group for reminders.")
            return

        # Loop through each topic and check inactivity
        for topic_id, data in topics.items():
            if isinstance(data, dict) and data.get("status") == "inactive":
                last_timestamp = data.get("timestamp", current_time)
                if current_time - last_timestamp >= threshold:
                    # Extract thread ID if present in the topic ID ("group_id:thread_id")
                    parts_topic = topic_id.split(":", 1)
                    thread_id = int(parts_topic[1]) if len(parts_topic) == 2 else None
                    # Send reminder to the topic
                    await client.send_message(
                        chat_id=int(group_id),
                        text=reminder_text,
                        message_thread_id=thread_id
                    )
                    count += 1
                    await asyncio.sleep(delay_between)

        # Update the same message with the final status
        if count > 0:
            await message.edit_text(f"✅ Sent reminders to {count} inactive topic(s) (threshold: {threshold} seconds).")
        else:
            await message.edit_text("⚠️ No inactive topics met the reminder criteria.")
    
    except Exception as e:
        await message.edit_text("❌ Something went wrong while sending reminders.")
        await client.send_message("me", f"❌ Error in group reminder command: {str(e)}")

# ------------------ Manage Reminder Settings ------------------
@Client.on_message(filters.command(["gfpr", "gfprmessage", "gfprseconds", "gfprreset"], prefix) & filters.me)
async def group_reminder_handler(client: Client, message: Message):
    """
    Manages reminder settings for the group where the command is executed.
    Works in group topics as well.

    Usage:
    - /gfpr message <new reminder message>
    - /gfpr seconds <new threshold>
    - /gfpr reset
    """
    try:
        group_id = str(message.chat.id)  
        parts = message.text.split(maxsplit=2)
        
        if len(parts) < 2:
            await message.edit_text(
                "Usage:\n"
                "/gfpr message <new reminder message>\n"
                "/gfpr seconds <new threshold>\n"
                "/gfpr reset"
            )
            return

        command_type = parts[1].strip().lower()

        if command_type == "message":
            if len(parts) < 3:
                await message.edit_text("Usage: /gfpr message <new reminder message>")
                return
            new_msg = parts[2].strip()
            group_reminder_settings[group_id] = group_reminder_settings.get(group_id, DEFAULT_REMINDER_SETTINGS.copy())
            group_reminder_settings[group_id]["reminder_message"] = new_msg
            db.set(collection, "group_reminder_settings", group_reminder_settings)
            await message.edit_text(f"✅ Reminder message updated for this group: {new_msg}")

        elif command_type == "seconds":
            if len(parts) < 3:
                await message.edit_text("Usage: /gfpr seconds <new threshold>")
                return
            try:
                new_threshold = int(parts[2])
                group_reminder_settings[group_id] = group_reminder_settings.get(group_id, DEFAULT_REMINDER_SETTINGS.copy())
                group_reminder_settings[group_id]["reminder_threshold"] = new_threshold
                db.set(collection, "group_reminder_settings", group_reminder_settings)
                await message.edit_text(f"✅ Reminder threshold updated for this group: {new_threshold} seconds")
            except ValueError:
                await message.edit_text("❌ Invalid threshold value. It must be an integer.")

        elif command_type == "reset":
            new_settings = db.get(collection, "group_reminder_settings") or {}
            group_reminder_settings[group_id] = new_settings.get(group_id, DEFAULT_REMINDER_SETTINGS.copy())
            await message.edit_text("✅ Group reminder settings reset for this group.")

        else:
            await message.edit_text(
                "Invalid command! Usage:\n"
                "/gfpr message <new reminder message>\n"
                "/gfpr seconds <new threshold>\n"
                "/gfpr reset"
            )

    except Exception as e:
        await client.send_message("me", f"❌ Error in gfpr reminder handler (Group {group_id}): {str(e)}")


# ------------------ End Group Follow-Up Feature ------------------

modules_help["gfollowup"] = {
    "gfp on": "Enable follow-up messages for all users.",
    "gfp off": "Disable follow-up messages for all users.",
    "gfp message <text>": "Set the follow-up message for all private chats.",
    "gfp timer <seconds>": "Set the follow-up delay (in seconds) for all private chats.",
    "gfprestore": "Restore timer after reboot  for all users.",
    "gfpinactive": "view inactive topic users.",
    "gfpreminder": "Send reminder messages to inactive private chat users (if they've been inactive for the threshold).",
    "gfpr message <text>": "Set the private follow-up reminder message.",
    "gfpr seconds <seconds>": "Set the private follow-up reminder threshold (in seconds).",
}
