import asyncio
import os
from asyncio import sleep
from pyrogram import Client, filters
from pyrogram.errors import (
    FileReferenceExpired,
    FileReferenceInvalid,
    TopicDeleted,
    TopicClosed,
)
from pyrogram.types import Message
from collections import defaultdict

from utils.db import db
from utils.misc import modules_help, prefix

mlog_enabled = filters.create(lambda _, __, ___: db.get("custom.mlog", "status", False))

user_media_cache = defaultdict(list)
media_processing_tasks = {}

def get_group_data(group_id):
    return db.get(f"custom.mlog", str(group_id), {})

def update_group_data(group_id, data):
    db.set(f"custom.mlog", str(group_id), data)

@Client.on_message(filters.command(["mlog"], prefix) & filters.me, group=5)
async def mlog(_, message: Message):
    if len(message.command) < 2 or message.command[1].lower() not in ["on", "off"]:
        return await message.edit(f"<b>Usage:</b> <code>{prefix}mlog [on/off]</code>")
    status = message.command[1].lower() == "on"
    db.set("custom.mlog", "status", status)
    await message.edit(f"<b>Media logging is now {'enabled' if status else 'disabled'}</b>")

@Client.on_message(filters.command(["msetchat"], prefix) & filters.me, group=5)
async def set_chat(_, message: Message):
    if len(message.command) < 2:
        return await message.edit(f"<b>Usage:</b> <code>{prefix}msetchat [chat_id]</code>")
    try:
        chat_id = message.command[1]
        chat_id = int("-100" + chat_id if not chat_id.startswith("-100") else chat_id)
        db.set("custom.mlog", "chat", chat_id)
        await message.edit(f"<b>Chat ID set to {chat_id}</b>")
    except ValueError:
        await message.edit("<b>Invalid chat ID</b>")

@Client.on_message(filters.command(["mrename", "m"], prefix) & filters.me, group=5)
async def rename_topic(client: Client, message: Message):
    try:
        user_id = int(message.command[1]) if len(message.command) > 1 else message.chat.id
        chat_id = db.get("custom.mlog", "chat")
        if not chat_id:
            status_msg = await message.edit(f"<b>No Chat ID is set. Use {prefix}msetchat to set the Chat ID.</b>")
            await sleep(1)
            return await status_msg.delete()
        group_data = get_group_data(chat_id)
        user_topics = group_data.get("user_topics", {})
        topic_id = user_topics.get(str(user_id))
        if not topic_id:
            status_msg = await message.edit(f"<b>No topic found for user ID {user_id}.</b>")
            await sleep(1)
            return await status_msg.delete()
        user = await client.get_users(user_id)
        try:
            await client.edit_forum_topic(chat_id=chat_id, topic_id=topic_id, title=user.first_name)
            status_msg = await message.edit(f"<b>Topic renamed to '{user.first_name}'.</b>")
        except Exception as e:
            if "TOPIC_NOT_MODIFIED" in str(e):
                status_msg = await message.edit(f"<b>Topic name is already '{user.first_name}'.</b>")
            else:
                raise e
        info_message = (
            f"<b>Chat Name:</b> {user.full_name}\n"
            f"<b>User ID:</b> {user.id}\n"
            f"<b>Username:</b> @{user.username or 'N/A'}\n"
            f"<b>Phone No:</b> +{user.phone_number or 'N/A'}"
        )
        info_msg = await client.send_message(chat_id=chat_id, message_thread_id=topic_id, text=info_message)
        await info_msg.pin()
        await sleep(1)
        await status_msg.delete()
    except ValueError:
        status_msg = await message.edit("<b>Invalid user ID</b>")
        await sleep(1)
        return await status_msg.delete()
    except Exception as e:
        status_msg = await message.edit(f"<b>Error:</b> {str(e)}")
        await sleep(1)
        return await status_msg.delete()

@Client.on_message(
    mlog_enabled
    & filters.incoming
    & filters.private
    & (filters.photo | filters.video | filters.document | filters.audio | filters.video_note | filters.voice)
    & ~filters.sticker
    & ~filters.animation
    & ~filters.me
    & ~filters.bot
)
async def media_log(client: Client, message: Message):
    user_id = message.from_user.id
    user_media_cache[user_id].append(message)
    if user_id not in media_processing_tasks:
        media_processing_tasks[user_id] = asyncio.create_task(process_media(client, message.from_user))

async def process_media(client: Client, user):
    await asyncio.sleep(5)
    user_id = user.id
    me = await client.get_me()
    if user_id == me.id:
        return
    chat_id = db.get("custom.mlog", "chat")
    if not chat_id:
        return await client.send_message(
            "me",
            f"Media Logger is on, but no Chat ID is set. Use {prefix}msetchat to set it.",
        )
    group_data = get_group_data(chat_id)
    user_topics = group_data.get("user_topics", {})
    topic_id = user_topics.get(str(user_id))
    if not topic_id:
        topic = await client.create_forum_topic(chat_id, user.first_name)
        topic_id = topic.id
        user_topics[str(user_id)] = topic_id
        update_group_data(chat_id, {"user_topics": user_topics})
        m = await client.send_message(
            chat_id=chat_id,
            message_thread_id=topic_id,
            text=f"<b>Chat Name:</b> {user.full_name}\n<b>User ID:</b> {user_id}\n<b>Username:</b> @{user.username or 'N/A'}\n<b>Phone No:</b> +{user.phone_number or 'N/A'}",
        )
        await m.pin()
    messages_to_process = user_media_cache.pop(user_id, [])
    for media_message in messages_to_process:
        try:
            await media_message.copy(chat_id=chat_id, message_thread_id=topic_id)
            await asyncio.sleep(1)
        except (FileReferenceExpired, FileReferenceInvalid):
            await handle_self_destruct_media(client, media_message, chat_id, topic_id)
        except TopicDeleted:
            topic = await client.create_forum_topic(chat_id, user.first_name)
            topic_id = topic.id
            user_topics[str(user_id)] = topic_id
            update_group_data(chat_id, {"user_topics": user_topics})
            await client.send_message(
                chat_id=chat_id,
                message_thread_id=topic_id,
                text=f"<b>Chat Name:</b> {user.full_name}\n<b>User ID:</b> {user_id}\n<b>Username:</b> @{user.username or 'N/A'}\n<b>Phone No:</b> +{user.phone_number or 'N/A'}",
            )
            await handle_self_destruct_media(client, media_message, chat_id, topic_id)
        except TopicClosed:
            await client.reopen_forum_topic(chat_id=chat_id, topic_id=topic_id)
            await media_message.copy(chat_id=chat_id, message_thread_id=topic_id)
    media_processing_tasks.pop(user_id, None)

async def handle_self_destruct_media(client: Client, message: Message, chat_id: int, topic_id: int):
    try:
        file_path = await message.download()
        if message.photo:
            await client.send_photo(chat_id, file_path, message_thread_id=topic_id)
        elif message.video_note:
            await client.send_video(chat_id, file_path, message_thread_id=topic_id)
        os.remove(file_path)
    except Exception as e:
        print(f"Error handling self-destructing media: {e}")

@Client.on_message(filters.command(["x"], prefix) & filters.me, group=2)
async def send_user_details_to_channel(client: Client, message: Message):
    # Delete the command message immediately
    await message.delete()
    
    try:
        # Get nickname if provided (support multi-word nicknames)
        nickname = " ".join(message.command[1:]) if len(message.command) > 1 else None
        user_id = message.chat.id  # Use current chat's user ID
        
        chat_id = db.get("custom.mlog", "chat")
        channel_id = db.get("custom.mlog", "channel")
        
        if not chat_id:
            return await client.send_message("me", f"<b>No Chat ID is set. Use {prefix}msetchat to set the Chat ID.</b>")
        if not channel_id:
            return await client.send_message("me", f"<b>No Channel ID is set. Use {prefix}msetchannel to set the Channel ID.</b>")
        
        group_data = get_group_data(chat_id)
        user_topics = group_data.get("user_topics", {})
        topic_id = user_topics.get(str(user_id))
        
        user = await client.get_users(user_id)
        
        # Generate topic link only if topic_id exists
        topic_link = None
        if topic_id:
            # Extract group ID without "-100" prefix
            group_id = str(chat_id).lstrip("-100")
            topic_link = f"t.me/c/{group_id}/{topic_id}"
        
        # Build info message, including nickname and topic link only if provided
        info_message = (
            f"<b>Chat Name:</b> {user.full_name}\n"
            f"<b>User ID:</b> {user.id}\n"
            f"<b>Username:</b> @{user.username or 'N/A'}\n"
            f"<b>Phone No:</b> +{user.phone_number or 'N/A'}"
        )
        if nickname:
            info_message += f"\n<b>Nickname:</b> {nickname}"
        if topic_link:
            info_message += f"\n<b>Group Topic Link:</b> {topic_link}"
        
        await client.send_message(channel_id, info_message)
        await client.send_message("me", f"<b>User details sent to channel for user ID {user_id}.</b>")
        
    except ValueError:
        await client.send_message("me", "<b>Invalid user ID</b>")
    except Exception as e:
        await client.send_message("me", f"<b>Error:</b> {str(e)}")



@Client.on_message(filters.command(["msetchannel"], prefix) & filters.me, group=2)
async def set_channel(_, message: Message):
    if len(message.command) < 2: 
        return await message.edit(f"<b>Usage:</b> <code>{prefix}msetchannel [channel_id]</code>")
    try:
        channel_id = message.command[1]
        channel_id = int("-100" + channel_id if not channel_id.startswith("-100") else channel_id)
        db.set("custom.mlog", "channel", channel_id)
        await message.edit(f"<b>Channel ID set to {channel_id}</b>")
    except ValueError:
        await message.edit("<b>Invalid channel ID</b>")

modules_help["mlog"] = {
    "mlog [on/off]": "Enable or disable media logging",
    "msetchat [chat_id]": "Set the chat ID for media logging",
    "msetchannel [channel_id]": "Set the channel ID for sending user details",
    "mrename [user_id]": "Rename a user's topic",
    "x [nickname]*": "Send user details with an optional nickname and group topic link to the specified channel",
}
