import asyncio
import aiohttp
import random
import logging
from pyrogram import Client, filters
from pyrogram.types import Message
from utils.db import db  # Your MongoDB wrapper
from utils.misc import modules_help, prefix  # Assumes you have a prefix defined

# =============================================================================
# Main Database Dictionary Setup
# =============================================================================

# All module data is stored under the main key "custom.ameeff"
MAIN_DB_KEY = "custom.ameeff"

# Load the main db dictionary (or initialize it)
main_db = db.get(MAIN_DB_KEY, "main") or {}
if "tokens" not in main_db:
    main_db["tokens"] = {}  # structure: { chat_id: { "1": token, "2": token, ... } }
if "sent_messages" not in main_db:
    main_db["sent_messages"] = {}  # structure: { chat_id: { "lounge": [user_ids], "chatroom": [chatroom_ids] } }
# Auto module settings: friend request automation always runs if module_enabled is True,
# while lounge and chatroom auto messaging run only if their individual settings are True.
if "auto_settings" not in main_db:
    main_db["auto_settings"] = {
        "module_enabled": False,
        "lounge_enabled": False,
        "chatroom_enabled": False,
        # We also store custom messages for lounge and chatroom here.
        "lounge_message": "Default lounge message",
        "chatroom_message": "Default chatroom message"
    }
db.set(MAIN_DB_KEY, "main", main_db)

# Aliases for easier access
tokens_db = main_db["tokens"]
sent_messages_db = main_db["sent_messages"]
settings = main_db["auto_settings"]

# In-memory tasks for manual automations
friend_req_tasks = {}
lounge_tasks = {}
chatroom_tasks = {}
unsubscribe_tasks = {}

# In-memory tasks for scheduled auto module – now split into three dictionaries:
auto_adding_tasks = {}      # Friend request automation (every 24h)
auto_lounge_tasks = {}      # Lounge messaging automation (every 15m)
auto_chatroom_tasks = {}    # Chatroom messaging automation (every 30m)

# =============================================================================
# Helper Functions
# =============================================================================

async def sleep_random_adder():
    await asyncio.sleep(random.uniform(2, 3))

async def sleep_random_lounge():
    await asyncio.sleep(random.uniform(2, 3))

async def sleep_random_chatroom():
    await asyncio.sleep(random.uniform(2, 3))

async def sleep_random_unsubscribe():
    await asyncio.sleep(random.uniform(2, 3))

def store_sent_message(chat_id: int, msg_type: str, identifier: str):
    """Stores an identifier (user ID for lounge or chatroom ID) to avoid duplicates."""
    chat_key = str(chat_id)
    if chat_key not in sent_messages_db:
        sent_messages_db[chat_key] = {"lounge": [], "chatroom": []}
    if identifier not in sent_messages_db[chat_key][msg_type]:
        sent_messages_db[chat_key][msg_type].append(identifier)
    db.set(MAIN_DB_KEY, "main", main_db)

def is_already_sent(chat_id: int, msg_type: str, identifier: str) -> bool:
    chat_key = str(chat_id)
    if chat_key not in sent_messages_db:
        return False
    return identifier in sent_messages_db[chat_key].get(msg_type, [])

def get_tokens(chat_id: int, token_arg: str = None):
    """
    Returns a list of token dicts for the given chat.
      - If token_arg is "all", returns all tokens.
      - Otherwise, returns the token matching the provided id.
    """
    chat_key = str(chat_id)
    if chat_key not in tokens_db:
        return None
    chat_tokens = tokens_db[chat_key]
    if token_arg is None:
        return None
    token_arg = token_arg.lower()
    if token_arg == "all":
        token_list = []
        for token_id, token_str in chat_tokens.items():
            if token_str:
                token_list.append({"id": token_id, "token": token_str})
        return token_list if token_list else None
    try:
        token_id = str(token_arg)
        token = chat_tokens.get(token_id)
        return [{"id": token_id, "token": token}] if token else None
    except ValueError:
        return None

# =============================================================================
# MEEFF Token Storage Command
# =============================================================================

@Client.on_message(filters.command("ameft", prefixes=prefix) & filters.me)
async def meft_command(client: Client, message: Message):
    args = message.text.strip().split(maxsplit=2)
    chat_key = str(message.chat.id)

    if chat_key not in tokens_db:
        tokens_db[chat_key] = {str(i): None for i in range(1, 11)}

    if len(args) < 2:
        await message.reply_text(f"Usage: {prefix}meft <token> OR {prefix}meft <id> <token>")
        return

    if len(args) == 2:
        # Auto-assign an ID
        token = args[1]
        available_ids = [i for i in range(1, 11) if not tokens_db[chat_key].get(str(i))]
        if not available_ids:
            await message.reply_text("❌ You’ve reached the maximum of 10 tokens.")
            return
        assigned_id = min(available_ids)
        tokens_db[chat_key][str(assigned_id)] = token
        db.set(MAIN_DB_KEY, "main", main_db)
        await message.reply_text(f"✅ Token auto-assigned with ID {assigned_id} saved successfully!")
    else:
        # Manual ID
        try:
            token_id = str(args[1])
            if int(token_id) < 1 or int(token_id) > 10:
                await message.reply_text("❌ Token ID must be between 1 and 10.")
                return
        except ValueError:
            await message.reply_text("❌ Token ID must be a number.")
            return

        token = args[2]
        tokens_db[chat_key][token_id] = token
        db.set(MAIN_DB_KEY, "main", main_db)
        await message.reply_text(f"✅ Token with ID {token_id} saved successfully!")


# =============================================================================
# Friend Request Automation (amef / amefstop)
# =============================================================================

async def fetch_users(token: str, session: aiohttp.ClientSession) -> list:
    url = "https://api.meeff.com/user/explore/v2?lng=-112.0613784790039&unreachableUserIds=&lat=33.437198638916016&locale=en"
    headers = {"meeff-access-token": token, "Connection": "keep-alive"}
    try:
        async with session.get(url, headers=headers) as response:
            if response.status == 429:
                logging.error("Request limit exceeded while fetching users.")
                return None
            if response.status != 200:
                logging.error(f"Error fetching users: {response.status} - {response.reason}")
                return []
            data = await response.json()
            return data.get("users", [])
    except Exception as e:
        logging.error(f"Fetch users failed: {e}")
        return []

async def send_friend_request(user_id: str, token: str, session: aiohttp.ClientSession) -> dict:
    url = f"https://api.meeff.com/user/undoableAnswer/v5/?userId={user_id}&isOkay=1"
    headers = {"meeff-access-token": token, "Connection": "keep-alive"}
    try:
        async with session.get(url, headers=headers) as response:
            data = await response.json()
            if data.get("errorCode") == "LikeExceeded":
                logging.info(f"Daily like limit reached while sending request to {user_id}.")
                return {"error": "daily_limit_exceeded"}
            return data
    except Exception as e:
        logging.error(f"Error sending friend request to {user_id}: {e}")
        return {}


async def automation_friend_req(token_data, client: Client, chat_id: int, message: Message):
    token_id = token_data["id"]
    token = token_data["token"]
    async with aiohttp.ClientSession() as session:
        counter = 0
        try:
            msg = await client.send_message(chat_id, f"✅ [Token ID: {token_id}] Starting friend request automation...")
            while friend_req_tasks.get((chat_id, token)):
                await sleep_random_adder()
                users = await fetch_users(token, session)
                if users is None:
                    await msg.edit(f"⚠️ [Token ID: {token_id}] Request limit exceeded while fetching users. Stopping automation for this token.")
                    return
                if len(users) < 5:
                    await msg.edit(f"❌ [Token ID: {token_id}] Only {len(users)} users found. Stopping automation for this token.")
                    return
                for user in users:
                    user_id = user.get("_id")
                    if not user_id:
                        continue
                    await msg.edit(f"📩 [Token ID: {token_id}] Sending friend request to user ID: {user_id}...")
                    result = await send_friend_request(user_id, token, session)
                    if result.get("error") == "daily_limit_exceeded":
                        await msg.edit(f"🚫 [Token ID: {token_id}] Daily friend request limit reached. Stopping automation for this token.")
                        return
                    counter += 1
                    # Only update progress after a set number of requests or when a major status change occurs
                    if counter % 5 == 0:  # Update progress every 5 users
                        await msg.edit(f"✅ [Token ID: {token_id}] Friend request sent to user ID: {user_id}. Total users added: {counter}")
                    if isinstance(result, dict) and len(result.keys()) > 1:
                        await msg.edit(f"⚠️ [Token ID: {token_id}] Possible restriction detected! Stopping automation for this token.")
                        return
                    await sleep_random_adder()
                await msg.edit(f"✅ [Token ID: {token_id}] Batch completed. Total users added: {counter}. Fetching new users...")
            await msg.edit(f"🛑 [Token ID: {token_id}] Friend request automation stopped. Total users added: {counter}")
        except Exception as e:
            await msg.edit(f"Error in automation loop for Token ID: {token_id}: {e}")
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            await msg.edit(f"🛑 [Token ID: {token_id}] Friend request automation cancelled. Total users added: {counter}")
            raise


@Client.on_message(filters.command("amef", prefixes=prefix) & filters.me)
async def mef_command(client: Client, message: Message):
    args = message.text.strip().split(maxsplit=1)
    token_arg = args[1] if len(args) > 1 else None
    chat_id = message.chat.id
    tokens_data = get_tokens(chat_id, token_arg)
    if not tokens_data:
        await message.reply_text(f"Usage: {prefix}amef <id|all>. Please specify token id or 'all'.")
        return
    if token_arg and token_arg.lower() == "all":
        await message.reply_text("✅ Starting friend request automation for all specified tokens sequentially.")
        pending_token_ids = ", ".join([token_data["id"] for token_data in tokens_data])
        await message.reply_text(f"⏳ Pending token tasks: Token ID {pending_token_ids}")
        for token_data in tokens_data:
            token = token_data["token"]
            key = (chat_id, token)
            if key in friend_req_tasks:
                continue
            task = asyncio.create_task(automation_friend_req(token_data, client, chat_id, message))
            friend_req_tasks[key] = task
            await task
            if friend_req_tasks.get(key) is None:
                del friend_req_tasks[key]
        await message.reply_text("✅ Friend request automation completed for all specified tokens.")
    else:
        for token_data in tokens_data:
            token = token_data["token"]
            key = (chat_id, token)
            if key in friend_req_tasks:
                continue
            task = asyncio.create_task(automation_friend_req(token_data, client, chat_id, message))
            friend_req_tasks[key] = task
        await message.reply_text(f"✅ Started friend request automation for Token ID: {tokens_data[0]['id']}.")

@Client.on_message(filters.command("amefstop", prefixes=prefix) & filters.me)
async def mef_stop_command(client: Client, message: Message):
    args = message.text.strip().split(maxsplit=1)
    token_arg = args[1] if len(args) > 1 else None
    chat_id = message.chat.id
    tokens_data = get_tokens(chat_id, token_arg if token_arg else "all")
    if not tokens_data:
        await message.reply_text("No valid token found to stop automation.")
        return
    stopped = 0
    stopped_ids = []
    for token_data in tokens_data:
        token = token_data["token"]
        key = (chat_id, token)
        task = friend_req_tasks.get(key)
        if task:
            task.cancel()
            del friend_req_tasks[key]
            stopped += 1
            stopped_ids.append(token_data["id"])
    if stopped:
        await message.reply_text(f"🛑 Stopped friend request automation for Token IDs: {', '.join(stopped_ids)}.")
    else:
        await message.reply_text("❌ No active friend request automation found for the given token(s).")

# =============================================================================
# Lounge Automation (amefl / ameflstop)
# =============================================================================

async def fetch_lounge_users(token):
    headers = {
        "User-Agent": "okhttp/4.12.0",
        "Accept-Encoding": "gzip",
        "content-type": "application/json; charset=utf-8",
        "meeff-access-token": token
    }
    params = {"locale": "en"}
    async with aiohttp.ClientSession() as session:
        async with session.get("https://api.meeff.com/lounge/dashboard/v1", params=params, headers=headers) as response:
            if response.status != 200:
                logging.error(f"Failed to fetch lounge users: {response.status}")
                return []
            data = await response.json()
            return data.get("both", [])

async def open_chatroom_lounge(token, user_id):
    headers = {
        "User-Agent": "okhttp/4.12.0",
        "Accept-Encoding": "gzip",
        "content-type": "application/json; charset=utf-8",
        "meeff-access-token": token
    }
    payload = {"waitingRoomId": user_id, "locale": "en"}
    async with aiohttp.ClientSession() as session:
        async with session.post("https://api.meeff.com/chatroom/open/v2", json=payload, headers=headers) as response:
            if response.status != 200:
                logging.error(f"Failed to open chatroom: {response.status}")
                return None
            data = await response.json()
            return data.get("chatRoom", {}).get("_id")

async def send_message_lounge(token, chatroom_id, message_text):
    headers = {
        "User-Agent": "okhttp/4.12.0",
        "Accept-Encoding": "gzip",
        "content-type": "application/json; charset=utf-8",
        "meeff-access-token": token
    }
    payload = {"chatRoomId": chatroom_id, "message": message_text, "locale": "en"}
    async with aiohttp.ClientSession() as session:
        async with session.post("https://api.meeff.com/chat/send/v2", json=payload, headers=headers) as response:
            if response.status != 200:
                logging.error(f"Failed to send message: {response.status}")
                return None
            return await response.json()

async def lounge_automation(token_data, client: Client, chat_id: int, message: Message, custom_message: str):
    token_id = token_data["id"]
    token = token_data["token"]
    sent_count = 0
    msg = await client.send_message(chat_id, f"✅ [Token ID: {token_id}] Starting Lounge automation...")
    try:
        while lounge_tasks.get((chat_id, token)):
            users = await fetch_lounge_users(token)
            if not users:
                await msg.edit(f"⚠️ [Token ID: {token_id}] No users found in the lounge. Stopping automation for this token.")
                return
            for user in users:
                user_id = user["user"].get("_id")
                if not user_id:
                    continue
                if is_already_sent(chat_id, "lounge", user_id):
                    continue
                chatroom_id = await open_chatroom_lounge(token, user_id)
                if chatroom_id:
                    result = await send_message_lounge(token, chatroom_id, custom_message)
                    if result is not None:
                        store_sent_message(chat_id, "lounge", user_id)
                        sent_count += 1
                        await msg.edit(f"📩 [Token ID: {token_id}] Sent message to {user['user'].get('name', 'Unknown')}. Total messages sent: {sent_count}")
                await sleep_random_lounge()
            await msg.edit(f"✅ [Token ID: {token_id}] Batch completed. Total messages sent: {sent_count}. Fetching more...")
        await msg.edit(f"🛑 [Token ID: {token_id}] Lounge automation stopped. Total messages sent: {sent_count}")
    except asyncio.CancelledError:
        await msg.edit(f"🛑 [Token ID: {token_id}] Lounge automation cancelled. Total messages sent: {sent_count}")
        raise
    except Exception as e:
        await msg.edit(f"Error in lounge automation for Token ID: {token_id}: {e}")
        await asyncio.sleep(5)

@Client.on_message(filters.command("amefl", prefixes=prefix) & filters.me)
async def mefl_command(client: Client, message: Message):
    args = message.text.strip().split(maxsplit=2)
    chat_id = message.chat.id
    if len(args) < 3:
        await message.reply_text(f"Usage: {prefix}amefl <id|all> <message>")
        return
    token_arg = args[1]
    custom_message = args[2]
    tokens_data = get_tokens(chat_id, token_arg)
    if not tokens_data:
        await message.reply_text(f"Usage: {prefix}amefl <id|all> <message>. Please specify token id or 'all'.")
        return
    started_count = 0
    if token_arg.lower() == "all":
        await message.reply_text(f"✅ Starting Lounge automation for all specified tokens concurrently with message: {custom_message}")
        for token_data in tokens_data:
            token = token_data["token"]
            key = (chat_id, token)
            if key in lounge_tasks:
                continue
            task = asyncio.create_task(lounge_automation(token_data, client, chat_id, message, custom_message))
            lounge_tasks[key] = task
            started_count += 1
    else:
        for token_data in tokens_data:
            token = token_data["token"]
            key = (chat_id, token)
            if key in lounge_tasks:
                continue
            task = asyncio.create_task(lounge_automation(token_data, client, chat_id, message, custom_message))
            lounge_tasks[key] = task
            started_count += 1
    if started_count:
        await message.reply_text(f"✅ Started Lounge automation for {started_count} token(s) with message: {custom_message}")
    else:
        await message.reply_text("Lounge automation is already running for the specified token(s).")

@Client.on_message(filters.command("ameflstop", prefixes=prefix) & filters.me)
async def mefl_stop_command(client: Client, message: Message):
    args = message.text.strip().split(maxsplit=1)
    token_arg = args[1] if len(args) > 1 else None
    chat_id = message.chat.id
    tokens_data = get_tokens(chat_id, token_arg if token_arg else "all")
    if not tokens_data:
        await message.reply_text("No valid token found to stop automation.")
        return
    stopped = 0
    stopped_ids = []
    for token_data in tokens_data:
        token = token_data["token"]
        key = (chat_id, token)
        task = lounge_tasks.get(key)
        if task:
            task.cancel()
            del lounge_tasks[key]
            stopped += 1
            stopped_ids.append(token_data["id"])
    if stopped:
        await message.reply_text(f"🛑 Stopped Lounge automation for Token IDs: {', '.join(stopped_ids)}.")
    else:
        await message.reply_text("❌ No active Lounge automation found for the given token(s).")

# =============================================================================
# Chatroom Messaging Automation (amefc / amefcstop)
# =============================================================================

async def fetch_chatrooms_chatroom(token: str, from_date=None):
    url = "https://api.meeff.com/chatroom/dashboard/v1"
    headers = {
        "meeff-access-token": token,
        "User-Agent": "okhttp/4.12.0",
        "Accept-Encoding": "gzip",
        "content-type": "application/json; charset=utf-8"
    }
    params = {"locale": "en"}
    if from_date:
        params["fromDate"] = from_date
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params, headers=headers) as response:
            if response.status != 200:
                return None, None
            data = await response.json()
            return data.get("rooms", []), data.get("next")

async def fetch_more_chatrooms_chatroom(token: str, from_date):
    url = "https://api.meeff.com/chatroom/more/v1"
    headers = {
        "meeff-access-token": token,
        "User-Agent": "okhttp/4.12.0",
        "Accept-Encoding": "gzip",
        "content-type": "application/json; charset=utf-8"
    }
    payload = {"fromDate": from_date, "locale": "en"}
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, headers=headers) as response:
            if response.status != 200:
                return None, None
            data = await response.json()
            return data.get("rooms", []), data.get("next")

async def send_message_chatroom(token: str, chatroom_id: str, custom_message: str):
    url = "https://api.meeff.com/chat/send/v2"
    headers = {
        "meeff-access-token": token,
        "User-Agent": "okhttp/4.12.0",
        "Accept-Encoding": "gzip",
        "content-type": "application/json; charset=utf-8"
    }
    payload = {"chatRoomId": chatroom_id, "message": custom_message, "locale": "en"}
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, headers=headers) as response:
            return await response.json() if response.status == 200 else None

async def chatroom_automation(token_data, client: Client, chat_id: int, message: Message, custom_message: str):
    token_id = token_data["id"]
    token = token_data["token"]
    counter = 0
    msg = await client.send_message(chat_id, f"✅ [Token ID: {token_id}] Starting chatroom messaging automation...")
    from_date = None
    try:
        while chatroom_tasks.get((chat_id, token)):
            await sleep_random_chatroom()
            if from_date is None:
                chatrooms, next_from_date = await fetch_chatrooms_chatroom(token)
            else:
                chatrooms, next_from_date = await fetch_more_chatrooms_chatroom(token, from_date)
            if not chatrooms:
                await msg.edit(f"⚠️ [Token ID: {token_id}] No more chatrooms found. Stopping automation for this token.")
                return
            for chatroom in chatrooms:
                if not chatroom_tasks.get((chat_id, token)):
                    return
                chatroom_id = chatroom["_id"]
                if is_already_sent(chat_id, "chatroom", chatroom_id):
                    continue
                await send_message_chatroom(token, chatroom_id, custom_message)
                store_sent_message(chat_id, "chatroom", chatroom_id)
                counter += 1
                await msg.edit(f"✅ [Token ID: {token_id}] Sent message to chatroom. Total chatrooms messaged: {counter}")
                await sleep_random_chatroom()
            if not next_from_date:
                await msg.edit(f"⚠️ [Token ID: {token_id}] No further chatrooms available. Stopping automation for this token.")
                return
            from_date = next_from_date
        await msg.edit(f"✅ [Token ID: {token_id}] Chatroom messaging automation completed. Total chatrooms messaged: {counter}")
    except asyncio.CancelledError:
        await msg.edit(f"🛑 [Token ID: {token_id}] Chatroom messaging automation stopped. Total chatrooms messaged: {counter}")
        raise
    except Exception as e:
        await msg.edit(f"Error in chatroom messaging automation for Token ID: {token_id}: {e}")
        await asyncio.sleep(5)

@Client.on_message(filters.command("amefc", prefixes=prefix) & filters.me)
async def mefc_command(client: Client, message: Message):
    args = message.text.strip().split(maxsplit=2)
    chat_id = message.chat.id
    if len(args) < 3:
        await message.reply_text(f"Usage: {prefix}amefc <id|all> <message>")
        return
    token_arg = args[1]
    custom_message = args[2]
    tokens_data = get_tokens(chat_id, token_arg)
    if not tokens_data:
        await message.reply_text(f"Usage: {prefix}amefc <id|all> <message>. Please specify token id or 'all'.")
        return
    started_count = 0
    if token_arg.lower() == "all":
        await message.reply_text(f"✅ Starting chatroom messaging automation for all specified tokens concurrently with message: {custom_message}")
        for token_data in tokens_data:
            token = token_data["token"]
            key = (chat_id, token)
            if key in chatroom_tasks:
                continue
            task = asyncio.create_task(chatroom_automation(token_data, client, chat_id, message, custom_message))
            chatroom_tasks[key] = task
            started_count += 1
    else:
        for token_data in tokens_data:
            token = token_data["token"]
            key = (chat_id, token)
            if key in chatroom_tasks:
                continue
            task = asyncio.create_task(chatroom_automation(token_data, client, chat_id, message, custom_message))
            chatroom_tasks[key] = task
            started_count += 1
    if started_count:
        await message.reply_text(f"✅ Started chatroom messaging automation for {started_count} token(s) with message: {custom_message}")
    else:
        await message.reply_text("Chatroom messaging automation is already running for the specified token(s).")

@Client.on_message(filters.command("amefcstop", prefixes=prefix) & filters.me)
async def mefc_stop_command(client: Client, message: Message):
    args = message.text.strip().split(maxsplit=1)
    token_arg = args[1] if len(args) > 1 else None
    chat_id = message.chat.id
    tokens_data = get_tokens(chat_id, token_arg if token_arg else "all")
    if not tokens_data:
        await message.reply_text("No valid token found to stop automation.")
        return
    stopped = 0
    stopped_ids = []
    for token_data in tokens_data:
        token = token_data["token"]
        key = (chat_id, token)
        task = chatroom_tasks.get(key)
        if task:
            task.cancel()
            del chatroom_tasks[key]
            stopped += 1
            stopped_ids.append(token_data["id"])
    if stopped:
        await message.reply_text(f"🛑 Stopped chatroom messaging automation for Token IDs: {', '.join(stopped_ids)}.")
    else:
        await message.reply_text("❌ No active chatroom messaging automation found for the given token(s).")

# =============================================================================
# Unsubscribe Automation (amefr / amefrstop)
# =============================================================================

UNSUBSCRIBE_URL = "https://api.meeff.com/chatroom/unsubscribe/v1"
CHATROOM_URL = "https://api.meeff.com/chatroom/dashboard/v1"
MORE_CHATROOMS_URL = "https://api.meeff.com/chatroom/more/v1"
UNSUB_HEADERS = {
    "User-Agent": "okhttp/4.12.0",
    "Accept-Encoding": "gzip",
    "content-type": "application/json; charset=utf-8"
}

async def fetch_chatrooms_unsub(token, from_date=None):
    headers = UNSUB_HEADERS.copy()
    headers["meeff-access-token"] = token
    params = {"locale": "en"}
    if from_date:
        params["fromDate"] = from_date
    async with aiohttp.ClientSession() as session:
        async with session.get(CHATROOM_URL, params=params, headers=headers) as response:
            if response.status != 200:
                return None, None
            data = await response.json()
            return data.get("rooms", []), data.get("next")

async def fetch_more_chatrooms_unsub(token, from_date):
    headers = UNSUB_HEADERS.copy()
    headers["meeff-access-token"] = token
    payload = {"fromDate": from_date, "locale": "en"}
    async with aiohttp.ClientSession() as session:
        async with session.post(MORE_CHATROOMS_URL, json=payload, headers=headers) as response:
            if response.status != 200:
                return None, None
            data = await response.json()
            return data.get("rooms", []), data.get("next")

async def unsubscribe_chatroom(token, chatroom_id):
    headers = UNSUB_HEADERS.copy()
    headers["meeff-access-token"] = token
    payload = {"chatRoomId": chatroom_id, "locale": "en"}
    async with aiohttp.ClientSession() as session:
        async with session.post(UNSUBSCRIBE_URL, json=payload, headers=headers) as response:
            return await response.json() if response.status == 200 else None

async def unsubscribe_automation(token_data, client: Client, chat_id: int, message: Message):
    token_id = token_data["id"]
    token = token_data["token"]
    total_unsubscribed = 0
    from_date = None
    msg = await client.send_message(chat_id, f"✅ [Token ID: {token_id}] Starting unsubscribe automation...")
    try:
        while unsubscribe_tasks.get((chat_id, token)):
            if from_date is None:
                chatrooms, next_from_date = await fetch_chatrooms_unsub(token)
            else:
                chatrooms, next_from_date = await fetch_more_chatrooms_unsub(token, from_date)
            if not chatrooms:
                await msg.edit(f"⚠️ [Token ID: {token_id}] No more chatrooms found. Stopping automation for this token.")
                return
            for chatroom in chatrooms:
                if not unsubscribe_tasks.get((chat_id, token)):
                    return
                chatroom_id = chatroom["_id"]
                await unsubscribe_chatroom(token, chatroom_id)
                total_unsubscribed += 1
                await msg.edit(f"✅ [Token ID: {token_id}] Unsubscribed from chatroom. Total chatrooms unsubscribed: {total_unsubscribed}")
                await sleep_random_unsubscribe()
            if not next_from_date:
                await msg.edit(f"⚠️ [Token ID: {token_id}] No further chatrooms available. Stopping automation for this token.")
                return
            from_date = next_from_date
        await msg.edit(f"✅ [Token ID: {token_id}] Unsubscribe automation completed. Total chatrooms unsubscribed: {total_unsubscribed}")
    except asyncio.CancelledError:
        await msg.edit(f"🛑 [Token ID: {token_id}] Unsubscribe automation stopped. Total chatrooms unsubscribed: {total_unsubscribed}")
        raise
    except Exception as e:
        await msg.edit(f"Error in unsubscribe automation for Token ID: {token_id}: {e}")
        await asyncio.sleep(5)

@Client.on_message(filters.command("amefr", prefixes=prefix) & filters.me)
async def mefr_command(client: Client, message: Message):
    args = message.text.strip().split(maxsplit=1)
    token_arg = args[1] if len(args) > 1 else None
    chat_id = message.chat.id
    tokens_data = get_tokens(chat_id, token_arg if token_arg else "all")
    if not tokens_data:
        await message.reply_text(f"Usage: {prefix}amefr <id|all>. Please specify token id or 'all'.")
        return
    started_count = 0
    for token_data in tokens_data:
        token = token_data["token"]
        key = (chat_id, token)
        if key in unsubscribe_tasks:
            continue
        task = asyncio.create_task(unsubscribe_automation(token_data, client, chat_id, message))
        unsubscribe_tasks[key] = task
        started_count += 1
    if started_count:
        await message.reply_text(f"✅ Starting unsubscribe automation for {started_count} token(s) concurrently.")
    else:
        await message.reply_text("Unsubscribe automation is already running for the specified token(s).")

@Client.on_message(filters.command("amefrstop", prefixes=prefix) & filters.me)
async def mefr_stop_command(client: Client, message: Message):
    args = message.text.strip().split(maxsplit=1)
    token_arg = args[1] if len(args) > 1 else None
    chat_id = message.chat.id
    tokens_data = get_tokens(chat_id, token_arg if token_arg else "all")
    if not tokens_data:
        await message.reply_text("No valid token found to stop automation.")
        return
    stopped = 0
    stopped_ids = []
    for token_data in tokens_data:
        token = token_data["token"]
        key = (chat_id, token)
        task = unsubscribe_tasks.get(key)
        if task:
            task.cancel()
            del unsubscribe_tasks[key]
            stopped += 1
            stopped_ids.append(token_data["id"])
    if stopped:
        await message.reply_text(f"🛑 Stopped unsubscribe automation for Token IDs: {', '.join(stopped_ids)}.")
    else:
        await message.reply_text("❌ No active unsubscribe automation found for the given token(s).")

# =============================================================================
# Auto Module: Scheduled Automation for Immediate Functions
# =============================================================================

async def auto_adding_immediate(client: Client, chat_id: str, token: str):
    await client.send_message(chat_id, "📢 Auto Adding: Running friend request automation.")

async def auto_lounge_immediate(client: Client, chat_id: str, token: str, message_text: str):
    await client.send_message(chat_id, f"📢 Auto Lounge: {message_text}")

async def auto_chatroom_immediate(client: Client, chat_id: str, token: str, message_text: str):
    await client.send_message(chat_id, f"📢 Auto Chatroom: {message_text}")

async def scheduled_auto_adding(client: Client, chat_id: str, token: str):
    while settings.get("module_enabled"):
        token_data = {"id": "auto", "token": token}
        key = (chat_id, token)
        if key not in friend_req_tasks or friend_req_tasks[key].done():
            friend_req_tasks[key] = asyncio.create_task(
                automation_friend_req(token_data, client, chat_id, None)
            )
        await asyncio.sleep(86400)  # 24 hours

async def scheduled_auto_lounge(client: Client, chat_id: str, token: str):
    while settings.get("module_enabled") and settings.get("lounge_enabled"):
        msg_text = settings.get("lounge_message", "Default lounge message")
        token_data = {"id": "auto", "token": token}
        key = (chat_id, token)
        if key not in lounge_tasks or lounge_tasks[key].done():
            lounge_tasks[key] = asyncio.create_task(
                lounge_automation(token_data, client, chat_id, None, msg_text)
            )
        await asyncio.sleep(900)  # 15 minutes

async def scheduled_auto_chatroom(client: Client, chat_id: str, token: str):
    while settings.get("module_enabled") and settings.get("chatroom_enabled"):
        msg_text = settings.get("chatroom_message", "Default chatroom message")
        token_data = {"id": "auto", "token": token}
        key = (chat_id, token)
        if key not in chatroom_tasks or chatroom_tasks[key].done():
            chatroom_tasks[key] = asyncio.create_task(
                chatroom_automation(token_data, client, chat_id, None, msg_text)
            )
        await asyncio.sleep(1800)  # 30 minutes


# =============================================================================
# /ameeff Command: on | off | lounge <message>| chatroom <message>
# =============================================================================

@Client.on_message(filters.command("ameeff", prefixes=prefix) & filters.me)
async def ameeff_command(client: Client, message: Message):
    args = message.text.strip().split(maxsplit=2)
    if len(args) < 2:
        await message.reply_text(f"Usage: {prefix}ameeff on|off|lounge <message>|chatroom <message>")
        return

    subcommand = args[1].lower()
    chat_key = str(message.chat.id)

    # Retrieve all available tokens
    tokens_list = get_tokens(message.chat.id, "all")
    if not tokens_list:
        await message.reply_text(f"⚠️ No tokens found. Use {prefix}meft <id> <token> to set one.")
        return

    if subcommand == "on":
        # Enable all three automations.
        settings["module_enabled"] = True
        settings["lounge_enabled"] = True
        settings["chatroom_enabled"] = True
        db.set(MAIN_DB_KEY, "main", main_db)
        # For each token, start friend request, lounge, and chatroom scheduled tasks.
        for token_data in tokens_list:
            token = token_data["token"]
            key = (chat_key, token)
            if key not in auto_adding_tasks or auto_adding_tasks[key].done():
                auto_adding_tasks[key] = asyncio.create_task(scheduled_auto_adding(client, chat_key, token))
            if key not in auto_lounge_tasks or auto_lounge_tasks[key].done():
                auto_lounge_tasks[key] = asyncio.create_task(scheduled_auto_lounge(client, chat_key, token))
            if key not in auto_chatroom_tasks or auto_chatroom_tasks[key].done():
                auto_chatroom_tasks[key] = asyncio.create_task(scheduled_auto_chatroom(client, chat_key, token))
        await message.reply_text("✅ Auto Module enabled for all stored tokens.\n• Friend request automation enabled (runs every 24 hours).\n• Lounge messaging enabled (runs every 15 minutes).\n• Chatroom messaging enabled (runs every 30 minutes).")

    elif subcommand == "off":
        settings["module_enabled"] = False
        settings["lounge_enabled"] = False
        settings["chatroom_enabled"] = False
        db.set(MAIN_DB_KEY, "main", main_db)
        # Cancel all scheduled tasks.
        for task in list(auto_adding_tasks.values()):
            task.cancel()
        for task in list(auto_lounge_tasks.values()):
            task.cancel()
        for task in list(auto_chatroom_tasks.values()):
            task.cancel()
        auto_adding_tasks.clear()
        auto_lounge_tasks.clear()
        auto_chatroom_tasks.clear()
        await message.reply_text("🛑 Auto Module disabled. All scheduled tasks stopped.")

    elif subcommand == "lounge":
        if len(args) < 3:
            await message.reply_text("❌ Please provide a custom lounge message.")
            return
        custom_message = args[2]
        settings["lounge_message"] = custom_message
        settings["lounge_enabled"] = True
        db.set(MAIN_DB_KEY, "main", main_db)
        await message.reply_text(f"✅ Lounge message updated to: {custom_message}")
        # (Scheduled tasks already read the new message on next iteration.)

    elif subcommand == "chatroom":
        if len(args) < 3:
            await message.reply_text("❌ Please provide a custom chatroom message.")
            return
        custom_message = args[2]
        settings["chatroom_message"] = custom_message
        settings["chatroom_enabled"] = True
        db.set(MAIN_DB_KEY, "main", main_db)
        await message.reply_text(f"✅ Chatroom message updated to: {custom_message}")
        # (Scheduled tasks already read the new message on next iteration.)

    else:
        await message.reply_text(f"Usage: {prefix}ameeff on|off|lounge <message>|chatroom <message>")

@Client.on_message(filters.command("mr", prefixes=prefix) & filters.me)
async def manual_request_command(client: Client, message: Message):
    args = message.text.strip().split(maxsplit=2)
    if len(args) < 3:
        await message.reply_text(f"Usage: {prefix}mr <token_id> <user_id>")
        return

    token_id = args[1]
    user_id = args[2]
    chat_id = message.chat.id

    tokens_data = get_tokens(chat_id, token_id)
    if not tokens_data:
        await message.reply_text("❌ Invalid token ID.")
        return

    token = tokens_data[0]["token"]

    async with aiohttp.ClientSession() as session:
        await message.reply_text(f"📨 Sending friend request to user ID: {user_id}...")
        result = await send_friend_request(user_id, token, session)
        if result.get("error") == "rate_limit_exceeded":
            await message.reply_text("⚠️ Rate limit exceeded. Try again later.")
        elif isinstance(result, dict) and len(result.keys()) > 1:
            await message.reply_text(f"⚠️ Possible restriction: {result}")
        else:
            await message.reply_text(f"✅ Friend request sent to {user_id} successfully.")


from pymongo import MongoClient
from utils.db import db  # Old bot database
from pyrogram import Client, filters

# Connect to meeff_bot database
MONGO_URI = "mongodb+srv://irexanon:xUf7PCf9cvMHy8g6@rexdb.d9rwo.mongodb.net/?retryWrites=true&w=majority&appName=RexDB"
client2 = MongoClient(MONGO_URI)
meeff_bot_db = client2["meeff_bot"]

@Client.on_message(filters.command("mc", prefixes=prefix) & filters.me)
async def migrate_entire_sent_messages(client: Client, message):
    try:
        # 1) Read old data from nick.custom.ameeff → main
        old = db.get("custom.ameeff", "main") or {}
        sent = old.get("sent_messages", {})
        if not sent:
            return await message.reply_text("❌ No sent_messages found in custom.ameeff.")

        # 2) Now we have all old data in `sent`

        # 3) Target new user collection
        user_collection = meeff_bot_db["user_7996471035"]

        # 4) Fetch existing 'sent_records' document
        existing_doc = user_collection.find_one({"type": "sent_records"})
        data = existing_doc.get("data", {}) if existing_doc else {}

        # 5) Merge all lounge and chatroom data from full sent
        lounge_total = []
        chatroom_total = []

        for record in sent.values():
            lounge_total.extend(record.get("lounge", []))
            chatroom_total.extend(record.get("chatroom", []))

        # Merge without duplicates
        data["lounge"] = list(set(data.get("lounge", []) + lounge_total))
        data["chatroom"] = list(set(data.get("chatroom", []) + chatroom_total))

        # 6) Update in the new collection
        user_collection.update_one(
            {"type": "sent_records"},
            {"$set": {"data": data}},
            upsert=True
        )

        await message.reply_text("✅ Successfully migrated full lounge/chatroom into `user_7405203657`.")

    except Exception as e:
        await message.reply_text(f"❌ Migration error: {e}")


# =============================================================================
# Modules Help
# =============================================================================

modules_help["ameeff"] = {
    "ameft <id> <token>": "Stores a MEEFF token with a specific ID (1-5).",
    "ameft <token>": "Stores a MEEFF token, auto-assigning an ID.",
    "amef <id|all>": "Starts friend request automation for a token ID or all tokens.",
    "amefstop <id|all>": "Stops friend request automation for a token ID or all tokens.",
    "amefl <id|all> <message>": "Starts lounge messaging automation for a token ID or all tokens.",
    "ameflstop <id|all>": "Stops lounge automation for a token ID or all tokens.",
    "amefc <id|all> <message>": "Starts chatroom messaging automation for a token ID or all tokens.",
    "amefcstop <id|all>": "Stops chatroom messaging automation for a token ID or all tokens.",
    "amefr <id|all>": "Starts chatroom unsubscribe automation for a token ID or all tokens.",
    "amefrstop <id|all>": "Stops chatroom unsubscribe automation for a token ID or all tokens.",
    "ameeff on": "Enables auto module automation for friend requests, lounge and chatroom messaging.",
    "ameeff off": "Disables all auto module automation.",
    "ameeff lounge <message>": "Updates the lounge message used in auto lounge messaging.",
    "ameeff chatroom <message>": "Updates the chatroom message used in auto chatroom messaging."
}
