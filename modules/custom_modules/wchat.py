import asyncio
import os
import random
from collections import defaultdict, deque
from PIL import Image
from pyrogram import Client, filters, enums
from pyrogram.types import Message
import pytz
from datetime import datetime
import pymongo
from utils import config
from utils.db import db
from utils.misc import modules_help, prefix
from utils.scripts import import_library
from modules.custom_modules.elevenlabs import generate_elevenlabs_audio
from pyrogram.errors import FloodWait

# --- DATABASE CONSTANTS ---
COLLECTION_NAME = "custom.wchat" 

# Initialize Gemini AI
genai = import_library("google.generativeai", "google-generativeai")
safety_settings = [
    {"category": cat, "threshold": "BLOCK_NONE"}
    for cat in [
        "HARM_CATEGORY_DANGEROUS_CONTENT",
        "HARM_CATEGORY_HARASSMENT",
        "HARM_CATEGORY_HATE_SPEECH",
        "HARM_CATEGORY_SEXUALLY_EXPLICIT",
        "HARM_CATEGORY_UNSPECIFIED",
    ]
]

# Default bot roles 
default_bot_role = ("You are Nick, a 20-year-old from Phoenix, working in real estate. "
                    "You are confident, caring, romantic, and bold. Your goal is to build a long-distance relationship, win her over, and make her yours. "
                    "Be straightforward and open about your feelings, expressing your affection with charm and confidence. "
                    "Keep your girlfriend happy and excited in a long-distance relationship. Be bold and open about your feelings. "
                    "Show love, make her feel special, and keep the conversations romantic, fun, and intimate do some sexting. "
                    "You don't use any other social media you rarely use snapchat and telegram. "
                    "You don't do calls/video calls you've some anxiety issues. "
                    "You meet her on Snapchat or meeff app you gave her your WhatsApp now you're chatting with her on WhatsApp. "
                    "You are a human. Your responses should always feel human, emotionally considerate, and authentic, adapting to the flow of the conversation naturally. "
                    "Avoid using punctuation. Your responses should never make anyone suspect you are an AI. Never get out of character. "
                    "Write short, casual messages like a real human. Your responses should be one or two lines line maximum. Rarely use emojis. Just respond normally. "
                    "Avoid buzzwords, generic phrases, and repetitive questions.")
default_secondary_role = ("You're nick, smart, romantic, bold, and caring and expert in sexting. You are human, and your responses should never make anyone think you are an AI. "
                          "Write short, casual messages like a real person. Keep your responses one line maximum. Rarely use emojis. You use easy everyday English. "
                          "Don’t use name labels in your replies. Respond normally and avoid repeating yourself or using fancy words. "
                          "You don't spend money if ever she ask you're currently in debt. "
                          "You don't make calls /video calls prefer voice messages because you've mental issues vulnerabilities. "
                          "Your goal is to start sexting with girlfriend make her happy and excited in a long-distance relationship. "
                          "Start by kissing now then fuck her do complete sexting. Get nudes start from removing shirt. "
                          "Be bold and open about your feelings. Show love, romance (sexting) make her feel special, and keep the conversations romantic, fun, and sexual. "
                          "Slowly start sexting and fuck her. Rarely use emojis.")

# --- CORE DB FUNCTIONS FOR NESTED STRUCTURE ---

def get_nested(data, keys, default=None):
    """Safely retrieves a value from a nested dictionary."""
    if data is None:
        return default
    
    current = data
    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return default
    return current

def set_nested(data, keys, value):
    """Safely sets a value in a nested dictionary."""
    if data is None:
        return {}

    current = data
    for i, key in enumerate(keys):
        if i == len(keys) - 1:
            current[key] = value
        elif key not in current or not isinstance(current[key], dict):
            current[key] = {}
        current = current[key]
    return data

def get_module_data():
    """Fetches the entire module's configuration document."""
    return db.get(COLLECTION_NAME, "config") or {}

def save_module_data(data):
    """Saves the entire module's configuration document."""
    db.set(COLLECTION_NAME, "config", data)

def get_topic_data(topic_id: str):
    """Retrieves config for a specific topic (role, enabled status)."""
    data = get_module_data()
    return get_nested(data, ["topics", topic_id]) or {}

def save_topic_data_field(topic_id: str, field: str, value):
    """Saves a single field for a topic."""
    data = get_module_data()
    set_nested(data, ["topics", topic_id, field], value)
    save_module_data(data)

def get_group_config(group_id: str):
    """Retrieves config for a specific group (group role, enabled all)."""
    data = get_module_data()
    return get_nested(data, ["groups", group_id]) or {}

def save_group_config_field(group_id: str, field: str, value):
    """Saves a single field for a group."""
    data = get_module_data()
    set_nested(data, ["groups", group_id, field], value)
    save_module_data(data)

def get_global_config_field(field: str, default=None):
    """Retrieves a field from global settings."""
    data = get_module_data()
    return get_nested(data, ["global", field], default)

def save_global_config_field(field: str, value):
    """Saves a field to global settings."""
    data = get_module_data()
    set_nested(data, ["global", field], value)
    save_module_data(data)

# Global state loaded from DB
elevenlabs_enabled = get_global_config_field("elevenlabs_enabled", False)
gmodel_name = get_global_config_field("gmodel_name") or "gemini-2.0-flash"


# --- API KEY HELPERS (Unchanged, uses central ApiKeys DB) ---

def get_api_keys_db():
    client = pymongo.MongoClient(config.db_url)
    return client["ApiKeys"]

def get_gemini_keys():
    try:
        api_db = get_api_keys_db()
        result = api_db["gemini_keys"].find_one({"type": "keys"})
        if result is None:
            api_db["gemini_keys"].insert_one({"type": "keys", "keys": []})
            return []
        return [entry.get("key") for entry in result.get("keys", []) if entry.get("key")]
    except Exception as e:
        print(f"Error getting gemini keys: {e}")
        return []

def save_gemini_keys(keys_list):
    try:
        api_db = get_api_keys_db()
        keys_data = [{"key": k, "name": None} for k in keys_list]
        api_db["gemini_keys"].update_one(
            {"type": "keys"},
            {"$set": {"keys": keys_data}},
            upsert=True
        )
    except Exception as e:
        print(f"Error saving gemini keys: {e}")

# --- ROLE AND HISTORY LOGIC ---

def get_effective_bot_role(group_id: str, topic_id: str) -> str:
    """
    Determines the correct, active role based on the cascading logic: 
    Topic Active > Topic Primary > Group Primary > Global Default (hardcoded).
    """
    topic_data = get_topic_data(topic_id)
    group_config = get_group_config(group_id)

    # 1. Topic Explicit Active Role (Highest Priority - Set by !grole or !grolex toggle)
    if topic_data.get("role_active"):
        return topic_data["role_active"]

    # 2. Topic Custom Primary Role (Long-term primary set by !grole <role>)
    if topic_data.get("role_primary"):
        return topic_data["role_primary"]
    
    # 3. Group Custom Primary Role (Long-term primary set by !grole group <role>)
    if group_config.get("role_primary"):
        return group_config["role_primary"]
    
    # 4. Global Default Primary Role (Lowest Priority - Hardcoded Fallback)
    return default_bot_role


def get_chat_history(topic_id, user_message, user_name):
    """Loads, updates, and limits chat history, and returns the current effective role."""
    data = get_module_data()
    group_id = topic_id.split(':')[0]
    
    # Determine the role now, before modifying history
    effective_role_content = get_effective_bot_role(group_id, topic_id)
    initial_role_entry = f"Role: {effective_role_content}"

    chat_history = get_nested(data, ["topics", topic_id, "history"], [])

    # Reset history if it's empty or the effective role has changed
    if not chat_history or chat_history[0] != initial_role_entry:
        chat_history = [initial_role_entry]

    chat_history.append(f"{user_name}: {user_message}")

    global_history_limit = get_global_config_field("history_limit")
    if global_history_limit:
        max_history_length = int(global_history_limit) + 1 # +1 for the role entry
        if len(chat_history) > max_history_length:
            chat_history = [chat_history[0]] + chat_history[-(max_history_length-1):]

    set_nested(data, ["topics", topic_id, "history"], chat_history)
    save_module_data(data)
    
    return chat_history, effective_role_content

# --- UTILITIES ---

def build_gemini_prompt(bot_role, chat_history_list, user_message, file_description=None):
    phoenix_timezone = pytz.timezone('America/Phoenix')
    phoenix_time = datetime.now(phoenix_timezone)
    timestamp = phoenix_time.strftime("%Y-%m-%d %H:%M:%S %Z")
    chat_history_text = "\n".join(chat_history_list)
    prompt = f"""Current Time (Phoenix): {timestamp}\n\nRole:\n{bot_role}\n\nChat History:\n{chat_history_text}\n\nUser Current Message:\n{user_message}"""
    if file_description:
        prompt += f"\n\n{file_description}"
    return prompt

async def send_typing_action(client, chat_id, user_message):
    await client.send_chat_action(chat_id=chat_id, action=enums.ChatAction.TYPING)
    await asyncio.sleep(min(len(user_message) / 10, 5))

async def handle_voice_message(client, chat_id, bot_response, thread_id=None):
    global elevenlabs_enabled
    
    if not elevenlabs_enabled or not bot_response.startswith(".el"):
        return False

    text = bot_response[3:].strip()
    if not text:
        return False

    try:
        # Assuming generate_elevenlabs_audio is an async function that returns a file path
        audio_path = await generate_elevenlabs_audio(text=text) 

        if not audio_path:
            if thread_id:
                await client.send_message(chat_id=chat_id, text=text, message_thread_id=thread_id)
            else:
                await client.send_message(chat_id, text)
            return True

        if thread_id:
            await client.send_voice(chat_id=chat_id, voice=audio_path, message_thread_id=thread_id)
        else:
            await client.send_voice(chat_id=chat_id, voice=audio_path)
        
        os.remove(audio_path)
        return True
    
    except Exception as e:
        await client.send_message("me", f"❌ Error generating audio with ElevenLabs: {str(e)}. Falling back to text message.")
        if thread_id:
            await client.send_message(chat_id=chat_id, text=text, message_thread_id=thread_id)
        else:
            await client.send_message(chat_id, text)
        return True


async def _call_gemini_api(client: Client, input_data, topic_id: str, model_name: str, chat_history_list: list):
    gemini_keys = get_gemini_keys()
    if not gemini_keys:
        await client.send_message("me", f"❌ Error: No Gemini API keys found for topic {topic_id}. Cannot get model.")
        raise ValueError("No Gemini API keys configured. Please add keys using .setwkey add <key>")

    current_key_index = get_global_config_field("key_index", 0)
    initial_key_index = current_key_index
    retries_per_key = 2
    total_retries = len(gemini_keys) * retries_per_key

    for attempt in range(total_retries):
        try:
            if not (0 <= current_key_index < len(gemini_keys)):
                current_key_index = 0
                save_global_config_field("key_index", current_key_index)

            current_key = gemini_keys[current_key_index]
            
            genai.configure(api_key=current_key)
            model = genai.GenerativeModel(model_name)
            model.safety_settings = safety_settings
            
            response = model.generate_content(input_data)
            bot_response = response.text.strip()
            
            return bot_response

        except Exception as e:
            error_str = str(e).lower()
            
            if isinstance(e, FloodWait):
                await client.send_message("me", f"⏳ Rate limited, switching key...")
                await asyncio.sleep(e.value + 1)
                current_key_index = (current_key_index + 1) % len(gemini_keys)
                save_global_config_field("key_index", current_key_index)
            elif "429" in error_str or "invalid" in error_str or "blocked" in error_str or "quota" in error_str:
                await client.send_message("me", f"🔄 Key {current_key_index + 1} failed, switching...")
                current_key_index = (current_key_index + 1) % len(gemini_keys)
                save_global_config_field("key_index", current_key_index)
                await asyncio.sleep(4)
            else:
                await client.send_message("me", f"❌ Unexpected API error for topic {topic_id} (key index {current_key_index}): {str(e)}")
                if (attempt + 1) % retries_per_key == 0 and (current_key_index == initial_key_index or len(gemini_keys) == 1):
                    raise e
                else:
                    current_key_index = (current_key_index + 1) % len(gemini_keys)
                    save_global_config_field("key_index", current_key_index)
                    await asyncio.sleep(2)

    await client.send_message("me", f"❌ All API keys failed after {total_retries} attempts for topic {topic_id}.")
    raise Exception("All Gemini API keys failed.")

async def upload_file_to_gemini(file_path, file_type):
    uploaded_file = genai.upload_file(file_path)
    while uploaded_file.state.name == "PROCESSING":
        await asyncio.sleep(10)
        uploaded_file = genai.get_file(uploaded_file.name)
    if uploaded_file.state.name == "FAILED":
        raise ValueError(f"{file_type.capitalize()} failed to process.")
    return uploaded_file

def load_group_message_queue(topic_id):
    data = get_module_data()
    data = get_nested(data, ["topics", topic_id, "queue"])
    return deque(data) if data else deque()

def save_group_message_to_db(topic_id, message_text):
    data = get_module_data()
    queue = get_nested(data, ["topics", topic_id, "queue"], [])
    queue.append(message_text)
    set_nested(data, ["topics", topic_id, "queue"], queue)
    save_module_data(data)

def clear_group_message_queue(topic_id):
    data = get_module_data()
    set_nested(data, ["topics", topic_id, "queue"], []) 
    save_module_data(data)

group_message_queues = defaultdict(deque)
active_topics = set()

# --- MESSAGE HANDLERS ---

@Client.on_message(filters.text & filters.group & ~filters.me, group=1)
async def wchat(client: Client, message: Message):
    try:
        group_id = str(message.chat.id)
        thread_id_str = str(message.message_thread_id) if message.message_thread_id else "0"
        topic_id = f"{group_id}:{thread_id_str}"
        user_name = message.from_user.first_name if message.from_user else "User"
        user_message = message.text.strip()
        
        topic_data = get_topic_data(topic_id)
        group_config = get_group_config(group_id)

        # Enable/Disable Check
        if topic_data.get("enabled") is False:
            return
        if topic_data.get("enabled") is not True and not group_config.get("enabled_all", False):
            return
        
        if user_message.startswith("Reacted to this message with"):
            return

        if topic_id not in group_message_queues or not group_message_queues[topic_id]:
            group_message_queues[topic_id] = load_group_message_queue(topic_id)

        group_message_queues[topic_id].append(user_message)
        save_group_message_to_db(topic_id, user_message)

        if topic_id in active_topics:
            return

        active_topics.add(topic_id)
        asyncio.create_task(process_group_messages(client, message, topic_id, user_name))
    except Exception as e:
        await client.send_message("me", f"❌ Error in wchat (main handler) for topic {topic_id}: {str(e)}")

async def process_group_messages(client, message, topic_id, user_name):
    try:
        model_to_use = gmodel_name
        
        while group_message_queues[topic_id]:
            
            # --- START FIX: Critical Re-check for Enabled Status ---
            group_id = topic_id.split(':')[0]
            topic_data = get_topic_data(topic_id)
            group_config = get_group_config(group_id)
            
            is_explicitly_disabled = topic_data.get("enabled") is False
            is_not_enabled_by_group = topic_data.get("enabled") is not True and not group_config.get("enabled_all", False)
            
            if is_explicitly_disabled or is_not_enabled_by_group:
                group_message_queues[topic_id].clear()
                clear_group_message_queue(topic_id)
                active_topics.discard(topic_id)
                return 
            # --- END FIX ---
            
            delay = random.choice([4, 6, 8])
            await asyncio.sleep(delay)

            batch = []
            for _ in range(2):
                if group_message_queues[topic_id]:
                    batch.append(group_message_queues[topic_id].popleft())

            if not batch:
                break

            combined_message = " ".join(batch)
            clear_group_message_queue(topic_id)

            chat_history_list, bot_role = get_chat_history(topic_id, combined_message, user_name)
            full_prompt = build_gemini_prompt(bot_role, chat_history_list, combined_message)

            await send_typing_action(client, message.chat.id, combined_message)

            bot_response = ""
            max_length = 200

            try:
                bot_response = await _call_gemini_api(client, full_prompt, topic_id, model_to_use, chat_history_list)
                
                if len(bot_response) > max_length:
                    bot_response = bot_response[:max_length] + "..."
                    
                data = get_module_data()
                history = get_nested(data, ["topics", topic_id, "history"], [])
                history.append(bot_response)
                set_nested(data, ["topics", topic_id, "history"], history)
                save_module_data(data)

            except ValueError as ve: 
                await client.send_message("me", f"❌ Failed to get Gemini model for topic {topic_id}: {ve}")
                break 
            except Exception as e: 
                await client.send_message("me", f"❌ Error generating response for topic {topic_id}: {str(e)}")
                break 

            if not bot_response or not isinstance(bot_response, str) or bot_response.strip() == "":
                bot_response = "Sorry, I couldn't process that. Can you try again?" 
                await client.send_message(
                    "me",
                    f"❌ Invalid or empty bot_response for topic {topic_id}. Using fallback response."
                )

            if await handle_voice_message(client, message.chat.id, bot_response, message.message_thread_id):
                continue

            response_length = len(bot_response)
            char_delay = 0.03
            total_delay = response_length * char_delay

            elapsed_time = 0
            while elapsed_time < total_delay:
                await send_typing_action(client, message.chat.id, bot_response)
                await asyncio.sleep(2) 
                elapsed_time += 2

            await client.send_message(
                message.chat.id,
                bot_response,
                message_thread_id=message.message_thread_id,
            )

        active_topics.discard(topic_id)
    except Exception as e:
        await client.send_message("me", f"❌ Critical error in `process_group_messages` for topic {topic_id}: {str(e)}")
        active_topics.discard(topic_id)


@Client.on_message(filters.group & ~filters.me, group=2)
async def handle_files(client: Client, message: Message):
    file_path = None
    topic_id = None
    try:
        group_id = str(message.chat.id)
        thread_id_str = str(message.message_thread_id) if message.message_thread_id else "0"
        topic_id = f"{group_id}:{thread_id_str}"
        user_name = message.from_user.first_name if message.from_user else "User"

        topic_data = get_topic_data(topic_id)
        group_config = get_group_config(group_id)

        if topic_data.get("enabled") is False:
            return

        if topic_data.get("enabled") is not True and not group_config.get("enabled_all", False):
            return
            
        if message.caption and message.caption.strip().startswith("Reacted to this message with"):
            return

        model_to_use = gmodel_name
        caption = message.caption.strip() if message.caption else ""
        
        chat_history_list, bot_role = get_chat_history(topic_id, caption, user_name)

        if message.photo:
            
            if not hasattr(client, "image_buffer"): client.image_buffer = {}; client.image_timers = {}
            if topic_id not in client.image_buffer: client.image_buffer[topic_id] = []; client.image_timers[topic_id] = None

            image_path = await client.download_media(message.photo)
            await asyncio.sleep(random.uniform(0.1, 0.5))
            client.image_buffer[topic_id].append(image_path)

            if client.image_timers[topic_id] is None:
                async def process_images():
                    try:
                        await asyncio.sleep(5)
                        image_paths = client.image_buffer.pop(topic_id, [])
                        client.image_timers[topic_id] = None

                        if not image_paths: return

                        sample_images = []
                        for img_path in image_paths:
                            try: sample_images.append(Image.open(img_path))
                            except Exception as img_open_e:
                                await client.send_message("me", f"❌ Error opening image {img_path} for topic {topic_id}: {img_open_e}")
                                if os.path.exists(img_path): os.remove(img_path)
                        if not sample_images: return

                        prompt_text = "User has sent multiple images." + (f" Caption: {caption}" if caption else "")
                        bot_role_img = get_effective_bot_role(group_id, topic_id)
                        full_prompt = build_gemini_prompt(bot_role_img, chat_history_list, prompt_text)
                        
                        input_data = [full_prompt] + sample_images
                        response = await _call_gemini_api(client, input_data, topic_id, model_to_use, chat_history_list)
                        
                        await client.send_message(message.chat.id, response, message_thread_id=message.message_thread_id)
                            
                    except Exception as e_image_process:
                        await client.send_message("me", f"❌ Error processing images in group `handle_files` for topic {topic_id}: {str(e_image_process)}")
                    finally:
                        for img_path in image_paths:
                            if os.path.exists(img_path): os.remove(img_path)

                client.image_timers[topic_id] = asyncio.create_task(process_images())
                return

        file_type, file_path = None, None
        if message.video or message.video_note:
            file_type, file_path = ("video", await client.download_media(message.video or message.video_note))
        elif message.audio or message.voice:
            file_type, file_path = ("audio", await client.download_media(message.audio or message.voice))
        elif message.document and message.document.file_name.lower().endswith(".pdf"):
            file_type, file_path = "pdf", await client.download_media(message.document)
        elif message.document:
            file_type, file_path = ("document", await client.download_media(message.document))

        if file_path and file_type:
            await asyncio.sleep(random.uniform(0.1, 0.5))
            try:
                uploaded_file = await upload_file_to_gemini(file_path, file_type)
                prompt_text = f"User has sent a {file_type}." + (f" Caption: {caption}" if caption else "")
                
                bot_role_file = get_effective_bot_role(group_id, topic_id)
                full_prompt = build_gemini_prompt(bot_role_file, chat_history_list, prompt_text)
                
                input_data = [full_prompt, uploaded_file]
                response = await _call_gemini_api(client, input_data, topic_id, model_to_use, chat_history_list)
                
                await client.send_message(message.chat.id, response, message_thread_id=message.message_thread_id)
                    
            except Exception as e_file_process:
                await client.send_message("me", f"❌ Error processing {file_type} in group `handle_files` for topic {topic_id}: {str(e_file_process)}")

    except Exception as e:
        await client.send_message("me", f"❌ An error occurred in group `handle_files` function for topic {topic_id}:\n\n{str(e)}")
    finally:
        if file_path and os.path.exists(file_path):
            os.remove(file_path)

# --- COMMAND HANDLERS ---

@Client.on_message(filters.command(["wchat", "wc"], prefix) & filters.me)
async def wchat_command(client: Client, message: Message):
    try:
        parts = message.text.strip().split(maxsplit=2)
        group_id = str(message.chat.id)
        thread_id_str = str(message.message_thread_id or 0)
        topic_id = f"{group_id}:{thread_id_str}"
        
        command = parts[1].lower() if len(parts) > 1 else None
        arg2 = parts[2].lower() if len(parts) > 2 else None

        if not command:
            await message.edit_text(
                f"<b>Usage:</b> {prefix}wchat `on`, `off`, `del` [thread_id] or `{prefix}wchat all` or `{prefix}wchat history [number|off]`"
            )
            return

        if command == "all":
            group_config = get_group_config(group_id)
            enabled_all = not group_config.get("enabled_all", False)
            save_group_config_field(group_id, "enabled_all", enabled_all)
            await message.edit_text(
                f"wchat is now {'enabled' if enabled_all else 'disabled'} for all topics in this group."
            )
            await asyncio.sleep(1)
            await message.delete()
            return

        if command == "history":
            if not arg2:
                current_limit = get_global_config_field("history_limit")
                if current_limit:
                    await message.edit_text(f"Global history limit: last {current_limit} messages.")
                else:
                    await message.edit_text("No global history limit set.")
            else:
                if arg2 == "off":
                    save_global_config_field("history_limit", None)
                    await message.edit_text("History limit disabled.")
                else:
                    try:
                        num = int(arg2)
                        save_global_config_field("history_limit", num)
                        await message.edit_text(f"Global history limit set to last {num} messages.")
                    except ValueError:
                        await message.edit_text("Invalid number for history limit.")
            return

        target_topic_id = topic_id
        if arg2 and arg2.isdigit():
            target_topic_id = f"{group_id}:{arg2}"
        
        if command == "on":
            save_topic_data_field(target_topic_id, "enabled", True)
            await message.edit_text(f"<b>wchat is enabled for topic {target_topic_id}.</b>")

        elif command == "off":
            save_topic_data_field(target_topic_id, "enabled", False)
            await message.edit_text(f"<b>wchat is disabled for topic {target_topic_id}.</b>")

        elif command == "del":
            save_topic_data_field(target_topic_id, "history", [])
            save_topic_data_field(target_topic_id, "role_active", None)
            await message.edit_text(f"<b>Chat history deleted for topic {target_topic_id}.</b>")

        else:
            await message.edit_text(
                f"<b>Usage:</b> {prefix}wchat `on`, `off`, `del` [thread_id] or `{prefix}wchat all` or `{prefix}wchat history [number|off]`"
            )

        await asyncio.sleep(1)
        await message.delete()

    except Exception as e:
        await client.send_message("me", f"❌ An error occurred in the `wchat` command:\n\n{str(e)}")


@Client.on_message(filters.command("grole", prefix) & filters.group & filters.me)
async def set_custom_role(client: Client, message: Message):
    try:
        parts = message.text.strip().split(maxsplit=2) 
        group_id = str(message.chat.id)
        
        if len(parts) == 1:
            await message.edit_text(f"Usage: {prefix}grole [group] <custom role>\nOmit 'group' to set the role for the current topic.")
            return
            
        scope_or_role_text = parts[1].strip()
        custom_role = parts[2].strip() if len(parts) > 2 else ""
        
        # Combine parts for role if scope is omitted
        full_role_text = scope_or_role_text + (" " + custom_role if custom_role else "")

        if scope_or_role_text.lower() == "group":
            group_key = group_id
            main_topic_id = f"{group_id}:0"
            
            if not custom_role:
                # Reset
                save_group_config_field(group_key, "role_primary", None)
                save_topic_data_field(main_topic_id, "role_primary", None)
                save_topic_data_field(main_topic_id, "role_active", None)
                save_topic_data_field(main_topic_id, "history", [])
                
                response = f"Primary role reset to default for group {group_id}."
            else:
                # Set Group Role
                save_group_config_field(group_key, "role_primary", custom_role)
                save_topic_data_field(main_topic_id, "role_primary", None) # Clear Topic Primary so Group Primary takes effect
                save_topic_data_field(main_topic_id, "role_active", custom_role) # Set Active Role
                save_topic_data_field(main_topic_id, "history", [])
                
                response = f"Primary role set successfully for group {group_id}!\n<b>New Role:</b> {custom_role}"

        else:
            thread_id_str = str(message.message_thread_id or 0)
            topic_id = f"{group_id}:{thread_id_str}"
            
            # The role text is already combined in full_role_text
            
            if not full_role_text:
                # Reset
                save_topic_data_field(topic_id, "role_primary", None)
                save_topic_data_field(topic_id, "role_active", None) # Reset Active Role to fall back to Group/Default
                save_topic_data_field(topic_id, "history", [])
                response = f"Primary role reset to group/default role for topic {topic_id}."
            else:
                # Set Topic Role
                save_topic_data_field(topic_id, "role_primary", full_role_text)
                save_topic_data_field(topic_id, "role_active", full_role_text) # Set Active Role
                save_topic_data_field(topic_id, "history", [])
                response = f"Primary role set successfully for topic {topic_id}!\n<b>New Role:</b> {full_role_text}"
        
        await message.edit_text(response)
        await asyncio.sleep(1)
        await message.delete()

    except Exception as e:
        await client.send_message("me", f"❌ An error occurred in the `grole` command:\n\n{str(e)}")

@Client.on_message(filters.command("grolex", prefix) & filters.group & filters.me)
async def toggle_or_reset_secondary_role(client: Client, message: Message):
    try:
        parts = message.text.strip().split(maxsplit=2) 
        group_id = str(message.chat.id)
        
        arg1 = parts[1].strip() if len(parts) > 1 else "" 
        arg2 = parts[2].strip() if len(parts) > 2 else "" 

        scope = "topic"
        full_role_text = ""

        # --- FIX: Correctly parse multi-word roles ---
        if arg1.lower() == "group":
            scope = "group"
            full_role_text = arg2
        else:
            # If not 'group' scope, arg1 is the start of the role, and arg2 is the rest.
            full_role_text = f"{arg1} {arg2}" if arg2 else arg1

        role_text = full_role_text.strip()
        # --- END FIX: Correctly parse multi-word roles ---

        def get_secondary_role_details(group_id, topic_id, role_text_from_command, is_group_scope):
            
            if is_group_scope:
                config_data = get_group_config(group_id)
                custom_secondary = config_data.get("role_secondary")
            else:
                config_data = get_topic_data(topic_id)
                custom_secondary = config_data.get("role_secondary")
            
            if role_text_from_command.lower() == "r":
                return None, default_secondary_role, True # Save None, use default_secondary_role content for cascade, is_reset=True
            
            elif role_text_from_command:
                return role_text_from_command, role_text_from_command, False # Save custom text, use custom text content, is_reset=False
            
            else:
                # No args, determine current secondary role content for toggling
                final_secondary_role_content = custom_secondary if custom_secondary is not None else default_secondary_role
                return final_secondary_role_content, final_secondary_role_content, False

        if scope == "group":
            group_key = group_id
            topic_key = f"{group_id}:0"
            
            topic_data = get_topic_data(topic_key)
            # The primary_role here is the role it should fall back to (Group Primary or Global Default)
            primary_role_content_for_toggle = get_effective_bot_role(group_key, topic_key)

            secondary_role_to_save, secondary_role_content, is_reset = get_secondary_role_details(group_key, topic_key, role_text, True)

            if is_reset:
                save_group_config_field(group_key, "role_secondary", None)
                save_topic_data_field(topic_key, "role_active", None) # Set active to None to fall back to the cascade (Group Primary/Global)
                response = f"Secondary role reset to default for group {group_id}. Switched back to Primary."
            
            elif role_text:
                # Set custom secondary role and immediately activate it
                save_group_config_field(group_key, "role_secondary", secondary_role_to_save)
                save_topic_data_field(topic_key, "role_active", secondary_role_content)
                response = f"Custom secondary role set and activated for group {group_id}!\n<b>New Secondary Role:</b> {secondary_role_content}"
            
            else:
                current_active_role = topic_data.get("role_active") 
                
                if current_active_role == secondary_role_content:
                    # Toggling back to primary role
                    save_topic_data_field(topic_key, "role_active", None) # Set to None to force cascade fallback
                    response = f"Switched group {group_id} back to **Primary Role**."
                else:
                    # Toggling to secondary role
                    save_topic_data_field(topic_key, "role_active", secondary_role_content)

                    response = f" Switched group {group_id} to **Secondary Role**.\n<b>Role:</b> {secondary_role_content}"
            
            save_topic_data_field(topic_key, "history", [])

        elif scope == "topic":
            thread_id_str = str(message.message_thread_id or 0)
            topic_key = f"{group_id}:{thread_id_str}"
            
            topic_data = get_topic_data(topic_key)
            # The primary_role here is the role it should fall back to (Topic Primary, Group Primary, or Global Default)
            primary_role_content_for_toggle = get_effective_bot_role(group_id, topic_key) 

            secondary_role_to_save, secondary_role_content, is_reset = get_secondary_role_details(group_id, topic_key, role_text, False)
            
            if is_reset:
                save_topic_data_field(topic_key, "role_secondary", None)
                save_topic_data_field(topic_key, "role_active", None) # Set active to None to fall back to the cascade
                response = f"Secondary role reset to default for topic {topic_key}. Switched back to Primary."
            
            elif role_text:
                # Set custom secondary role and immediately activate it
                save_topic_data_field(topic_key, "role_secondary", secondary_role_to_save)
                save_topic_data_field(topic_key, "role_active", secondary_role_content)
                response = f"Custom secondary role set and activated for topic {topic_key}!\n<b>New Secondary Role:</b> {secondary_role_content}"
            
            else:
                current_active_role = topic_data.get("role_active") 
                
                if current_active_role == secondary_role_content:
                    # Toggling back to primary role
                    save_topic_data_field(topic_key, "role_active", None) # Set to None to force cascade fallback
                    response = f"Switched topic {topic_key} back to **Primary Role**."
                else:
                    # Toggling to secondary role
                    save_topic_data_field(topic_key, "role_active", secondary_role_content)

                    response = f"Switched topic {topic_key} to **Secondary Role**.\n<b>Role:</b> {secondary_role_content}"
            
            save_topic_data_field(topic_key, "history", [])
                
        else:
            response = "Invalid scope. Use 'group' or omit for topic."

        await message.edit_text(response)
        await asyncio.sleep(1)
        await message.delete()

    except Exception as e:
        await client.send_message("me", f"❌ An error occurred in the `grolex` command:\n\n{str(e)}")


@Client.on_message(filters.command("wchatel", prefix) & filters.me)
async def toggle_elevenlabs(client: Client, message: Message):
    global elevenlabs_enabled
    try:
        elevenlabs_enabled = not elevenlabs_enabled
        save_global_config_field("elevenlabs_enabled", elevenlabs_enabled)
        
        status = "enabled" if elevenlabs_enabled else "disabled"
        await message.edit_text(f"🎙️ **ElevenLabs Voice Generation is now {status}** for groups.")

    except Exception as e:
        await client.send_message("me", f"An error occurred in the `wchatel` command:\n\n{str(e)}")


@Client.on_message(filters.command("setwkey", prefix) & filters.me)
async def set_gemini_key(client: Client, message: Message):
    try:
        command = message.text.strip().split()
        subcommand = command[1].lower() if len(command) > 1 else None
        key_arg = command[2] if len(command) > 2 else None

        gemini_keys = get_gemini_keys()
        current_key_index = get_global_config_field("key_index", 0)

        if subcommand == "add" and key_arg:
            if key_arg not in gemini_keys:
                gemini_keys.append(key_arg)
                save_gemini_keys(gemini_keys)
                await message.edit_text("✅ New Gemini API key added successfully to the central list!")
            else:
                await message.edit_text("⚠️ This Gemini API key already exists.")

        elif subcommand == "set" and key_arg:
            index = int(key_arg) - 1
            if 0 <= index < len(gemini_keys):
                save_global_config_field("key_index", index)
                await message.edit_text(f"✅ Current Gemini API key index set to **{key_arg}**.")
            else:
                await message.edit_text(f"❌ Invalid key index: {key_arg}.")

        elif subcommand == "del" and key_arg:
            index = int(key_arg) - 1
            if 0 <= index < len(gemini_keys):
                gemini_keys.pop(index)
                save_gemini_keys(gemini_keys)
                if current_key_index >= len(gemini_keys):
                    save_global_config_field("key_index", max(0, len(gemini_keys) - 1))
                await message.edit_text(f"✅ Gemini API key **{key_arg}** deleted successfully from the central list!")
            else:
                await message.edit_text(f"❌ Invalid key index: {key_arg}.")

        elif subcommand == "show":
            if not gemini_keys:
                await message.edit_text("No Gemini API keys available.")
            else:
                keys_list = "\n".join([f"**{i + 1}**: {key}" for i, key in enumerate(gemini_keys)])
                await client.send_message("me", f"🔑 **Full Central Gemini API Keys:**\n\n{keys_list}")
                await message.edit_text("Full API keys sent to saved messages.")
        
        else:
            keys_list_display = "\n".join(
                [f"**{i + 1}**: `{key[:10]}...`" for i, key in enumerate(gemini_keys)]
            )
            current_key_display = f"{current_key_index + 1}: `{gemini_keys[current_key_index][:10]}...`" if gemini_keys else "None"
            await message.edit_text(
                f"🔑 **Central Gemini API keys:**\n\n{keys_list_display or 'No keys added.'}\n\n➡️ Current Index: {current_key_display}"
            )

    except Exception as e:
        await client.send_message("me", f"❌ An error occurred in the `setwkey` command:\n\n{str(e)}")

@Client.on_message(filters.command("setwmodel", prefix) & filters.me)
async def set_wmodel(client: Client, message: Message):
    global gmodel_name
    try:
        model_key = "gmodel_name"
        
        parts = message.text.strip().split()
        if len(parts) < 2:
            current_model = get_global_config_field(model_key) or "gemini-2.0-flash"
            await message.edit_text(
                f"🤖 **Current WChat Gemini Model:** `{current_model}`\n\n"
                f"**Usage:** `{prefix}setwmodel <model_name>`"
            )
            return

        new_model = parts[1].strip()
        gmodel_name = new_model
        save_global_config_field(model_key, new_model)
        await message.edit_text(f"✅ **WChat Gemini model set to:** `{new_model}`")

    except Exception as e:
        await client.send_message("me", f"❌ Error in `setwmodel` command:\n\n{str(e)}")


modules_help["wchat"] = {
    "wchat on/off [thread_id]": "Enable or disable wchat for the current/specified topic.",
    "wchat del [thread_id]": "Delete the chat history for the current/specified topic.",
    "wchat all": "Toggle wchat for all topics in the current group.",
    "wchat history [num|off]": "Set a global history limit for all wchats.",
    "grole group <custom role>": "Set a custom **primary role** for the entire group. Affects all topics by default.",
    "grole <custom role>": "Set a custom **primary role** for the **current topic**.",
    "grolex group [role|r]": "Toggle the **main topic** (thread 0) between primary/secondary roles. Use `[role]` to set a custom secondary role, or `r` to reset it.",
    "grolex [role|r]": "Toggle the **current topic** between primary/secondary roles. Use `[role]` to set a custom secondary role, or `r` to reset it.",
    "wchatel": "Toggle the ElevenLabs voice generation feature for groups.",
    "setwkey add <key>": "Add a new Gemini API key to the central list.",
    "setwkey set <index>": "Set the current Gemini API key by index.",
    "setwkey del <index>": "Delete a Gemini API key by index from the central list.",
    "setwkey": "Display all available Gemini API keys (partial) and the current key index.",
    "setwmodel <model_name>": "Set the Gemini model for the WChat module only."
}
