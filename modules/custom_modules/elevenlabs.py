import os
import httpx
import subprocess
from pyrogram import Client, filters, enums
from pyrogram.types import Message
from utils.misc import modules_help, prefix
from utils.db import db
import pymongo
from utils import config

# --- Default Parameters ---

DEFAULT_PARAMS = {
    "voice_id": "QLDNM6o3lDbtfJLUO890",
    "stability": 0.3,
    "similarity_boost": 0.9,
}

# --- Centralized API Key Management ---

def get_api_keys_db():
    """Get connection to the separate API Keys database."""
    client = pymongo.MongoClient(config.db_url)
    return client["ApiKeys"]

def get_elevenlabs_keys():
    """Get ElevenLabs API keys from the centralized ApiKeys database."""
    try:
        api_db = get_api_keys_db()
        result = api_db["elevenlabs_keys"].find_one({"type": "keys"})
        if result is None:
            # If the collection doesn't exist, create it
            api_db["elevenlabs_keys"].insert_one({"type": "keys", "keys": []})
            return []
        return result.get("keys", [])
    except Exception as e:
        print(f"Error getting elevenlabs keys: {e}")
        return []

def save_elevenlabs_keys(keys):
    """Save ElevenLabs API keys to the centralized ApiKeys database."""
    try:
        api_db = get_api_keys_db()
        api_db["elevenlabs_keys"].update_one(
            {"type": "keys"},
            {"$set": {"keys": keys}},
            upsert=True
        )
    except Exception as e:
        print(f"Error saving elevenlabs keys: {e}")

def add_elevenlabs_key(new_key):
    """Add a new ElevenLabs API key if it doesn't already exist."""
    keys = get_elevenlabs_keys()
    if new_key not in keys:
        keys.append(new_key)
        save_elevenlabs_keys(keys)
        return True
    return False

# --- End of Centralized API Key Management ---

def process_audio(input_path: str, output_path: str, speed: float, volume: float):
    """
    Process the audio file using FFmpeg.
    Adjusts speed, volume, and applies filters for natural sound.
    :param input_path: Path to the original audio file.
    :param output_path: Path to save the processed audio file.
    :param speed: Speed adjustment factor (e.g., 1.0 for normal speed, 0.9 for slower).
    :param volume: Volume adjustment factor (e.g., 1.0 for no change, 0.8 for reduced volume).
    """
    subprocess.run(
        [
            "ffmpeg",
            "-i", input_path,
            "-filter:a",
            f"atempo={speed},volume={volume},acompressor=threshold=-20dB:ratio=2.5:attack=5:release=50",
            "-vn",  # No video
            "-c:a", "libopus", # Specify Opus codec for OGG
            output_path,
        ],
        check=True
    )

async def generate_elevenlabs_audio(text: str):
    """
    Generate audio using ElevenLabs API with adjusted parameters.
    :param text: Text to convert to speech.
    :return: Path to the generated audio file.
    """
    api_keys = get_elevenlabs_keys() # Uses centralized DB
    current_key_index = db.get("custom.elevenlabs", "current_key_index", 0)
    
    if not api_keys:
        # CHANGED: set_el to setel
        raise ValueError(f"No API keys configured! Use {prefix}setel add <key>")

    params = {key: db.get("custom.elevenlabs", key, DEFAULT_PARAMS[key]) for key in DEFAULT_PARAMS}
    
    for attempt in range(len(api_keys)):
        # --- FIX APPLIED HERE: Extract the key string from the dictionary ---
        api_key = api_keys[current_key_index]["key"] 
        # --- END FIX ---
        
        headers = {
            "xi-api-key": api_key,
            "Content-Type": "application/json",
        }
        data = {
            "text": text,
            "voice_settings": {
                "stability": params["stability"],
                "similarity_boost": params["similarity_boost"],
            },
        }

        voice_id = params["voice_id"]
        original_audio_path = "elevenlabs_voice.mp3" # ElevenLabs typically returns MP3

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.post(
                    f"https://api.elevenlabs.io/v1/text-to-speech/{voice_id}",
                    headers=headers,
                    json=data,
                )

                if response.status_code == 200:
                    with open(original_audio_path, "wb") as f:
                        f.write(response.content)
                    return original_audio_path
                
                error_data = response.json()
                error_status = error_data.get("detail", {}).get("status", "")
                
                if error_status in ["quota_exceeded", "invalid_api_key", "too_many_concurrent_requests"]:
                    current_key_index = (current_key_index + 1) % len(api_keys)
                    db.set("custom.elevenlabs", "current_key_index", current_key_index)
                else:
                    raise ValueError(f"API Error: {error_data.get('detail', {}).get('message', 'Unknown error')}")

        except Exception as e:
            current_key_index = (current_key_index + 1) % len(api_keys)
            db.set("custom.elevenlabs", "current_key_index", current_key_index)

    raise ValueError("All API keys failed. Please add more keys or check existing ones.")

@Client.on_message(filters.command(["elevenlabs", "el"], prefix) & filters.me)
async def elevenlabs_command(client: Client, message: Message):
    """
    Handle the ElevenLabs text-to-speech command.
    """
    original_audio_path = None
    processed_audio_path = None
    try:
        if len(message.command) < 2:
            await message.edit_text(
                f"**Usage:** `{prefix}elevenlabs [text]`",
                parse_mode=enums.ParseMode.MARKDOWN
            )
            return

        text = " ".join(message.command[1:]).strip()
        await message.delete()

        original_audio_path = await generate_elevenlabs_audio(text)
        processed_audio_path = "elevenlabs_voice_processed.ogg"
        
        process_audio(original_audio_path, processed_audio_path, speed=0.9, volume=0.9)

        await client.send_voice(chat_id=message.chat.id, voice=processed_audio_path)

    except Exception as e:
        await client.send_message(
            message.chat.id,
            f"**Error:**\n`{e}`",
            parse_mode=enums.ParseMode.MARKDOWN
        )
    finally:
        for path in [original_audio_path, processed_audio_path]:
            if path and os.path.exists(path):
                try:
                    os.remove(path)
                except Exception as cleanup_error:
                    print(f"Cleanup error: {cleanup_error}")

@Client.on_message(filters.command(["vl"], prefix) & filters.me)
async def elevenlabs_video_command(client: Client, message: Message):
    """
    Generate ElevenLabs audio and convert it to an MP4 video with minimal size.
    """
    audio_path = None
    video_path = None

    try:
        if len(message.command) < 2:
            return await message.edit_text(f"**Usage:** `{prefix}vl [text]`", parse_mode=enums.ParseMode.MARKDOWN)

        text = " ".join(message.command[1:]).strip()
        await message.delete()

        audio_path = await generate_elevenlabs_audio(text)
        video_path = "elevenlabs_voice_video.mp4"

        subprocess.run(
            [
                "ffmpeg",
                "-f", "lavfi",
                "-i", "color=c=black:s=480x480",
                "-i", audio_path,
                "-c:v", "libx264",
                "-crf", "35",
                "-preset", "ultrafast",
                "-r", "10",
                "-c:a", "aac",
                "-b:a", "192k",
                "-shortest",
                "-pix_fmt", "yuv420p",
                video_path
            ],
            check=True
        )

        await client.send_video(chat_id=message.chat.id, video=video_path, caption=f"🎙️ {text}")

    except Exception as e:
        await client.send_message(
            message.chat.id,
            f"**Error:**\n`{e}`",
            parse_mode=enums.ParseMode.MARKDOWN
        )
    finally:
        for path in [audio_path, video_path]:
            if path and os.path.exists(path):
                try:
                    os.remove(path)
                except Exception as cleanup_error:
                    print(f"Cleanup error: {cleanup_error}")

@Client.on_message(filters.command(["set_elevenlabs", "setel"], prefix) & filters.me)
async def set_elevenlabs_config(client: Client, message: Message):
    """
    Configure ElevenLabs settings using centralized key management.
    """
    args = message.command
    api_keys = get_elevenlabs_keys()
    current_key_index = db.get("custom.elevenlabs", "current_key_index", 0)

    if len(args) == 1:
        current_values = {key: db.get("custom.elevenlabs", key, DEFAULT_PARAMS[key]) for key in DEFAULT_PARAMS}
        
        if current_key_index >= len(api_keys) and api_keys:
            current_key_index = 0
            db.set("custom.elevenlabs", "current_key_index", 0)

        keys_list = "\n".join([f"**{i + 1}**: `{entry['key']}`{' (current)' if i == current_key_index else ''}" for i, entry in enumerate(api_keys)])
        response = (
            "**ElevenLabs Configuration**\n\n"
            f"🔑 **API Keys ({len(api_keys)})**:\n{keys_list or 'No keys added'}\n\n"
            f"⚙️ **Parameters**:\n"
            + "\n".join([f"• `{key}`: `{value}`" for key, value in current_values.items()])
            + f"\n\n**Commands**:\n"
            f"`{prefix}setel add <key>` - Add new API key\n"
            f"`{prefix}setel del <num>` - Delete API key by number\n"
            f"`{prefix}setel set <num>` - Set active API key\n"
            f"`{prefix}setel voice_id <id>` - Set voice id\n"
            f"`{prefix}setel stability <value>` - Set stability (0.0 to 1.0)\n"
            f"`{prefix}setel similarity_boost <value>` - Set similarity boost (0.0 to 1.0)"
        )
        return await message.edit_text(response, parse_mode=enums.ParseMode.MARKDOWN)

    action = args[1].lower()

    if action == "add" and len(args) >= 3:
        new_key = " ".join(args[2:])
        if add_elevenlabs_key(new_key):
            total_keys = len(get_elevenlabs_keys())
            return await message.edit_text(f"✅ Added new key (Total: {total_keys})")
        return await message.edit_text("⚠️ Key already exists")

    if action == "del" and len(args) >= 3:
        try:
            index = int(args[2]) - 1
            if 0 <= index < len(api_keys):
                deleted = api_keys.pop(index)
                save_elevenlabs_keys(api_keys)
                if current_key_index >= len(api_keys):
                    db.set("custom.elevenlabs", "current_key_index", max(0, len(api_keys)-1))
                return await message.edit_text(f"✅ Deleted key: `{deleted['key']}`")
            return await message.edit_text("❌ Invalid key number")
        except ValueError:
            return await message.edit_text("❌ Invalid key number")

    if action == "set" and len(args) >= 3:
        try:
            index = int(args[2]) - 1
            if 0 <= index < len(api_keys):
                db.set("custom.elevenlabs", "current_key_index", index)
                return await message.edit_text(f"✅ Active key set to #{index+1}")
            return await message.edit_text("❌ Invalid key number")
        except ValueError:
            return await message.edit_text("❌ Invalid key number")

    if len(args) < 3:
        return await message.edit_text("❌ Invalid command format")

    key = args[1].lower()
    value = " ".join(args[2:])
    
    if key in ["stability", "similarity_boost"]:
        try:
            value = float(value)
        except ValueError:
            return await message.edit_text("❌ Value must be a number")
    
    if key in DEFAULT_PARAMS:
        db.set("custom.elevenlabs", key, value)
        await message.edit_text(f"✅ Updated `{key}` to `{value}`")
    else:
        await message.edit_text("❌ Invalid parameter")
        
@Client.on_message(filters.command(["tovn"], prefix) & filters.me)
async def convert_to_voice_note(client: Client, message: Message):
    """
    Convert a replied MP3 file into a Telegram voice note (OGG/Opus).
    Usage: Reply to an mp3 file and type .tovn
    """
    input_path = None
    output_path = None

    try:
        # Must reply to an audio file
        if not message.reply_to_message or not message.reply_to_message.audio:
            return await message.edit_text(
                f"❌ Reply to an MP3 file first.\n\n**Usage:** `{prefix}tovn`",
                parse_mode=enums.ParseMode.MARKDOWN
            )

        await message.edit_text("⏳ Downloading MP3...")

        # Download replied MP3
        input_path = await message.reply_to_message.download()
        output_path = "converted_voice_note.ogg"

        await message.edit_text("🎙️ Converting to voice note...")

        # Convert MP3 → OGG Opus (Voice Note Format)
        subprocess.run(
            [
                "ffmpeg",
                "-i", input_path,
                "-c:a", "libopus",
                "-b:a", "64k",
                "-vbr", "on",
                "-compression_level", "10",
                output_path
            ],
            check=True
        )

        await message.delete()

        # Send as Telegram voice note
        await client.send_voice(
            chat_id=message.chat.id,
            voice=output_path
        )

    except Exception as e:
        await client.send_message(
            message.chat.id,
            f"**Error:**\n`{e}`",
            parse_mode=enums.ParseMode.MARKDOWN
        )

    finally:
        # Cleanup
        for path in [input_path, output_path]:
            if path and os.path.exists(path):
                try:
                    os.remove(path)
                except Exception as cleanup_error:
                    print(f"Cleanup error: {cleanup_error}")


@Client.on_message(filters.command(["tov"], prefix) & filters.me)
async def convert_mp3_to_video(client: Client, message: Message):
    """
    Convert a replied MP3 file into an MP4 video.
    Usage: Reply to an MP3 file and type .tov
    """
    audio_path = None
    video_path = None

    try:
        # Must reply to an MP3 audio file
        if not message.reply_to_message or not message.reply_to_message.audio:
            return await message.edit_text(
                f"❌ Reply to an MP3 file first.\n\n**Usage:** `{prefix}tov`",
                parse_mode=enums.ParseMode.MARKDOWN
            )

        await message.edit_text("⏳ Downloading MP3...")

        # Download the replied MP3
        audio_path = await message.reply_to_message.download()
        video_path = "converted_audio_video.mp4"

        await message.edit_text("🎬 Converting MP3 → Video...")

        # Convert MP3 → MP4 Video (black background)
        subprocess.run(
            [
                "ffmpeg",
                "-f", "lavfi",
                "-i", "color=c=black:s=480x480",
                "-i", audio_path,
                "-c:v", "libx264",
                "-preset", "ultrafast",
                "-crf", "35",
                "-r", "10",
                "-c:a", "aac",
                "-b:a", "192k",
                "-shortest",
                "-pix_fmt", "yuv420p",
                video_path
            ],
            check=True
        )

        await message.delete()

        # Send as Telegram video
        await client.send_video(
            chat_id=message.chat.id,
            video=video_path,
            caption="🎵 Converted MP3 → Video"
        )

    except Exception as e:
        await client.send_message(
            message.chat.id,
            f"**Error:**\n`{e}`",
            parse_mode=enums.ParseMode.MARKDOWN
        )

    finally:
        # Cleanup files
        for path in [audio_path, video_path]:
            if path and os.path.exists(path):
                try:
                    os.remove(path)
                except Exception as cleanup_error:
                    print(f"Cleanup error: {cleanup_error}")

modules_help["elevenlabs"] = {
    "el [text]*": "Generate voice message using ElevenLabs",
    "vl [text]*": "Generate a video note with ElevenLabs audio",
    "setel": "Show configuration",
    "setel add <key>": "Add new API key to central DB",
    "setel del <num>": "Delete API key by number from central DB",
    "setel set <num>": "Set active API key",
    "setel voice_id <id>": "Set voice id",
    "setel stability <value>": "Set stability (0.0 to 1.0)",
    "setel similarity_boost <value>": "Set similarity boost (0.0 to 1.0)",
}
