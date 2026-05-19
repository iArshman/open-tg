import sys
import os
import humanize
from datetime import datetime
import pytz
import asyncio
from pyrogram import Client, filters
from pyrogram.types import Message
from utils.db import db
from utils.scripts import import_library
from PIL import Image
from PIL.ExifTags import TAGS
from mutagen import File as MutagenFile

# Fix Python path to include parent directory
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

try:
    from utils.config import prefix
except ImportError:
    prefix = "."  # Fallback prefix
    print("Warning: Could not import prefix from utils.config. Using fallback prefix '.'")

# Initialize libraries
humanize = import_library("humanize", "python-humanize")
Pillow = import_library("PIL", "Pillow")
mutagen = import_library("mutagen", "mutagen")

# Database collection
collection = "tinfo"

@Client.on_message(filters.command("tinfo", prefix) & filters.me)
async def get_file_info(client: Client, message: Message):
    """
    Extract and display detailed information about a replied file.
    Command: .tinfo (in reply to a file message)
    """
    try:
        # Check if the message is a reply
        if not message.reply_to_message:
            await message.reply_text(
                "Please reply to a message containing a file.",
                reply_to_message_id=message.id
            )
            await asyncio.sleep(0.02)
            await message.delete()
            return

        replied = message.reply_to_message
        file_path = None
        info = []

        # Initialize common metadata
        sender = replied.from_user.first_name if replied.from_user else "Unknown"
        sender_id = replied.from_user.id if replied.from_user else "N/A"
        chat_title = replied.chat.title or replied.chat.first_name or "Private Chat"
        chat_id = replied.chat.id
        topic_id = replied.reply_to_top_message_id or None
        sent_date = replied.date.astimezone(pytz.timezone('America/Phoenix')).strftime("%Y-%m-%d %H:%M:%S %Z")
        file_id = None
        file_type = "Unknown"
        file_size = 0
        file_name = None
        mime_type = None

        # Helper function to format file size
        def format_size(bytes_size):
            return f"{bytes_size} bytes ({humanize.naturalsize(bytes_size)})"

        # Process different file types
        if replied.photo:
            file_type = "Photo"
            photo = replied.photo
            file_id = photo.file_id
            file_size = photo.file_size
            mime_type = "image/jpeg"
            file_path = await client.download_media(photo)
            dimensions = f"{photo.width}x{photo.height}"
            info.append(f"**Type**: {file_type}")
            info.append(f"**Dimensions**: {dimensions}")
            if file_path:
                try:
                    img = Image.open(file_path)
                    exif_data = img.getexif()
                    if exif_data:
                        exif_info = []
                        for tag_id, value in exif_data.items():
                            tag = TAGS.get(tag_id, tag_id)
                            exif_info.append(f"{tag}: {value}")
                        info.append("**EXIF Data**:\n" + "\n".join(exif_info))
                except Exception as e:
                    info.append(f"**EXIF Error**: {str(e)}")

        elif replied.video or replied.video_note:
            file_type = "Video" if replied.video else "Video Note"
            video = replied.video or replied.video_note
            file_id = video.file_id
            file_size = video.file_size
            file_name = video.file_name if replied.video else None
            mime_type = video.mime_type
            duration = humanize.naturaldelta(video.duration) if video.duration else "N/A"
            dimensions = f"{video.width}x{video.height}" if video.width and video.height else "N/A"
            file_path = await client.download_media(video)
            info.append(f"**Type**: {file_type}")
            info.append(f"**Duration**: {duration}")
            info.append(f"**Dimensions**: {dimensions}")
            if file_path:
                try:
                    meta = MutagenFile(file_path)
                    if meta:
                        meta_info = []
                        for key, value in meta.items():
                            meta_info.append(f"{key}: {value}")
                        if meta_info:
                            info.append("**Metadata**:\n" + "\n".join(meta_info))
                except Exception as e:
                    info.append(f"**Metadata Error**: {str(e)}")

        elif replied.audio or replied.voice:
            file_type = "Audio" if replied.audio else "Voice"
            audio = replied.audio or replied.voice
            file_id = audio.file_id
            file_size = audio.file_size
            file_name = audio.file_name if replied.audio else None
            mime_type = audio.mime_type
            duration = humanize.naturaldelta(audio.duration) if audio.duration else "N/A"
            file_path = await client.download_media(audio)
            info.append(f"**Type**: {file_type}")
            info.append(f"**Duration**: {duration}")
            if file_path:
                try:
                    meta = MutagenFile(file_path)
                    if meta:
                        meta_info = []
                        for key, value in meta.items():
                            meta_info.append(f"{key}: {value}")
                        if meta_info:
                            info.append("**Metadata**:\n" + "\n".join(meta_info))
                except Exception as e:
                    info.append(f"**Metadata Error**: {str(e)}")

        elif replied.document:
            file_type = "Document"
            document = replied.document
            file_id = document.file_id
            file_size = document.file_size
            file_name = document.file_name
            mime_type = document.mime_type
            file_path = await client.download_media(document)
            info.append(f"**Type**: {file_type}")

        elif replied.sticker:
            file_type = "Sticker"
            sticker = replied.sticker
            file_id = sticker.file_id
            file_size = sticker.file_size
            mime_type = sticker.mime_type or "image/webp"
            dimensions = f"{sticker.width}x{sticker.height}"
            is_animated = "Yes" if sticker.is_animated else "No"
            is_video = "Yes" if sticker.is_video else "No"
            emoji = sticker.emoji or "N/A"
            info.append(f"**Type**: {file_type}")
            info.append(f"**Dimensions**: {dimensions}")
            info.append(f"**Animated**: {is_animated}")
            info.append(f"**Video**: {is_video}")
            info.append(f"**Emoji**: {emoji}")

        elif replied.animation:
            file_type = "Animation (GIF)"
            animation = replied.animation
            file_id = animation.file_id
            file_size = animation.file_size
            file_name = animation.file_name
            mime_type = animation.mime_type
            dimensions = f"{animation.width}x{animation.height}"
            duration = humanize.naturaldelta(animation.duration) if animation.duration else "N/A"
            file_path = await client.download_media(animation)
            info.append(f"**Type**: {file_type}")
            info.append(f"**Dimensions**: {dimensions}")
            info.append(f"**Duration**: {duration}")

        else:
            await message.reply_text(
                "The replied message does not contain a supported file type.",
                reply_to_message_id=message.id
            )
            await asyncio.sleep(0.02)
            await message.delete()
            return

        # Common metadata
        info.insert(0, f"**File ID**: {file_id}")
        info.insert(1, f"**File Size**: {format_size(file_size)}")
        if file_name:
            info.insert(2, f"**File Name**: {file_name}")
        if mime_type:
            info.insert(3, f"**MIME Type**: {mime_type}")
        info.append(f"**Sender**: {sender} (ID: {sender_id})")
        info.append(f"**Chat**: {chat_title} (ID: {chat_id})")
        if topic_id:
            info.append(f"**Topic ID**: {topic_id}")
        info.append(f"**Sent Date**: {sent_date}")

        # Log command usage in database (similar to gchat's chat_history)
        log_entry = {
            "user_id": message.from_user.id,
            "chat_id": chat_id,
            "topic_id": topic_id,
            "timestamp": datetime.now(pytz.UTC).isoformat(),
            "file_type": file_type,
            "file_id": file_id
        }
        db.set(collection, f"usage.{chat_id}.{message.id}", log_entry)

        # Format and send response
        response = "\n".join(info)
        await message.reply_text(
            f"**File Information**:\n\n{response}",
            reply_to_message_id=replied.id
        )

        # Mark message as seen
        await client.read_history(chat_id=message.chat.id, max_id=message.id)

        await asyncio.sleep(0.02)
        await message.delete()

    except Exception as e:
        await client.send_message("me", f"An error occurred in `tinfo` command:\n\n{str(e)}")
        await asyncio.sleep(0.02)
        await message.delete()
    finally:
        # Clean up
        if file_path and os.path.exists(file_path):
            os.remove(file_path)
