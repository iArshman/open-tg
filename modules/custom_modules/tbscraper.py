import re
import time
import asyncio
from pyrogram import Client, filters
from pyrogram.types import Message

from utils.db import db
from utils.misc import modules_help, prefix
from utils.scripts import format_exc

import json
from pathlib import Path
import aiofiles

# === CONSTANTS ===
TERABOX_REGEX = re.compile(
    r"https?://[^\s]*?(?:terabox|teraboxapp|teraboxshare|nephobox|1024tera|teraboxurl|1024terabox|freeterabox|terasharefile|terasharelink|mirrobox|momerybox|teraboxlink)\.[^\s]+",
    re.IGNORECASE
)

TERABOX_KEY = "terabox"
ALLLINKS_KEY = "alllinks"

# === ALL LINKS DB FUNCTIONS ===
def get_all_links():
    """Get all stored links from DB"""
    return db.get(ALLLINKS_KEY, "links", [])

def save_all_links(links):
    """Save links to DB"""
    db.set(ALLLINKS_KEY, "links", links)

def normalize_link(link: str) -> str:
    """Normalize TeraBox links for comparison"""
    return link.rstrip("/").lower()

def add_links_to_db(new_links: list) -> dict:
    """
    Add new links to DB without duplicates
    Returns: {"added": count, "duplicates_skipped": count, "total": count}
    """
    existing_links = get_all_links()
    existing_normalized = {normalize_link(l) for l in existing_links}
    
    added_count = 0
    duplicates_skipped = 0
    
    for link in new_links:
        normalized = normalize_link(link)
        if normalized not in existing_normalized:
            existing_links.append(link)
            existing_normalized.add(normalized)
            added_count += 1
        else:
            duplicates_skipped += 1
    
    save_all_links(existing_links)
    
    return {
        "added": added_count,
        "duplicates_skipped": duplicates_skipped,
        "total": len(existing_links)
    }

def clear_all_links_db():
    """Clear all links from DB"""
    save_all_links([])
    return True

def get_links_count():
    """Get total count of stored links"""
    return len(get_all_links())

# === NEW UNIFIED DB STRUCTURE ===
def get_terabox_config():
    """Return the full terabox config dict"""
    return db.get(TERABOX_KEY, "config", {
        "enabled": False,
        "target": None,
        "sources": [],
        "seen_links": [],
    })

def save_terabox_config(config):
    """Save the full terabox config dict"""
    db.set(TERABOX_KEY, "config", config)

# === CONFIG HELPERS ===
def is_terabox_enabled():
    return get_terabox_config().get("enabled", False)

def toggle_terabox():
    cfg = get_terabox_config()
    cfg["enabled"] = not cfg.get("enabled", False)
    save_terabox_config(cfg)
    return cfg["enabled"]

def get_target_chat():
    return get_terabox_config().get("target")

def set_target_chat(chat_id):
    cfg = get_terabox_config()
    cfg["target"] = chat_id
    save_terabox_config(cfg)

def get_sources():
    return get_terabox_config().get("sources", [])

def add_source(chat_id):
    cfg = get_terabox_config()
    if chat_id not in cfg["sources"]:
        cfg["sources"].append(chat_id)
        save_terabox_config(cfg)

def remove_source(chat_id):
    cfg = get_terabox_config()
    if chat_id in cfg["sources"]:
        cfg["sources"].remove(chat_id)
        save_terabox_config(cfg)

def record_link(link: str):
    """Store link in terabox seen links"""
    cfg = get_terabox_config()
    normalized = normalize_link(link)
    seen_normalized = [normalize_link(l) for l in cfg["seen_links"]]
    
    if normalized not in seen_normalized:
        cfg["seen_links"].append(link)
        save_terabox_config(cfg)
        return True
    return False

def clear_terabox_db():
    cfg = get_terabox_config()
    cfg["seen_links"] = []
    save_terabox_config(cfg)
    return True

# === HELPERS ===
def extract_terabox_links(text: str):
    if not text:
        return []
    return TERABOX_REGEX.findall(text)

# === IMPORT LINKS FROM FILE ===
@Client.on_message(filters.command("importlinks", prefix) & filters.me)
async def import_links_from_file(client: Client, message: Message):
    """
    Reply to a JSON file with: .importlinks
    Fetches all links and saves to DB without duplicates
    """
    status_msg = await message.edit("🔍 Analyzing replied file...")
    
    try:
        if not message.reply_to_message or not message.reply_to_message.document:
            return await status_msg.edit(
                f"❌ Reply to a JSON file!\n"
                f"Usage: Reply to JSON file → <code>{prefix}importlinks</code>"
            )
        
        replied_msg = message.reply_to_message
        
        # Download file
        temp_dir = Path("temp_import")
        temp_dir.mkdir(exist_ok=True)
        
        await status_msg.edit("📥 Downloading file...")
        
        file_path = await client.download_media(
            replied_msg.document,
            file_name=temp_dir / replied_msg.document.file_name
        )
        
        await status_msg.edit("📖 Reading file...")
        
        # Read and parse JSON
        async with aiofiles.open(file_path, "r", encoding="utf-8") as f:
            content = await f.read()
            data = json.loads(content)
        
        # Handle different JSON structures
        if isinstance(data, dict) and "links" in data:
            links = data.get("links", [])
        else:
            links = data if isinstance(data, list) else []
        
        if not links:
            return await status_msg.edit("⚠️ No links found in file!")
        
        await status_msg.edit(f"💾 Saving {len(links)} links to DB (checking for duplicates)...")
        
        # Add links to DB
        result = add_links_to_db(links)
        
        # Send response
        await status_msg.edit(
            f"✅ **Import Complete!**\n\n"
            f"📊 **Stats:**\n"
            f"• Links in file: {len(links)}\n"
            f"• Added to DB: {result['added']}\n"
            f"• Duplicates skipped: {result['duplicates_skipped']}\n"
            f"• Total in DB: {result['total']}"
        )
        
        # Cleanup
        Path(file_path).unlink(missing_ok=True)
        temp_dir.rmdir()
        
    except json.JSONDecodeError:
        await status_msg.edit("❌ Invalid JSON file format!")
    except Exception as e:
        await status_msg.edit(f"❌ Error: {format_exc(e)}")


# === BATCH IMPORT MULTIPLE FILES ===
@Client.on_message(filters.command("batchimport", prefix) & filters.me)
async def batch_import_files(client: Client, message: Message):
    """
    Reply to message with multiple files: .batchimport
    Imports all files and saves to DB
    """
    status_msg = await message.edit("🔍 Checking for files...")
    
    try:
        if not message.reply_to_message:
            return await status_msg.edit(
                f"❌ Reply to message with files!\n"
                f"Usage: Reply → <code>{prefix}batchimport</code>"
            )
        
        replied_msg = message.reply_to_message
        
        if not replied_msg.document:
            return await status_msg.edit("❌ No files found in replied message!")
        
        temp_dir = Path("temp_batch_import")
        temp_dir.mkdir(exist_ok=True)
        
        total_links = 0
        total_added = 0
        total_duplicates = 0
        files_processed = 0
        
        # Download and process file
        await status_msg.edit("📥 Downloading file...")
        
        file_path = await client.download_media(
            replied_msg.document,
            file_name=temp_dir / replied_msg.document.file_name
        )
        
        await status_msg.edit("📖 Reading and processing file...")
        
        # Read JSON
        async with aiofiles.open(file_path, "r", encoding="utf-8") as f:
            content = await f.read()
            data = json.loads(content)
        
        # Extract links
        if isinstance(data, dict) and "links" in data:
            links = data.get("links", [])
        else:
            links = data if isinstance(data, list) else []
        
        if links:
            result = add_links_to_db(links)
            total_links += len(links)
            total_added += result['added']
            total_duplicates += result['duplicates_skipped']
            files_processed += 1
        
        # Cleanup
        Path(file_path).unlink(missing_ok=True)
        temp_dir.rmdir()
        
        # Final response
        await status_msg.edit(
            f"✅ **Batch Import Complete!**\n\n"
            f"📊 **Stats:**\n"
            f"• Files processed: {files_processed}\n"
            f"• Total links read: {total_links}\n"
            f"• Added to DB: {total_added}\n"
            f"• Duplicates skipped: {total_duplicates}\n"
            f"• Total in DB: {get_links_count()}"
        )
        
    except json.JSONDecodeError:
        await status_msg.edit("❌ Invalid JSON in one or more files!")
    except Exception as e:
        await status_msg.edit(f"❌ Error: {format_exc(e)}")


# === VIEW DB STATS ===
@Client.on_message(filters.command("dbstats", prefix) & filters.me)
async def view_db_stats(client: Client, message: Message):
    """View all links stored in DB"""
    links = get_all_links()
    count = len(links)
    
    text = f"📦 **TeraBox Links DB Stats**\n\n"
    text += f"Total Links Stored: <code>{count}</code>\n"
    
    if count > 0:
        text += f"\n✅ Database is populated"
    else:
        text += f"\n⚠️ Database is empty"
    
    await message.edit(text)


# === EXPORT DB TO FILE ===
@Client.on_message(filters.command("exportdb", prefix) & filters.me)
async def export_db_to_file(client: Client, message: Message):
    """Export all links from DB to JSON file"""
    status_msg = await message.edit("📤 Exporting links from DB...")
    
    try:
        links = get_all_links()
        
        if not links:
            return await status_msg.edit("⚠️ No links in DB to export!")
        
        # Save to file
        output_file = Path("terabox_all_links.json")
        async with aiofiles.open(output_file, "w", encoding="utf-8") as f:
            await f.write(json.dumps(links, ensure_ascii=False, indent=2))
        
        # Send file
        await client.send_document(
            chat_id=message.chat.id,
            document=str(output_file),
            caption=f"✅ Exported {len(links)} TeraBox links from DB"
        )
        
        await status_msg.delete()
        output_file.unlink(missing_ok=True)
        
    except Exception as e:
        await status_msg.edit(f"❌ Error: {format_exc(e)}")


# === CLEAR DB ===
@Client.on_message(filters.command("cleardb", prefix) & filters.me)
async def clear_all_links(client: Client, message: Message):
    """Clear all links from DB"""
    clear_all_links_db()
    await message.edit("🧹 Cleared all links from DB!")


# === ORIGINAL TERABOX COMMANDS ===
@Client.on_message(filters.command("autoterabox", prefix) & filters.me, group=20)
async def toggle_autoterabox(client: Client, message: Message):
    state = toggle_terabox()
    await message.edit(f"{'✅' if state else '❌'} <b>Auto TeraBox Forward</b> {'enabled' if state else 'disabled'}.")

@Client.on_message(filters.command("settb", prefix) & filters.me)
async def set_tbox_target(client: Client, message: Message):
    if len(message.command) < 2:
        return await message.edit(f"Usage: <code>{prefix}settb [chat_id]</code>")
    
    try:
        chat_id = int(message.command[1])
        set_target_chat(chat_id)
        await message.edit(f"✅ Set TeraBox target to <code>{chat_id}</code>")
    except ValueError:
        await message.edit("❌ Invalid chat ID. Must be a number.")

@Client.on_message(filters.command("addtb", prefix) & filters.me)
async def add_tbox_source(client: Client, message: Message):
    if len(message.command) < 2:
        return await message.edit(f"Usage: <code>{prefix}addtb [chat_id]</code>")
    
    try:
        chat_id = int(message.command[1])
        add_source(chat_id)
        await message.edit(f"✅ Added TeraBox source <code>{chat_id}</code>")
    except ValueError:
        await message.edit("❌ Invalid chat ID. Must be a number.")

@Client.on_message(filters.command("deltb", prefix) & filters.me)
async def del_tbox_source(client: Client, message: Message):
    if len(message.command) < 2:
        return await message.edit(f"Usage: <code>{prefix}deltb [chat_id]</code>")
    
    try:
        chat_id = int(message.command[1])
        remove_source(chat_id)
        await message.edit(f"🗑 Removed TeraBox source <code>{chat_id}</code>")
    except ValueError:
        await message.edit("❌ Invalid chat ID. Must be a number.")

@Client.on_message(filters.command("listtb", prefix) & filters.me)
async def list_tbox_sources(client: Client, message: Message):
    cfg = get_terabox_config()
    status = "✅ Enabled" if cfg.get("enabled") else "❌ Disabled"
    text = f"<b>📦 TeraBox Auto-Forward</b>\n\n<b>Status:</b> {status}\n"
    text += f"<b>Target:</b> <code>{cfg.get('target')}</code>\n\n<b>Sources:</b>\n"
    if not cfg["sources"]:
        text += "• None"
    else:
        text += "\n".join(f"• <code>{x}</code>" for x in cfg["sources"])
    text += f"\n\n<b>Seen Links:</b> {len(cfg.get('seen_links', []))}"
    await message.edit(text)

@Client.on_message(filters.command("cleartbdb", prefix) & filters.me)
async def clear_tb_db_cmd(client: Client, message: Message):
    clear_terabox_db()
    await message.edit("🧹 Cleared TeraBox forwarded link database!")

# === AUTO FORWARD ===
@Client.on_message(~filters.me)
async def terabox_auto_forward(client: Client, message: Message):
    if not is_terabox_enabled():
        return

    sources = get_sources()
    target = get_target_chat()
    if not sources or not target:
        return

    if message.chat.id not in sources:
        return

    text = message.text or message.caption
    if not text:
        return

    links = extract_terabox_links(text)
    if not links:
        return

    new_links = [link for link in links if record_link(link)]
    if not new_links:
        return

    link_text = "\n".join(new_links)

    try:
        if getattr(message, "media", None):
            await message.copy(int(target), caption=link_text)
        else:
            await client.send_message(int(target), link_text)
        await asyncio.sleep(2.5)
    except Exception as e:
        print(f"[Terabox AutoForward] Error: {e}")

@Client.on_message(filters.command("exporttb", prefix) & filters.me, group=21)
async def scrapetb_send(client: Client, message: Message):
    """
    Fast scrape TeraBox links from a source chat, save to JSON, and send the file.
    Usage: .scrapetb_send [source_chat_id] [limit|all]
    """
    try:
        args = message.text.split()
        if len(args) < 2:
            return await message.edit(
                f"<b>Usage:</b> <code>{prefix}scrapetb_send [source_chat_id] [limit|all]</code>"
            )

        source_id = int(args[1])
        limit_arg = args[2] if len(args) > 2 else "1000"
        limit = None if limit_arg.lower() == "all" else int(limit_arg)

        links = []
        msg_count = 0

        status_msg = await message.edit(f"🔍 Fetching messages from <code>{source_id}</code>...")

        # Fetch messages
        async for msg in client.get_chat_history(source_id, limit=limit):
            msg_count += 1
            text = msg.text or msg.caption
            if text:
                msg_links = extract_terabox_links(text)
                if msg_links:
                    links.extend(msg_links)

            # Update progress every 50 messages
            if msg_count % 50 == 0:
                try:
                    await status_msg.edit(f"📤 Fetched {msg_count} messages | Links found: {len(links)}")
                except Exception:
                    pass

        if not links:
            return await message.edit(f"⚠️ No TeraBox links found in {msg_count} messages.")

        # Save to JSON async
        save_path = Path("terabox_links.json")
        async with aiofiles.open(save_path, "w", encoding="utf-8") as f:
            await f.write(json.dumps(links, ensure_ascii=False, indent=2))

        # Send the file (correct way for Pyrogram)
        await client.send_document(
            chat_id=message.chat.id,
            document=str(save_path),  # ✅ Just pass the path as string
            caption=f"✅ Scraped {len(links)} TeraBox links from {msg_count} messages."
        )

        # Delete the status message after sending file
        await status_msg.delete()
        
        # Clean up the file
        save_path.unlink(missing_ok=True)

    except Exception as e:
        await message.edit(format_exc(e))
############################



# === HELP MENU ===
modules_help["tbscraper"] = {
    "importlinks": "Import links from JSON file to DB (reply to file)",
    "batchimport": "Batch import multiple files to DB (reply to message)",
    "dbstats": "View total links in DB",
    "exportdb": "Export all links from DB to JSON file",
    "cleardb": "Clear all links from DB",
    "autoterabox": "Toggle automatic TeraBox link forwarding",
    "settb [chat_id]": "Set target chat for TeraBox forwards",
    "addtb [chat_id]": "Add a source channel for TeraBox links",
    "deltb [chat_id]": "Remove a source channel",
    "listtb": "Show TeraBox forwarding config",
    "cleartbdb": "Clear seen links (allow re-forwarding)",
}
