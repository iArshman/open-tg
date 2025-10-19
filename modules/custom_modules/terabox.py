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
    r"https?://[^\s]*?(?:terabox|teraboxapp|teraboxshare|nephobox|1024tera)\.[^\s]+",
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
@Client.on_message(filters.command("autoterabox", prefix) & filters.me)
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



import os
import time
import asyncio
from pyrogram import Client, filters
from pyrogram.types import Message
from utils.misc import modules_help, prefix
from utils.scripts import import_library

aiohttp = import_library("aiohttp")
aiofiles = import_library("aiofiles")




async def fetch_terabox_info(session, url):
    api_url = f"https://terabox.itxarshman.workers.dev/api?url={url}"
    async with session.get(api_url) as response:
        if response.status == 200:
            return await response.json()
        return None


async def download_file_optimized(session, url, file_path, chunk_size=1024*1024):
    async with session.get(url) as response:
        if response.status == 200:
            total_size = int(response.headers.get('content-length', 0))
            downloaded = 0
            start_time = time.time()

            async with aiofiles.open(file_path, 'wb') as f:
                async for chunk in response.content.iter_chunked(chunk_size):
                    await f.write(chunk)
                    downloaded += len(chunk)

            elapsed_time = time.time() - start_time
            speed_mbps = (downloaded / (1024 * 1024)) / elapsed_time if elapsed_time > 0 else 0
            return True, speed_mbps, downloaded
        return False, 0, 0


@Client.on_message(filters.command("tbdl", prefix) & filters.me)
async def terabox_download(client: Client, message: Message):
    if len(message.command) < 2:
        await message.edit("<b>Usage:</b> <code>.tbdl [terabox_url]</code>")
        return

    url = message.command[1]
    status_msg = await message.edit("<b>Fetching file info...</b>")

    try:
        async with aiohttp.ClientSession() as session:
            data = await fetch_terabox_info(session, url)

            if not data or data.get("count", 0) == 0:
                await status_msg.edit("<b>Failed to fetch file info from API</b>")
                return

            links = data.get("links", [])
            if not links:
                await status_msg.edit("<b>No downloadable files found</b>")
                return

            file_info = links[0]
            file_name = file_info.get("name", "terabox_file")
            file_size_mb = file_info.get("size_mb", 0)
            download_url = file_info.get("direct_url")

            await status_msg.edit(
                f"<b>File:</b> <code>{file_name}</code>\n"
                f"<b>Size:</b> <code>{file_size_mb} MB</code>\n"
                f"<b>Downloading...</b>"
            )

            temp_file = f"/tmp/{file_name}"

            success, speed, downloaded_bytes = await download_file_optimized(
                session, download_url, temp_file
            )

            if not success:
                await status_msg.edit("<b>Download failed</b>")
                return

            await status_msg.edit(
                f"<b>File:</b> <code>{file_name}</code>\n"
                f"<b>Size:</b> <code>{file_size_mb} MB</code>\n"
                f"<b>Speed:</b> <code>{speed:.2f} MB/s</code>\n"
                f"<b>Uploading to Telegram...</b>"
            )

            category = file_info.get("category", "1")
            thumb = file_info.get("thumb")

            if category == "1":
                await client.send_video(
                    message.chat.id,
                    video=temp_file,
                    reply_to_message_id=message.reply_to_message.id if message.reply_to_message else None
                )
            else:
                await client.send_document(
                    message.chat.id,
                    document=temp_file,
                    reply_to_message_id=message.reply_to_message.id if message.reply_to_message else None
                )

            await status_msg.delete()

            if os.path.exists(temp_file):
                os.remove(temp_file)

    except Exception as e:
        await status_msg.edit(f"<b>Error:</b> <code>{str(e)}</code>")


from asyncio import Semaphore, gather, create_task

MAX_PARALLEL = 10  # limit to 10 downloads at once


async def download_and_upload(session, client, link, chat_id, semaphore, output_dir: Path):
    """
    Downloads one TeraBox link and uploads it to Telegram after download.
    Returns a result dict for summary.
    """
    async with semaphore:
        try:
            info = await fetch_terabox_info(session, link)
            if not info or not info.get("links"):
                return {"url": link, "status": "❌ No file info"}

            file_info = info["links"][0]
            file_name = file_info.get("name", "unknown_file")
            file_path = output_dir / file_name
            file_size = file_info.get("size_mb", 0)
            download_url = file_info.get("direct_url")
            category = file_info.get("category", "1")

            # === Download ===
            success, speed, downloaded_bytes = await download_file_optimized(session, download_url, file_path)
            if not success:
                return {"url": link, "status": "❌ Download failed"}

            # === Upload ===
            caption = (
                f"<b>{file_name}</b>\n"
                f"💾 <code>{file_size} MB</code>\n"
                f"⚡ <code>{speed:.2f} MB/s</code>"
            )

            if category == "1":
                await client.send_video(chat_id, video=str(file_path), caption=caption)
            else:
                await client.send_document(chat_id, document=str(file_path), caption=caption)

            # Delete file after upload to save space
            os.remove(file_path)

            return {
                "url": link,
                "name": file_name,
                "size": file_size,
                "speed": f"{speed:.2f} MB/s",
                "status": "✅ Uploaded"
            }

        except Exception as e:
            return {"url": link, "status": f"⚠️ Error: {str(e)}"}


@Client.on_message(filters.command("bulktbdl", prefix) & filters.me)
async def batch_terabox_download(client: Client, message: Message):
    """
    📦 Batch TeraBox Downloader
    Usage: Reply to a JSON file with .bulktbdl
    Downloads all links in parallel (max 10) and uploads automatically.
    """
    status_msg = await message.edit("📂 Reading links from JSON file...")

    try:
        if not message.reply_to_message or not message.reply_to_message.document:
            return await status_msg.edit("❌ Reply to a JSON file containing links!")

        # === Download JSON file ===
        temp_dir = Path("temp_batch_dl")
        temp_dir.mkdir(exist_ok=True)
        json_file_path = await client.download_media(
            message.reply_to_message.document,
            file_name=temp_dir / message.reply_to_message.document.file_name
        )

        # === Parse JSON ===
        async with aiofiles.open(json_file_path, "r", encoding="utf-8") as f:
            data = json.loads(await f.read())

        if isinstance(data, dict) and "links" in data:
            links = data["links"]
        elif isinstance(data, list):
            links = data
        else:
            return await status_msg.edit("⚠️ Invalid JSON — must be list or {links: []}")

        if not links:
            return await status_msg.edit("⚠️ No links found in file!")

        await status_msg.edit(f"📦 Found {len(links)} links — downloading (max {MAX_PARALLEL} at once)...")

        semaphore = Semaphore(MAX_PARALLEL)
        output_dir = Path("terabox_batch_downloads")
        output_dir.mkdir(exist_ok=True)
        results = []

        async with aiohttp.ClientSession() as session:
            async def worker(link):
                result = await download_and_upload(session, client, link, message.chat.id, semaphore, output_dir)
                results.append(result)
                done = len(results)
                if done % 3 == 0 or done == len(links):
                    await status_msg.edit(f"⬇️ Progress: {done}/{len(links)} completed...")
                return result

            tasks = [create_task(worker(link)) for link in links]
            await gather(*tasks)

        # === Summarize Results ===
        success = sum(1 for r in results if r["status"].startswith("✅"))
        failed = len(results) - success

        summary = (
            f"✅ <b>Batch Download Complete!</b>\n\n"
            f"📊 <b>Stats:</b>\n"
            f"• Total links: <code>{len(results)}</code>\n"
            f"• Uploaded: <code>{success}</code>\n"
            f"• Failed: <code>{failed}</code>"
        )

        # Save summary JSON
        result_file = output_dir / "batch_results.json"
        async with aiofiles.open(result_file, "w", encoding="utf-8") as f:
            await f.write(json.dumps(results, ensure_ascii=False, indent=2))

        await client.send_document(
            message.chat.id,
            document=str(result_file),
            caption="📄 Batch Download Report"
        )

        await status_msg.edit(summary)

        # Cleanup
        Path(json_file_path).unlink(missing_ok=True)

    except Exception as e:
        await status_msg.edit(f"❌ Error: <code>{format_exc(e)}</code>")

# === HELP MENU ===
modules_help["terabox"] = {
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
    "tbdl [url]": "Download videos/files from TeraBox with optimized server-side downloading",
}
