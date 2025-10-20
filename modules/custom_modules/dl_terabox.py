import os
import time
import asyncio
import json
import re
from pathlib import Path
from pyrogram import Client, filters
from pyrogram.types import Message
from asyncio import Semaphore, gather, create_task

# Assuming 'utils' is in the Python path and provides 'db', 'prefix', 'format_exc', and 'import_library'
from utils.db import db # Needs to be accessible
from utils.misc import modules_help, prefix
from utils.scripts import format_exc, import_library

# Import necessary libraries (assuming they are installed and in utils.scripts)
aiohttp = import_library("aiohttp")
aiofiles = import_library("aiofiles")

# === CONSTANTS from original module ===
TERABOX_KEY = "terabox"
MAX_PARALLEL = 25 # limit to parallel downloads

# Regex for extracting TeraBox links
TERABOX_REGEX = re.compile(
    r"https?://[^\s]*?(?:terabox|teraboxapp|teraboxshare|nephobox|1024tera|1024terabox|freeterabox|terasharefile|terasharelink|mirrobox|momerybox|teraboxlink)\.[^\s]+",
    re.IGNORECASE
)

# === CONFIG & HELPER FUNCTIONS (REPLICATED/EXTRACTED) ===

def get_terabox_config():
    """Return the full terabox config dict"""
    return db.get(TERABOX_KEY, "config", {
        "enabled": False,
        "target": None,
        "sources": [],
        "seen_links": [],
        "download_target": None, # New Download config
        "download_source": None, # New Download config
        "auto_download_enabled": False, # New Download config
    })

def save_terabox_config(config):
    """Save the full terabox config dict"""
    db.set(TERABOX_KEY, "config", config)

def get_download_target():
    """Get the target channel for auto-downloads"""
    return get_terabox_config().get("download_target")

def set_download_target(chat_id):
    """Set target channel for auto-downloads"""
    cfg = get_terabox_config()
    cfg["download_target"] = chat_id
    save_terabox_config(cfg)

def is_auto_download_enabled():
    """Check if auto download is enabled"""
    return get_terabox_config().get("auto_download_enabled", False)

def toggle_auto_download():
    """Toggle auto download feature"""
    cfg = get_terabox_config()
    cfg["auto_download_enabled"] = not cfg.get("auto_download_enabled", False)
    save_terabox_config(cfg)
    return cfg["auto_download_enabled"]

def get_download_source():
    """Get the source channel to monitor for downloads"""
    return get_terabox_config().get("download_source")

def set_download_source(chat_id):
    """Set source channel to monitor for auto-downloads"""
    cfg = get_terabox_config()
    cfg["download_source"] = chat_id
    save_terabox_config(cfg)

def extract_terabox_links(text: str):
    if not text:
        return []
    return TERABOX_REGEX.findall(text)

# === CORE DOWNLOAD FUNCTIONS ===

async def fetch_terabox_info(session, url):
    """Fetches direct download links and file info from a worker API."""
    api_url = f"https://terabox.itxarshman.workers.dev/api?url={url}"
    async with session.get(api_url) as response:
        if response.status == 200:
            return await response.json()
        return None

async def download_file_optimized(session, url, file_path, chunk_size=1024*1024):
    """Downloads a file asynchronously with progress simulation (simple time calculation)"""
    async with session.get(url) as response:
        if response.status == 200:
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

# === INDIVIDUAL DOWNLOAD COMMAND ===

@Client.on_message(filters.command("tbdl", prefix) & filters.me)
async def terabox_download(client: Client, message: Message):
    """Downloads a single TeraBox link and uploads it to Telegram."""
    if len(message.command) < 2:
        await message.edit("<b>Usage:</b> <code>.tbdl [terabox_url]</code>")
        return

    url = message.command[1]
    status_msg = await message.edit("<b>Fetching file info...</b>")

    try:
        async with aiohttp.ClientSession() as session:
            data = await fetch_terabox_info(session, url)

            if not data or data.get("count", 0) == 0 or not data.get("links"):
                await status_msg.edit("<b>Failed to fetch file info or no downloadable file found.</b>")
                return

            file_info = data["links"][0]
            file_name = file_info.get("name", "terabox_file")
            file_size_mb = file_info.get("size_mb", 0)
            download_url = file_info.get("direct_url")

            await status_msg.edit(
                f"<b>File:</b> <code>{file_name}</code>\n"
                f"<b>Size:</b> <code>{file_size_mb} MB</code>\n"
                f"<b>Downloading...</b>"
            )

            # Use /tmp for system-level temp directory
            temp_file = Path(f"/tmp/{file_name}").resolve()
            
            success, speed, _ = await download_file_optimized(
                session, download_url, str(temp_file)
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

            category = file_info.get("category", "1") # '1' typically means video

            reply_id = message.reply_to_message.id if message.reply_to_message else None

            if category == "1":
                await client.send_video(
                    message.chat.id,
                    video=str(temp_file),
                    reply_to_message_id=reply_id
                )
            else:
                await client.send_document(
                    message.chat.id,
                    document=str(temp_file),
                    reply_to_message_id=reply_id
                )

            await status_msg.delete()

            if temp_file.exists():
                os.remove(str(temp_file))

    except Exception as e:
        await status_msg.edit(f"<b>Error:</b> <code>{format_exc(e)}</code>")
        if 'temp_file' in locals() and temp_file.exists():
            os.remove(str(temp_file))

# === BATCH DOWNLOAD COMMANDS ===

async def download_and_upload(session, client, link, chat_id, semaphore, output_dir: Path):
    """Handles the download, upload, and cleanup for one link in a batch."""
    async with semaphore:
        file_path = None
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
            success, speed, _ = await download_file_optimized(
                session, download_url, str(file_path)
            )
            
            if not success:
                return {"url": link, "status": "❌ Download failed"}
            
            # Simple check
            if not os.path.exists(str(file_path)) or os.path.getsize(str(file_path)) == 0:
                return {"url": link, "status": "❌ File not downloaded properly"}

            # === Upload ===
            try:
                if category == "1":
                    await client.send_video(chat_id, video=str(file_path))
                else:
                    await client.send_document(chat_id, document=str(file_path))
            except Exception as upload_error:
                return {"url": link, "status": f"⚠️ Upload failed: {str(upload_error)}"}

            # Cleanup
            try:
                os.remove(str(file_path))
            except:
                pass

            return {
                "url": link,
                "name": file_name,
                "size": file_size,
                "speed": f"{speed:.2f} MB/s",
                "status": "✅ Uploaded"
            }

        except Exception as e:
            # Cleanup on error
            if file_path and os.path.exists(str(file_path)):
                try:
                    os.remove(str(file_path))
                except:
                    pass
            return {"url": link, "status": f"⚠️ Error: {str(e)}"}


async def update_status_message(status_msg, links_count, results):
    """Compiles and updates the live status message."""
    done = len(results)
    
    # Calculate counts based on current results
    success_count = sum(1 for r in results if r.get("status", "").startswith("✅"))
    failed_count = sum(1 for r in results if r.get("status", "").startswith(("❌", "⚠️")))
    
    # Links that are in the queue or currently being processed
    in_progress_count = links_count - done

    # We can't know the exact count of "downloading" vs "uploading" without 
    # tracking state inside the semaphore, so we report a general "In Progress"
    # based on the remaining tasks.

    text = (
        f"⬇️ **TeraBox Batch Download Status**\n\n"
        f"📊 **Progress:** `{done}/{links_count}` Completed\n\n"
        f"🟢 **Uploaded:** `{success_count}`\n"
        f"🔴 **Failed/Errors:** `{failed_count}`\n"
        f"🟡 **In Progress:** `{in_progress_count}` (Downloading/Uploading)\n\n"
        f"_(Updating every 5 tasks or every 10 seconds...)_"
    )
    
    await status_msg.edit(text)


@Client.on_message(filters.command("bulktbdl", prefix) & filters.me)
async def batch_terabox_download(client: Client, message: Message):
    """Downloads all links in a JSON file in parallel and uploads them with live status."""
    status_msg = await message.edit("📂 Reading links from JSON file...")

    temp_dir = Path("temp_batch_dl")
    json_file_path = None
    last_update_time = time.time()
    
    try:
        # ... (File download and JSON parsing remains the same) ...
        if not message.reply_to_message or not message.reply_to_message.document:
            return await status_msg.edit("❌ Reply to a JSON file containing links!")

        # === Download JSON file ===
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
        
        links_count = len(links)
        await status_msg.edit(f"📦 Found {links_count} links — downloading (max {MAX_PARALLEL} at once)...")

        semaphore = Semaphore(MAX_PARALLEL)
        output_dir = Path("terabox_batch_downloads")
        output_dir.mkdir(exist_ok=True)
        results = []

        async with aiohttp.ClientSession() as session:
            
            async def worker(link):
                nonlocal last_update_time
                
                # The download_and_upload function is run here, which adds to results.
                result = await download_and_upload(session, client, link, message.chat.id, semaphore, output_dir)
                results.append(result)
                
                # Live Status Update Logic
                done = len(results)
                
                # Update either every 5 tasks OR if 10 seconds have passed since the last update
                current_time = time.time()
                if done % 5 == 0 or done == links_count or (current_time - last_update_time) > 10:
                    await update_status_message(status_msg, links_count, results)
                    last_update_time = current_time
                    
                return result

            tasks = [create_task(worker(link)) for link in links]
            await gather(*tasks)

        # === Summarize Results ===
        # Use the existing update_status_message one last time for the final count
        await update_status_message(status_msg, links_count, results)
        
        # Save summary JSON
        result_file = Path("batch_results.json")
        async with aiofiles.open(result_file, "w", encoding="utf-8") as f:
            await f.write(json.dumps(results, ensure_ascii=False, indent=2))

        await client.send_document(
            message.chat.id,
            document=str(result_file),
            caption="📄 Batch Download Report"
        )
        
        # Final cleanup message
        success = sum(1 for r in results if r["status"].startswith("✅"))
        failed = links_count - success
        summary = (
            f"✅ **Batch Download Complete!**\n\n"
            f"📊 **Final Stats:**\n"
            f"• Total links: `{links_count}`\n"
            f"• Uploaded: `{success}`\n"
            f"• Failed: `{failed}`\n\n"
            f"_(See attached JSON for details.)_"
        )
        await status_msg.edit(summary)


        # Cleanup
        result_file.unlink(missing_ok=True)
        Path(json_file_path).unlink(missing_ok=True)
        temp_dir.rmdir()
        
    except Exception as e:
        await status_msg.edit(f"❌ Error: <code>{format_exc(e)}</code>")
    finally:
        # Ensure cleanup in case of error
        if json_file_path and Path(json_file_path).exists():
            Path(json_file_path).unlink(missing_ok=True)
        if temp_dir.exists():

            pass # Keep temp_dir.rmdir() inside try/except/finally for safety


# === AUTO DOWNLOAD CONFIGURATION COMMANDS ===

@Client.on_message(filters.command("settbdl", prefix) & filters.me)
async def set_tbdl_target(client: Client, message: Message):
    """Set target channel for auto-downloading TeraBox links."""
    if len(message.command) < 2:
        return await message.edit(f"Usage: <code>{prefix}settbdl [chat_id]</code>")
    
    try:
        chat_id = int(message.command[1])
        set_download_target(chat_id)
        await message.edit(f"✅ Set TeraBox auto-download target to <code>{chat_id}</code>")
    except ValueError:
        await message.edit("❌ Invalid chat ID. Must be a number.")


@Client.on_message(filters.command("setdsrc", prefix) & filters.me)
async def set_tbdl_source(client: Client, message: Message):
    """Set source channel to monitor for TeraBox links to auto-download."""
    if len(message.command) < 2:
        return await message.edit(f"Usage: <code>{prefix}setdsrc [chat_id]</code>")
    
    try:
        chat_id = int(message.command[1])
        set_download_source(chat_id)
        await message.edit(f"✅ Set TeraBox auto-download source to <code>{chat_id}</code>")
    except ValueError:
        await message.edit("❌ Invalid chat ID. Must be a number.")


@Client.on_message(filters.command("autotbdl", prefix) & filters.me)
async def toggle_auto_tbdl(client: Client, message: Message):
    """Toggle automatic TeraBox downloading."""
    state = toggle_auto_download()
    await message.edit(
        f"{'✅' if state else '❌'} <b>Auto TeraBox Download</b> {'enabled' if state else 'disabled'}.\n\n"
        f"💡 Links from source channel will be automatically downloaded and uploaded to target channel."
    )


@Client.on_message(filters.command("tbdlstatus", prefix) & filters.me)
async def show_tbdl_status(client: Client, message: Message):
    """Show auto-download configuration status."""
    cfg = get_terabox_config()
    enabled = cfg.get("auto_download_enabled", False)
    source = cfg.get("download_source")
    target = cfg.get("download_target")
    
    status = "✅ Enabled" if enabled else "❌ Disabled"
    
    text = (
        f"<b>🤖 TeraBox Auto-Download Status</b>\n\n"
        f"<b>Status:</b> {status}\n"
        f"<b>Source Channel:</b> <code>{source if source else 'Not set'}</code>\n"
        f"<b>Target Channel:</b> <code>{target if target else 'Not set'}</code>\n\n"
        f"💡 <b>How it works:</b>\n"
        f"• Monitors source channel for TeraBox links\n"
        f"• Auto-downloads files\n"
        f"• Uploads to target channel\n"
    )
    
    await message.edit(text)

# === AUTO DOWNLOAD HANDLER ===

@Client.on_message(~filters.me)
async def terabox_auto_download_handler(client: Client, message: Message):
    """
    Automatically download and upload TeraBox links from monitored channel.
    This runs in the background for messages *not* sent by the userbot owner.
    """
    # Check if auto-download is enabled
    if not is_auto_download_enabled():
        return
    
    # Get config
    source = get_download_source()
    target = get_download_target()
    
    # Validate configuration
    if not source or not target:
        return
    
    # Check if message is from the source channel
    if message.chat.id != source:
        return
    
    # Extract text from message
    text = message.text or message.caption
    if not text:
        return
    
    # Find TeraBox links
    links = extract_terabox_links(text)
    if not links:
        return
    
    # Process each link
    for link in links:
        temp_file = None
        try:
            async with aiohttp.ClientSession() as session:
                # Fetch file info
                data = await fetch_terabox_info(session, link)
                
                if not data or data.get("count", 0) == 0:
                    continue
                
                file_links = data.get("links", [])
                if not file_links:
                    continue
                
                file_info = file_links[0]
                file_name = file_info.get("name", "terabox_file")
                download_url = file_info.get("direct_url")
                category = file_info.get("category", "1")
                
                # Download file
                temp_file = Path(f"/tmp/{file_name}").resolve()
                success, _, _ = await download_file_optimized(
                    session, download_url, str(temp_file)
                )
                
                if not success:
                    continue
                
                # Upload to target channel
                if category == "1":
                    await client.send_video(
                        int(target),
                        video=str(temp_file)
                    )
                else:
                    await client.send_document(
                        int(target),
                        document=str(temp_file)
                    )
                
                # Cleanup
                if os.path.exists(str(temp_file)):
                    os.remove(str(temp_file))
                
                # Small delay to avoid rate limiting
                await asyncio.sleep(3)
                
        except Exception as e:
            print(f"[TeraBox AutoDL] Error processing {link}: {e}")
            if temp_file and os.path.exists(str(temp_file)):
                os.remove(str(temp_file))
            continue

# === HELP MENU ENTRY ===
modules_help["terabox"] = {
    "tbdl [url]": "Download videos/files from TeraBox (single link)",
    "bulktbdl": "Batch download links from JSON file (reply to file)",
    "settbdl [chat_id]": "Set target channel for auto-downloading TeraBox links",
    "setdsrc [chat_id]": "Set source channel to monitor for TeraBox links (new name)",
    "autotbdl": "Toggle automatic TeraBox download & upload",
    "tbdlstatus": "Show auto-download configuration status",
}
