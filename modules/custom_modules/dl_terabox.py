import os
import time
import asyncio
import json
import re
from pathlib import Path
from pyrogram import Client, filters
from pyrogram.types import Message
from asyncio import Semaphore, gather, create_task, Queue

# Assuming 'utils' is in the Python path and provides 'db', 'prefix', 'format_exc', and 'import_library'
# NOTE: These imports are crucial for the script to run in its intended environment.
from utils.db import db
from utils.misc import modules_help, prefix
from utils.scripts import format_exc, import_library

# Import necessary libraries (assuming they are installed and in utils.scripts)
aiohttp = import_library("aiohttp")
aiofiles = import_library("aiofiles")

# === CONSTANTS from original module ===
TERABOX_KEY = "terabox"
MAX_PARALLEL = 25 # limit to parallel downloads (used for Downloader workers)

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
        "download_target": None,
        "download_source": None,
        "auto_download_enabled": False,
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

    # Use a new session for the single download
    async with aiohttp.ClientSession() as session:
        temp_file = None
        try:
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
            if temp_file and temp_file.exists():
                os.remove(str(temp_file))


# === BATCH DOWNLOAD WORKERS (PIPELINE STAGES) ===

async def update_status_message(status_msg, links_count, results, download_active, upload_active):
    """
    Compiles and updates the live status message. 
    It infers the number of waiting files from the total.
    """
    done = len(results)
    
    success_count = sum(1 for r in results if r.get("status", "").startswith("✅"))
    failed_count = sum(1 for r in results if r.get("status", "").startswith(("❌", "⚠️")))
    
    # Files remaining in the entire pipeline
    files_remaining = links_count - done
    
    # Files actively waiting in the download queue (inferred)
    # Total remaining minus those actively being DL'd or UL'd
    queue_waiting = max(0, files_remaining - download_active - upload_active)

    text = (
        f"⬇️ **TeraBox Batch Download Status**\n\n"
        f"📊 **Progress:** `{done}/{links_count}` Completed\n\n"
        f"🟢 **Uploaded:** `{success_count}`\n"
        f"🔴 **Failed/Errors:** `{failed_count}`\n"
        f"🟡 **Downloading:** `{download_active}` of {MAX_PARALLEL}\n"
        f"🔵 **Uploading:** `{upload_active}`\n"
        f"📦 **Waiting in Queue:** `{queue_waiting}`\n\n"
        f"**🌀 Total Remaining in Pipeline:** `{files_remaining}`\n"
        f"_(Updating every 5 files or every 10 seconds...)_"
    )
    
    try:
        await status_msg.edit(text)
    except Exception:
        pass # Ignore update failures to keep the pipeline moving


async def fetcher_worker(session, links, download_queue):
    """STAGE 1 (Producer): Fetches API data for all links and populates the download queue."""
    for link in links:
        try:
            info = await fetch_terabox_info(session, link)
            if info and info.get("links"):
                file_info = info["links"][0]
                item_data = {
                    "link": link,
                    "url": file_info.get("direct_url"),
                    "name": file_info.get("name", "unknown_file"),
                    "category": file_info.get("category", "1"),
                    "size_mb": file_info.get("size_mb", 0)
                }
                await download_queue.put(item_data)
            else:
                await download_queue.put({"link": link, "status": "❌ Fetch failed"})
        except Exception as e:
            await download_queue.put({"link": link, "status": f"❌ Fetch error: {str(e)}"})

    # Sentinel value to signal the end of the queue for all consumers (MAX_PARALLEL downloaders)
    for _ in range(MAX_PARALLEL):
        await download_queue.put(None)


async def downloader_worker(session, download_queue, upload_queue, semaphore, output_dir):
    """STAGE 2 (Consumer/Producer): Downloads the file, and populates the upload queue."""
    while True:
        item = await download_queue.get()
        
        if item is None:
            await download_queue.put(None) # Re-add sentinel for next downloader
            download_queue.task_done()
            break
        
        # If fetch failed, pass it directly to the Uploader to log
        if item.get("status", "").startswith("❌"):
            await upload_queue.put(item)
            download_queue.task_done()
            continue

        link = item['link']
        
        # Use the semaphore to limit concurrent downloads to MAX_PARALLEL
        async with semaphore:
            file_path = None
            try:
                file_name = item['name']
                file_path = output_dir / file_name
                download_url = item['url']

                # === Download ===
                success, speed, _ = await download_file_optimized(
                    session, download_url, str(file_path)
                )
                
                if not success or not file_path.exists() or os.path.getsize(str(file_path)) == 0:
                    result = {"url": link, "status": "❌ Download failed"}
                else:
                    # Success: Put into upload queue
                    result = {
                        "url": link,
                        "name": file_name,
                        "path": str(file_path),
                        "category": item['category'],
                        "size": os.path.getsize(str(file_path)) / (1024*1024),
                        "speed": f"{speed:.2f} MB/s",
                        "status": "Ready for upload" # Temporary status
                    }

            except Exception as e:
                result = {"url": link, "status": f"⚠️ Download error: {str(e)}"}
            
            await upload_queue.put(result)
            download_queue.task_done()


async def uploader_worker(client, upload_queue, results, links_count, status_msg, semaphore):
    """STAGE 3 (Consumer): Uploads the file, performs cleanup, and updates status."""
    active_uploads = 0
    last_update_time = time.time()
    
    while len(results) < links_count:
        
        item = None
        try:
            # Wait for 1 second max to allow for periodic status updates if the queue is empty
            item = await asyncio.wait_for(upload_queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            pass # Item is None, continue to status update check
            
        # --- Status Update Check ---
        current_time = time.time()
        # Update if an item was processed, 10 seconds passed, or the batch is nearly finished
        if item is None or len(results) % 5 == 0 or (current_time - last_update_time) > 10 or len(results) == links_count - 1:
            
            # This is the correct way to get the number of active downloaders
            download_active = MAX_PARALLEL - semaphore._value 
            
            await update_status_message(status_msg, links_count, results, 
                                        download_active, active_uploads)
            last_update_time = current_time

        if item is None:
            continue

        # --- Item Processing ---
        active_uploads += 1
        
        # Check for immediate failure results passed from previous stages
        if item.get("status", "").startswith(("❌", "⚠️")):
            results.append(item)
            active_uploads -= 1
            upload_queue.task_done()
            continue

        # Item is a downloaded file ready for upload
        file_path = Path(item.get('path'))
        
        try:
            # === Upload ===
            if item['category'] == "1":
                await client.send_video(
                    status_msg.chat.id, 
                    video=str(file_path), 
                    caption=f"Uploaded from TeraBox: `{item['name']}`"
                ) 
            else:
                await client.send_document(
                    status_msg.chat.id, 
                    document=str(file_path),
                    caption=f"Uploaded from TeraBox: `{item['name']}`"
                )

            item["status"] = "✅ Uploaded"
            
        except Exception as upload_error:
            item["status"] = f"⚠️ Upload failed: {str(upload_error)}"
            
        finally:
            # Cleanup and final result tracking
            if file_path and file_path.exists():
                try:
                    os.remove(str(file_path))
                except:
                    pass

            results.append(item)
            active_uploads -= 1
            upload_queue.task_done()


@Client.on_message(filters.command("bulktbdl", prefix) & filters.me)
async def batch_terabox_download(client: Client, message: Message):
    """Downloads all links in a JSON file using a 3-stage pipeline (Fetcher, 25x Downloader, Uploader)"""
    status_msg = await message.edit("📂 Reading links from JSON file...")

    temp_dir = Path("temp_batch_dl")
    json_file_path = None
    
    try:
        if not message.reply_to_message or not message.reply_to_message.document:
            return await status_msg.edit("❌ Reply to a JSON file containing links!")

        # --- JSON Parsing ---
        temp_dir.mkdir(exist_ok=True)
        json_file_path = await client.download_media(
            message.reply_to_message.document,
            file_name=temp_dir / message.reply_to_message.document.file_name
        )
        async with aiofiles.open(json_file_path, "r", encoding="utf-8") as f:
            data = json.loads(await f.read())
        
        links = data.get("links") if isinstance(data, dict) and "links" in data else (data if isinstance(data, list) else [])
        links_count = len(links)
        
        if not links:
            return await status_msg.edit("⚠️ No links found in file!")

        await status_msg.edit(f"📦 Found {links_count} links. Starting pipeline (25 parallel downloads)...")
        
        # --- Setup Pipeline Resources ---
        download_queue = Queue()
        upload_queue = Queue()
        semaphore = Semaphore(MAX_PARALLEL) # Controls Downloader limit
        output_dir = Path("terabox_batch_downloads")
        output_dir.mkdir(exist_ok=True)
        results = []

        # ClientSession must be created here to be passed to aiohttp calls
        async with aiohttp.ClientSession() as session:
            
            # --- Start Workers ---
            # 1. Fetcher (Producer)
            fetcher_task = create_task(fetcher_worker(session, links, download_queue))
            
            # 2. Downloader (Consumer/Producer, MAX_PARALLEL instances)
            downloader_tasks = [
                create_task(downloader_worker(session, download_queue, upload_queue, semaphore, output_dir))
                for _ in range(MAX_PARALLEL)
            ]
            
            # 3. Uploader (Consumer, 1 instance)
            # FIX: Removed the extra 'download_queue' argument which caused the TypeError
            uploader_task = create_task(uploader_worker(client, upload_queue, results, links_count, status_msg, semaphore))

            # Wait for all workers to complete
            await gather(fetcher_task, *downloader_tasks, uploader_task)

        # --- Final Summary and Report ---
        
        # Final cleanup message
        success = sum(1 for r in results if r["status"].startswith("✅"))
        failed = links_count - success
        
        # Ensure final status message is accurate
        download_active = MAX_PARALLEL - semaphore._value # Should be 0 here
        await update_status_message(status_msg, links_count, results, download_active, 0)
        
        summary = (
            f"✅ **Batch Download Complete!**\n\n"
            f"📊 **Final Stats:**\n"
            f"• Total links: `{links_count}`\n"
            f"• Uploaded: `{success}`\n"
            f"• Failed: `{failed}`\n\n"
            f"_(See attached JSON for details.)_"
        )
        await status_msg.edit(summary)

        # Save summary JSON
        result_file = Path("batch_results.json")
        async with aiofiles.open(result_file, "w", encoding="utf-8") as f:
            await f.write(json.dumps(results, ensure_ascii=False, indent=2))

        await client.send_document(
            message.chat.id,
            document=str(result_file),
            caption="📄 Batch Download Report"
        )

        # Cleanup
        result_file.unlink(missing_ok=True)
        Path(json_file_path).unlink(missing_ok=True)
        
    except Exception as e:
        await status_msg.edit(f"❌ Error: <code>{format_exc(e)}</code>")
    finally:
        # Final cleanup safety net
        if json_file_path and Path(json_file_path).exists():
            Path(json_file_path).unlink(missing_ok=True)
        if temp_dir.exists():
             try:
                 temp_dir.rmdir()
             except OSError:
                 pass # Directory might not be empty

# === AUTO DOWNLOAD CONFIGURATION COMMANDS (UNCHANGED) ===

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

# === AUTO DOWNLOAD HANDLER (UNCHANGED) ===

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
        # Use a new session for the auto-download
        async with aiohttp.ClientSession() as session:
            try:
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
    "bulktbdl": "Batch download links from JSON file (reply to file). Uses 3-stage pipeline.",
    "settbdl [chat_id]": "Set target channel for auto-downloading TeraBox links",
    "setdsrc [chat_id]": "Set source channel to monitor for TeraBox links (new name)",
    "autotbdl": "Toggle automatic TeraBox download & upload",
    "tbdlstatus": "Show auto-download configuration status",
}
