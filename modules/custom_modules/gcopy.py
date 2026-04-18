import asyncio
import os
import re
import shutil
import zipfile
from collections import defaultdict
from datetime import datetime

from pyrogram import Client, filters, enums
from pyrogram.errors import FloodWait, RPCError
from pyrogram.raw import functions, types as raw_types
from pyrogram.types import Message

from utils.misc import modules_help, prefix
from utils.scripts import format_exc

# ── state ────────────────────────────────────────────────────────────────────
_running: bool = False
_cancel:  bool = False

TEMP_DIR   = "gcopy_temp"
SEND_DELAY = 1.2


def _reset():
    global _running, _cancel
    _running = False
    _cancel  = False


def _ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _safe_name(name: str) -> str:
    return re.sub(r'[\\/:*?"<>|]', "_", str(name)).strip() or "unnamed"


def _is_service(msg: Message) -> bool:
    return bool(
        msg.new_chat_members
        or msg.left_chat_member
        or msg.new_chat_title
        or msg.new_chat_photo
        or msg.pinned_message
        or msg.migrate_to_chat_id
        or msg.migrate_from_chat_id
        or getattr(msg, "service", None)
    )


def _msg_to_text(msg: Message) -> str | None:
    if _is_service(msg) or not msg.text:
        return None
    dt = msg.date.strftime("%Y-%m-%d %H:%M:%S") if msg.date else "?"
    sender = (
        (msg.from_user.full_name if msg.from_user else None)
        or (msg.sender_chat.title if msg.sender_chat else None)
        or "Unknown"
    )
    return f"[{dt}] {sender}: {msg.text}"


# ── fetch all forum topic names via raw API ───────────────────────────────────

async def _get_topic_names(client: Client, chat_id: int) -> dict[int, str]:
    """
    Returns {topic_id: topic_name} for all topics in a forum group.
    Uses raw GetForumTopics API — works on any pyrofork version.
    """
    topic_names = {}
    try:
        peer = await client.resolve_peer(chat_id)
        offset_id    = 0
        offset_date  = 0
        offset_topic = 0
        limit        = 100

        while True:
            result = await client.invoke(
                functions.channels.GetForumTopics(
                    channel=peer,
                    q="",
                    offset_date=offset_date,
                    offset_id=offset_id,
                    offset_topic=offset_topic,
                    limit=limit,
                )
            )
            for topic in result.topics:
                topic_names[topic.id] = topic.title
            if len(result.topics) < limit:
                break
            last = result.topics[-1]
            offset_topic = last.id
            offset_date  = last.date
            offset_id    = 0

    except Exception:
        pass  # not a forum group or no permission — silent fallback

    return topic_names


# ── fast fetch ────────────────────────────────────────────────────────────────

async def _fetch_all(client, src_id, thread_id, status_msg) -> list:
    msgs  = []
    count = 0
    async for msg in client.get_chat_history(src_id):
        if _cancel:
            break
        count += 1
        if thread_id and msg.message_thread_id != thread_id:
            continue
        msgs.append(msg)
        if count % 100 == 0:
            try:
                await status_msg.edit(
                    f"⏳ Fetching… {count} scanned, {len(msgs)} matched"
                )
            except Exception:
                pass
    msgs.reverse()
    return msgs


# ── .gcopy ───────────────────────────────────────────────────────────────────

@Client.on_message(filters.command(["gcopy"], prefix) & filters.me)
async def gcopy(client: Client, message: Message):
    global _running, _cancel

    args        = message.command[1:]
    auto_src    = message.chat.id
    auto_thread = message.message_thread_id

    if len(args) == 1:
        try:
            dst_id = int(args[0]); src_id = auto_src; thread_id = auto_thread
        except ValueError:
            return await message.edit("❌ Invalid destination ID.")
    elif len(args) == 2:
        try:
            src_id = int(args[0]); dst_id = int(args[1]); thread_id = None
        except ValueError:
            return await message.edit("❌ Invalid IDs.")
    elif len(args) == 3:
        try:
            src_id = int(args[0]); thread_id = int(args[1]); dst_id = int(args[2])
        except ValueError:
            return await message.edit("❌ Invalid IDs.")
    else:
        return await message.edit(
            f"<b>Usage:</b>\n"
            f"<code>{prefix}gcopy &lt;dst&gt;</code> — current thread → dst\n"
            f"<code>{prefix}gcopy &lt;src&gt; &lt;dst&gt;</code> — full group\n"
            f"<code>{prefix}gcopy &lt;src&gt; &lt;thread_id&gt; &lt;dst&gt;</code> — specific thread"
        )

    if _running:
        return await message.edit(f"⚠️ Job running. Use <code>{prefix}gcopystop</code> to cancel.")

    _running = True; _cancel = False
    copied = skipped = failed = 0
    last_err = ""

    status = await message.edit(
        f"⏳ <b>Fetching…</b> <code>{src_id}</code>"
        + (f" › thread <code>{thread_id}</code>" if thread_id else "")
        + f" → <code>{dst_id}</code>"
    )

    # Get topic names upfront if copying a thread
    dst_thread_id = None
    if thread_id:
        await status.edit("⏳ Resolving topic name…")
        topic_names = await _get_topic_names(client, src_id)
        thread_name = topic_names.get(thread_id, f"Thread {thread_id}")

        try:
            created = await client.create_forum_topic(dst_id, thread_name)
            dst_thread_id = created.id
            await status.edit(f"✅ Created topic '<b>{thread_name}</b>' in dst. Fetching messages…")
        except Exception as e:
            dst_thread_id = None
            await status.edit(f"⚠️ Could not create topic ({e}), copying to general…")

    # Fast fetch
    try:
        msgs = await _fetch_all(client, src_id, thread_id, status)
    except Exception as e:
        _reset()
        return await status.edit(f"❌ Fetch failed:\n<code>{format_exc(e)}</code>")

    total = len(msgs)
    if total == 0:
        _reset()
        return await status.edit("⚠️ No messages found.")

    await status.edit(
        f"📋 <b>{total} messages.</b> Copying…\n"
        + (f"→ topic: <b>{thread_name}</b>\n" if thread_id and dst_thread_id else "")
        + f"<code>{prefix}gcopystop</code> to cancel."
    )

    for i, msg in enumerate(msgs, 1):
        if _cancel:
            break
        if _is_service(msg):
            skipped += 1
            continue

        try:
            await msg.copy(chat_id=dst_id, message_thread_id=dst_thread_id)
            copied += 1
        except FloodWait as fw:
            await asyncio.sleep(fw.value + 1)
            try:
                await msg.copy(chat_id=dst_id, message_thread_id=dst_thread_id)
                copied += 1
            except Exception as e:
                last_err = str(e)[:80]; failed += 1
        except Exception as e:
            last_err = str(e)[:80]; failed += 1

        if i % 25 == 0 or i == total:
            try:
                await status.edit(
                    f"⏳ {i}/{total} — ✅ {copied}  ⏭ {skipped}  ❌ {failed}"
                    + (f"\n<code>{last_err}</code>" if last_err else "")
                )
            except Exception:
                pass
        await asyncio.sleep(SEND_DELAY)

    _reset()
    await status.edit(
        f"{'🛑 Cancelled' if _cancel else '✅ Done!'}\n\n"
        f"Total: {total} | Copied: {copied} | Skipped: {skipped} | Failed: {failed}"
        + (f"\nLast error: <code>{last_err}</code>" if last_err else "")
    )


# ── .gdownload ───────────────────────────────────────────────────────────────

@Client.on_message(filters.command(["gdownload", "gdl"], prefix) & filters.me)
async def gdownload(client: Client, message: Message):
    global _running, _cancel

    args = message.command[1:]
    auto_src = message.chat.id
    auto_thread = message.message_thread_id
    password = None

    # --- Feature: Smart Password Detection ---
 
    if args and not args[-1].lstrip('-').isdigit():
        password = args.pop()

    # --- Argument Parsing ---
    if len(args) == 0:
        src_id = auto_src; thread_id = auto_thread
    elif len(args) == 1:
        try:
            src_id = int(args[0]); thread_id = None
        except ValueError:
            return await message.edit("❌ Invalid chat ID.")
    elif len(args) == 2:
        try:
            src_id = int(args[0]); thread_id = int(args[1])
        except ValueError:
            return await message.edit("❌ Invalid IDs.")
    else:
        return await message.edit(
            f"<b>Usage:</b>\n"
            f"<code>{prefix}gdl [password]</code>\n"
            f"<code>{prefix}gdl &lt;chat_id&gt; [password]</code>\n"
            f"<code>{prefix}gdl &lt;chat_id&gt; &lt;thread_id&gt; [password]</code>"
        )

    if _running:
        return await message.edit(f"⚠️ Job running. Use <code>{prefix}gcopystop</code> to cancel.")

    _running = True; _cancel = False

    try:
        chat_obj = await client.get_chat(src_id)
        chat_title = _safe_name(chat_obj.title or chat_obj.first_name or str(src_id))
    except Exception:
        chat_title = f"chat_{abs(src_id)}"

    run_dir = os.path.join(TEMP_DIR, _ts())
    os.makedirs(run_dir, exist_ok=True)

    status = await message.edit(f"⏳ Resolving topic names for <b>{chat_title}</b>…")
    topic_names = await _get_topic_names(client, src_id)

    await status.edit(
        f"⏳ <b>Fetching messages fast…</b>\n"
        f"<b>{chat_title}</b> — {len(topic_names)} topics found"
        + (f" › thread <code>{thread_id}</code>" if thread_id else "")
    )

    try:
        msgs = await _fetch_all(client, src_id, thread_id, status)
    except Exception as e:
        _reset(); shutil.rmtree(run_dir, ignore_errors=True)
        return await status.edit(f"❌ Fetch failed:\n<code>{format_exc(e)}</code>")

    total = len(msgs)
    if total == 0:
        _reset(); shutil.rmtree(run_dir, ignore_errors=True)
        return await status.edit("⚠️ No messages found.")

    threads = defaultdict(list)
    for msg in msgs:
        threads[msg.message_thread_id].append(msg)

    media_total = sum(1 for m in msgs if m.media and not _is_service(m))
    await status.edit(
        f"📥 <b>{total} messages</b> in {len(threads)} threads — {media_total} media\n"
        f"<code>{prefix}gcopystop</code> to cancel."
    )

    downloaded = failed = processed = 0
    for tid, thread_msgs in threads.items():
        if _cancel: break
        folder_name = "general" if tid is None else _safe_name(topic_names.get(tid, f"thread_{tid}"))
        thread_dir = os.path.join(run_dir, folder_name)
        media_dir = os.path.join(thread_dir, "media")
        os.makedirs(media_dir, exist_ok=True)
        text_lines = []

        for msg in thread_msgs:
            if _cancel: break
            processed += 1
            line = _msg_to_text(msg)
            if line: text_lines.append(line)

            if msg.media and not _is_service(msg):
                try:
                    await client.download_media(msg, file_name=media_dir + "/")
                    downloaded += 1
                except FloodWait as fw:
                    await asyncio.sleep(fw.value + 1)
                    await client.download_media(msg, file_name=media_dir + "/")
                    downloaded += 1
                except Exception: failed += 1

            if processed % 20 == 0:
                await status.edit(f"📥 <b>{processed}/{total}</b> — 📁 <b>{folder_name}</b>\nMedia: ✅ {downloaded} ❌ {failed}")
            await asyncio.sleep(0)

        with open(os.path.join(thread_dir, "messages.txt"), "w", encoding="utf-8") as f:
            f.write(f"Chat: {chat_title} ({src_id})\nExported: {datetime.now()}\n\n" + "\n".join(text_lines))

    if _cancel:
        _reset(); shutil.rmtree(run_dir, ignore_errors=True)
        return await status.edit("🛑 Cancelled.")

    # --- Feature: Conditional ZIP (Hybrid) ---
    await status.edit(f"🗜 Creating {'Locked ' if password else ''}ZIP…")
    zip_label = _safe_name(topic_names.get(thread_id, chat_title)) if thread_id else chat_title
    zip_name = os.path.join(TEMP_DIR, f"{zip_label}.zip")

    try:
        all_files = []
        for root, _, files in os.walk(run_dir):
            for f in files:
                all_files.append(os.path.join(root, f))
        
        if password:
            import pyminizip
            arc_names = [os.path.relpath(f, run_dir) for f in all_files]
            # Level 5 compression used
            pyminizip.compress_multiple(all_files, arc_names, zip_name, password, 5)
        else:
            with zipfile.ZipFile(zip_name, "w", zipfile.ZIP_DEFLATED) as zf:
                for f in all_files:
                    zf.write(f, arcname=os.path.relpath(f, run_dir))
    except Exception as e:
        _reset(); shutil.rmtree(run_dir, ignore_errors=True)
        return await status.edit(f"❌ ZIP failed: {e}")

    zip_mb = os.path.getsize(zip_name) / (1024 * 1024)
    await status.edit(f"📤 Uploading <b>{zip_label}.zip</b> ({zip_mb:.1f} MB)…")

    try:
        caption = (
            f"📦 <b>{zip_label}</b>\n"
            f"Status: {'🔐 Password Protected' if password else '🔓 Open ZIP'}\n"
            f"Threads: {len(threads)} | Msgs: {total} | Size: {zip_mb:.1f} MB"
        )
        await client.send_document(
            "me", document=zip_name, file_name=f"{zip_label}.zip",
            caption=caption, parse_mode=enums.ParseMode.HTML
        )
    except Exception as e:
        await status.edit(f"❌ Upload failed: {e}")
    finally:
        shutil.rmtree(run_dir, ignore_errors=True)
        if os.path.exists(zip_name): os.remove(zip_name)

    _reset()
    await status.edit(f"✅ <b>Done!</b> Archive sent to <b>Saved Messages</b>.")

# ── .gcopystop ───────────────────────────────────────────────────────────────

@Client.on_message(filters.command(["gcopystop", "gcstop"], prefix) & filters.me)
async def gcopystop(client: Client, message: Message):
    global _cancel
    if not _running:
        return await message.edit("ℹ️ No job is currently running.")
    _cancel = True
    await message.edit("🛑 <b>Stop signal sent.</b> Job will halt shortly.")


# ── help ─────────────────────────────────────────────────────────────────────

modules_help["gcopy"] = {
    "gcopy <dst>": "Copy current thread → dst (auto-detects src+thread)",
    "gcopy <src> <dst>": "Copy full group src → dst",
    "gcopy <src> <thread_id> <dst>": "Copy specific thread → dst (creates matching topic in dst)",
    "gdownload": "Download current thread (auto) → ZIP → Saved Messages",
    "gdownload <chat_id>": "Download ALL threads, one folder per thread → ZIP",
    "gdownload <chat_id> <thread_id>": "Download one specific thread → ZIP",
    "gdl": "Alias for gdownload",
    "gcopystop / gcstop": "Cancel running job",
}
