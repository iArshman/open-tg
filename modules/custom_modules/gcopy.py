import asyncio
import os
import re
import shutil
import zipfile
from collections import defaultdict
from datetime import datetime
import pyzipper

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

    args        = message.command[1:]
    auto_src    = message.chat.id
    auto_thread = message.message_thread_id

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
            f"<code>{prefix}gdownload</code> — current thread (auto)\n"
            f"<code>{prefix}gdownload &lt;chat_id&gt;</code> — all threads, folder per thread\n"
            f"<code>{prefix}gdownload &lt;chat_id&gt; &lt;thread_id&gt;</code> — one thread"
        )

    if _running:
        return await message.edit(f"⚠️ Job running. Use <code>{prefix}gcopystop</code> to cancel.")

    _running = True; _cancel = False

    try:
        chat_obj   = await client.get_chat(src_id)
        chat_title = _safe_name(chat_obj.title or chat_obj.first_name or str(src_id))
    except Exception:
        chat_title = f"chat_{abs(src_id)}"

    run_dir = os.path.join(TEMP_DIR, _ts())
    os.makedirs(run_dir, exist_ok=True)

    status = await message.edit(f"⏳ Resolving topic names for <b>{chat_title}</b>…")

    # Fetch ALL topic names upfront via raw API
    topic_names = await _get_topic_names(client, src_id)

    await status.edit(
        f"⏳ <b>Fetching messages fast…</b>\n"
        f"<b>{chat_title}</b> — {len(topic_names)} topics found"
        + (f" › thread <code>{thread_id}</code>" if thread_id else "")
    )

    try:
        msgs = await _fetch_all(client, src_id, thread_id, status)
    except Exception as e:
        _reset()
        shutil.rmtree(run_dir, ignore_errors=True)
        return await status.edit(f"❌ Fetch failed:\n<code>{format_exc(e)}</code>")

    total = len(msgs)
    if total == 0:
        _reset()
        shutil.rmtree(run_dir, ignore_errors=True)
        return await status.edit("⚠️ No messages found.")

    # Group by thread
    threads: dict[int | None, list] = defaultdict(list)
    for msg in msgs:
        threads[msg.message_thread_id].append(msg)

    media_total = sum(1 for m in msgs if m.media and not _is_service(m))
    await status.edit(
        f"📥 <b>{total} messages</b> in <b>{len(threads)} thread(s)</b> — {media_total} media\n"
        f"<code>{prefix}gcopystop</code> to cancel."
    )

    downloaded = failed = processed = 0

    for tid, thread_msgs in threads.items():
        if _cancel:
            break

        # Folder name = actual topic name from API, not thread_id
        if tid is None:
            folder_name = "general"
        else:
            folder_name = _safe_name(topic_names.get(tid, f"thread_{tid}"))

        thread_dir = os.path.join(run_dir, folder_name)
        media_dir  = os.path.join(thread_dir, "media")
        os.makedirs(media_dir, exist_ok=True)

        text_lines = []

        for msg in thread_msgs:
            if _cancel:
                break
            processed += 1

            line = _msg_to_text(msg)
            if line:
                text_lines.append(line)

            if msg.media and not _is_service(msg):
                try:
                    await client.download_media(msg, file_name=media_dir + "/")
                    downloaded += 1
                except FloodWait as fw:
                    await asyncio.sleep(fw.value + 1)
                    try:
                        await client.download_media(msg, file_name=media_dir + "/")
                        downloaded += 1
                    except Exception:
                        failed += 1
                except Exception:
                    failed += 1

            if processed % 20 == 0:
                try:
                    await status.edit(
                        f"📥 <b>{processed}/{total}</b> — 📁 <b>{folder_name}</b>\n"
                        f"Media: ✅ {downloaded} ❌ {failed}"
                    )
                except Exception:
                    pass

            await asyncio.sleep(0)

        # Write messages.txt (text only)
        txt_path = os.path.join(thread_dir, "messages.txt")
        with open(txt_path, "w", encoding="utf-8") as f:
            header = f"Chat: {chat_title} ({src_id})"
            if tid:
                header += f" | Topic: {folder_name} (id:{tid})"
            f.write(header + "\n")
            f.write(f"Exported: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Text messages: {len(text_lines)}\n")
            f.write("=" * 60 + "\n\n")
            f.write("\n".join(text_lines))

    if _cancel:
        _reset()
        shutil.rmtree(run_dir, ignore_errors=True)
        return await status.edit(f"🛑 Cancelled. Processed {processed}/{total}.")

    await status.edit("🗜 Creating ZIP…")

    if thread_id:
        zip_label = _safe_name(topic_names.get(thread_id, f"thread_{thread_id}"))
    else:
        zip_label = chat_title

    zip_name = os.path.join(TEMP_DIR, f"{zip_label}.zip")
    try:
        with zipfile.ZipFile(zip_name, "w", zipfile.ZIP_DEFLATED) as zf:
            for root, _, files in os.walk(run_dir):
                for fname in files:
                    full     = os.path.join(root, fname)
                    arc_name = os.path.relpath(full, run_dir)
                    zf.write(full, arcname=arc_name)
    except Exception as e:
        _reset()
        shutil.rmtree(run_dir, ignore_errors=True)
        return await status.edit(f"❌ ZIP failed:\n<code>{format_exc(e)}</code>")

    zip_mb = os.path.getsize(zip_name) / (1024 * 1024)
    await status.edit(f"📤 Uploading <b>{zip_label}.zip</b> ({zip_mb:.1f} MB)…")

    try:
        await client.send_document(
            "me",
            document=zip_name,
            file_name=f"{zip_label}.zip",
            caption=(
                f"📦 <b>{zip_label}</b>\nGroup: {chat_title}\n"
                f"Threads: {len(threads)} | Messages: {total} | Media: {downloaded} | {zip_mb:.1f} MB"
            ),
            parse_mode=enums.ParseMode.HTML,
        )
    except Exception as e:
        _reset()
        return await status.edit(f"❌ Upload failed:\n<code>{format_exc(e)}</code>")
    finally:
        shutil.rmtree(run_dir, ignore_errors=True)
        try:
            os.remove(zip_name)
        except Exception:
            pass

    _reset()
    await status.edit(
        f"<b>Done!</b> — <code>{zip_label}.zip</code>\n\n"
        f"Threads: {len(threads)} | Messages: {total}\n"
        f"Media: {downloaded} | Failed: {failed} | Size: {zip_mb:.1f} MB\n\n"
        f"Check your <b>Saved Messages</b>."
    )

# ── .gdlp (Password Protected Download) ──────────────────────────────────────

@Client.on_message(filters.command(["gdlp"], prefix) & filters.me)
async def gdownload_protected(client: Client, message: Message):
    global _running, _cancel

    args = message.command[1:]
    auto_src = message.chat.id
    auto_thread = message.message_thread_id

    # Password check
    if len(args) == 0:
        return await message.edit(f"❌ <b>Password required!</b>\nUsage: <code>{prefix}gdlp &lt;password&gt; [chat_id] [thread_id]</code>")

    password = args[0]
    target_args = args[1:]

    # Target assignment
    if len(target_args) == 0:
        src_id = auto_src; thread_id = auto_thread
    elif len(target_args) == 1:
        try:
            src_id = int(target_args[0]); thread_id = None
        except ValueError:
            return await message.edit("❌ Invalid chat ID.")
    elif len(target_args) == 2:
        try:
            src_id = int(target_args[0]); thread_id = int(target_args[1])
        except ValueError:
            return await message.edit("❌ Invalid IDs.")
    else:
        return await message.edit(
            f"<b>Usage:</b>\n"
            f"<code>{prefix}gdlp &lt;pass&gt;</code> — current thread\n"
            f"<code>{prefix}gdlp &lt;pass&gt; &lt;chat_id&gt;</code> — all threads\n"
            f"<code>{prefix}gdlp &lt;pass&gt; &lt;chat_id&gt; &lt;thread_id&gt;</code> — one thread"
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
        _reset()
        shutil.rmtree(run_dir, ignore_errors=True)
        return await status.edit(f"❌ Fetch failed:\n<code>{format_exc(e)}</code>")

    total = len(msgs)
    if total == 0:
        _reset()
        shutil.rmtree(run_dir, ignore_errors=True)
        return await status.edit("⚠️ No messages found.")

    threads: dict[int | None, list] = defaultdict(list)
    for msg in msgs:
        threads[msg.message_thread_id].append(msg)

    media_total = sum(1 for m in msgs if m.media and not _is_service(m))
    await status.edit(
        f"📥 <b>{total} messages</b> in <b>{len(threads)} thread(s)</b> — {media_total} media\n"
        f"<code>{prefix}gcopystop</code> to cancel."
    )

    downloaded = failed = processed = 0

    for tid, thread_msgs in threads.items():
        if _cancel:
            break

        if tid is None:
            folder_name = "general"
        else:
            folder_name = _safe_name(topic_names.get(tid, f"thread_{tid}"))

        thread_dir = os.path.join(run_dir, folder_name)
        media_dir = os.path.join(thread_dir, "media")
        os.makedirs(media_dir, exist_ok=True)
        text_lines = []

        for msg in thread_msgs:
            if _cancel:
                break
            processed += 1

            line = _msg_to_text(msg)
            if line:
                text_lines.append(line)

            if msg.media and not _is_service(msg):
                try:
                    await client.download_media(msg, file_name=media_dir + "/")
                    downloaded += 1
                except FloodWait as fw:
                    await asyncio.sleep(fw.value + 1)
                    try:
                        await client.download_media(msg, file_name=media_dir + "/")
                        downloaded += 1
                    except Exception:
                        failed += 1
                except Exception:
                    failed += 1

            if processed % 20 == 0:
                try:
                    await status.edit(
                        f"📥 <b>{processed}/{total}</b> — 📁 <b>{folder_name}</b>\n"
                        f"Media: ✅ {downloaded} ❌ {failed}"
                    )
                except Exception:
                    pass
            await asyncio.sleep(0)

        txt_path = os.path.join(thread_dir, "messages.txt")
        with open(txt_path, "w", encoding="utf-8") as f:
            header = f"Chat: {chat_title} ({src_id})"
            if tid:
                header += f" | Topic: {folder_name} (id:{tid})"
            f.write(header + "\n")
            f.write(f"Exported: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Text messages: {len(text_lines)}\n")
            f.write("=" * 60 + "\n\n")
            f.write("\n".join(text_lines))

    if _cancel:
        _reset()
        shutil.rmtree(run_dir, ignore_errors=True)
        return await status.edit(f"🛑 Cancelled. Processed {processed}/{total}.")

    await status.edit(f"🗜 Creating Password-Protected ZIP (Pass: {password})…")

    if thread_id:
        zip_label = _safe_name(topic_names.get(thread_id, f"thread_{thread_id}"))
    else:
        zip_label = chat_title

    zip_name = os.path.join(TEMP_DIR, f"{zip_label}_locked.zip")
    
    # Secure Password Protected Zip Creation using Pyzipper
    try:
        with pyzipper.AESZipFile(zip_name, 'w', compression=pyzipper.ZIP_DEFLATED, encryption=pyzipper.WZ_AES) as zf:
            zf.setpassword(password.encode('utf-8'))
            for root, _, files in os.walk(run_dir):
                for fname in files:
                    full = os.path.join(root, fname)
                    arc_name = os.path.relpath(full, run_dir)
                    zf.write(full, arcname=arc_name)
    except Exception as e:
        _reset()
        shutil.rmtree(run_dir, ignore_errors=True)
        return await status.edit(f"❌ Protected ZIP failed:\n<code>{format_exc(e)}</code>")

    zip_mb = os.path.getsize(zip_name) / (1024 * 1024)
    await status.edit(f"📤 Uploading <b>{zip_label}_locked.zip</b> ({zip_mb:.1f} MB)…")

    try:
        await client.send_document(
            "me",
            document=zip_name,
            file_name=f"{zip_label}_locked.zip",
            caption=(
                f"📦 <b>{zip_label} (LOCKED)</b>\nGroup: {chat_title}\n"
                f"Password: <code>{password}</code>\n"
                f"Threads: {len(threads)} | Messages: {total} | Media: {downloaded} | {zip_mb:.1f} MB"
            ),
            parse_mode=enums.ParseMode.HTML,
        )
    except Exception as e:
        _reset()
        return await status.edit(f"❌ Upload failed:\n<code>{format_exc(e)}</code>")
    finally:
        shutil.rmtree(run_dir, ignore_errors=True)
        try:
            os.remove(zip_name)
        except Exception:
            pass

    _reset()
    await status.edit(
        f"✅ <b>Done!</b> — <code>{zip_label}_locked.zip</code>\n\n"
        f"Password: <code>{password}</code>\n"
        f"Threads: {len(threads)} | Messages: {total}\n"
        f"Media: {downloaded} | Failed: {failed} | Size: {zip_mb:.1f} MB\n\n"
        f"Check your <b>Saved Messages</b>."
    )
    
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
