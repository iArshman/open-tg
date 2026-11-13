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
MIN_PASSWORD_LEN = 10
DL_CONCURRENCY = 6     # media files downloaded at the same time
FILE_SEM = 16          # pyrogram chunk requests in flight (library default is 1!)


def _reset():
    global _running, _cancel, _flood_until
    _flood_until = 0.0
    _running = False
    _cancel  = False


def _ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _norm_id(n: int) -> int:
    """Fix supergroup/channel IDs typed without the leading minus (1002586861559 -> -1002586861559)."""
    if n > 0 and len(str(n)) >= 13 and str(n).startswith("100"):
        return -n
    return n


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

# ── shared flood-wait gate + throttled progress (a FloodWait hits the whole account) ──

_flood_until: float = 0.0
_last_edit: float = 0.0
EDIT_INTERVAL = 4.0   # seconds between status edits (Telegram flood-limits edits quickly)


def _now() -> float:
    return asyncio.get_event_loop().time()


async def _gate():
    """Every request waits here while an account-wide flood wait is active."""
    while True:
        remaining = _flood_until - _now()
        if remaining <= 0:
            return
        await asyncio.sleep(min(remaining, 5))


def _note_flood(seconds: int):
    global _flood_until
    _flood_until = max(_flood_until, _now() + seconds + 1)


async def _progress(status, text: str, force: bool = False):
    """Edit the status message at most once per EDIT_INTERVAL; never raises."""
    global _last_edit
    if not force and _now() - _last_edit < EDIT_INTERVAL:
        return
    _last_edit = _now()
    try:
        await status.edit(text)
    except FloodWait as fw:
        _note_flood(fw.value)
    except Exception:
        pass


async def _upload_progress(current, total, status, label, started):
    """pyrogram progress callback: live upload %, MB and speed in the status message."""
    elapsed = max(_now() - started, 0.001)
    speed = current / elapsed / (1024 * 1024)
    pct = (current / total * 100) if total else 0
    filled = int(pct // 10)
    bar = "█" * filled + "░" * (10 - filled)
    eta = ""
    if speed > 0 and total:
        left = int((total - current) / (speed * 1024 * 1024))
        eta = f" | ETA {left // 60}m {left % 60}s" if left >= 60 else f" | ETA {left}s"
    await _progress(
        status,
        f"📤 <b>{label}</b>\n{bar} <b>{pct:.0f}%</b>\n"
        f"{current / 1048576:.1f} / {total / 1048576:.1f} MB | {speed:.1f} MB/s{eta}",
        force=(current >= total),
    )


async def _boost(client: Client):
    """pyrogram limits chunk requests to 1 at a time by default; this is the main
    reason downloads crawl. Raise it for this client."""
    try:
        client.get_file_semaphore = asyncio.Semaphore(FILE_SEM)
        client.save_file_semaphore = asyncio.Semaphore(FILE_SEM)
    except Exception:
        pass


async def _warm_peer(client: Client, chat_id: int):
    """Session is in_memory, so the peer cache is empty after every restart.
    Load dialogs once if the chat cannot be resolved yet."""
    try:
        await client.get_chat(chat_id)
        return
    except Exception:
        pass
    try:
        async for _ in client.get_dialogs():
            pass
    except Exception:
        pass


async def _fetch_all(client, src_id, thread_id, status_msg) -> list:
    msgs  = []
    count = 0
    last  = 0.0

    async def _tick():
        nonlocal last
        await _gate()
        await _progress(status_msg, f"⏳ Fetching… {count} scanned, {len(msgs)} matched")

    # Fast path: ask the SERVER for only this topic's messages instead of
    # scanning the whole group history and throwing most of it away.
    if thread_id:
        try:
            async for msg in client.search_messages(src_id, query="", thread_id=thread_id):
                if _cancel:
                    break
                count += 1
                msgs.append(msg)
                await _tick()
        except Exception:
            msgs.clear(); count = 0
        if msgs:
            msgs.reverse()
            return msgs

    async for msg in client.get_chat_history(src_id):
        if _cancel:
            break
        count += 1
        if thread_id and msg.message_thread_id != thread_id:
            await _tick()
            continue
        msgs.append(msg)
        await _tick()
    msgs.reverse()
    return msgs


async def _dl_one(client, msg, media_dir, sem, stats):
    async with sem:
        for _ in range(3):
            if _cancel:
                return
            await _gate()
            try:
                await client.download_media(msg, file_name=media_dir + "/")
                stats["ok"] += 1
                return
            except FloodWait as fw:
                _note_flood(fw.value)
            except Exception:
                break
        stats["fail"] += 1


async def _process_thread(client, thread_msgs, media_dir, status, label, done_so_far, total):
    """Collect text instantly, download all media of the thread in parallel."""
    text_lines, media_msgs = [], []
    for msg in thread_msgs:
        line = _msg_to_text(msg)
        if line:
            text_lines.append(line)
        if msg.media and not _is_service(msg):
            media_msgs.append(msg)

    stats = {"ok": 0, "fail": 0}
    sem = asyncio.Semaphore(DL_CONCURRENCY)
    pending = {asyncio.create_task(_dl_one(client, m, media_dir, sem, stats)) for m in media_msgs}
    all_tasks = list(pending)
    while pending:
        _, pending = await asyncio.wait(pending, timeout=3)
        if _cancel:
            for t in pending:
                t.cancel()
            break
        await _progress(
            status,
            f"📥 <b>{done_so_far + len(thread_msgs)}/{total}</b> msgs\n"
            f"Media: ✅ {stats['ok']} ❌ {stats['fail']} / {len(media_msgs)}"
            + (f"\n⏸ Flood wait, resuming shortly…" if _flood_until > _now() else "")
        )
    await asyncio.gather(*all_tasks, return_exceptions=True)
    return text_lines, stats["ok"], stats["fail"]


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
            src_id = _norm_id(int(args[0])); dst_id = int(args[1]); thread_id = None
        except ValueError:
            return await message.edit("❌ Invalid IDs.")
    elif len(args) == 3:
        try:
            src_id = _norm_id(int(args[0])); thread_id = int(args[1]); dst_id = int(args[2])
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

        await _gate()
        try:
            await msg.copy(chat_id=dst_id, message_thread_id=dst_thread_id)
            copied += 1
        except FloodWait as fw:
            _note_flood(fw.value)
            await _gate()
            try:
                await msg.copy(chat_id=dst_id, message_thread_id=dst_thread_id)
                copied += 1
            except Exception as e:
                last_err = str(e)[:80]; failed += 1
        except Exception as e:
            last_err = str(e)[:80]; failed += 1

        await _progress(
            status,
            f"⏳ {i}/{total} — ✅ {copied}  ⏭ {skipped}  ❌ {failed}"
            + (f"\n<code>{last_err}</code>" if last_err else ""),
            force=(i == total),
        )
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
            src_id = _norm_id(int(args[0])); thread_id = None
        except ValueError:
            return await message.edit("❌ Invalid chat ID.")
    elif len(args) == 2:
        try:
            src_id = _norm_id(int(args[0])); thread_id = int(args[1])
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
    await _boost(client)
    await _warm_peer(client, src_id)

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

        text_lines, ok, bad = await _process_thread(
            client, thread_msgs, media_dir, status, folder_name, processed, total
        )
        processed += len(thread_msgs)
        downloaded += ok
        failed += bad

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
            progress=_upload_progress,
            progress_args=(status, "Uploading", _now()),
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


# ── .gdlp (Private / Encrypted Download) ─────────────────────────────────────
#
# Privacy design:
#   * The command message containing the password is DELETED immediately.
#   * Password is NEVER written to captions, status messages or logs.
#   * Output is a 2-layer archive: the inner ZIP (with real folder/file names)
#     is stored as a single AES-256 encrypted entry called "data.bin" inside
#     a generic-named outer ZIP, so file names / chat names / topic names are
#     NOT visible without the password.
#   * Caption, file name and status messages contain no chat/topic names.
#   * All temp files are removed afterwards.
#
# To open:  extract outer zip with password (7-Zip / WinRAR) -> you get
#           data.bin -> rename to data.zip -> extract (no password needed).

def _build_locked_zip(run_dir: str, inner_path: str, outer_path: str, password: str):
    """Blocking helper (run in a thread). Builds inner zip then AES-256 wraps it."""
    with zipfile.ZipFile(inner_path, "w", zipfile.ZIP_STORED) as zf:
        for root, _, files in os.walk(run_dir):
            for fname in files:
                full = os.path.join(root, fname)
                zf.write(full, arcname=os.path.relpath(full, run_dir))

    with pyzipper.AESZipFile(
        outer_path, "w",
        compression=pyzipper.ZIP_DEFLATED,
        encryption=pyzipper.WZ_AES,
    ) as zf:
        zf.setpassword(password.encode("utf-8"))
        zf.setencryption(pyzipper.WZ_AES, nbits=256)
        zf.write(inner_path, arcname="data.bin")


@Client.on_message(filters.command(["gdlp"], prefix) & filters.me)
async def gdownload_protected(client: Client, message: Message):
    global _running, _cancel

    args = message.command[1:]
    auto_src = message.chat.id
    auto_thread = message.message_thread_id
    cmd_chat = message.chat.id

    if len(args) == 0:
        return await message.edit(
            f"❌ <b>Password required!</b>\n"
            f"Usage: <code>{prefix}gdlp &lt;password&gt; [chat_id] [thread_id] [-n]</code>\n"
            f"<code>-n</code> = use the real chat/topic name as the zip file name (visible to anyone who sees the file).\n"
            f"Password min {MIN_PASSWORD_LEN} chars, no spaces."
        )

    password = args[0]
    use_name = "-n" in args[1:]
    target_args = [a for a in args[1:] if a != "-n"]

    # Remove the command (which contains the password) from chat right away.
    try:
        await message.delete()
    except Exception:
        try:
            await message.edit("🔐 …")
        except Exception:
            pass

    status = await client.send_message(
        cmd_chat, "🔐 Preparing…", message_thread_id=auto_thread
    )

    if len(password) < MIN_PASSWORD_LEN:
        return await status.edit(
            f"❌ Password too short (min {MIN_PASSWORD_LEN} characters). Use a long random one."
        )

    # Target assignment
    if len(target_args) == 0:
        src_id = auto_src; thread_id = auto_thread
    elif len(target_args) == 1:
        try:
            src_id = _norm_id(int(target_args[0])); thread_id = None
        except ValueError:
            return await status.edit("❌ Invalid chat ID.")
    elif len(target_args) == 2:
        try:
            src_id = _norm_id(int(target_args[0])); thread_id = int(target_args[1])
        except ValueError:
            return await status.edit("❌ Invalid IDs.")
    else:
        return await status.edit(
            f"<b>Usage:</b>\n"
            f"<code>{prefix}gdlp &lt;pass&gt;</code> — current thread\n"
            f"<code>{prefix}gdlp &lt;pass&gt; &lt;chat_id&gt;</code> — all threads\n"
            f"<code>{prefix}gdlp &lt;pass&gt; &lt;chat_id&gt; &lt;thread_id&gt;</code> — one thread"
        )

    if _running:
        return await status.edit(f"⚠️ Job running. Use <code>{prefix}gcopystop</code> to cancel.")

    _running = True; _cancel = False
    await _boost(client)
    await _warm_peer(client, src_id)

    try:
        chat_obj = await client.get_chat(src_id)
        chat_title = _safe_name(chat_obj.title or chat_obj.first_name or str(src_id))
    except Exception:
        chat_title = f"chat_{abs(src_id)}"

    run_dir = os.path.join(TEMP_DIR, _ts())
    os.makedirs(run_dir, exist_ok=True)

    topic_names = await _get_topic_names(client, src_id)

    await status.edit("⏳ <b>Fetching messages…</b>")

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
        f"📥 <b>{total} messages</b> — {media_total} media\n"
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

        text_lines, ok, bad = await _process_thread(
            client, thread_msgs, media_dir, status, folder_name, processed, total
        )
        processed += len(thread_msgs)
        downloaded += ok
        failed += bad

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

    await status.edit("🗜 Encrypting (AES-256)…")

    stamp = _ts()
    inner_path = os.path.join(TEMP_DIR, f"{stamp}_inner.tmp")
    if use_name:
        base = topic_names.get(thread_id, f"thread_{thread_id}") if thread_id else chat_title
        pretty = datetime.now().strftime("%Y-%m-%d_%H-%M")
        out_name = f"{_safe_name(base)[:80]}_{pretty}_locked.zip"
    else:
        out_name = f"backup_{stamp}.zip"
    zip_name = os.path.join(TEMP_DIR, f"{stamp}_{out_name}")

    def _cleanup_files():
        shutil.rmtree(run_dir, ignore_errors=True)
        for p in (inner_path, zip_name):
            try:
                os.remove(p)
            except Exception:
                pass

    try:
        await asyncio.to_thread(_build_locked_zip, run_dir, inner_path, zip_name, password)
    except Exception as e:
        _reset()
        _cleanup_files()
        return await status.edit(f"❌ Encryption failed:\n<code>{format_exc(e)}</code>")

    # plaintext no longer needed
    shutil.rmtree(run_dir, ignore_errors=True)
    try:
        os.remove(inner_path)
    except Exception:
        pass

    zip_mb = os.path.getsize(zip_name) / (1024 * 1024)
    await status.edit(f"📤 Uploading encrypted backup ({zip_mb:.1f} MB)…")

    try:
        await client.send_document(
            "me",
            document=zip_name,
            file_name=out_name,
            caption=(
                f"🔒 <b>{_safe_name(base)[:80]}</b>\n📅 {datetime.now().strftime('%Y-%m-%d %H:%M')} | {zip_mb:.1f} MB"
                if use_name else f"🔒 Encrypted backup | {zip_mb:.1f} MB"
            ),
            parse_mode=enums.ParseMode.HTML,
            progress=_upload_progress,
            progress_args=(status, "Uploading encrypted backup", _now()),
        )
    except Exception as e:
        _reset()
        return await status.edit(f"❌ Upload failed:\n<code>{format_exc(e)}</code>")
    finally:
        _cleanup_files()

    _reset()
    password = None  # drop reference
    await status.edit(
        f"✅ <b>Done!</b> Encrypted backup sent to <b>Saved Messages</b>.\n"
        f"Messages: {total} | Media: {downloaded} | Failed: {failed} | {zip_mb:.1f} MB\n\n"
        f"Password was not stored anywhere. Keep it in your password manager."
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
    "gdlp <pass> [chat_id] [thread_id] [-n]": "PRIVATE download: AES-256, hidden file names, password deleted from chat & never stored. Zip is named backup_<time>.zip; add -n to name it <chat/topic>_<date>_locked.zip with a date caption. Run it from Saved Messages with explicit IDs",
    "gcopystop / gcstop": "Cancel running job",
}
