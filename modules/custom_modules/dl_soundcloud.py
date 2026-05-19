import httpx
import time
import os
from pyrogram import Client, filters
from pyrogram.types import Message
from utils.misc import modules_help, prefix
from utils.scripts import progress

SEARCH_API = "https://archive.lick.eu.org/api/search/soundcloud"
DOWNLOAD_API = "https://archive.lick.eu.org/api/download/soundcloud"

@Client.on_message(filters.command(["soundcloud", "sc"], prefix))
async def soundcloud_music(client: Client, message: Message):
    query = (
        message.text.split(maxsplit=1)[1]
        if len(message.command) > 1
        else message.reply_to_message.text
        if message.reply_to_message
        else None
    )
    if not query:
        await (message.edit if getattr(message.from_user, "is_self", False) else message.reply)(
            f"<b>Usage:</b> <code>{prefix}scmusic [song name]</code>"
        )
        return

    status_message = await (message.edit_text if getattr(message.from_user, "is_self", False) else message.reply)(
        f"<code>Searching for {query} on SoundCloud...</code>"
    )

    async with httpx.AsyncClient() as client_httpx:
        try:
            search_resp = await client_httpx.get(SEARCH_API, params={"query": query})
            search = search_resp.json()
            song = search.get("result", [{}])[0] if search.get("status") and search.get("result") else None
            if not song or not song.get("url"):
                await status_message.edit_text("<code>No results found.</code>")
                return
        except Exception:
            await status_message.edit_text("<code>Failed to search. The API may be down.</code>")
            return

        song_title = song.get("title", "Unknown Title")
        song_url = song.get("url")

        await status_message.edit_text(f"<code>Found: {song_title}</code>\n<code>Fetching download link...</code>")

        try:
            dl_resp = await client_httpx.get(DOWNLOAD_API, params={"url": song_url})
            dl = dl_resp.json()
            result = dl.get("result", {}) if dl.get("status") else {}
            song_download_link = result.get("url")
            song_author = result.get("author", {}).get("username", "Unknown Artist")
            song_thumbnail = result.get("imageURL")
            if not song_download_link or not song_download_link.startswith("http"):
                await status_message.edit_text("<code>Song isn't available for download.</code>")
                return
        except Exception:
            await status_message.edit_text("<code>Failed to fetch download link. The API may be down.</code>")
            return

        await status_message.edit_text(f"<code>Downloading {song_title}...</code>")
        try:
            if song_thumbnail:
                thumb = await client_httpx.get(song_thumbnail)
                with open(f"{song_title}.jpg", "wb") as f:
                    f.write(thumb.content)
            audio = await client_httpx.get(song_download_link)
            if "audio" not in audio.headers.get("content-type", ""):
                raise Exception
            with open(f"{song_title}.mp3", "wb") as f:
                f.write(audio.content)
        except Exception:
            await status_message.edit_text("<code>Failed to download audio.</code>")
            return

    await status_message.edit_text(f"<code>Uploading {song_title}...</code>")
    try:
        c_time = time.time()
        await client.send_audio(
            message.chat.id,
            f"{song_title}.mp3",
            caption=f"<b>Song Name:</b> {song_title}\n<b>Artist:</b> {song_author}",
            progress=progress,
            progress_args=(status_message, c_time, f"<code>Uploading {song_title}...</code>"),
            thumb=f"{song_title}.jpg" if os.path.exists(f"{song_title}.jpg") else None,
        )
    except Exception:
        await status_message.edit_text("<code>Failed to upload.</code>")
        return
    finally:
        for file in [f"{song_title}.jpg", f"{song_title}.mp3"]:
            if os.path.exists(file):
                os.remove(file)
    await status_message.delete()

modules_help["soundcloud"] = {
    "soundcloud": "search, download, and upload songs."
}
