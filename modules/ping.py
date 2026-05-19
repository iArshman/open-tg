from pyrogram import Client, filters
from pyrogram.types import Message
import time

from utils.misc import modules_help, prefix


@Client.on_message(filters.command(["ping", "p"], prefix) & filters.me)
async def ping(client: Client, message: Message):
    start = time.time()
    sent = await message.reply("🏓")
    end = time.time()
    latency = (end - start) * 1000  # in milliseconds
    await sent.edit(f"<b>Pong! {latency:.2f}ms</b>")


modules_help["ping"] = {
    "ping": "Check bot responsiveness (ping latency in ms)",
}
