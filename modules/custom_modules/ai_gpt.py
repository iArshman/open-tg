import requests
from pyrogram import Client, enums, filters
from pyrogram.types import Message
from utils.misc import modules_help, prefix

GPT_API_URL = "https://delirius-apiofc.vercel.app/ia/gptweb?text="

async def fetch_gpt_response(query: str, message: Message, is_self: bool):
    """Fetch a response from the GPT API and send it to the user."""
    response_msg = await (message.edit("<code>Umm, lemme think...</code>") if is_self else message.reply("<code>Umm, lemme think...</code>"))
    
    try:
        response = requests.get(f"{GPT_API_URL}{query.strip()}")
        response.raise_for_status()
        data = response.json()

        if data.get("status", False):
            gpt_response = data.get("data", "No response found.")
            formatted_response = f"**Question:**\n{query}\n**Answer:**\n{gpt_response}"
        else:
            formatted_response = "Failed to fetch a response. Please try again."

        await response_msg.edit_text(formatted_response, parse_mode=enums.ParseMode.MARKDOWN)
    except requests.exceptions.RequestException:
        await response_msg.edit_text("An error occurred while connecting to the API. Please try again later.")
    except Exception:
        await response_msg.edit_text("An unexpected error occurred. Please try again.")

@Client.on_message(filters.command("gpt", prefix))
async def gpt_command(client: Client, message: Message):
    """Handle the GPT command."""
    if len(message.command) < 2:
        await message.reply(f"<b>Usage:</b> <code>{prefix}gpt [prompt]</code>")
        return

    query = " ".join(message.command[1:])

    await fetch_gpt_response(query, message, message.from_user.is_self)

modules_help["gpt"] = {
    "gpt [query]*": "Ask anything to GPT (via custom API)",
}
