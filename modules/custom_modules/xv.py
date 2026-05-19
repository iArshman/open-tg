import requests
from pyrogram import Client, filters, enums
from pyrogram.types import Message
from io import BytesIO
from utils.misc import modules_help, prefix

BASE_URL = "https://api-aswin-sparky.koyeb.app/api"
SEARCH_ENDPOINT = "/search/xvideos?search="
DOWNLOAD_ENDPOINT = "/downloader/xdl?url="

search_results = {}

def fetch_data(endpoint, param):
    try:
        url = f"{BASE_URL}{endpoint}{requests.utils.quote(param)}"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching data from {url}: {e}")
        return None

def video_search(query):
    data = fetch_data(SEARCH_ENDPOINT, query)
    return data.get("data") if data and data.get("status") else None

def get_video_download_link(video_url):
    data = fetch_data(DOWNLOAD_ENDPOINT, video_url)
    if data and data.get("status"):
        return data["data"]
    return None

@Client.on_message(filters.command(["xvideos", "xv"], prefix))
async def xvideos_command(client, message: Message):
    global search_results

    if len(message.command) < 2:
        await message.edit(f"**Usage:** `{prefix}xvideos [query]`", parse_mode=enums.ParseMode.MARKDOWN)
        return

    arg = " ".join(message.command[1:])

    if arg.isdigit():
        number = int(arg) - 1

        if message.chat.id not in search_results or number < 0 or number >= len(search_results[message.chat.id]):
            await message.edit("Use a valid number from the search results.", parse_mode=enums.ParseMode.MARKDOWN)
            return

        selected_result = search_results[message.chat.id][number]
        await message.edit(f"Downloading **{selected_result['title']}**, please wait...", parse_mode=enums.ParseMode.MARKDOWN)

        try:
            download_link = get_video_download_link(selected_result["url"])
            if not download_link:
                await message.edit("Failed to fetch the download link.", parse_mode=enums.ParseMode.MARKDOWN)
                return

            video_data = BytesIO(requests.get(download_link).content)
            video_data.name = f"{number + 1}.mp4"

            caption = f">**Title:** {selected_result['title']}\n**Duration:** {selected_result['duration']}"

            await client.send_video(
                chat_id=message.chat.id,
                video=video_data,
                caption=caption,
                parse_mode=enums.ParseMode.MARKDOWN
            )

            await message.delete()
        except Exception as e:
            await message.edit(f"An error occurred: {str(e)}", parse_mode=enums.ParseMode.MARKDOWN)
        return

    query = arg
    await message.edit(f"Searching for `{query}`...", parse_mode=enums.ParseMode.MARKDOWN)

    try:
        results = video_search(query)
        if not results:
            await message.edit("No results found. Please try again.", parse_mode=enums.ParseMode.MARKDOWN)
            return

        search_results[message.chat.id] = results

        results_text = "\n".join(
            [f"{i + 1}. [{item['title']}]({item['url']})\n> Duration: {item['duration']}" for i, item in enumerate(results[:15])]
        )
        await message.edit(
            f"**XVideos Search Results for `{query}`:**\n\n{results_text}\n\n**Usage:** `{prefix}xvideos [number]` to download a video.", 
            parse_mode=enums.ParseMode.MARKDOWN,
            disable_web_page_preview=True
        )

    except Exception as e:
        await message.edit(f"An error occurred: {str(e)}", parse_mode=enums.ParseMode.MARKDOWN)

modules_help["xvideos"] = {
    "xvideos [query]": "Search for videos and display results from XVideos.",
    "xvideos [number]": "Download the selected video by providing its number.",
    "xv [query]": "Shortened command for `.xvideos [query]`.",
  }
