import os
import logging
import asyncio
import socket
import platform
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer, make_server

from pyrogram import Client, idle, errors
from pyrogram.enums.parse_mode import ParseMode
from pyrogram.raw.functions.account import GetAuthorizations, DeleteAccount
import requests
from bottle import ServerAdapter

from app import bottle_app

from utils import config
from utils.db import db
from utils.misc import gitrepo, userbot_version
from utils.scripts import restart
from utils.rentry import rentry_cleanup_job
from utils.module import ModuleManager

SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))
if SCRIPT_PATH != os.getcwd():
    os.chdir(SCRIPT_PATH)


# Custom AsyncWSGIRefServer — runs Bottle in a thread alongside asyncio
# Based on https://github.com/bottlepy/bottle/blob/master/bottle.py#L3419
class AsyncWSGIRefServer(ServerAdapter):
    srv = None

    def run(self, app):
        class QuietHandler(WSGIRequestHandler):
            def log_request(*args, **kw):
                pass

        handler_cls = self.options.get("handler_class", QuietHandler)
        server_cls = self.options.get("server_class", WSGIServer)

        if ":" in self.host:
            if getattr(server_cls, "address_family") == socket.AF_INET:
                class IPv6Server(server_cls):
                    address_family = socket.AF_INET6
                server_cls = IPv6Server

        self.srv = make_server(self.host, self.port, app, server_cls, handler_cls)
        self.srv.serve_forever()

    def shutdown(self):
        if self.srv:
            self.srv.shutdown()

if not config.STRINGSESSION:
    raise RuntimeError(
        "STRINGSESSION is required! Please set it in your .env file.\n"
        "Generate one using: python string_gen.py"
    )

common_params = {
    "api_id": config.api_id,
    "api_hash": config.api_hash,
    "session_string": config.STRINGSESSION,
    "name": ":memory:",
    "hide_password": True,
    "workdir": SCRIPT_PATH,
    "app_version": userbot_version,
    "device_model": f"Moon-Userbot @ {gitrepo.head.commit.hexsha[:7]}",
    "system_version": platform.version() + " " + platform.machine(),
    "sleep_threshold": 30,
    "test_mode": config.test_server,
    "parse_mode": ParseMode.HTML,
    "in_memory": True,
}

app = Client(**common_params)


def load_missing_modules():
    all_modules = db.get("custom.modules", "allModules", [])
    if not all_modules:
        return

    custom_modules_path = f"{SCRIPT_PATH}/modules/custom_modules"
    os.makedirs(custom_modules_path, exist_ok=True)

    try:
        f = requests.get(
            "https://raw.githubusercontent.com/The-MoonTg-project/custom_modules/main/full.txt"
        ).text
    except Exception:
        logging.error("Failed to fetch custom modules list")
        return
    modules_dict = {
        line.split("/")[-1].split()[0]: line.strip() for line in f.splitlines()
    }

    for module_name in all_modules:
        module_path = f"{custom_modules_path}/{module_name}.py"
        if not os.path.exists(module_path) and module_name in modules_dict:
            url = f"https://raw.githubusercontent.com/The-MoonTg-project/custom_modules/main/{modules_dict[module_name]}.py"
            resp = requests.get(url)
            if resp.ok:
                with open(module_path, "wb") as f:
                    f.write(resp.content)
                logging.info("Loaded missing module: %s", module_name)
            else:
                logging.warning("Failed to load module: %s", module_name)


async def main():
    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler("moonlogs.txt"), logging.StreamHandler()],
        level=logging.INFO,
    )
    DeleteAccount.__new__ = None

    # Patch Pyrogram's remove_handler to suppress ValueError
    from pyrogram import dispatcher
    original_remove_handler = dispatcher.Dispatcher.remove_handler

    async def patched_remove_handler(self, handler, group):
        async def wrapped():
            try:
                self.groups[group].remove(handler)
            except ValueError:
                pass

        await self.handler_worker_tasks[0]
        await self.locks_list[group].acquire()

        task = self.loop.create_task(wrapped())
        await task

        self.locks_list[group].release()

    dispatcher.Dispatcher.remove_handler = patched_remove_handler

    try:
        await app.start()
    except (errors.NotAcceptable, errors.Unauthorized) as e:
        logging.error(
            "%s: %s\nYour string session is invalid or expired!\n"
            "Please generate a new one using: python string_gen.py",
            e.__class__.__name__,
            e,
        )
        raise

    load_missing_modules()
    module_manager = ModuleManager.get_instance()
    await module_manager.load_modules(app)

    if info := db.get("core.updater", "restart_info"):
        text = {
            "restart": "<b>Restart completed!</b>",
            "update": "<b>Update process completed!</b>",
        }[info["type"]]
        try:
            await app.edit_message_text(info["chat_id"], info["message_id"], text)
        except errors.RPCError:
            pass
        db.remove("core.updater", "restart_info")

    # required for sessionkiller module
    if db.get("core.sessionkiller", "enabled", False):
        db.set(
            "core.sessionkiller",
            "auths_hashes",
            [
                auth.hash
                for auth in (await app.invoke(GetAuthorizations())).authorizations
            ],
        )

    logging.info("Moon-Userbot started!")

    server = AsyncWSGIRefServer(host="0.0.0.0", port=config.port)
    webui_task = asyncio.create_task(asyncio.to_thread(server.run, bottle_app))
    logging.info("Web UI started on http://0.0.0.0:%d", config.port)

    app.loop.create_task(rentry_cleanup_job())

    await idle()

    await app.stop()
    server.shutdown()
    try:
        await asyncio.gather(webui_task, return_exceptions=True)
    except asyncio.CancelledError:
        logging.info("Web UI task cancelled")


if __name__ == "__main__":
    app.run(main())
