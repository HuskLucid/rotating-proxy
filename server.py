from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import random
import string
import time
from collections import defaultdict
from pathlib import Path
from typing import Optional

import aiohttp
from aiohttp import web

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("auth-proxy")

USERS_FILE = Path("users.json")
ADMIN_KEY = os.environ.get("ADMIN_KEY", "admin123")


def load_users() -> dict:
    if USERS_FILE.exists():
        return json.loads(USERS_FILE.read_text())
    return {}


def save_users(users: dict):
    USERS_FILE.write_text(json.dumps(users, indent=2))


def generate_credentials() -> tuple[str, str]:
    user = "user_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
    password = "".join(random.choices(string.ascii_letters + string.digits, k=16))
    return user, password


class Stats:
    def __init__(self):
        self.total_requests = 0
        self.success_count = 0
        self.fail_count = 0
        self.requests_by_user = defaultdict(int)
        self.start_time = time.time()

    def record(self, username: str, success: bool):
        self.total_requests += 1
        self.requests_by_user[username] += 1
        if success:
            self.success_count += 1
        else:
            self.fail_count += 1

    def to_dict(self) -> dict:
        uptime = time.time() - self.start_time
        return {
            "uptime_seconds": round(uptime, 1),
            "total_requests": self.total_requests,
            "success_count": self.success_count,
            "fail_count": self.fail_count,
            "success_rate": round(self.success_count / max(self.total_requests, 1) * 100, 1),
            "requests_by_user": dict(self.requests_by_user),
        }


stats = Stats()
users = load_users()

if not users:
    admin_user, admin_pass = generate_credentials()
    users[admin_user] = {"password": admin_pass, "created": time.time(), "enabled": True}
    save_users(users)
    logger.info("Created default user: %s:%s", admin_user, admin_pass)


def authenticate(request: web.Request) -> Optional[str]:
    auth_header = request.headers.get("Proxy-Authorization", "")
    if not auth_header.startswith("Basic "):
        return None
    try:
        decoded = base64.b64decode(auth_header[6:]).decode()
        username, password = decoded.split(":", 1)
        if username in users and users[username].get("enabled", True):
            if users[username]["password"] == password:
                return username
    except Exception:
        pass
    return None


def unauthorized():
    return web.Response(
        status=407,
        text="Proxy Authentication Required",
        headers={"Proxy-Authenticate": 'Basic realm="Auth Proxy"'},
    )


async def handle_connect(request: web.Request) -> web.StreamResponse:
    username = authenticate(request)
    if not username:
        return unauthorized()

    target = request.path.strip("/")
    if ":" not in target:
        return web.Response(status=400, text="Invalid CONNECT target")

    host, port_str = target.rsplit(":", 1)
    port = int(port_str)
    logger.info("[%s] CONNECT %s:%d", username, host, port)

    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=10
        )
    except Exception as e:
        stats.record(username, False)
        return web.Response(status=502, text=f"Cannot connect to {host}:{port}: {e}")

    transport = request.transport
    if transport is None:
        writer.close()
        stats.record(username, False)
        return web.Response(status=500, text="No transport")

    response = web.StreamResponse(status=200)
    response.force_close()
    await response.prepare(request)

    async def pipe(src, dst):
        try:
            while True:
                data = await src.read(8192)
                if not data:
                    break
                dst.write(data)
                await dst.drain()
        except Exception:
            pass

    await asyncio.gather(
        pipe(reader, transport), pipe(transport, reader), return_exceptions=True
    )
    writer.close()
    stats.record(username, True)
    return response


async def handle_forward(request: web.Request) -> web.StreamResponse:
    username = authenticate(request)
    if not username:
        return unauthorized()

    url = str(request.url)
    logger.info("[%s] %s %s", username, request.method, url[:80])

    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            body = await request.read()
            headers = {
                k: v for k, v in request.headers.items()
                if k.lower() not in ("host", "proxy-connection", "proxy-authorization")
            }
            headers["User-Agent"] = (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )

            start = time.time()
            async with session.request(
                method=request.method,
                url=url,
                headers=headers,
                data=body if body else None,
                allow_redirects=False,
            ) as resp:
                latency = (time.time() - start) * 1000
                resp_body = await resp.read()
                resp_headers = {
                    k: v for k, v in resp.headers.items()
                    if k.lower() not in ("transfer-encoding", "connection")
                }
                stats.record(username, resp.status < 400)
                return web.Response(status=resp.status, headers=resp_headers, body=resp_body)
    except Exception as e:
        stats.record(username, False)
        return web.Response(status=502, text=f"Forward error: {e}")


async def handle_api_create_user(request: web.Request) -> web.Response:
    auth = request.headers.get("Authorization", "")
    if auth != f"Bearer {ADMIN_KEY}":
        return web.json_response({"error": "Unauthorized"}, status=401)

    data = await request.json() if request.content_length else {}
    custom_user = data.get("username", "")
    custom_pass = data.get("password", "")

    if custom_user and custom_pass:
        if custom_user in users:
            return web.json_response({"error": "User already exists"}, status=409)
        users[custom_user] = {"password": custom_pass, "created": time.time(), "enabled": True}
    else:
        custom_user, custom_pass = generate_credentials()
        users[custom_user] = {"password": custom_pass, "created": time.time(), "enabled": True}

    save_users(users)
    hostname = os.environ.get("RENDER_EXTERNAL_URL", f"0.0.0.0:{os.environ.get('PORT', '8080')}")
    return web.json_response({
        "status": "ok",
        "username": custom_user,
        "password": custom_pass,
        "proxy": f"http://{custom_user}:{custom_pass}@{hostname}",
    })


async def handle_api_delete_user(request: web.Request) -> web.Response:
    auth = request.headers.get("Authorization", "")
    if auth != f"Bearer {ADMIN_KEY}":
        return web.json_response({"error": "Unauthorized"}, status=401)

    username = request.match_info.get("username", "")
    if username not in users:
        return web.json_response({"error": "User not found"}, status=404)

    del users[username]
    save_users(users)
    return web.json_response({"status": "ok", "deleted": username})


async def handle_api_list_users(request: web.Request) -> web.Response:
    auth = request.headers.get("Authorization", "")
    if auth != f"Bearer {ADMIN_KEY}":
        return web.json_response({"error": "Unauthorized"}, status=401)

    hostname = os.environ.get("RENDER_EXTERNAL_URL", f"0.0.0.0:{os.environ.get('PORT', '8080')}")
    user_list = []
    for uname, udata in users.items():
        user_list.append({
            "username": uname,
            "proxy": f"http://{uname}:{udata['password']}@{hostname}",
            "enabled": udata.get("enabled", True),
            "created": udata.get("created", 0),
            "requests": stats.requests_by_user.get(uname, 0),
        })

    return web.json_response({"status": "ok", "users": user_list, "total": len(user_list)})


async def handle_api_toggle_user(request: web.Request) -> web.Response:
    auth = request.headers.get("Authorization", "")
    if auth != f"Bearer {ADMIN_KEY}":
        return web.json_response({"error": "Unauthorized"}, status=401)

    username = request.match_info.get("username", "")
    if username not in users:
        return web.json_response({"error": "User not found"}, status=404)

    users[username]["enabled"] = not users[username].get("enabled", True)
    save_users(users)
    return web.json_response({
        "status": "ok",
        "username": username,
        "enabled": users[username]["enabled"],
    })


async def handle_api_stats(request: web.Request) -> web.Response:
    return web.json_response({"status": "ok", **stats.to_dict()})


async def handle_health(request: web.Request) -> web.Response:
    return web.json_response({"status": "ok"})


async def handle_dashboard(request: web.Request) -> web.Response:
    html = (Path(__file__).parent / "dashboard.html").read_text(encoding="utf-8")
    return web.Response(text=html, content_type="text/html")


def create_app() -> web.Application:
    app = web.Application()
    app.router.add_route("GET", "/", handle_dashboard)
    app.router.add_route("GET", "/health", handle_health)
    app.router.add_route("CONNECT", "/{target}", handle_connect)
    app.router.add_route("*", "/{path:.*}", handle_forward)
    app.router.add_route("POST", "/api/users", handle_api_create_user)
    app.router.add_route("GET", "/api/users", handle_api_list_users)
    app.router.add_route("DELETE", "/api/users/{username}", handle_api_delete_user)
    app.router.add_route("POST", "/api/users/{username}/toggle", handle_api_toggle_user)
    app.router.add_route("GET", "/api/stats", handle_api_stats)
    return app


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    logger.info("Auth Proxy Server starting on port %d", port)
    logger.info("Admin key: %s", ADMIN_KEY)
    web.run_app(create_app(), host="0.0.0.0", port=port)
