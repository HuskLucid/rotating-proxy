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
PROXY_SOURCES = [
    "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=protocolipport&format=text&protocol=http&timeout=3000",
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
    "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
]


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


class ProxyPool:
    def __init__(self):
        self._proxies: list[str] = []
        self._proxy_health: dict[str, float] = {}
        self._proxy_latency: dict[str, float] = {}
        self._last_fetch: float = 0
        self._fetch_interval: float = 180
        self._checking = False
        self._lock = asyncio.Lock()

    async def fetch_proxies(self) -> list[str]:
        proxies = []
        try:
            timeout = aiohttp.ClientTimeout(total=8)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                for url in PROXY_SOURCES:
                    try:
                        async with session.get(url) as resp:
                            if resp.status == 200:
                                text = await resp.text()
                                for line in text.splitlines():
                                    line = line.strip()
                                    if not line or line.startswith("#"):
                                        continue
                                    if "://" in line:
                                        line = line.split("://", 1)[1]
                                    parts = line.split(":")
                                    if len(parts) >= 2:
                                        ip = parts[0].strip()
                                        port = parts[1].strip()
                                        if self._validate_ip(ip) and port.isdigit():
                                            proxy_url = f"http://{ip}:{port}"
                                            if proxy_url not in self._proxies:
                                                proxies.append(proxy_url)
                    except Exception as e:
                        logger.warning("Fetch failed %s: %s", url[:40], e)
        except Exception as e:
            logger.error("Fetch error: %s", e)

        random.shuffle(proxies)
        logger.info("Fetched %d unique proxies", len(proxies))
        return proxies[:300]

    @staticmethod
    def _validate_ip(ip: str) -> bool:
        parts = ip.split(".")
        if len(parts) != 4:
            return False
        for part in parts:
            try:
                num = int(part)
                if num < 0 or num > 255:
                    return False
            except ValueError:
                return False
        return True

    async def check_proxy(self, proxy: str) -> tuple[bool, float]:
        try:
            start = time.time()
            timeout = aiohttp.ClientTimeout(total=3)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get("http://httpbin.org/ip", proxy=proxy) as resp:
                    latency = (time.time() - start) * 1000
                    if resp.status == 200:
                        return True, latency
        except Exception:
            pass
        return False, 9999

    async def check_batch(self, proxies: list[str], batch_size: int = 100) -> list[str]:
        logger.info("Checking %d proxies...", len(proxies))
        start = time.time()
        semaphore = asyncio.Semaphore(batch_size)

        async def check_one(p):
            async with semaphore:
                ok, lat = await self.check_proxy(p)
                if ok:
                    self._proxy_latency[p] = lat
                return p if ok else None

        tasks = [check_one(p) for p in proxies]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        alive = [r for r in results if r and not isinstance(r, Exception)]

        elapsed = time.time() - start
        logger.info("Check done: %d/%d alive (%.1fs)", len(alive), len(proxies), elapsed)
        return alive

    async def refresh(self):
        async with self._lock:
            if self._checking:
                return
            self._checking = True

        try:
            raw = await self.fetch_proxies()
            if raw:
                alive = await self.check_batch(raw, batch_size=200)

                for p in alive:
                    if p not in self._proxy_health:
                        self._proxy_health[p] = time.time()

                for p in list(self._proxy_health.keys()):
                    if p not in alive:
                        del self._proxy_health[p]
                        self._proxy_latency.pop(p, None)

                self._proxies = sorted(
                    alive,
                    key=lambda x: self._proxy_latency.get(x, 9999)
                )
                self._last_fetch = time.time()
                logger.info("Pool refreshed: %d alive (latency: %d-%dms)",
                           len(self._proxies),
                           min(self._proxy_latency.values()) if self._proxy_latency else 0,
                           max(self._proxy_latency.values()) if self._proxy_latency else 0)
        except Exception as e:
            logger.error("Refresh error: %s", e)
        finally:
            self._checking = False

    def get_upstream(self, index: int) -> Optional[str]:
        if not self._proxies:
            return None
        return self._proxies[index % len(self._proxies)]

    def get_exit_ip(self, proxy: str) -> str:
        if proxy:
            return proxy.split("://")[1].split(":")[0]
        return "N/A"

    def get_latency(self, proxy: str) -> float:
        return self._proxy_latency.get(proxy, 9999)

    @property
    def pool_size(self) -> int:
        return len(self._proxies)

    async def mark_dead(self, proxy: str):
        async with self._lock:
            if proxy in self._proxy_health:
                del self._proxy_health[proxy]
            if proxy in self._proxy_latency:
                del self._proxy_latency[proxy]
            if proxy in self._proxies:
                self._proxies.remove(proxy)
                logger.info("Removed dead proxy: %s (pool: %d)", proxy, len(self._proxies))

    async def run_loop(self):
        while True:
            await self.refresh()
            await asyncio.sleep(self._fetch_interval)


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
proxy_pool = ProxyPool()
users = load_users()

if not users:
    admin_user, admin_pass = generate_credentials()
    users[admin_user] = {"password": admin_pass, "created": time.time(), "enabled": True, "upstream_index": 0}
    save_users(users)
    logger.info("Created default user: %s:%s", admin_user, admin_pass)


def get_hostname() -> str:
    render_url = os.environ.get("RENDER_EXTERNAL_URL", "")
    if render_url:
        return render_url.replace("https://", "").replace("http://", "")
    return f"0.0.0.0:{os.environ.get('PORT', '8080')}"


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


async def pipe_streams(reader, writer):
    try:
        while True:
            data = await reader.read(131072)
            if not data:
                break
            writer.write(data)
            await writer.drain()
    except Exception:
        pass


async def handle_connect(request: web.Request) -> web.StreamResponse:
    username = authenticate(request)
    if not username:
        return unauthorized()

    target = request.path.strip("/")
    if ":" not in target:
        return web.Response(status=400, text="Invalid CONNECT target")

    host, port_str = target.rsplit(":", 1)
    port = int(port_str)

    user_data = users.get(username, {})
    upstream_index = user_data.get("upstream_index", 0)
    upstream = proxy_pool.get_upstream(upstream_index)

    if not upstream:
        stats.record(username, False)
        return web.Response(status=503, text="No upstream proxies available")

    logger.info("[%s] CONNECT %s:%d via %s", username, host, port, upstream)

    proxy_writer = None
    try:
        upstream_host = upstream.split("://")[1].split(":")[0]
        upstream_port = int(upstream.split("://")[1].split(":")[1])

        proxy_reader, proxy_writer = await asyncio.wait_for(
            asyncio.open_connection(upstream_host, upstream_port), timeout=5
        )

        connect_req = f"CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n\r\n"
        proxy_writer.write(connect_req.encode())
        await proxy_writer.drain()

        status_line = await asyncio.wait_for(proxy_reader.readline(), timeout=5)
        if b"200" not in status_line:
            proxy_writer.close()
            stats.record(username, False)
            await proxy_pool.mark_dead(upstream)
            return web.Response(status=502, text="Upstream CONNECT failed")

    except asyncio.TimeoutError:
        stats.record(username, False)
        await proxy_pool.mark_dead(upstream)
        if proxy_writer:
            proxy_writer.close()
        return web.Response(status=502, text="Upstream timeout")
    except Exception as e:
        stats.record(username, False)
        await proxy_pool.mark_dead(upstream)
        if proxy_writer:
            proxy_writer.close()
        return web.Response(status=502, text=f"Upstream error: {e}")

    transport = request.transport
    if transport is None:
        proxy_writer.close()
        stats.record(username, False)
        return web.Response(status=500, text="No transport")

    response = web.StreamResponse(status=200)
    response.force_close()
    await response.prepare(request)

    try:
        await asyncio.gather(
            pipe_streams(proxy_reader, transport),
            pipe_streams(transport, proxy_writer),
            return_exceptions=True,
        )
    except Exception:
        pass

    proxy_writer.close()
    stats.record(username, True)
    return response


async def handle_forward(request: web.Request) -> web.StreamResponse:
    username = authenticate(request)
    if not username:
        return unauthorized()

    url = str(request.url)

    user_data = users.get(username, {})
    upstream_index = user_data.get("upstream_index", 0)
    upstream = proxy_pool.get_upstream(upstream_index)

    logger.info("[%s] %s %s via %s", username, request.method, url[:80], upstream or "direct")

    try:
        timeout = aiohttp.ClientTimeout(total=10)
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
                proxy=upstream,
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
        if upstream:
            await proxy_pool.mark_dead(upstream)
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
        upstream_index = len(users) % max(proxy_pool.pool_size, 1)
        users[custom_user] = {
            "password": custom_pass,
            "created": time.time(),
            "enabled": True,
            "upstream_index": upstream_index,
        }
    else:
        custom_user, custom_pass = generate_credentials()
        upstream_index = len(users) % max(proxy_pool.pool_size, 1)
        users[custom_user] = {
            "password": custom_pass,
            "created": time.time(),
            "enabled": True,
            "upstream_index": upstream_index,
        }

    save_users(users)

    hostname = get_hostname()
    upstream = proxy_pool.get_upstream(upstream_index)
    exit_ip = proxy_pool.get_exit_ip(upstream) if upstream else "N/A"

    return web.json_response({
        "status": "ok",
        "username": custom_user,
        "password": custom_pass,
        "ip": hostname.split(":")[0],
        "port": 443,
        "exit_ip": exit_ip,
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

    hostname = get_hostname()
    user_list = []
    for uname, udata in users.items():
        upstream_index = udata.get("upstream_index", 0)
        upstream = proxy_pool.get_upstream(upstream_index)
        exit_ip = proxy_pool.get_exit_ip(upstream) if upstream else "N/A"
        latency = proxy_pool.get_latency(upstream) if upstream else 0

        user_list.append({
            "username": uname,
            "password": udata["password"],
            "ip": hostname.split(":")[0],
            "port": 443,
            "exit_ip": exit_ip,
            "latency": round(latency, 1),
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


async def handle_api_rotate_user(request: web.Request) -> web.Response:
    auth = request.headers.get("Authorization", "")
    if auth != f"Bearer {ADMIN_KEY}":
        return web.json_response({"error": "Unauthorized"}, status=401)

    username = request.match_info.get("username", "")
    if username not in users:
        return web.json_response({"error": "User not found"}, status=404)

    if proxy_pool.pool_size == 0:
        return web.json_response({"error": "No proxies in pool"}, status=503)

    new_index = random.randint(0, proxy_pool.pool_size - 1)
    users[username]["upstream_index"] = new_index
    save_users(users)

    upstream = proxy_pool.get_upstream(new_index)
    exit_ip = proxy_pool.get_exit_ip(upstream) if upstream else "N/A"
    latency = proxy_pool.get_latency(upstream) if upstream else 0

    return web.json_response({
        "status": "ok",
        "username": username,
        "exit_ip": exit_ip,
        "latency": round(latency, 1),
    })


async def handle_api_pool(request: web.Request) -> web.Response:
    proxies_info = []
    for p in proxy_pool._proxies[:50]:
        proxies_info.append({
            "proxy": p,
            "exit_ip": proxy_pool.get_exit_ip(p),
            "latency": round(proxy_pool.get_latency(p), 1),
        })

    return web.json_response({
        "status": "ok",
        "pool_size": proxy_pool.pool_size,
        "last_fetch": proxy_pool._last_fetch,
        "proxies": proxies_info,
    })


async def handle_api_stats(request: web.Request) -> web.Response:
    return web.json_response({
        "status": "ok",
        "pool_size": proxy_pool.pool_size,
        **stats.to_dict(),
    })


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
    app.router.add_route("POST", "/api/users/{username}/rotate", handle_api_rotate_user)
    app.router.add_route("GET", "/api/pool", handle_api_pool)
    app.router.add_route("GET", "/api/stats", handle_api_stats)
    return app


async def on_startup(app):
    asyncio.create_task(proxy_pool.run_loop())


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    logger.info("Auth Proxy Server starting on port %d", port)
    logger.info("Admin key: %s", ADMIN_KEY)
    logger.info("Hostname: %s", get_hostname())

    app = create_app()
    app.on_startup.append(on_startup)
    web.run_app(app, host="0.0.0.0", port=port)
