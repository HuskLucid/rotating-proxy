from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import random
import socket
import string
import struct
import time
from collections import defaultdict
from pathlib import Path
from typing import Optional

import aiohttp
from aiohttp import web

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("proxy")

USERS_FILE = Path("users.json")
ADMIN_KEY = os.environ.get("ADMIN_KEY", "admin123")
HTTP_SOURCES = [
    "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=protocolipport&format=text&protocol=http&timeout=3000",
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
]
SOCKS5_SOURCES = [
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt",
]


def load_users() -> dict:
    if USERS_FILE.exists():
        return json.loads(USERS_FILE.read_text())
    return {}


def save_users(u: dict):
    USERS_FILE.write_text(json.dumps(u, indent=2))


def gen_creds() -> tuple[str, str]:
    u = "user_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
    p = "".join(random.choices(string.ascii_letters + string.digits, k=16))
    return u, p


def get_hostname() -> str:
    r = os.environ.get("RENDER_EXTERNAL_URL", "")
    if r:
        return r.replace("https://", "").replace("http://", "")
    return f"0.0.0.0:{os.environ.get('PORT', '8080')}"


# ─── Proxy Pool ───────────────────────────────────────────────────────────────

class ProxyPool:
    def __init__(self):
        self.http: list[str] = []
        self.socks5: list[str] = []
        self.latency: dict[str, float] = {}
        self._last_fetch = 0.0
        self._interval = 180.0
        self._checking = False
        self._lock = asyncio.Lock()

    async def _fetch_list(self, sources: list[str], tag: str) -> list[str]:
        out = []
        timeout = aiohttp.ClientTimeout(total=8)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            for url in sources:
                try:
                    async with s.get(url) as r:
                        if r.status == 200:
                            text = await r.text()
                            for line in text.splitlines():
                                line = line.strip()
                                if not line or line.startswith("#"):
                                    continue
                                if "://" in line:
                                    line = line.split("://", 1)[1]
                                parts = line.split(":")
                                if len(parts) >= 2:
                                    ip, port = parts[0].strip(), parts[1].strip()
                                    if self._valid_ip(ip) and port.isdigit():
                                        p = f"http://{ip}:{port}"
                                        if p not in out:
                                            out.append(p)
                except Exception as e:
                    logger.warning("%s fetch err %s: %s", tag, url[:30], e)
        return out

    async def _fetch_socks5(self) -> list[str]:
        out = []
        timeout = aiohttp.ClientTimeout(total=8)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            for url in SOCKS5_SOURCES:
                try:
                    async with s.get(url) as r:
                        if r.status == 200:
                            text = await r.text()
                            for line in text.splitlines():
                                line = line.strip()
                                if not line or line.startswith("#"):
                                    continue
                                parts = line.split(":")
                                if len(parts) >= 2:
                                    ip, port = parts[0].strip(), parts[1].strip()
                                    if self._valid_ip(ip) and port.isdigit():
                                        p = f"socks5://{ip}:{port}"
                                        if p not in out:
                                            out.append(p)
                except Exception as e:
                    logger.warning("socks5 fetch err %s: %s", url[:30], e)
        return out

    @staticmethod
    def _valid_ip(ip: str) -> bool:
        parts = ip.split(".")
        if len(parts) != 4:
            return False
        try:
            return all(0 <= int(p) <= 255 for p in parts)
        except ValueError:
            return False

    async def _check_http(self, proxy: str) -> tuple[bool, float]:
        try:
            t0 = time.time()
            timeout = aiohttp.ClientTimeout(total=3)
            async with aiohttp.ClientSession(timeout=timeout) as s:
                async with s.get("http://httpbin.org/ip", proxy=proxy) as r:
                    lat = (time.time() - t0) * 1000
                    return (True, lat) if r.status == 200 else (False, 9999)
        except Exception:
            return False, 9999

    async def _check_socks(self, proxy: str) -> tuple[bool, float]:
        try:
            hp = proxy.split("://")[1]
            h, p = hp.split(":")[0], int(hp.split(":")[1])
            t0 = time.time()
            rd, wr = await asyncio.wait_for(asyncio.open_connection(h, p), timeout=3)
            wr.write(b"\x05\x01\x00")
            await wr.drain()
            resp = await asyncio.wait_for(rd.read(2), timeout=3)
            lat = (time.time() - t0) * 1000
            wr.close()
            await wr.wait_closed()
            if resp and resp[0] == 0x05 and resp[1] == 0x00:
                return True, lat
        except Exception:
            pass
        return False, 9999

    async def _batch_check(self, proxies: list[str], is_socks: bool, sem: asyncio.Semaphore) -> list[str]:
        async def one(p):
            async with sem:
                ok, lat = await (self._check_socks(p) if is_socks else self._check_http(p))
                if ok:
                    self.latency[p] = lat
                return p if ok else None
        tasks = [one(p) for p in proxies]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return [r for r in results if r and not isinstance(r, Exception)]

    async def refresh(self):
        async with self._lock:
            if self._checking:
                return
            self._checking = True
        try:
            http_raw = await self._fetch_list(HTTP_SOURCES, "http")
            socks_raw = await self._fetch_socks5()
            logger.info("Fetched %d HTTP, %d SOCKS5", len(http_raw), len(socks_raw))

            sem_h = asyncio.Semaphore(200)
            sem_s = asyncio.Semaphore(100)
            http_alive, socks_alive = await asyncio.gather(
                self._batch_check(http_raw, False, sem_h),
                self._batch_check(socks_raw, True, sem_s),
            )

            self.http = sorted(http_alive, key=lambda x: self.latency.get(x, 9999))
            self.socks5 = sorted(socks_alive, key=lambda x: self.latency.get(x, 9999))
            self._last_fetch = time.time()
            logger.info("Pool: %d HTTP, %d SOCKS5 alive", len(self.http), len(self.socks5))
        except Exception as e:
            logger.error("Refresh error: %s", e)
        finally:
            self._checking = False

    def get_http(self, idx: int) -> Optional[str]:
        return self.http[idx % len(self.http)] if self.http else None

    def get_socks5(self, idx: int) -> Optional[str]:
        return self.socks5[idx % len(self.socks5)] if self.socks5 else None

    def exit_ip(self, p: str) -> str:
        try:
            return p.split("://")[1].split(":")[0]
        except Exception:
            return "N/A"

    def get_lat(self, p: str) -> float:
        return self.latency.get(p, 0)

    @property
    def size(self) -> int:
        return len(self.http) + len(self.socks5)

    async def mark_dead(self, p: str):
        async with self._lock:
            self.latency.pop(p, None)
            if p in self.http:
                self.http.remove(p)
            if p in self.socks5:
                self.socks5.remove(p)
            logger.info("Dead removed: %s (pool: %d)", p, self.size)

    async def loop(self):
        while True:
            await self.refresh()
            await asyncio.sleep(self._interval)


# ─── Stats ────────────────────────────────────────────────────────────────────

class Stats:
    def __init__(self):
        self.total = 0
        self.ok = 0
        self.fail = 0
        self.by_user: dict[str, int] = defaultdict(int)
        self.start = time.time()

    def rec(self, user: str, success: bool):
        self.total += 1
        self.by_user[user] += 1
        if success:
            self.ok += 1
        else:
            self.fail += 1

    def to_dict(self):
        uptime = time.time() - self.start
        return {
            "uptime_seconds": round(uptime, 1),
            "total_requests": self.total,
            "success_count": self.ok,
            "fail_count": self.fail,
            "success_rate": round(self.ok / max(self.total, 1) * 100, 1),
            "requests_by_user": dict(self.by_user),
        }


# ─── Globals ──────────────────────────────────────────────────────────────────

stats = Stats()
pool = ProxyPool()
users = load_users()
if not users:
    u, p = gen_creds()
    users[u] = {"password": p, "created": time.time(), "enabled": True, "upstream_index": 0}
    save_users(users)
    logger.info("Default user: %s:%s", u, p)


def auth_basic(user: str, pwd: str) -> bool:
    return user in users and users[user].get("enabled", True) and users[user]["password"] == pwd


# ─── Utilities ────────────────────────────────────────────────────────────────

async def pipe(r, w):
    try:
        while True:
            d = await r.read(131072)
            if not d:
                break
            w.write(d)
            await w.drain()
    except Exception:
        pass


def http_response(status: int, text: str, extra_headers: dict | None = None) -> bytes:
    hdrs = f"HTTP/1.1 {status} OK\r\n" if status == 200 else f"HTTP/1.1 {status} Error\r\n"
    hdrs += "Connection: close\r\n"
    if extra_headers:
        for k, v in extra_headers.items():
            hdrs += f"{k}: {v}\r\n"
    body = text.encode()
    hdrs += f"Content-Length: {len(body)}\r\n\r\n"
    return hdrs.encode() + body


# ─── HTTP Request Parser ──────────────────────────────────────────────────────

async def parse_http_request(reader: asyncio.StreamReader) -> dict:
    line = await asyncio.wait_for(reader.readline(), timeout=10)
    parts = line.decode(errors="ignore").strip().split(" ")
    if len(parts) < 3:
        return {}

    method, path, version = parts[0], parts[1], parts[2]
    headers = {}
    while True:
        hline = await asyncio.wait_for(reader.readline(), timeout=5)
        hline_str = hline.decode(errors="ignore").strip()
        if not hline_str:
            break
        if ": " in hline_str:
            k, v = hline_str.split(": ", 1)
            headers[k] = v

    content_length = int(headers.get("Content-Length", "0"))
    body = b""
    if content_length > 0:
        body = await asyncio.wait_for(reader.readexactly(content_length), timeout=10)

    return {"method": method, "path": path, "version": version, "headers": headers, "body": body}


# ─── HTTP CONNECT ─────────────────────────────────────────────────────────────

async def do_connect(user: str, host: str, port: int, client_writer: asyncio.StreamWriter):
    uidx = users.get(user, {}).get("upstream_index", 0)
    upstream = pool.get_http(uidx)
    if not upstream:
        stats.rec(user, False)
        client_writer.write(http_response(503, "No upstream proxies"))
        await client_writer.drain()
        client_writer.close()
        return

    logger.info("[%s] CONNECT %s:%d via %s", user, host, port, upstream)

    up_host = upstream.split("://")[1].split(":")[0]
    up_port = int(upstream.split("://")[1].split(":")[1])
    proxy_writer = None
    try:
        proxy_reader, proxy_writer = await asyncio.wait_for(
            asyncio.open_connection(up_host, up_port), timeout=5
        )
        proxy_writer.write(f"CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n\r\n".encode())
        await proxy_writer.drain()
        status = await asyncio.wait_for(proxy_reader.readline(), timeout=5)
        if b"200" not in status:
            proxy_writer.close()
            stats.rec(user, False)
            await pool.mark_dead(upstream)
            client_writer.write(http_response(502, "Upstream CONNECT failed"))
            await client_writer.drain()
            client_writer.close()
            return
    except Exception as e:
        stats.rec(user, False)
        if upstream:
            await pool.mark_dead(upstream)
        if proxy_writer:
            proxy_writer.close()
        client_writer.write(http_response(502, f"Upstream error: {e}"))
        await client_writer.drain()
        client_writer.close()
        return

    client_writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
    await client_writer.drain()

    stats.rec(user, True)
    try:
        await asyncio.gather(
            pipe(proxy_reader, client_writer),
            pipe(client_writer, proxy_writer),
            return_exceptions=True,
        )
    except Exception:
        pass
    proxy_writer.close()


# ─── HTTP Forward ─────────────────────────────────────────────────────────────

async def do_forward(user: str, method: str, url: str, headers: dict, body: bytes):
    uidx = users.get(user, {}).get("upstream_index", 0)
    upstream = pool.get_http(uidx)
    logger.info("[%s] %s %s via %s", user, method, url[:80], upstream or "direct")
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            hdrs = {k: v for k, v in headers.items()
                    if k.lower() not in ("host", "proxy-connection", "proxy-authorization", "content-length")}
            hdrs["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
            async with session.request(method=method, url=url, headers=hdrs,
                                       data=body if body else None, proxy=upstream, allow_redirects=False) as resp:
                rb = await resp.read()
                rh = {k: v for k, v in resp.headers.items() if k.lower() not in ("transfer-encoding", "connection")}
                stats.rec(user, resp.status < 400)
                return resp.status, rh, rb
    except Exception as e:
        stats.rec(user, False)
        if upstream:
            await pool.mark_dead(upstream)
        return 502, {}, f"Forward error: {e}".encode()


# ─── API JSON Handlers ────────────────────────────────────────────────────────

async def api_create_user(data: dict) -> tuple[int, dict]:
    cu = data.get("username", "")
    cp = data.get("password", "")
    if cu and cp:
        if cu in users:
            return 409, {"error": "User already exists"}
        idx = len(users) % max(pool.size, 1)
        users[cu] = {"password": cp, "created": time.time(), "enabled": True, "upstream_index": idx}
    else:
        cu, cp = gen_creds()
        idx = len(users) % max(pool.size, 1)
        users[cu] = {"password": cp, "created": time.time(), "enabled": True, "upstream_index": idx}
    save_users(users)
    h = get_hostname()
    port = h.split(":")[-1] if ":" in h else "443"
    up = pool.get_http(idx)
    ex = pool.exit_ip(up) if up else "N/A"
    lat = pool.get_lat(up) if up else 0
    sup = pool.get_socks5(idx)
    sex = pool.exit_ip(sup) if sup else "N/A"
    return 200, {
        "status": "ok", "username": cu, "password": cp,
        "ip": h.split(":")[0], "port": port, "exit_ip": ex, "socks_exit_ip": sex, "latency": round(lat, 1),
        "proxy_http": f"http://{cu}:{cp}@{h}", "proxy_socks5": f"socks5://{cu}:{cp}@{h}",
    }


async def api_list_users() -> tuple[int, dict]:
    h = get_hostname()
    port = h.split(":")[-1] if ":" in h else "443"
    ul = []
    for un, ud in users.items():
        idx = ud.get("upstream_index", 0)
        up = pool.get_http(idx)
        ex = pool.exit_ip(up) if up else "N/A"
        lat = pool.get_lat(up) if up else 0
        sup = pool.get_socks5(idx)
        sex = pool.exit_ip(sup) if sup else "N/A"
        ul.append({
            "username": un, "password": ud["password"], "ip": h.split(":")[0], "port": port,
            "exit_ip": ex, "socks_exit_ip": sex, "latency": round(lat, 1),
            "proxy_http": f"http://{un}:{ud['password']}@{h}",
            "proxy_socks5": f"socks5://{un}:{ud['password']}@{h}",
            "enabled": ud.get("enabled", True), "created": ud.get("created", 0),
            "requests": stats.by_user.get(un, 0),
        })
    return 200, {"status": "ok", "users": ul, "total": len(ul)}


async def api_delete_user(username: str) -> tuple[int, dict]:
    if username not in users:
        return 404, {"error": "User not found"}
    del users[username]
    save_users(users)
    return 200, {"status": "ok", "deleted": username}


async def api_toggle_user(username: str) -> tuple[int, dict]:
    if username not in users:
        return 404, {"error": "User not found"}
    users[username]["enabled"] = not users[username].get("enabled", True)
    save_users(users)
    return 200, {"status": "ok", "username": username, "enabled": users[username]["enabled"]}


async def api_rotate_user(username: str) -> tuple[int, dict]:
    if username not in users:
        return 404, {"error": "User not found"}
    if pool.size == 0:
        return 503, {"error": "No proxies in pool"}
    idx = random.randint(0, pool.size - 1)
    users[username]["upstream_index"] = idx
    save_users(users)
    up = pool.get_http(idx)
    ex = pool.exit_ip(up) if up else "N/A"
    lat = pool.get_lat(up) if up else 0
    sup = pool.get_socks5(idx)
    sex = pool.exit_ip(sup) if sup else "N/A"
    return 200, {"status": "ok", "username": username, "exit_ip": ex, "socks_exit_ip": sex, "latency": round(lat, 1)}


async def api_pool() -> dict:
    return {"status": "ok", "pool_size": pool.size, "http_pool": len(pool.http), "socks5_pool": len(pool.socks5), "last_fetch": pool._last_fetch}


async def api_stats() -> dict:
    return {"status": "ok", "pool_size": pool.size, "http_pool": len(pool.http), "socks5_pool": len(pool.socks5), **stats.to_dict()}


# ─── SOCKS5 ───────────────────────────────────────────────────────────────────

async def handle_socks5(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, addr):
    username = None
    try:
        hdr = await asyncio.wait_for(reader.read(2), timeout=5)
        if len(hdr) < 2 or hdr[0] != 0x05:
            writer.close()
            return
        nmethods = hdr[1]
        methods = await asyncio.wait_for(reader.read(nmethods), timeout=5)
        need_auth = 0x02 in methods
        writer.write(bytes([0x05, 0x02 if need_auth else 0x00]))
        await writer.drain()

        if need_auth:
            ad = await asyncio.wait_for(reader.read(2), timeout=5)
            if len(ad) < 2 or ad[0] != 0x01:
                writer.close()
                return
            ulen = ad[1]
            ubytes = await asyncio.wait_for(reader.read(ulen), timeout=5)
            plen = (await asyncio.wait_for(reader.read(1), timeout=5))[0]
            pbytes = await asyncio.wait_for(reader.read(plen), timeout=5)
            username = ubytes.decode(errors="ignore")
            password = pbytes.decode(errors="ignore")
            if not auth_basic(username, password):
                writer.write(b"\x01\x01")
                await writer.drain()
                writer.close()
                return
            writer.write(b"\x01\x00")
            await writer.drain()

        req = await asyncio.wait_for(reader.read(4), timeout=5)
        if len(req) < 4:
            writer.close()
            return
        _, cmd, _, atyp = req
        if cmd != 1:
            writer.write(b"\x05\x07\x00\x01\x00\x00\x00\x00\x00\x00")
            await writer.drain()
            writer.close()
            return

        if atyp == 1:
            d = await asyncio.wait_for(reader.read(4), timeout=5)
            host = socket.inet_ntoa(d)
        elif atyp == 3:
            dl = (await asyncio.wait_for(reader.read(1), timeout=5))[0]
            host = (await asyncio.wait_for(reader.read(dl), timeout=5)).decode()
        elif atyp == 4:
            d = await asyncio.wait_for(reader.read(16), timeout=5)
            host = socket.inet_ntop(socket.AF_INET6, d)
        else:
            writer.close()
            return

        port = struct.unpack(">H", await asyncio.wait_for(reader.read(2), timeout=5))[0]
        su = username or "socks5-anon"
        logger.info("[SOCKS5] %s CONNECT %s:%d", su, host, port)

        uidx = users.get(username, {}).get("upstream_index", 0) if username else 0
        upstream = pool.get_socks5(uidx)

        if upstream:
            try:
                uh = upstream.split("://")[1].split(":")[0]
                up = int(upstream.split("://")[1].split(":")[1])
                rr, rw = await asyncio.wait_for(asyncio.open_connection(uh, up), timeout=5)
                sreq = bytearray([0x05, 0x01, 0x00, 0x03, len(host.encode())])
                sreq.extend(host.encode())
                sreq.extend(struct.pack(">H", port))
                rw.write(bytes(sreq))
                await rw.drain()
                resp = await asyncio.wait_for(rr.read(10), timeout=5)
                if len(resp) < 2 or resp[1] != 0:
                    rw.close()
                    raise Exception("upstream socks5 failed")
                writer.write(b"\x05\x00\x00\x01\x00\x00\x00\x00\x00\x00")
                await writer.drain()
                await asyncio.gather(pipe(reader, rw), pipe(rr, writer), return_exceptions=True)
                rw.close()
                stats.rec(su, True)
                return
            except Exception as e:
                logger.warning("socks5 upstream err: %s", e)
                if upstream:
                    await pool.mark_dead(upstream)

        try:
            rr, rw = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=5)
            writer.write(b"\x05\x00\x00\x01\x00\x00\x00\x00\x00\x00")
            await writer.drain()
            await asyncio.gather(pipe(reader, rw), pipe(rr, writer), return_exceptions=True)
            rw.close()
            stats.rec(su, True)
        except Exception:
            writer.write(b"\x05\x05\x00\x01\x00\x00\x00\x00\x00\x00")
            await writer.drain()
            writer.close()
            stats.rec(su, False)
    except asyncio.TimeoutError:
        try:
            writer.close()
        except Exception:
            pass
    except Exception as e:
        logger.warning("socks5 err %s: %s", addr, e)
        try:
            writer.close()
        except Exception:
            pass


# ─── SOCKS4 ───────────────────────────────────────────────────────────────────

async def handle_socks4(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, addr):
    try:
        hdr = await asyncio.wait_for(reader.read(8), timeout=5)
        if len(hdr) < 8 or hdr[0] != 0x04:
            writer.close()
            return
        cmd, port = hdr[1], struct.unpack(">H", hdr[2:4])[0]
        ip = hdr[4:8]
        host = socket.inet_ntoa(ip)

        udbuf = b""
        while True:
            b = await asyncio.wait_for(reader.read(1), timeout=5)
            if b == b"\x00" or not b:
                break
            udbuf += b
        username = udbuf.decode(errors="ignore").strip()

        if cmd != 1:
            writer.close()
            return

        su = username or "socks4-anon"
        logger.info("[SOCKS4] %s CONNECT %s:%d", su, host, port)

        try:
            rr, rw = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=5)
            writer.write(struct.pack(">BBH", 0, 0x5A, port) + ip)
            await writer.drain()
            await asyncio.gather(pipe(reader, rw), pipe(rr, writer), return_exceptions=True)
            rw.close()
            stats.rec(su, True)
        except Exception:
            writer.write(struct.pack(">BBH", 0, 0x5B, port) + ip)
            await writer.drain()
            writer.close()
            stats.rec(su, False)
    except asyncio.TimeoutError:
        try:
            writer.close()
        except Exception:
            pass
    except Exception as e:
        logger.warning("socks4 err %s: %s", addr, e)
        try:
            writer.close()
        except Exception:
            pass


# ─── HTTP Handler (via raw TCP) ───────────────────────────────────────────────

async def handle_http(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, addr):
    try:
        req = await parse_http_request(reader)
        if not req:
            writer.close()
            return

        method = req["method"]
        path = req["path"]
        headers = req["headers"]
        body = req["body"]

        # ── Auth ──
        auth_header = headers.get("Proxy-Authorization", "")
        username = None
        if auth_header.startswith("Basic "):
            try:
                decoded = base64.b64decode(auth_header[6:]).decode()
                u, p = decoded.split(":", 1)
                if auth_basic(u, p):
                    username = u
            except Exception:
                pass

        # ── CONNECT ──
        if method == "CONNECT":
            if not username:
                writer.write(http_response(407, "Proxy Auth Required", {"Proxy-Authenticate": 'Basic realm="Proxy"'}))
                await writer.drain()
                writer.close()
                return
            target = path.strip("/")
            if ":" not in target:
                writer.write(http_response(400, "Invalid target"))
                await writer.drain()
                writer.close()
                return
            host, port_s = target.rsplit(":", 1)
            await do_connect(username, host, int(port_s), writer)
            return

        # ── API routes ──
        if path.startswith("/api/"):
            admin = headers.get("Authorization", "") == f"Bearer {ADMIN_KEY}"
            if not admin and path != "/api/stats":
                writer.write(http_response(401, json.dumps({"error": "Unauthorized"}), {"Content-Type": "application/json"}))
                await writer.drain()
                writer.close()
                return

            status_code = 200
            resp_body = {}

            if path == "/api/users" and method == "GET":
                status_code, resp_body = await api_list_users()
            elif path == "/api/users" and method == "POST":
                status_code, resp_body = await api_create_user(json.loads(body) if body else {})
            elif path.startswith("/api/users/") and method == "DELETE":
                uname = path.split("/api/users/")[1].split("/")[0].split("?")[0]
                status_code, resp_body = await api_delete_user(uname)
            elif path.endswith("/toggle") and method == "POST":
                uname = path.split("/api/users/")[1].split("/")[0]
                status_code, resp_body = await api_toggle_user(uname)
            elif path.endswith("/rotate") and method == "POST":
                uname = path.split("/api/users/")[1].split("/")[0]
                status_code, resp_body = await api_rotate_user(uname)
            elif path == "/api/pool":
                resp_body = await api_pool()
            elif path == "/api/stats":
                resp_body = await api_stats()
            else:
                status_code = 404
                resp_body = {"error": "Not found"}

            data = json.dumps(resp_body).encode()
            writer.write(f"HTTP/1.1 {status_code} OK\r\nContent-Type: application/json\r\nContent-Length: {len(data)}\r\nConnection: close\r\n\r\n".encode())
            writer.write(data)
            await writer.drain()
            writer.close()
            return

        # ── Dashboard ──
        if path == "/" or path == "/index.html":
            try:
                html = (Path(__file__).parent / "dashboard.html").read_text(encoding="utf-8")
                data = html.encode()
                writer.write(f"HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {len(data)}\r\nConnection: close\r\n\r\n".encode())
                writer.write(data)
            except Exception:
                writer.write(http_response(404, "Dashboard not found"))
            await writer.drain()
            writer.close()
            return

        # ── Health ──
        if path == "/health":
            data = json.dumps({"status": "ok"}).encode()
            writer.write(f"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {len(data)}\r\nConnection: close\r\n\r\n".encode())
            writer.write(data)
            await writer.drain()
            writer.close()
            return

        # ── Forward ──
        if not username:
            writer.write(http_response(407, "Proxy Auth Required", {"Proxy-Authenticate": 'Basic realm="Proxy"'}))
            await writer.drain()
            writer.close()
            return

        full_url = path if path.startswith("http") else f"http://{headers.get('Host', 'localhost')}{path}"
        sc, sh, sb = await do_forward(username, method, full_url, headers, body)

        hdr_lines = f"HTTP/1.1 {sc} OK\r\nConnection: close\r\n"
        for k, v in sh.items():
            hdr_lines += f"{k}: {v}\r\n"
        hdr_lines += f"Content-Length: {len(sb)}\r\n\r\n"
        writer.write(hdr_lines.encode())
        writer.write(sb)
        await writer.drain()
        writer.close()

    except asyncio.TimeoutError:
        try:
            writer.close()
        except Exception:
            pass
    except Exception as e:
        logger.warning("http err %s: %s", addr, e)
        try:
            writer.close()
        except Exception:
            pass


# ─── Single-Port Protocol Router ──────────────────────────────────────────────

async def on_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    addr = writer.get_extra_info("peername")
    try:
        first = await asyncio.wait_for(reader.read(1), timeout=5)
        if not first:
            writer.close()
            return

        if first[0] == 0x05:
            await handle_socks5(reader, writer, addr)
        elif first[0] == 0x04:
            await handle_socks4(reader, writer, addr)
        elif first[0] >= 0x20 and first[0] < 0x7F:
            await handle_http_protocol(reader, writer, addr, first)
        else:
            logger.warning("Unknown protocol byte 0x%02x from %s", first[0], addr)
            writer.close()
    except asyncio.TimeoutError:
        try:
            writer.close()
        except Exception:
            pass
    except Exception as e:
        logger.warning("Client err %s: %s", addr, e)
        try:
            writer.close()
        except Exception:
            pass


async def handle_http_protocol(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, addr, first_byte: bytes):
    try:
        line_buf = first_byte
        while True:
            chunk = await asyncio.wait_for(reader.read(1), timeout=10)
            if not chunk:
                writer.close()
                return
            line_buf += chunk
            if line_buf.endswith(b"\r\n\r\n"):
                break

        request_line = line_buf.split(b"\r\n")[0].decode(errors="ignore")
        parts = request_line.split(" ")
        if len(parts) < 3:
            writer.close()
            return

        method, path, version = parts[0], parts[1], parts[2]
        header_block = line_buf.split(b"\r\n\r\n")[0]
        header_lines = header_block.split(b"\r\n")[1:]
        headers = {}
        for hl in header_lines:
            hl_str = hl.decode(errors="ignore")
            if ": " in hl_str:
                k, v = hl_str.split(": ", 1)
                headers[k] = v

        content_length = int(headers.get("Content-Length", "0"))
        body = b""
        if content_length > 0:
            already = len(line_buf.split(b"\r\n\r\n", 1)[1]) if b"\r\n\r\n" in line_buf else 0
            body = line_buf.split(b"\r\n\r\n", 1)[1] if b"\r\n\r\n" in line_buf else b""
            remaining = content_length - len(body)
            if remaining > 0:
                body += await asyncio.wait_for(reader.readexactly(remaining), timeout=10)

        auth_header = headers.get("Proxy-Authorization", "")
        username = None
        if auth_header.startswith("Basic "):
            try:
                decoded = base64.b64decode(auth_header[6:]).decode()
                u, p = decoded.split(":", 1)
                if auth_basic(u, p):
                    username = u
            except Exception:
                pass

        if method == "CONNECT":
            if not username:
                writer.write(http_response(407, "Proxy Auth Required", {"Proxy-Authenticate": 'Basic realm="Proxy"'}))
                await writer.drain()
                writer.close()
                return
            target = path.strip("/")
            if ":" not in target:
                writer.write(http_response(400, "Invalid target"))
                await writer.drain()
                writer.close()
                return
            host, port_s = target.rsplit(":", 1)
            await do_connect(username, host, int(port_s), writer)
            return

        if path.startswith("/api/"):
            admin = headers.get("Authorization", "") == f"Bearer {ADMIN_KEY}"
            if not admin and path != "/api/stats":
                writer.write(http_response(401, json.dumps({"error": "Unauthorized"}), {"Content-Type": "application/json"}))
                await writer.drain()
                writer.close()
                return

            status_code = 200
            resp_body = {}

            if path == "/api/users" and method == "GET":
                status_code, resp_body = await api_list_users()
            elif path == "/api/users" and method == "POST":
                status_code, resp_body = await api_create_user(json.loads(body) if body else {})
            elif path.startswith("/api/users/") and method == "DELETE":
                uname = path.split("/api/users/")[1].split("/")[0].split("?")[0]
                status_code, resp_body = await api_delete_user(uname)
            elif path.endswith("/toggle") and method == "POST":
                uname = path.split("/api/users/")[1].split("/")[0]
                status_code, resp_body = await api_toggle_user(uname)
            elif path.endswith("/rotate") and method == "POST":
                uname = path.split("/api/users/")[1].split("/")[0]
                status_code, resp_body = await api_rotate_user(uname)
            elif path == "/api/pool":
                resp_body = await api_pool()
            elif path == "/api/stats":
                resp_body = await api_stats()
            else:
                status_code = 404
                resp_body = {"error": "Not found"}

            data = json.dumps(resp_body).encode()
            writer.write(f"HTTP/1.1 {status_code} OK\r\nContent-Type: application/json\r\nContent-Length: {len(data)}\r\nConnection: close\r\n\r\n".encode())
            writer.write(data)
            await writer.drain()
            writer.close()
            return

        if path == "/" or path == "/index.html":
            try:
                html = (Path(__file__).parent / "dashboard.html").read_text(encoding="utf-8")
                data = html.encode()
                writer.write(f"HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {len(data)}\r\nConnection: close\r\n\r\n".encode())
                writer.write(data)
            except Exception:
                writer.write(http_response(404, "Dashboard not found"))
            await writer.drain()
            writer.close()
            return

        if path == "/health":
            data = json.dumps({"status": "ok"}).encode()
            writer.write(f"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {len(data)}\r\nConnection: close\r\n\r\n".encode())
            writer.write(data)
            await writer.drain()
            writer.close()
            return

        if not username:
            writer.write(http_response(407, "Proxy Auth Required", {"Proxy-Authenticate": 'Basic realm="Proxy"'}))
            await writer.drain()
            writer.close()
            return

        full_url = path if path.startswith("http") else f"http://{headers.get('Host', 'localhost')}{path}"
        sc, sh, sb = await do_forward(username, method, full_url, headers, body)

        hdr_lines = f"HTTP/1.1 {sc} OK\r\nConnection: close\r\n"
        for k, v in sh.items():
            hdr_lines += f"{k}: {v}\r\n"
        hdr_lines += f"Content-Length: {len(sb)}\r\n\r\n"
        writer.write(hdr_lines.encode())
        writer.write(sb)
        await writer.drain()
        writer.close()

    except asyncio.TimeoutError:
        try:
            writer.close()
        except Exception:
            pass
    except Exception as e:
        logger.warning("http protocol err %s: %s", addr, e)
        try:
            writer.close()
        except Exception:
            pass


# ─── Main ─────────────────────────────────────────────────────────────────────

async def on_startup(_app):
    asyncio.create_task(pool.loop())


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    logger.info("========================================")
    logger.info("  Multi-Protocol Proxy Server")
    logger.info("  Port: %d (HTTP + HTTPS + SOCKS4 + SOCKS5)", port)
    logger.info("  Admin key: %s", ADMIN_KEY)
    logger.info("  Hostname: %s", get_hostname())
    logger.info("========================================")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def main():
        asyncio.create_task(pool.loop())
        server = await asyncio.start_server(on_client, "0.0.0.0", port)
        logger.info("Listening on 0.0.0.0:%d", port)
        async with server:
            await server.serve_forever()

    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("Shutting down.")
