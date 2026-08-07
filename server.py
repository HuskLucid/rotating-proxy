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
    "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
    "https://raw.githubusercontent.com/mmpx12/proxy-list/master/http.txt",
    "https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTP_RAW.txt",
]
SOCKS5_SOURCES = [
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt",
    "https://raw.githubusercontent.com/mmpx12/proxy-list/master/socks5.txt",
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

    @staticmethod
    def _valid_ip(ip: str) -> bool:
        parts = ip.split(".")
        if len(parts) != 4:
            return False
        try:
            return all(0 <= int(p) <= 255 for p in parts)
        except ValueError:
            return False

    def _parse_lines(self, text: str, prefix: str) -> list[str]:
        out = []
        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith("#") or line.startswith("//"):
                continue
            if "://" in line:
                line = line.split("://", 1)[1]
            line = line.split("#")[0].strip()
            parts = line.split(":")
            if len(parts) >= 2:
                ip, port = parts[0].strip(), parts[1].strip().split()[0] if parts[1].strip() else ""
                if self._valid_ip(ip) and port.isdigit() and 1 <= int(port) <= 65535:
                    p = f"{prefix}{ip}:{port}"
                    if p not in out:
                        out.append(p)
        return out

    async def _fetch(self, sources: list[str], prefix: str) -> list[str]:
        out = []
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout, headers={"User-Agent": "Mozilla/5.0"}) as s:
            for url in sources:
                try:
                    async with s.get(url) as r:
                        if r.status == 200:
                            text = await r.text()
                            out.extend(self._parse_lines(text, prefix))
                except Exception:
                    pass
        return out

    async def _check_http(self, proxy: str) -> tuple[bool, float]:
        try:
            t0 = time.time()
            timeout = aiohttp.ClientTimeout(total=5)
            async with aiohttp.ClientSession(timeout=timeout) as s:
                async with s.get("http://httpbin.org/ip", proxy=proxy, ssl=False) as r:
                    lat = (time.time() - t0) * 1000
                    return (True, lat) if r.status == 200 else (False, 9999)
        except Exception:
            return False, 9999

    async def _check_socks(self, proxy: str) -> tuple[bool, float]:
        try:
            hp = proxy.split("://")[1]
            h, p = hp.split(":")[0], int(hp.split(":")[1])
            t0 = time.time()
            rd, wr = await asyncio.wait_for(asyncio.open_connection(h, p), timeout=5)
            wr.write(b"\x05\x01\x00")
            await wr.drain()
            resp = await asyncio.wait_for(rd.read(2), timeout=5)
            lat = (time.time() - t0) * 1000
            wr.close()
            await wr.wait_closed()
            if resp and len(resp) >= 2 and resp[0] == 0x05 and resp[1] == 0x00:
                return True, lat
        except Exception:
            pass
        return False, 9999

    async def _batch(self, proxies: list[str], check_fn, sem: asyncio.Semaphore) -> list[str]:
        async def one(p):
            async with sem:
                ok, lat = await check_fn(p)
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
            http_raw = await self._fetch(HTTP_SOURCES, "http://")
            socks_raw = await self._fetch(SOCKS5_SOURCES, "socks5://")
            logger.info("Fetched %d HTTP, %d SOCKS5", len(http_raw), len(socks_raw))

            sem_h = asyncio.Semaphore(200)
            sem_s = asyncio.Semaphore(100)
            http_alive, socks_alive = await asyncio.gather(
                self._batch(http_raw, self._check_http, sem_h),
                self._batch(socks_raw, self._check_socks, sem_s),
            )
            self.http = sorted(http_alive, key=lambda x: self.latency.get(x, 9999))
            self.socks5 = sorted(socks_alive, key=lambda x: self.latency.get(x, 9999))
            self._last_fetch = time.time()
            logger.info("Pool alive: %d HTTP, %d SOCKS5", len(self.http), len(self.socks5))
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
                logger.info("Dead HTTP removed: %s (pool: %d HTTP)", p, len(self.http))
            if p in self.socks5:
                self.socks5.remove(p)
                logger.info("Dead SOCKS5 removed: %s (pool: %d SOCKS5)", p, len(self.socks5))

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


def extract_auth(request: web.Request) -> Optional[str]:
    auth = request.headers.get("Proxy-Authorization", "")
    if not auth.startswith("Basic "):
        return None
    try:
        decoded = base64.b64decode(auth[6:]).decode()
        u, p = decoded.split(":", 1)
        if auth_basic(u, p):
            return u
    except Exception:
        pass
    return None


def unauthorized():
    return web.Response(
        status=407, text="Proxy Authentication Required",
        headers={"Proxy-Authenticate": 'Basic realm="Proxy"'},
    )


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


# ─── HTTP Handlers (aiohttp) ──────────────────────────────────────────────────

async def handle_connect(request: web.Request) -> web.StreamResponse:
    username = extract_auth(request)
    if not username:
        return unauthorized()

    target = request.path.strip("/")
    if ":" not in target:
        return web.Response(status=400, text="Invalid CONNECT target")
    host, port_s = target.rsplit(":", 1)
    port = int(port_s)

    uidx = users.get(username, {}).get("upstream_index", 0)
    upstream = pool.get_http(uidx)
    if not upstream:
        stats.rec(username, False)
        return web.Response(status=503, text="No upstream proxies available")

    logger.info("[%s] CONNECT %s:%d via %s", username, host, port, upstream)
    pw = None
    try:
        uh = upstream.split("://")[1].split(":")[0]
        up = int(upstream.split("://")[1].split(":")[1])
        pr, pw = await asyncio.wait_for(asyncio.open_connection(uh, up), timeout=5)
        pw.write(f"CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n\r\n".encode())
        await pw.drain()
        status = await asyncio.wait_for(pr.readline(), timeout=5)
        if b"200" not in status:
            pw.close()
            stats.rec(username, False)
            await pool.mark_dead(upstream)
            return web.Response(status=502, text="Upstream CONNECT rejected")
    except asyncio.TimeoutError:
        stats.rec(username, False)
        await pool.mark_dead(upstream)
        if pw:
            pw.close()
        return web.Response(status=502, text="Upstream timeout")
    except Exception as e:
        stats.rec(username, False)
        await pool.mark_dead(upstream)
        if pw:
            pw.close()
        return web.Response(status=502, text=str(e))

    transport = request.transport
    if transport is None:
        pw.close()
        stats.rec(username, False)
        return web.Response(status=500, text="No transport")

    response = web.StreamResponse(status=200)
    response.force_close()
    await response.prepare(request)

    try:
        await asyncio.gather(pipe(pr, transport), pipe(transport, pw), return_exceptions=True)
    except Exception:
        pass
    pw.close()
    stats.rec(username, True)
    return response


async def handle_forward(request: web.Request) -> web.StreamResponse:
    username = extract_auth(request)
    if not username:
        return unauthorized()

    url = str(request.url)
    uidx = users.get(username, {}).get("upstream_index", 0)
    upstream = pool.get_http(uidx)
    logger.info("[%s] %s %s via %s", username, request.method, url[:80], upstream or "direct")

    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            body = await request.read()
            hdrs = {k: v for k, v in request.headers.items()
                    if k.lower() not in ("host", "proxy-connection", "proxy-authorization")}
            hdrs["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
            async with session.request(
                method=request.method, url=url, headers=hdrs,
                data=body if body else None, proxy=upstream, allow_redirects=False,
            ) as resp:
                rb = await resp.read()
                rh = {k: v for k, v in resp.headers.items() if k.lower() not in ("transfer-encoding", "connection")}
                stats.rec(username, resp.status < 400)
                return web.Response(status=resp.status, headers=rh, body=rb)
    except Exception as e:
        stats.rec(username, False)
        if upstream:
            await pool.mark_dead(upstream)
        return web.Response(status=502, text=f"Forward error: {e}")


# ─── API ──────────────────────────────────────────────────────────────────────

async def api_create(data: dict) -> tuple[int, dict]:
    cu, cp = data.get("username", ""), data.get("password", "")
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
        "proxy_http": f"http://{cu}:{cp}@{h}",
        "proxy_socks5": f"socks5://{cu}:{cp}@{h}",
    }


async def api_list() -> tuple[int, dict]:
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


# ─── SOCKS4/5 raw TCP server (same port via protocol sniffing) ───────────────

async def handle_socks5(reader, writer, addr):
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
        logger.info("[SOCKS5] %s -> %s:%d", su, host, port)

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
                    raise Exception("upstream socks5 rejected")
                writer.write(b"\x05\x00\x00\x01\x00\x00\x00\x00\x00\x00")
                await writer.drain()
                await asyncio.gather(pipe(reader, rw), pipe(rr, writer), return_exceptions=True)
                rw.close()
                stats.rec(su, True)
                return
            except Exception:
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
    except Exception:
        try:
            writer.close()
        except Exception:
            pass


async def handle_socks4(reader, writer, addr):
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
        logger.info("[SOCKS4] %s -> %s:%d", su, host, port)
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
    except Exception:
        try:
            writer.close()
        except Exception:
            pass


# ─── Single-port protocol router ──────────────────────────────────────────────

async def on_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    addr = writer.get_extra_info("peername")
    try:
        first = await asyncio.wait_for(reader.read(1), timeout=5)
        if not first:
            writer.close()
            return

        b = first[0]
        if b == 0x05:
            logger.debug("SOCKS5 from %s", addr)
            await handle_socks5(reader, writer, addr)
        elif b == 0x04:
            logger.debug("SOCKS4 from %s", addr)
            await handle_socks4(reader, writer, addr)
        elif 0x20 <= b < 0x7F:
            logger.debug("HTTP from %s", addr)
            await route_http(reader, writer, addr, first)
        else:
            logger.warning("Unknown proto 0x%02x from %s", b, addr)
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


async def route_http(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, addr, first_byte: bytes):
    try:
        buf = first_byte
        while not buf.endswith(b"\r\n\r\n"):
            chunk = await asyncio.wait_for(reader.read(4096), timeout=10)
            if not chunk:
                writer.close()
                return
            buf += chunk

        header_end = buf.index(b"\r\n\r\n") + 4
        raw_headers = buf[:header_end].decode(errors="ignore")
        leftover = buf[header_end:]

        lines = raw_headers.split("\r\n")
        parts = lines[0].split(" ")
        if len(parts) < 3:
            writer.close()
            return
        method, path, version = parts[0], parts[1], parts[2]

        headers = {}
        for line in lines[1:]:
            if ": " in line:
                k, v = line.split(": ", 1)
                headers[k] = v

        content_length = int(headers.get("Content-Length", "0"))
        body = leftover
        need = content_length - len(body)
        if need > 0:
            body += await asyncio.wait_for(reader.readexactly(need), timeout=10)

        username = None
        auth = headers.get("Proxy-Authorization", "")
        if auth.startswith("Basic "):
            try:
                decoded = base64.b64decode(auth[6:]).decode()
                u, p = decoded.split(":", 1)
                if auth_basic(u, p):
                    username = u
            except Exception:
                pass

        if method == "CONNECT":
            if not username:
                w = "HTTP/1.1 407 Proxy Authentication Required\r\nProxy-Authenticate: Basic realm=\"Proxy\"\r\nContent-Length: 0\r\n\r\n"
                writer.write(w.encode())
                await writer.drain()
                writer.close()
                return
            target = path.strip("/")
            if ":" not in target:
                writer.write(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n")
                await writer.drain()
                writer.close()
                return
            host, port_s = target.rsplit(":", 1)
            port = int(port_s)

            uidx = users.get(username, {}).get("upstream_index", 0)
            upstream = pool.get_http(uidx)
            if not upstream:
                stats.rec(username, False)
                writer.write(b"HTTP/1.1 503 No Upstream\r\nContent-Length: 0\r\n\r\n")
                await writer.drain()
                writer.close()
                return

            logger.info("[%s] CONNECT %s:%d via %s", username, host, port, upstream)
            uh = upstream.split("://")[1].split(":")[0]
            up = int(upstream.split("://")[1].split(":")[1])
            pw = None
            try:
                pr, pw = await asyncio.wait_for(asyncio.open_connection(uh, up), timeout=5)
                pw.write(f"CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n\r\n".encode())
                await pw.drain()
                status = await asyncio.wait_for(pr.readline(), timeout=5)
                if b"200" not in status:
                    pw.close()
                    stats.rec(username, False)
                    await pool.mark_dead(upstream)
                    writer.write(b"HTTP/1.1 502 Upstream Rejected\r\nContent-Length: 0\r\n\r\n")
                    await writer.drain()
                    writer.close()
                    return
            except Exception as e:
                stats.rec(username, False)
                if upstream:
                    await pool.mark_dead(upstream)
                if pw:
                    pw.close()
                writer.write(f"HTTP/1.1 502 {e}\r\nContent-Length: 0\r\n\r\n".encode())
                await writer.drain()
                writer.close()
                return

            writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            await writer.drain()
            stats.rec(username, True)
            try:
                await asyncio.gather(pipe(pr, writer), pipe(reader, pw), return_exceptions=True)
            except Exception:
                pass
            pw.close()
            return

        if path.startswith("/api/"):
            admin = headers.get("Authorization", "") == f"Bearer {ADMIN_KEY}"
            if not admin and path != "/api/stats":
                resp = json.dumps({"error": "Unauthorized"}).encode()
                writer.write(f"HTTP/1.1 401 Unauthorized\r\nContent-Type: application/json\r\nContent-Length: {len(resp)}\r\nConnection: close\r\n\r\n".encode())
                writer.write(resp)
                await writer.drain()
                writer.close()
                return

            sc, rb = 200, {}
            if path == "/api/users" and method == "GET":
                sc, rb = await api_list()
            elif path == "/api/users" and method == "POST":
                sc, rb = await api_create(json.loads(body) if body else {})
            elif path.startswith("/api/users/") and method == "DELETE":
                un = path.split("/api/users/")[1].split("/")[0].split("?")[0]
                if un not in users:
                    sc, rb = 404, {"error": "Not found"}
                else:
                    del users[un]
                    save_users(users)
                    sc, rb = 200, {"status": "ok", "deleted": un}
            elif path.endswith("/toggle") and method == "POST":
                un = path.split("/api/users/")[1].split("/")[0]
                if un not in users:
                    sc, rb = 404, {"error": "Not found"}
                else:
                    users[un]["enabled"] = not users[un].get("enabled", True)
                    save_users(users)
                    sc, rb = 200, {"status": "ok", "username": un, "enabled": users[un]["enabled"]}
            elif path.endswith("/rotate") and method == "POST":
                un = path.split("/api/users/")[1].split("/")[0]
                if un not in users:
                    sc, rb = 404, {"error": "Not found"}
                elif pool.size == 0:
                    sc, rb = 503, {"error": "No proxies"}
                else:
                    idx = random.randint(0, pool.size - 1)
                    users[un]["upstream_index"] = idx
                    save_users(users)
                    up = pool.get_http(idx)
                    ex = pool.exit_ip(up) if up else "N/A"
                    sup = pool.get_socks5(idx)
                    sex = pool.exit_ip(sup) if sup else "N/A"
                    lat = pool.get_lat(up) if up else 0
                    sc, rb = 200, {"status": "ok", "exit_ip": ex, "socks_exit_ip": sex, "latency": round(lat, 1)}
            elif path == "/api/pool":
                rb = {"status": "ok", "pool_size": pool.size, "http_pool": len(pool.http), "socks5_pool": len(pool.socks5), "last_fetch": pool._last_fetch}
            elif path == "/api/stats":
                rb = {"status": "ok", "pool_size": pool.size, "http_pool": len(pool.http), "socks5_pool": len(pool.socks5), **stats.to_dict()}
            else:
                sc, rb = 404, {"error": "Not found"}

            resp = json.dumps(rb).encode()
            writer.write(f"HTTP/1.1 {sc} OK\r\nContent-Type: application/json\r\nContent-Length: {len(resp)}\r\nConnection: close\r\n\r\n".encode())
            writer.write(resp)
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
                writer.write(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")
            await writer.drain()
            writer.close()
            return

        if path == "/health":
            resp = b'{"status":"ok"}'
            writer.write(f"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {len(resp)}\r\nConnection: close\r\n\r\n".encode())
            writer.write(resp)
            await writer.drain()
            writer.close()
            return

        if not username:
            w = "HTTP/1.1 407 Proxy Authentication Required\r\nProxy-Authenticate: Basic realm=\"Proxy\"\r\nContent-Length: 0\r\n\r\n"
            writer.write(w.encode())
            await writer.drain()
            writer.close()
            return

        full_url = path if path.startswith("http") else f"http://{headers.get('Host', 'localhost')}{path}"
        uidx = users.get(username, {}).get("upstream_index", 0)
        upstream = pool.get_http(uidx)
        logger.info("[%s] %s %s via %s", username, method, full_url[:80], upstream or "direct")

        try:
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                hdrs = {k: v for k, v in headers.items()
                        if k.lower() not in ("host", "proxy-connection", "proxy-authorization", "content-length")}
                hdrs["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
                async with session.request(method=method, url=full_url, headers=hdrs,
                                           data=body if body else None, proxy=upstream, allow_redirects=False) as resp:
                    rb = await resp.read()
                    resp_hdrs = f"HTTP/1.1 {resp.status} OK\r\nConnection: close\r\n"
                    for k, v in resp.headers.items():
                        if k.lower() not in ("transfer-encoding", "connection"):
                            resp_hdrs += f"{k}: {v}\r\n"
                    resp_hdrs += f"Content-Length: {len(rb)}\r\n\r\n"
                    writer.write(resp_hdrs.encode())
                    writer.write(rb)
                    stats.rec(username, resp.status < 400)
        except Exception as e:
            stats.rec(username, False)
            if upstream:
                await pool.mark_dead(upstream)
            writer.write(f"HTTP/1.1 502 {e}\r\nContent-Length: 0\r\n\r\n".encode())
        await writer.drain()
        writer.close()

    except asyncio.TimeoutError:
        try:
            writer.close()
        except Exception:
            pass
    except Exception as e:
        logger.warning("HTTP route err %s: %s", addr, e)
        try:
            writer.close()
        except Exception:
            pass


# ─── Main ─────────────────────────────────────────────────────────────────────

async def main():
    port = int(os.environ.get("PORT", 8080))
    logger.info("=" * 50)
    logger.info("  Multi-Protocol Proxy Server")
    logger.info("  Single port %d: HTTP+HTTPS+SOCKS4+SOCKS5", port)
    logger.info("  Admin key: %s", ADMIN_KEY)
    logger.info("  Hostname: %s", get_hostname())
    logger.info("=" * 50)
    asyncio.create_task(pool.loop())
    server = await asyncio.start_server(on_client, "0.0.0.0", port)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
