from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import aiohttp
from aiohttp import web

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("rotating-proxy")


@dataclass
class Proxy:
    ip: str
    port: int
    protocol: str = "http"
    source: str = ""
    fetched_at: float = 0.0
    last_checked: float = 0.0
    latency_ms: float = 9999.0
    is_alive: bool = False
    consecutive_failures: int = 0
    total_checks: int = 0
    total_failures: int = 0
    times_used: int = 0

    @property
    def address(self) -> str:
        return f"{self.ip}:{self.port}"

    @property
    def url(self) -> str:
        return f"{self.protocol}://{self.ip}:{self.port}"

    @property
    def uptime(self) -> float:
        if self.total_checks == 0:
            return 0.0
        return 1.0 - (self.total_failures / self.total_checks)

    def mark_success(self, latency_ms: float) -> None:
        self.last_checked = time.time()
        self.latency_ms = latency_ms
        self.consecutive_failures = 0
        self.is_alive = True
        self.total_checks += 1

    def mark_failure(self) -> None:
        self.last_checked = time.time()
        self.consecutive_failures += 1
        self.total_checks += 1
        self.total_failures += 1
        if self.consecutive_failures >= 3:
            self.is_alive = False

    def to_dict(self) -> dict:
        return {
            "ip": self.ip,
            "port": self.port,
            "protocol": self.protocol,
            "source": self.source,
            "latency_ms": round(self.latency_ms, 1),
            "is_alive": self.is_alive,
            "uptime": round(self.uptime, 4),
            "times_used": self.times_used,
            "address": self.address,
        }


class ProxyPool:
    def __init__(self, max_size: int = 500):
        self.max_size = max_size
        self._proxies: dict[str, Proxy] = {}
        self._alive_list: list[Proxy] = []
        self._last_rotation: float = 0.0
        self._rotation_index: int = 0

    def add_proxies(self, proxies: list[Proxy]) -> int:
        added = 0
        for p in proxies:
            key = p.address
            if key not in self._proxies:
                p.fetched_at = time.time()
                self._proxies[key] = p
                added += 1

        if len(self._proxies) > self.max_size:
            sorted_by_score = sorted(
                self._proxies.values(),
                key=lambda x: (x.is_alive, -x.latency_ms, x.times_used),
            )
            to_remove = sorted_by_score[: len(self._proxies) - self.max_size]
            for p in to_remove:
                del self._proxies[p.address]

        self._rebuild_alive_list()
        return added

    def _rebuild_alive_list(self) -> None:
        self._alive_list = sorted(
            [p for p in self._proxies.values() if p.is_alive],
            key=lambda x: (x.latency_ms, x.times_used),
        )

    def get_proxy(self) -> Optional[Proxy]:
        if not self._alive_list:
            self._rebuild_alive_list()
        if not self._alive_list:
            return None

        proxy = self._alive_list[self._rotation_index % len(self._alive_list)]
        proxy.times_used += 1
        self._rotation_index += 1
        if self._rotation_index >= len(self._alive_list):
            self._rotation_index = 0
            random.shuffle(self._alive_list)
        return proxy

    def get_random_proxy(self) -> Optional[Proxy]:
        if not self._alive_list:
            self._rebuild_alive_list()
        if not self._alive_list:
            return None
        proxy = random.choice(self._alive_list)
        proxy.times_used += 1
        return proxy

    def mark_dead(self, address: str) -> None:
        if address in self._proxies:
            self._proxies[address].mark_failure()
            self._rebuild_alive_list()

    def cleanup_stale(self, max_age: float = 3600) -> int:
        now = time.time()
        to_remove = []
        for addr, p in self._proxies.items():
            if not p.is_alive and (now - p.last_checked) > max_age:
                to_remove.append(addr)
            elif p.consecutive_failures >= 5:
                to_remove.append(addr)
        for addr in to_remove:
            del self._proxies[addr]
        self._rebuild_alive_list()
        return len(to_remove)

    @property
    def alive_count(self) -> int:
        return len(self._alive_list)

    @property
    def total_count(self) -> int:
        return len(self._proxies)

    def get_stats(self) -> dict:
        alive = [p for p in self._proxies.values() if p.is_alive]
        return {
            "total": self.total_count,
            "alive": self.alive_count,
            "avg_latency": round(
                sum(p.latency_ms for p in alive) / max(len(alive), 1), 1
            ),
            "sources": {},
        }

    def get_all_dicts(self) -> list[dict]:
        alive = [p for p in self._proxies.values() if p.is_alive]
        dead = [p for p in self._proxies.values() if not p.is_alive]
        return [p.to_dict() for p in alive] + [p.to_dict() for p in dead[:50]]


class ProxyFetcher:
    SOURCES = [
        {
            "name": "proxyscrape-http",
            "url": "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=protocolipport&format=text&protocol=http",
        },
        {
            "name": "proxyscrape-socks5",
            "url": "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=protocolipport&format=text&protocol=socks5",
        },
        {
            "name": "speedx-http",
            "url": "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
        },
        {
            "name": "speedx-socks5",
            "url": "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt",
        },
        {
            "name": "monosans-http",
            "url": "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
        },
        {
            "name": "monosans-socks5",
            "url": "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt",
        },
        {
            "name": "roosterkid",
            "url": "https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTP_RAW.txt",
        },
        {
            "name": "clarketm",
            "url": "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
        },
    ]

    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=10)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    def _parse_line(self, line: str, default_protocol: str = "http") -> Optional[dict]:
        line = line.strip()
        if not line or line.startswith("#"):
            return None

        protocol = default_protocol
        if "://" in line:
            protocol, rest = line.split("://", 1)
            line = rest

        parts = line.split(":")
        if len(parts) < 2:
            return None

        ip = parts[0].strip()
        try:
            port = int(parts[1].strip())
        except (ValueError, IndexError):
            return None

        if not self._validate_ip(ip) or port < 1 or port > 65535:
            return None

        return {"ip": ip, "port": port, "protocol": protocol}

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

    async def fetch_source(self, source: dict) -> list[Proxy]:
        name = source["name"]
        url = source["url"]
        protocol = "socks5" if "socks5" in name.lower() else "http"

        try:
            session = await self._get_session()
            async with session.get(url) as resp:
                if resp.status != 200:
                    return []
                text = await resp.text()
        except Exception as e:
            logger.warning("Failed to fetch %s: %s", name, e)
            return []

        proxies = []
        for line in text.splitlines():
            parsed = self._parse_line(line, default_protocol=protocol)
            if parsed:
                proxies.append(Proxy(
                    ip=parsed["ip"],
                    port=parsed["port"],
                    protocol=parsed["protocol"],
                    source=name,
                ))

        logger.info("Fetched %d proxies from %s", len(proxies), name)
        return proxies

    async def fetch_all(self) -> list[Proxy]:
        tasks = [self.fetch_source(s) for s in self.SOURCES]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_proxies = []
        for i, result in enumerate(results):
            if isinstance(result, list):
                all_proxies.extend(result)

        seen = {}
        for p in all_proxies:
            key = p.address
            if key not in seen:
                seen[key] = p
        deduped = list(seen.values())

        logger.info("Total unique proxies fetched: %d", len(deduped))
        return deduped


class HealthChecker:
    def __init__(self, concurrency: int = 500, timeout: float = 2.0):
        self.concurrency = concurrency
        self.timeout = timeout
        self.test_url = "http://httpbin.org/ip"
        self._semaphore = asyncio.Semaphore(concurrency)
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(
                limit=self.concurrency,
                ttl_dns_cache=300,
            )
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            self._session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
            )
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def check_single(self, proxy: Proxy) -> None:
        async with self._semaphore:
            try:
                session = await self._get_session()
                start = time.time()
                async with session.get(self.test_url, proxy=proxy.url) as resp:
                    latency = (time.time() - start) * 1000
                    if resp.status == 200:
                        proxy.mark_success(latency)
                    else:
                        proxy.mark_failure()
            except Exception:
                proxy.mark_failure()

    async def check_all(self, pool: ProxyPool) -> None:
        proxies = list(pool._proxies.values())
        if not proxies:
            return

        logger.info("Checking %d proxies...", len(proxies))
        start = time.time()

        tasks = [self.check_single(p) for p in proxies]
        await asyncio.gather(*tasks, return_exceptions=True)

        pool._rebuild_alive_list()
        elapsed = time.time() - start
        rate = len(proxies) / max(elapsed, 0.01)
        logger.info(
            "Check done: %d/%d alive (%.1fs, %.0f/sec)",
            pool.alive_count, len(proxies), elapsed, rate,
        )


class RotatingProxyService:
    def __init__(self):
        self.pool = ProxyPool(max_size=500)
        self.fetcher = ProxyFetcher()
        self.checker = HealthChecker(concurrency=500, timeout=2.0)
        self.rotation_interval = 300
        self._running = False
        self._start_time = time.time()
        self._request_count = 0

    async def fetch_and_check(self):
        try:
            proxies = await self.fetcher.fetch_all()
            added = self.pool.add_proxies(proxies)
            logger.info("Added %d new proxies (total: %d)", added, self.pool.total_count)

            await self.checker.check_all(self.pool)
            self.pool.cleanup_stale(max_age=1800)
        except Exception as e:
            logger.error("Fetch/check error: %s", e)

    async def rotation_loop(self):
        self._running = True
        logger.info("Rotation started (interval=%ds)", self.rotation_interval)

        await self.fetch_and_check()

        while self._running:
            await asyncio.sleep(self.rotation_interval)
            await self.fetch_and_check()

    def get_proxy(self) -> Optional[dict]:
        proxy = self.pool.get_proxy()
        if proxy:
            self._request_count += 1
            return {
                "proxy": proxy.url,
                "ip": proxy.ip,
                "port": proxy.port,
                "protocol": proxy.protocol,
                "latency_ms": round(proxy.latency_ms, 1),
                "times_used": proxy.times_used,
            }
        return None

    def get_stats(self) -> dict:
        uptime = time.time() - self._start_time
        return {
            "uptime_seconds": round(uptime, 1),
            "total_requests": self._request_count,
            "pool": self.pool.get_stats(),
            "rotation_interval": self.rotation_interval,
            "last_rotation": self.pool._last_rotation,
        }


service = RotatingProxyService()

DASHBOARD_HTML = Path(__file__).parent / "dashboard.html"


async def handle_dashboard(request: web.Request) -> web.Response:
    html = DASHBOARD_HTML.read_text(encoding="utf-8")
    return web.Response(text=html, content_type="text/html")


async def handle_get_proxy(request: web.Request) -> web.Response:
    proxy = service.get_proxy()
    if proxy:
        return web.json_response({"status": "ok", **proxy})
    return web.json_response({"status": "error", "message": "No alive proxies"}, status=503)


async def handle_get_multiple(request: web.Request) -> web.Response:
    count = int(request.query.get("count", 1))
    count = min(count, 50)

    proxies = []
    used = set()
    for _ in range(count):
        p = service.pool.get_random_proxy()
        if p and p.address not in used:
            proxies.append({
                "proxy": p.url,
                "ip": p.ip,
                "port": p.port,
                "protocol": p.protocol,
            })
            used.add(p.address)

    if proxies:
        return web.json_response({
            "status": "ok",
            "count": len(proxies),
            "proxies": proxies,
        })
    return web.json_response({"status": "error", "message": "No alive proxies"}, status=503)


async def handle_proxies_list(request: web.Request) -> web.Response:
    return web.json_response({
        "status": "ok",
        **service.pool.get_stats(),
        "proxies": service.pool.get_all_dicts(),
    })


async def handle_stats(request: web.Request) -> web.Response:
    return web.json_response({"status": "ok", **service.get_stats()})


async def handle_health(request: web.Request) -> web.Response:
    return web.json_response({"status": "ok"})


async def handle_refresh(request: web.Request) -> web.Response:
    asyncio.create_task(service.fetch_and_check())
    return web.json_response({"status": "ok", "message": "Refresh started"})


def create_app() -> web.Application:
    app = web.Application()
    app.router.add_route("GET", "/", handle_dashboard)
    app.router.add_route("GET", "/get", handle_get_proxy)
    app.router.add_route("GET", "/get/{count}", handle_get_multiple)
    app.router.add_route("GET", "/proxies", handle_proxies_list)
    app.router.add_route("GET", "/stats", handle_stats)
    app.router.add_route("GET", "/health", handle_health)
    app.router.add_route("GET", "/refresh", handle_refresh)
    return app


async def on_startup(app):
    asyncio.create_task(service.rotation_loop())


async def on_cleanup(app):
    service._running = False
    await service.fetcher.close()
    await service.checker.close()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    logger.info("Starting Rotating Proxy Service on port %d", port)

    app = create_app()
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    web.run_app(app, host="0.0.0.0", port=port)
