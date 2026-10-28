"""Part 15: a small, deterministic stand-in for the marketplace sellers API (someone else's system).

uv run python -m scripts.part15_sellers_api            # serve on http://localhost:8765

GET  /v1/sellers?updated_since=<ISO-8601>&limit=<1-100>&cursor=<opaque>
     Sellers with updated_at >= updated_since, ordered by (updated_at, seller_id), one page at a time.
     The response is {"data": [...], "next_cursor": "..." | null}. The cursor is keyset pagination: it encodes the
     last (updated_at, seller_id) returned, so rows changing while you page can't shift you onto a duplicate.
     Rate limit: after every 5 pages the next request gets 429 with `Retry-After: 1`, and requests keep getting 429
     until that second has passed.
POST /_admin/clock {"at": "<ISO-8601>"}   lab only: move the simulated clock (the API serves the world as of then)
GET  /_admin/stats                        lab only: pages served and 429s sent since the last clock change

The data comes from datagen.sellers (seed 42): 1,200 sellers at the first pull, and the changes before the second.
"""

import argparse
import base64
import json
import threading
import time
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from datagen import sellers

PORT = 8765
PAGES_PER_WINDOW = 5
RETRY_AFTER_S = 1


def iso(t: datetime) -> str:
    return t.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


class SellersApi:
    def __init__(self, seed: int = 42) -> None:
        self.world = sellers.build(seed)
        self.lock = threading.Lock()
        self.set_clock(sellers.FIRST_PULL)

    def set_clock(self, at: datetime) -> None:
        with self.lock:
            self.clock = at
            rows = sorted(self.world.as_of(at), key=lambda r: (r["updated_at"], r["seller_id"]))
            self.rows = [{**r, "created_at": iso(r["created_at"]), "updated_at": iso(r["updated_at"])} for r in rows]
            self.pages = self.throttled = self.served_in_window = 0
            self.blocked_until = 0.0

    def admit(self) -> float | None:
        """None if the request may proceed, else the seconds the client must wait."""
        with self.lock:
            now = time.monotonic()
            if now < self.blocked_until:
                self.throttled += 1
                return self.blocked_until - now
            if self.served_in_window == PAGES_PER_WINDOW:
                self.served_in_window = 0
                self.blocked_until = now + RETRY_AFTER_S
                self.throttled += 1
                return RETRY_AFTER_S
            self.served_in_window += 1
            self.pages += 1
            return None

    def page(self, updated_since: str, limit: int, cursor: str | None) -> dict:
        since = iso(parse(updated_since))
        after = tuple(json.loads(base64.urlsafe_b64decode(cursor))) if cursor else ("", "")
        rows = [r for r in self.rows if r["updated_at"] >= since and (r["updated_at"], r["seller_id"]) > after]
        data = rows[:limit]
        if len(rows) <= limit:
            return {"data": data, "next_cursor": None}
        last = json.dumps([data[-1]["updated_at"], data[-1]["seller_id"]]).encode()
        return {"data": data, "next_cursor": base64.urlsafe_b64encode(last).decode()}


def handler(api: SellersApi):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args) -> None:  # quiet
            pass

        def reply(self, status: int, body: dict, headers: dict | None = None) -> None:
            raw = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            for k, v in (headers or {}).items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(raw)

        def do_GET(self) -> None:
            url = urlparse(self.path)
            q = {k: v[0] for k, v in parse_qs(url.query).items()}
            if url.path == "/_admin/stats":
                return self.reply(200, {"clock": iso(api.clock), "pages": api.pages, "throttled": api.throttled})
            if url.path != "/v1/sellers":
                return self.reply(404, {"error": "not found"})
            wait = api.admit()
            if wait is not None:
                return self.reply(429, {"error": "rate limited"}, {"Retry-After": str(max(1, round(wait)))})
            try:
                limit = min(100, max(1, int(q.get("limit", "100"))))
                body = api.page(q.get("updated_since", "1970-01-01T00:00:00Z"), limit, q.get("cursor"))
            except ValueError as e:
                return self.reply(400, {"error": str(e)})
            self.reply(200, body)

        def do_POST(self) -> None:
            if urlparse(self.path).path != "/_admin/clock":
                return self.reply(404, {"error": "not found"})
            body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0"))) or b"{}")
            api.set_clock(parse(body["at"]))
            self.reply(200, {"clock": iso(api.clock), "sellers": len(api.rows)})

    return Handler


def serve(port: int = PORT, seed: int = 42) -> ThreadingHTTPServer:
    """Start the API on a background thread (tests); the caller shuts it down."""
    server = ThreadingHTTPServer(("127.0.0.1", port), handler(SellersApi(seed)))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--port", type=int, default=PORT)
    a = p.parse_args()
    server = ThreadingHTTPServer(("127.0.0.1", a.port), handler(SellersApi()))
    print(f"sellers API on http://localhost:{a.port}/v1/sellers (clock {iso(sellers.FIRST_PULL)})", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
