"""server.py — cockpit control server (aiohttp, VIRTUAL ONLY).

One process, one asyncio event loop: N PaperTrader WS-receive tasks + this
web server. Because everything shares the loop, parameter updates from HTTP
handlers are atomic w.r.t. event processing (no locks needed).

Endpoints:
  GET    /                          dashboard (static/index.html)
  GET    /ws                        WebSocket; server pushes all-market
                                    snapshots every --push-interval seconds
  GET    /api/markets               one-shot snapshot (poll fallback)
  POST   /api/markets/{slug}/params {"gamma": 1.5, ...} -> 200 applied / 400 rejected
  POST   /api/markets/{slug}/stop   stop the market's WS task (persists state)
  POST   /api/markets/{slug}/start  restart (state reloads from snapshot)
  POST   /api/markets               add {"token_id","slug","label","params","loss_limit"}
  DELETE /api/markets/{slug}        stop + remove

SAFETY: no order-routing code. No place/cancel/submit API, no signing, no
keys, no funds path — this server only mutates in-memory strategy configs and
reads state for display.

Run (from the repo root):
    python paper_trading/cockpit/server.py --config paper_trading/cockpit/markets.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from typing import List, Optional, Set

_HERE = os.path.dirname(os.path.abspath(__file__))
_PT_DIR = os.path.dirname(_HERE)
_ROOT = os.path.dirname(_PT_DIR)
for _p in (_HERE, _PT_DIR, _ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from aiohttp import WSMsgType, web  # noqa: E402

from orchestrator import MarketSpec, Orchestrator  # noqa: E402

STATIC_DIR = os.path.join(_HERE, "static")


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------
def _orch(request: web.Request) -> Orchestrator:
    return request.app["orchestrator"]


def _runner_or_404(request: web.Request):
    slug = request.match_info["slug"]
    runner = _orch(request).runners.get(slug)
    if runner is None:
        raise web.HTTPNotFound(text=json.dumps({"error": f"unknown market {slug!r}"}),
                               content_type="application/json")
    return runner


async def index(request: web.Request) -> web.FileResponse:
    return web.FileResponse(os.path.join(STATIC_DIR, "index.html"))


async def get_markets(request: web.Request) -> web.Response:
    return web.json_response(_orch(request).snapshot_all())


async def post_params(request: web.Request) -> web.Response:
    runner = _runner_or_404(request)
    try:
        updates = await request.json()
    except json.JSONDecodeError:
        return web.json_response({"ok": False, "errors": {"_request": "invalid JSON"}},
                                 status=400)
    result = runner.apply_params(updates if isinstance(updates, dict) else {})
    return web.json_response(result, status=200 if result["ok"] else 400)


async def post_stop(request: web.Request) -> web.Response:
    runner = _runner_or_404(request)
    if runner.running:
        await runner.stop(reason="manual")
    return web.json_response({"ok": True, "running": runner.running})


async def post_start(request: web.Request) -> web.Response:
    runner = _runner_or_404(request)
    runner.start()
    return web.json_response({"ok": True, "running": runner.running})


async def post_add_market(request: web.Request) -> web.Response:
    try:
        body = await request.json()
        spec = MarketSpec.from_dict(body)
        runner = _orch(request).add_market(spec, start=True)
    except (json.JSONDecodeError, ValueError, KeyError) as exc:
        return web.json_response({"ok": False, "error": str(exc)}, status=400)
    return web.json_response({"ok": True, "slug": runner.spec.slug})


async def delete_market(request: web.Request) -> web.Response:
    _runner_or_404(request)
    await _orch(request).remove_market(request.match_info["slug"])
    return web.json_response({"ok": True})


async def websocket_handler(request: web.Request) -> web.WebSocketResponse:
    ws = web.WebSocketResponse(heartbeat=30)
    await ws.prepare(request)
    clients: Set[web.WebSocketResponse] = request.app["ws_clients"]
    clients.add(ws)
    try:
        # immediate first frame so the UI renders without waiting a full tick
        await ws.send_json({"type": "snapshot",
                            "markets": _orch(request).snapshot_all()})
        async for msg in ws:
            if msg.type in (WSMsgType.CLOSE, WSMsgType.ERROR):
                break
    finally:
        clients.discard(ws)
    return ws


# ---------------------------------------------------------------------------
# Push loop: history sampling + loss-limit enforcement + broadcast
# ---------------------------------------------------------------------------
async def push_loop(app: web.Application) -> None:
    orch: Orchestrator = app["orchestrator"]
    interval: float = app["push_interval"]
    while True:
        await asyncio.sleep(interval)
        for runner in orch.breached_runners():          # safety: loss limit
            await runner.stop(reason="loss_limit")
        for runner in orch.runners.values():
            runner.record_history()
        clients: Set[web.WebSocketResponse] = app["ws_clients"]
        if clients:
            payload = {"type": "snapshot", "markets": orch.snapshot_all()}
            dead = []
            for ws in clients:
                try:
                    await ws.send_json(payload)
                except Exception:
                    dead.append(ws)
            for ws in dead:
                clients.discard(ws)


async def on_startup(app: web.Application) -> None:
    app["orchestrator"].start_all()
    app["push_task"] = asyncio.ensure_future(push_loop(app))


async def on_cleanup(app: web.Application) -> None:
    app["push_task"].cancel()
    await app["orchestrator"].stop_all()


# ---------------------------------------------------------------------------
# App factory + CLI
# ---------------------------------------------------------------------------
def build_app(orch: Orchestrator, push_interval: float) -> web.Application:
    app = web.Application()
    app["orchestrator"] = orch
    app["push_interval"] = push_interval
    app["ws_clients"] = set()
    app.router.add_get("/", index)
    app.router.add_get("/ws", websocket_handler)
    app.router.add_get("/api/markets", get_markets)
    app.router.add_post("/api/markets", post_add_market)
    app.router.add_post("/api/markets/{slug}/params", post_params)
    app.router.add_post("/api/markets/{slug}/stop", post_stop)
    app.router.add_post("/api/markets/{slug}/start", post_start)
    app.router.add_delete("/api/markets/{slug}", delete_market)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app


def load_specs(path: str) -> List[MarketSpec]:
    with open(path, "r", encoding="utf-8") as fh:
        raw = json.load(fh)
    return [MarketSpec.from_dict(m) for m in raw["markets"]]


def main(argv: Optional[List[str]] = None) -> None:
    p = argparse.ArgumentParser(description="MM cockpit server (paper trading only).")
    p.add_argument("--config", default=os.path.join(_HERE, "markets.json"))
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=8080)
    p.add_argument("--push-interval", type=float, default=1.0,
                   help="seconds between snapshot pushes (raise to 2-3 if the VM struggles)")
    p.add_argument("--history-len", type=int, default=300,
                   help="ring-buffer points per market (lower to save memory)")
    p.add_argument("--log-dir", default=os.path.join(_HERE, "logs"))
    a = p.parse_args(argv)

    specs = load_specs(a.config)
    orch = Orchestrator(specs, log_dir=a.log_dir, history_len=a.history_len)
    app = build_app(orch, push_interval=a.push_interval)
    print(f"[cockpit] {len(specs)} market(s); dashboard at http://{a.host}:{a.port}/ "
          f"(push every {a.push_interval}s) — PAPER TRADING ONLY, no real orders.")
    web.run_app(app, host=a.host, port=a.port)


if __name__ == "__main__":
    main()
