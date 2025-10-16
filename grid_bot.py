#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Grid-Trading-Bot für Alpaca Crypto (Paper & Live)

ENV Variablen:
- APCA_API_BASE_URL           z.B. https://paper-api.alpaca.markets
- APCA_API_KEY_ID
- APCA_API_SECRET_KEY

- SYMBOL                      z.B. ETH/USD
- GRID_LOW                    z.B. 4000
- GRID_HIGH                   z.B. 4400
- GRID_LEVELS                 z.B. 10
- QTY_PER_LEVEL               z.B. 0.01
- TP_PCT                      z.B. 0.5   (Take-Profit in %)

- BREAK_BUFFER_PCT            z.B. 1.0
- LIQUIDATE_ON_BREAK          true/false
- MAX_OPEN_BUYS               z.B. 50
- LOOP_INTERVAL_SEC           z.B. 30
- REBUILD_ON_START            true/false
"""

import os
import sys
import time
import uuid
from dataclasses import dataclass
from typing import List

import requests  # <- wir nutzen HTTP für Market Data (versionssicher)

# Trading (Orders) – das bleibt wie gehabt
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import LimitOrderRequest, GetOrdersRequest
from alpaca.trading.enums import OrderSide, TimeInForce, OrderType, QueryOrderStatus


# -------------------- ENV Helpers --------------------
def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name, "").strip().lower()
    if v in ("1", "true", "yes", "y", "on"):
        return True
    if v in ("0", "false", "no", "n", "off"):
        return False
    return default


def env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except Exception:
        return float(default)


def env_int(name: str, default: int) -> int:
    try:
        return int(float(os.getenv(name, default)))
    except Exception:
        return int(default)


@dataclass
class Config:
    base_url: str
    key_id: str
    secret: str

    symbol: str
    grid_low: float
    grid_high: float
    grid_levels: int
    qty_per_level: float
    tp_pct: float

    break_buffer_pct: float
    liquidate_on_break: bool
    max_open_buys: int
    loop_interval_sec: int
    rebuild_on_start: bool


def load_config() -> Config:
    return Config(
        base_url=os.getenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets"),
        key_id=os.getenv("APCA_API_KEY_ID", ""),
        secret=os.getenv("APCA_API_SECRET_KEY", ""),

        symbol=os.getenv("SYMBOL", "ETH/USD"),
        grid_low=env_float("GRID_LOW", 3300.0),
        grid_high=env_float("GRID_HIGH", 3700.0),
        grid_levels=env_int("GRID_LEVELS", 10),
        qty_per_level=env_float("QTY_PER_LEVEL", 0.01),
        tp_pct=env_float("TP_PCT", 0.5),

        break_buffer_pct=env_float("BREAK_BUFFER_PCT", 1.0),
        liquidate_on_break=env_bool("LIQUIDATE_ON_BREAK", False),
        max_open_buys=env_int("MAX_OPEN_BUYS", 50),
        loop_interval_sec=env_int("LOOP_INTERVAL_SEC", 30),
        rebuild_on_start=env_bool("REBUILD_ON_START", True),
    )


cfg = load_config()

if not cfg.key_id or not cfg.secret:
    print("[FATAL] API-Key/Secret fehlen. Bitte ENV prüfen.", flush=True)
    sys.exit(1)

# Trading-Client (für Orders)
trading = TradingClient(
    api_key=cfg.key_id,
    secret_key=cfg.secret,
    paper=("paper" in cfg.base_url),
)

# -------------------- Preisabfrage (HTTP, versionssicher) --------------------
DATA_BASE = "https://data.alpaca.markets"
HEADERS = {
    "APCA-API-KEY-ID": cfg.key_id,
    "APCA-API-SECRET-KEY": cfg.secret,
}

def get_last_price(symbol: str) -> float:
    """
    Robust gegen unterschiedliche alpaca-py-Versionen:
    1) REST: /v1beta3/crypto/us/latest/trades?symbols=ETH/USD
    2) Fallback: /v1beta3/crypto/us/bars/latest?symbols=ETH/USD (Close-Preis)
    """
    try:
        r = requests.get(
            f"{DATA_BASE}/v1beta3/crypto/us/latest/trades",
            params={"symbols": symbol},
            headers=HEADERS,
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()  # {'trades': {'ETH/USD': {'p': 4242.0, ...}}}
        trade = data.get("trades", {}).get(symbol)
        if isinstance(trade, dict) and "p" in trade:
            return float(trade["p"])
    except Exception as e:
        # weiter zum Fallback
        pass

    # Fallback: letzter Bar (Close)
    r2 = requests.get(
        f"{DATA_BASE}/v1beta3/crypto/us/bars/latest",
        params={"symbols": symbol, "timeframe": "1Min"},
        headers=HEADERS,
        timeout=10,
    )
    r2.raise_for_status()
    d2 = r2.json()  # {'bars': {'ETH/USD': {'c': 4240.5, ...}}}
    bar = d2.get("bars", {}).get(symbol)
    if not isinstance(bar, dict) or "c" not in bar:
        raise RuntimeError("Kein Preis gefunden (latest trades/bars).")
    return float(bar["c"])


# -------------------- Grid-Logik --------------------
def unique_cid(prefix: str, price: float) -> str:
    return f"{prefix}-{price:.1f}-{uuid.uuid4().hex[:8]}"

def build_grid_levels(low: float, high: float, levels: int) -> List[float]:
    if levels <= 0 or high <= low:
        return []
    step = (high - low) / levels
    return [round(low + i * step, 2) for i in range(levels)]  # bis < high

def cancel_open_orders(symbol: str) -> int:
    req = GetOrdersRequest(status=QueryOrderStatus.OPEN, symbols=[symbol])
    orders = trading.get_orders(filter=req)
    cnt = 0
    for o in orders:
        try:
            trading.cancel_order_by_id(o.id)
            cnt += 1
        except Exception:
            pass
    return cnt

def submit_grid_buys(symbol: str, prices: List[float], qty: float, max_orders: int) -> int:
    placed = 0
    for p in prices:
        if placed >= max_orders:
            break
        try:
            order = LimitOrderRequest(
                symbol=symbol,
                qty=qty,
                side=OrderSide.BUY,
                type=OrderType.LIMIT,
                time_in_force=TimeInForce.GTC,
                limit_price=p,
                client_order_id=unique_cid("GRIDBUY", p),
            )
            trading.submit_order(order)
            print(f"[ORDER] BUY-LIMIT {symbol} @ {p} qty={qty} (cid={order.client_order_id})", flush=True)
            placed += 1
        except Exception as e:
            print(f"[ERR] submit_limit_buy: {e}", flush=True)
    return placed

def place_take_profits_if_needed(symbol: str, tp_pct: float):
    # Minimalvariante – kann später mit echter Positionslogik erweitert werden
    print("[BOT] Keine neuen TP-Orders benötigt.", flush=True)

def handle_range_break(last_price: float, low: float, high: float, buffer_pct: float) -> bool:
    out_low = low * (1 - buffer_pct / 100.0)
    out_high = high * (1 + buffer_pct / 100.0)
    if last_price < out_low or last_price > out_high:
        print(f"[BOT] RANGE-BREAK! Preis={last_price:.2f} (Buffer={buffer_pct}%)", flush=True)
        return True
    return False

def run_once():
    print(f"[BOT] Grid-Start • Symbol={cfg.symbol} • Range={cfg.grid_low}-{cfg.grid_high} • TP={cfg.tp_pct}% • QTY/Level={cfg.qty_per_level}", flush=True)

    try:
        last = get_last_price(cfg.symbol)
        pos_qty = 0.0  # optional: echte Positionsabfrage
        print(f"[BOT] Last={last:.2f} • PosQty={pos_qty}", flush=True)
    except Exception as e:
        print(f"[ERR] Preisabruf fehlgeschlagen: {e}", flush=True)
        return

    if handle_range_break(last, cfg.grid_low, cfg.grid_high, cfg.break_buffer_pct):
        if cfg.liquidate_on_break:
            print("[BOT] LIQUIDATE_ON_BREAK aktiv – (Positionsschließung hier optional ergänzen).", flush=True)
        return

    levels = build_grid_levels(cfg.grid_low, cfg.grid_high, cfg.grid_levels)
    if not levels:
        print("[ERR] Ungültige Grid-Parameter.", flush=True)
        return

    submit_grid_buys(cfg.symbol, levels, cfg.qty_per_level, cfg.max_open_buys)
    place_take_profits_if_needed(cfg.symbol, cfg.tp_pct)
    print("[BOT] Runde fertig.", flush=True)

def main():
    if cfg.rebuild_on_start:
        print("[BOT] REBUILD_ON_START aktiv → offene Orders canceln & Grid neu setzen.", flush=True)
        cancelled = cancel_open_orders(cfg.symbol)
        print(f"[BOT] {cancelled} offene Orders storniert.", flush=True)

    run_once()

    if "--loop" not in sys.argv:
        return

    while True:
        try:
            run_once()
        except Exception as e:
            print(f"[FATAL] Unerwarteter Fehler in run_once: {e}", flush=True)
        time.sleep(cfg.loop_interval_sec)

if __name__ == "__main__":
    print("==> Running 'python grid_bot.py --loop'" if "--loop" in sys.argv else "==> Running 'python grid_bot.py'", flush=True)
    main()
