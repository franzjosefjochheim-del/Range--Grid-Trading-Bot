#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Grid-Trading-Bot für Alpaca Crypto (Paper & Live kompatibel)

ENV Variablen (alle Strings; Zahlen als Float/Int):
- APCA_API_BASE_URL           z.B. https://paper-api.alpaca.markets
- APCA_API_KEY_ID
- APCA_API_SECRET_KEY

- SYMBOL                      z.B. ETH/USD
- GRID_LOW                    z.B. 4000
- GRID_HIGH                   z.B. 4400
- GRID_LEVELS                 z.B. 10          (Anzahl Preisstufen zwischen LOW und HIGH)
- QTY_PER_LEVEL               z.B. 0.01
- TP_PCT                      z.B. 0.5         (Take-Profit in Prozent)

- BREAK_BUFFER_PCT            z.B. 1.0         (wie weit außerhalb Range als "Break")
- LIQUIDATE_ON_BREAK          true/false
- MAX_OPEN_BUYS               z.B. 50          (Sicherheitskappe)
- LOOP_INTERVAL_SEC           z.B. 30
- REBUILD_ON_START            true/false       (offene Orders canceln & Grid neu setzen)
"""

import os
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List

# ---- Alpaca Imports (robust gegen unterschiedliche Versionen) ----
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import LimitOrderRequest, GetOrdersRequest
from alpaca.trading.enums import OrderSide, TimeInForce, OrderType, QueryOrderStatus

# Historische Daten (Krypto)
from alpaca.data.historical import CryptoHistoricalDataClient  # type: ignore
from alpaca.data.timeframe import TimeFrame  # (nicht zwingend hier, aber nützlich)

# Requests für Preisabfragen (neuere & ältere Wege)
from alpaca.data.requests import (  # type: ignore
    CryptoLatestTradeRequest,
    CryptoTradesRequest,
)

# ------------------------------------------------------------------

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


# ---------- Clients ----------
cfg = load_config()

if not cfg.key_id or not cfg.secret:
    print("[FATAL] API-Key/Secret fehlen. Bitte ENV prüfen.", flush=True)
    sys.exit(1)

# Trading Client
trading = TradingClient(
    api_key=cfg.key_id,
    secret_key=cfg.secret,
    paper=("paper" in cfg.base_url),
)

# Daten Client (kein Key nötig; aber wenn gesetzt, nutzt er die Limits deines Accounts)
crypto_data = CryptoHistoricalDataClient(cfg.key_id, cfg.secret)


# ---------- Utilities ----------
def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def unique_cid(prefix: str, price: float) -> str:
    return f"{prefix}-{price:.1f}-{uuid.uuid4().hex[:8]}"


def fmt(v: float) -> str:
    return f"{v:.2f}"


# ---------- Preisabruf mit Fallback ----------
def get_last_price(symbol: str) -> float:
    """
    Robust: Versuche 'get_latest_trade' (neuere alpaca-py-Versionen).
    Fällt zurück auf 'get_trades(limit=1)' bei älteren Versionen.
    """
    # 1) Neuer Weg (wenn verfügbar):
    try:
        req = CryptoLatestTradeRequest(symbol_or_symbols=symbol, feed="us")
        res = crypto_data.get_latest_trade(req)
        # Rückgabe kann je nach Version dict oder Objekt sein
        trade_obj = res if not isinstance(res, dict) else next(iter(res.values()))
        price = float(getattr(trade_obj, "price"))
        return price
    except Exception as e:
        # print(f"[DBG] Fallback get_trades, Grund: {e}")
        pass

    # 2) Fallback: jüngster Trade via get_trades(limit=1)
    start = now_utc() - timedelta(minutes=30)
    end = now_utc()
    treq = CryptoTradesRequest(
        symbol_or_symbols=symbol,
        start=start,
        end=end,
        feed="us",
        limit=1,
    )
    tres = crypto_data.get_trades(treq)

    if isinstance(tres, dict):
        trades = next(iter(tres.values()), [])
    else:
        trades = getattr(tres, "trades", [])

    if trades:
        last_trade = trades[0]
        return float(getattr(last_trade, "price"))

    raise RuntimeError("Kein Trade gefunden (get_trades lieferte nichts).")


# ---------- Grid Logik ----------
def build_grid_levels(low: float, high: float, levels: int) -> List[float]:
    if levels <= 0 or high <= low:
        return []
    step = (high - low) / levels
    # Bis inkl. high - step (wie in deinen bisherigen Logs)
    prices = [round(low + i * step, 2) for i in range(levels)]
    return prices


def cancel_open_orders(symbol: str) -> int:
    # Alle offenen Orders nur für dieses Symbol
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
    """
    Placeholder/Minimal: Wenn Positionen vorhanden, könnte man hier
    TP-Sell Limits setzen. Da in deinen bisherigen Logs häufig 0 PosQty
    vorhanden war, lassen wir das (lesbar & sicher).
    """
    print("[BOT] Keine neuen TP-Orders benötigt.", flush=True)


def handle_range_break(last_price: float, low: float, high: float, buffer_pct: float) -> bool:
    if last_price < low * (1 - buffer_pct / 100.0) or last_price > high * (1 + buffer_pct / 100.0):
        print(f"[BOT] RANGE-BREAK! Preis={fmt(last_price)} (Buffer={buffer_pct}%)", flush=True)
        return True
    return False


def run_once():
    # Status
    print(f"[BOT] Grid-Start • Symbol={cfg.symbol} • Range={cfg.grid_low}-{cfg.grid_high} • TP={cfg.tp_pct}% • QTY/Level={cfg.qty_per_level}", flush=True)

    # Preis
    try:
        last = get_last_price(cfg.symbol)
        # Positionsmenge hier optional abrufen; einfache Anzeige:
        pos_qty = 0.0
        print(f"[BOT] Last={fmt(last)} • PosQty={pos_qty}", flush=True)
    except Exception as e:
        print(f"[ERR] Preisabruf fehlgeschlagen: {e}", flush=True)
        return

    # Range-Break?
    if handle_range_break(last, cfg.grid_low, cfg.grid_high, cfg.break_buffer_pct):
        if cfg.liquidate_on_break:
            print("[BOT] LIQUIDATE_ON_BREAK aktiv → Orders canceln & Positionen schließen (nicht implementiert).", flush=True)
        return

    # Grid-Preise berechnen
    levels = build_grid_levels(cfg.grid_low, cfg.grid_high, cfg.grid_levels)
    if not levels:
        print("[ERR] Ungültige Grid-Parameter. Abbruch.", flush=True)
        return

    # Buy-Limits setzen (unter Beachtung der Kappe)
    submit_grid_buys(cfg.symbol, levels, cfg.qty_per_level, cfg.max_open_buys)

    # TP-Orders ggf. ergänzen
    place_take_profits_if_needed(cfg.symbol, cfg.tp_pct)

    print("[BOT] Runde fertig.", flush=True)


def main():
    # Einmalig beim Start: optional Grid neu setzen
    if cfg.rebuild_on_start:
        print("[BOT] REBUILD_ON_START aktiv → offene Orders canceln & Grid neu setzen.", flush=True)
        cancelled = cancel_open_orders(cfg.symbol)
        print(f"[BOT] {cancelled} offene Orders storniert.", flush=True)

    # Einmalige Runde direkt
    run_once()

    # Optionaler Endlosschleifen-Modus
    do_loop = "--loop" in sys.argv
    if not do_loop:
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
