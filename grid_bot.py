#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Alpaca Grid/Range-Trading Bot (LONG-ONLY, Crypto)
- Platziert Buy-Limit-Orders gestaffelt im unteren Range-Bereich.
- Sobald ein Buy gefüllt wurde, legt der Bot eine Take-Profit-Sell-Limit-Order
  bei (Fill * (1 + TP_PCT/100)) an.
- Bricht der Kurs aus der Range (mit Puffer) aus, werden alle offenen Orders
  gecancelt und (optional) die komplette Position glattgestellt.
- Läuft als --once oder --loop.

ENV Variablen (Beispiele weiter unten):
  APCA_API_KEY_ID
  APCA_API_SECRET_KEY
  APCA_API_BASE_URL=https://paper-api.alpaca.markets

  SYMBOL=ETH/USDT                # Alpaca Crypto Symbol, z.B. ETH/USDT
  GRID_LOW=4300                  # Untere Range-Grenze
  GRID_HIGH=4700                 # Obere Range-Grenze
  GRID_LEVELS=10                 # Anzahl Zwischenstufen (über gesamte Range)
  QTY_PER_LEVEL=0.01             # Kaufmenge je Grid-Level (Basis-Asset)
  TP_PCT=0.5                     # Take-Profit in %, relativ zum Fill
  REBUILD_ON_START=true          # Bei Start: offene Orders für SYMBOL canceln & Grid neu anlegen
  BREAK_BUFFER_PCT=0.5           # Range-Break-Puffer in %
  LIQUIDATE_ON_BREAK=true        # Bei Range-Break Position glattstellen
  LOOP_INTERVAL_SEC=30           # Polling-Intervall
  MAX_OPEN_BUYS=999              # Sicherheitslimit für offene Buy-Orders
"""

import os
import time
import math
import argparse
from decimal import Decimal, ROUND_DOWN
from typing import List, Dict, Optional

import pandas as pd
import numpy as np

# Alpaca Trading (Orders/Positions)
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, OrderType, TimeInForce, QueryOrderStatus
from alpaca.trading.requests import (
    GetOrdersRequest,
    MarketOrderRequest,
    LimitOrderRequest,
)

# Alpaca Market Data (für aktuellen Preis)
from alpaca.data.historical.crypto import CryptoHistoricalDataClient
from alpaca.data.timeframe import TimeFrame
from alpaca.data.requests import CryptoBarsRequest


# ----------------------------- Konfiguration -----------------------------

def getenv_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None or v.strip() == "" else v.strip()

def getenv_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.lower() in ("1", "true", "yes", "y", "t")

def getenv_float(name: str, default: float) -> float:
    v = os.getenv(name)
    try:
        return float(v)
    except:
        return default

def getenv_int(name: str, default: int) -> int:
    v = os.getenv(name)
    try:
        return int(v)
    except:
        return default


APCA_API_KEY_ID = getenv_str("APCA_API_KEY_ID")
APCA_API_SECRET_KEY = getenv_str("APCA_API_SECRET_KEY")
APCA_API_BASE_URL = getenv_str("APCA_API_BASE_URL", "https://paper-api.alpaca.markets")

SYMBOL = getenv_str("SYMBOL", "ETH/USDT")

GRID_LOW = getenv_float("GRID_LOW", 4300.0)
GRID_HIGH = getenv_float("GRID_HIGH", 4700.0)
GRID_LEVELS = getenv_int("GRID_LEVELS", 10)  # Anzahl Zwischenpunkte (über gesamte Range)
QTY_PER_LEVEL = getenv_float("QTY_PER_LEVEL", 0.01)
TP_PCT = getenv_float("TP_PCT", 0.5)  # in %

REBUILD_ON_START = getenv_bool("REBUILD_ON_START", True)
BREAK_BUFFER_PCT = getenv_float("BREAK_BUFFER_PCT", 0.5)   # in %
LIQUIDATE_ON_BREAK = getenv_bool("LIQUIDATE_ON_BREAK", True)

LOOP_INTERVAL_SEC = getenv_int("LOOP_INTERVAL_SEC", 30)
MAX_OPEN_BUYS = getenv_int("MAX_OPEN_BUYS", 999)

# Runden auf 2 Dezimalstellen für USDT-Preise, Menge 6 Dezimal (Krypto)
def round_price(p: float) -> float:
    return float(Decimal(p).quantize(Decimal("0.01"), rounding=ROUND_DOWN))

def round_qty(q: float) -> float:
    return float(Decimal(q).quantize(Decimal("0.000001"), rounding=ROUND_DOWN))


# ----------------------------- Alpaca Clients -----------------------------

if not APCA_API_KEY_ID or not APCA_API_SECRET_KEY:
    raise RuntimeError("Fehlende Alpaca API Keys (APCA_API_KEY_ID / APCA_API_SECRET_KEY).")

trading = TradingClient(APCA_API_KEY_ID, APCA_API_SECRET_KEY, paper=APCA_API_BASE_URL.endswith("paper-api.alpaca.markets"))

# Crypto-Market-Data (für Last Price via 1m-Bar)
data_client = CryptoHistoricalDataClient()


# ----------------------------- Hilfsfunktionen -----------------------------

def log(msg: str) -> None:
    print(msg, flush=True)

def now_price(symbol: str) -> Optional[float]:
    """Holt den letzten Close (1m-Bar) als aktuellen Preis-Proxy."""
    try:
        req = CryptoBarsRequest(
            symbol_or_symbols=symbol,
            timeframe=TimeFrame.Minute,
            limit=1,
        )
        bars = data_client.get_crypto_bars(req)
        if symbol in bars.data and len(bars.data[symbol]) > 0:
            return float(bars.data[symbol][-1].close)
    except Exception as e:
        log(f"[ERR] Preisabruf fehlgeschlagen: {e}")
    return None

def open_orders_for_symbol(symbol: str) -> List:
    try:
        req = GetOrdersRequest(
            status=QueryOrderStatus.OPEN,
            symbols=[symbol],
            nested=False,
        )
        return trading.get_orders(filter=req)
    except Exception as e:
        log(f"[ERR] open_orders_for_symbol: {e}")
        return []

def cancel_all_open_orders(symbol: str) -> None:
    try:
        orders = open_orders_for_symbol(symbol)
        for o in orders:
            try:
                trading.cancel_order_by_id(o.id)
            except Exception as ce:
                log(f"[WARN] Cancel Order {o.id} failed: {ce}")
        if orders:
            log(f"[BOT] {len(orders)} offene Orders storniert.")
    except Exception as e:
        log(f"[ERR] cancel_all_open_orders: {e}")

def get_position_qty(symbol: str) -> float:
    """Aktuelle Positionsgröße (Basis-Asset) – 0 wenn keine Position."""
    try:
        pos = trading.get_open_position(symbol)
        return float(pos.qty)
    except Exception:
        return 0.0

def list_filled_grid_buys(symbol: str) -> List:
    """Alle gefüllten Buy-Orders mit unserem Client-ID-Präfix GRIDBUY- (nur heute)."""
    try:
        req = GetOrdersRequest(
            status=QueryOrderStatus.CLOSED,
            symbols=[symbol],
            nested=False,
        )
        orders = trading.get_orders(filter=req)
        filled_buys = []
        for o in orders:
            if o.client_order_id and o.client_order_id.startswith("GRIDBUY-") and str(o.filled_avg_price) != "None":
                filled_buys.append(o)
        return filled_buys
    except Exception as e:
        log(f"[ERR] list_filled_grid_buys: {e}")
        return []

def list_open_tp_orders(symbol: str) -> Dict[str, float]:
    """Map {buy_client_id -> tp_price} anhand Client-ID 'GRIDTP-<BUY_ID>' aus offenen Orders."""
    mapping = {}
    try:
        orders = open_orders_for_symbol(symbol)
        for o in orders:
            if o.client_order_id and o.client_order_id.startswith("GRIDTP-"):
                parts = o.client_order_id.split("GRIDTP-")
                if len(parts) == 2:
                    buy_id = parts[1]
                    mapping[buy_id] = float(o.limit_price) if o.limit_price else None
    except Exception as e:
        log(f"[ERR] list_open_tp_orders: {e}")
    return mapping

def submit_limit_buy(symbol: str, qty: float, price: float, cid: str) -> None:
    try:
        order = LimitOrderRequest(
            symbol=symbol,
            qty=str(round_qty(qty)),
            side=OrderSide.BUY,
            time_in_force=TimeInForce.GTC,
            limit_price=Decimal(str(round_price(price))),
            client_order_id=cid,
        )
        trading.submit_order(order_data=order)
        log(f"[ORDER] BUY-LIMIT {symbol} @ {round_price(price)} qty={round_qty(qty)} (cid={cid})")
    except Exception as e:
        log(f"[ERR] submit_limit_buy: {e}")

def submit_limit_sell(symbol: str, qty: float, price: float, tp_cid: str) -> None:
    try:
        order = LimitOrderRequest(
            symbol=symbol,
            qty=str(round_qty(qty)),
            side=OrderSide.SELL,
            time_in_force=TimeInForce.GTC,
            limit_price=Decimal(str(round_price(price))),
            client_order_id=tp_cid,
        )
        trading.submit_order(order_data=order)
        log(f"[ORDER] TP-SELL {symbol} @ {round_price(price)} qty={round_qty(qty)} (cid={tp_cid})")
    except Exception as e:
        log(f"[ERR] submit_limit_sell: {e}")

def market_liquidate(symbol: str) -> None:
    qty = get_position_qty(symbol)
    if qty > 0:
        try:
            order = MarketOrderRequest(
                symbol=symbol,
                side=OrderSide.SELL,
                qty=str(round_qty(qty)),
                time_in_force=TimeInForce.GTC,
            )
            trading.submit_order(order_data=order)
            log(f"[ORDER] LIQUIDATE {symbol} qty={round_qty(qty)} (Market)")
        except Exception as e:
            log(f"[ERR] market_liquidate: {e}")

def build_price_grid(low: float, high: float, levels: int) -> List[float]:
    """Erzeugt eine Preisliste (inkl. LOW & HIGH) mit gleichmäßigen Abständen."""
    if levels < 2:
        return [round_price(low), round_price(high)]
    step = (high - low) / levels
    prices = [round_price(low + i * step) for i in range(levels + 1)]
    # Doppelte/zu enge Level entfernen
    dedup = sorted(set(prices))
    return dedup

def ensure_buy_grid(symbol: str, grid_prices: List[float], qty_per_level: float, max_open: int) -> None:
    """Lege Buy-Limits unterhalb der Range-Mitte an (wenn nicht schon offen)."""
    mid = (GRID_LOW + GRID_HIGH) / 2.0
    wants = [p for p in grid_prices if p < mid]
    open_orders = open_orders_for_symbol(symbol)
    open_buys = [o for o in open_orders if o.side == OrderSide.BUY]
    if len(open_buys) >= max_open:
        log(f"[BOT] Schon {len(open_buys)} offene Buys ≥ MAX_OPEN_BUYS={max_open}.")
        return

    # Preise, die schon als offene Buy-Limits existieren (±0.01 tol)
    def same_price(a: float, b: float) -> bool:
        return abs(a - b) <= 0.01

    already = []
    for o in open_buys:
        if o.limit_price is not None:
            already.append(float(o.limit_price))

    created = 0
    for p in wants:
        exists = any(same_price(p, op) for op in already)
        if exists:
            continue
        if len(open_buys) + created >= max_open:
            break
        cid = f"GRIDBUY-{p}"
        submit_limit_buy(symbol, qty_per_level, p, cid)
        created += 1

    if created == 0:
        log("[BOT] Kein neues Buy-Limit nötig (alles vorhanden).")

def ensure_tp_after_fills(symbol: str, tp_pct: float) -> None:
    """Für alle gefüllten GRIDBUY-Orders, die noch keinen TP haben, eine TP-SELL anlegen."""
    filled_buys = list_filled_grid_buys(symbol)
    if not filled_buys:
        return

    open_tp_map = list_open_tp_orders(symbol)  # {buy_client_id -> tp_price}
    created = 0
    for o in filled_buys:
        buy_cid = o.client_order_id
        if not buy_cid:
            continue
        # TP existiert bereits?
        if buy_cid in open_tp_map:
            continue
        # Falls Order "cancelled" oder "rejected" war, überspringen
        if str(o.status).lower() not in ("filled", "partially_filled", "closed"):
            continue
        try:
            qty = float(o.filled_qty)
            fill_price = float(o.filled_avg_price)
        except Exception:
            continue
        if qty <= 0 or fill_price <= 0:
            continue

        tp_price = round_price(fill_price * (1.0 + tp_pct / 100.0))
        tp_cid = f"GRIDTP-{buy_cid}"
        submit_limit_sell(symbol, qty, tp_price, tp_cid)
        created += 1

    if created == 0:
        log("[BOT] Keine neuen TP-Orders benötigt.")

def range_break_action(symbol: str, price: float) -> None:
    """Bei Range-Break: alle Orders canceln und (optional) Position liquidieren."""
    cancel_all_open_orders(symbol)
    if LIQUIDATE_ON_BREAK:
        market_liquidate(symbol)

def check_range_break(price: float) -> bool:
    low_break = GRID_LOW * (1.0 - BREAK_BUFFER_PCT / 100.0)
    high_break = GRID_HIGH * (1.0 + BREAK_BUFFER_PCT / 100.0)
    return price < low_break or price > high_break


# ----------------------------- Main-Logik -----------------------------

def trade_once() -> None:
    log(f"[BOT] Grid-Start • Symbol={SYMBOL} • Range={GRID_LOW}-{GRID_HIGH} • TP={TP_PCT}% • QTY/Level={QTY_PER_LEVEL}")
    price = now_price(SYMBOL)
    if price is None:
        log("[BOT] Kein Preis verfügbar – Runde übersprungen.")
        return

    log(f"[BOT] Last={round_price(price)} • PosQty={round_qty(get_position_qty(SYMBOL))}")

    # Range-Break?
    if check_range_break(price):
        log(f"[BOT] RANGE-BREAK! Preis={round_price(price)} (Buffer={BREAK_BUFFER_PCT}%)")
        range_break_action(SYMBOL, price)
        return

    # Grid-Preise bauen
    grid = build_price_grid(GRID_LOW, GRID_HIGH, GRID_LEVELS)

    # Buy-Limits sicherstellen
    ensure_buy_grid(SYMBOL, grid, QTY_PER_LEVEL, MAX_OPEN_BUYS)

    # Für gefüllte Buys TP-Orders anlegen
    ensure_tp_after_fills(SYMBOL, TP_PCT)

    log("[BOT] Runde fertig.")

def loop_forever(interval_sec: int) -> None:
    if REBUILD_ON_START:
        log("[BOT] REBUILD_ON_START aktiv → offene Orders canceln & Grid neu setzen.")
        cancel_all_open_orders(SYMBOL)
    while True:
        try:
            trade_once()
        except Exception as e:
            log(f"[ERR] Runde: {e}")
        time.sleep(interval_sec)

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--once", action="store_true", help="Eine Runde ausführen und beenden")
    p.add_argument("--loop", action="store_true", help="Endlosschleife")
    args = p.parse_args()

    if args.once:
        trade_once()
        return
    if args.loop:
        loop_forever(LOOP_INTERVAL_SEC)
        return
    trade_once()

if __name__ == "__main__":
    main()
