#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Grid-Trading-Bot für Alpaca (Paper/Live) – Crypto (z.B. ETH/USD)

WICHTIGSTE FIXES ggü. vorher:
1) open_orders_for_symbol() holt ALLE offenen Orders und filtert lokal
   → behebt das Crypto-Problem mit "ETH/USD" & doppelten Orders.
2) In-Memory-Dedupe gegen sofortige Mehrfach-Submits pro Loop.
3) Stabile, eindeutige client_order_id (CID) + Retry bei Kollision.

Benötigte ENV-Variablen (alle Strings; bools als 'true'/'false'):
- APCA_API_BASE_URL
- APCA_API_KEY_ID
- APCA_API_SECRET_KEY
- SYMBOL                  (z.B. 'ETH/USD')
- GRID_LOW                (float)
- GRID_HIGH               (float)
- GRID_LEVELS             (int, Anzahl Levels inkl. Ober-/Unterkante)
- QTY_PER_LEVEL           (float, Ordermenge je Level)
- TP_PCT                  (float, z.B. 0.5 für 0.5% Take-Profit)
- LOOP_INTERVAL_SEC       (int, z.B. 30)
- MAX_OPEN_BUYS           (int, Sicherheitslimit für offene BUY-Orders)
- BREAK_BUFFER_PCT        (float, z.B. 1.0 → Range-Break Log)
- REBUILD_ON_START        ('true'|'false')
- LIQUIDATE_ON_BREAK      ('true'|'false') – (optional, aktuell nur geloggt)
"""

import os
import time
import uuid
import math
import random
from typing import List

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetOrdersRequest, MarketOrderRequest, LimitOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce, QueryOrderStatus
from alpaca.data.historical import CryptoHistoricalDataClient
from alpaca.data.requests import LatestCryptoTradeRequest


# ============== ENV ==============

APCA_API_BASE_URL = os.getenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets").strip()
APCA_API_KEY_ID = os.getenv("APCA_API_KEY_ID", "").strip()
APCA_API_SECRET_KEY = os.getenv("APCA_API_SECRET_KEY", "").strip()

SYMBOL = os.getenv("SYMBOL", "ETH/USD").strip()
GRID_LOW = float(os.getenv("GRID_LOW", "3300"))
GRID_HIGH = float(os.getenv("GRID_HIGH", "3700"))
GRID_LEVELS = int(os.getenv("GRID_LEVELS", "10"))
QTY_PER_LEVEL = float(os.getenv("QTY_PER_LEVEL", "0.01"))
TP_PCT = float(os.getenv("TP_PCT", "0.5"))  # Prozent
LOOP_INTERVAL_SEC = int(os.getenv("LOOP_INTERVAL_SEC", "30"))
MAX_OPEN_BUYS = int(os.getenv("MAX_OPEN_BUYS", "50"))
BREAK_BUFFER_PCT = float(os.getenv("BREAK_BUFFER_PCT", "1.0"))

REBUILD_ON_START = os.getenv("REBUILD_ON_START", "true").lower() == "true"
LIQUIDATE_ON_BREAK = os.getenv("LIQUIDATE_ON_BREAK", "false").lower() == "true"


# ============== Clients ==============

# Trading (Orders/Konten)
trading = TradingClient(APCA_API_KEY_ID, APCA_API_SECRET_KEY, paper="paper" in APCA_API_BASE_URL)

# Price Feed (Crypto)
data_client = CryptoHistoricalDataClient(APCA_API_KEY_ID, APCA_API_SECRET_KEY)


# ============== Utils ==============

def log(msg: str):
    print(msg, flush=True)

def round_price(p: float) -> float:
    # Crypto ist meist mit 0.01 ausreichend (Alpaca rundet serverseitig auf ihre Inkremente)
    return float(f"{p:.2f}")

def _norm_symbol(s: str) -> str:
    # Normalisierte Form ohne Slash – so kommt es oft in API-Antworten
    return s.replace("/", "").upper()

def now_ms() -> int:
    return int(time.time() * 1000)


# In-Memory Dedupe: Preise, die im aktuellen Prozess kürzlich gesendet wurden
RECENT_BUY_PRICE_KEYS: dict = {}  # price -> timestamp

def _remember_price(p: float):
    RECENT_BUY_PRICE_KEYS[round_price(p)] = time.time()

def _seen_recently(p: float, ttl: int = 180) -> bool:
    t = RECENT_BUY_PRICE_KEYS.get(round_price(p))
    return (t is not None) and (time.time() - t < ttl)


# ============== Markt-/Positionsdaten ==============

def get_last_price(symbol: str) -> float:
    """Letzter gehandelter Preis aus dem Crypto-Feed (US feed)."""
    req = LatestCryptoTradeRequest(symbol_or_symbols=symbol, feed="us")
    trade = data_client.get_latest_crypto_trade(req)
    # Antwort kann dict-like sein bei mehreren; hier Einzelsymbol:
    px = float(trade.price)
    return px


def get_position_qty(symbol: str) -> float:
    """Aktuelle Positionsmenge (kann bei Crypto leer sein)."""
    try:
        pos = trading.get_open_position(symbol.replace("/", ""))
        return float(pos.qty)
    except Exception:
        # keine Position vorhanden
        return 0.0


# ============== Orders & Grid ==============

def open_orders_for_symbol(symbol: str):
    """Alle offenen Orders laden und lokal nach Symbol filtern (Crypto-freundlich)."""
    try:
        req = GetOrdersRequest(status=QueryOrderStatus.OPEN, nested=False)
        orders = trading.get_orders(filter=req)
        want = _norm_symbol(symbol)
        return [o for o in orders if _norm_symbol(o.symbol) == want]
    except Exception as e:
        log(f"[ERR] open_orders_for_symbol: {e}")
        return []


def cancel_all_open_orders(symbol: str) -> int:
    """Alle offenen Orders für das Symbol stornieren."""
    orders = open_orders_for_symbol(symbol)
    n = 0
    for o in orders:
        try:
            trading.cancel_order_by_id(o.id)
            n += 1
        except Exception as e:
            log(f"[ERR] cancel_order {o.id}: {e}")
    return n


def unique_cid(prefix: str, price: float) -> str:
    # Preis (gerundet) + kurze random-Komponente – sehr geringe Kollisionschance
    rp = round_price(price)
    rnd = uuid.uuid4().hex[:8]
    return f"{prefix}-{rp}-{rnd}"


def submit_limit_buy(symbol: str, price: float, qty: float) -> bool:
    """BUY-Limit mit eindeutiger CID. Verhindert Duplikate & retried bei CID-Kollision."""
    price = round_price(price)
    if _seen_recently(price):
        return False  # gerade erst versucht

    cid = unique_cid("GRIDBUY", price)

    order = LimitOrderRequest(
        symbol=symbol.replace("/", ""),
        qty=str(qty),
        side=OrderSide.BUY,
        limit_price=price,
        time_in_force=TimeInForce.GTC,
        client_order_id=cid
    )

    try:
        trading.submit_order(order_data=order)
        log(f"[ORDER] BUY-LIMIT {symbol} @ {price} qty={qty} (cid={cid})")
        _remember_price(price)
        return True
    except Exception as e:
        msg = str(e)
        # Falls (selten) CID-Kollision: einmal neu probieren
        if "client_order_id must be unique" in msg.lower():
            time.sleep(0.2 + random.random() * 0.3)
            try:
                order.client_order_id = unique_cid("GRIDBUY", price)
                trading.submit_order(order_data=order)
                log(f"[ORDER] BUY-LIMIT {symbol} @ {price} qty={qty} (cid={order.client_order_id})")
                _remember_price(price)
                return True
            except Exception as e2:
                log(f"[ERR] submit_limit_buy retry: {e2}")
                return False
        else:
            log(f"[ERR] submit_limit_buy: {e}")
            return False


def ensure_buy_grid(symbol: str, levels: List[float], qty: float):
    """Sorgt dafür, dass an allen benötigten Levels eine BUY-Limit-Order liegt."""
    if not levels:
        return

    open_orders = open_orders_for_symbol(symbol)
    already_prices = []
    for o in open_orders:
        try:
            if o.side.name == "BUY":
                already_prices.append(round_price(float(o.limit_price)))
        except Exception:
            pass

    placed = 0

    def price_exists(p: float) -> bool:
        # bestehende Server-Orders oder kürzlich gesendete
        return any(abs(p - ap) <= 0.01 for ap in already_prices) or _seen_recently(p)

    for p in levels:
        rp = round_price(p)
        if price_exists(rp):
            continue
        if placed + len(already_prices) >= MAX_OPEN_BUYS:
            break
        if submit_limit_buy(symbol, rp, qty):
            placed += 1

    if placed == 0:
        log("[BOT] Keine neuen TP-Orders benötigt.")
    else:
        log(f"[BOT] {placed} neue BUY-Orders platziert.")


def compute_grid_levels(low: float, high: float, n_levels: int) -> List[float]:
    """Erzeuge gleichmäßig verteilte BUY-Level (exklusive Oberkante)."""
    low = float(low)
    high = float(high)
    n = max(2, int(n_levels))

    step = (high - low) / (n - 1)
    # BUYs typischerweise unterhalb des letzten Preises – wir legen alle außer oberster Kante
    levels = [round_price(low + i * step) for i in range(n) if i < n - 1]
    return sorted(set(levels))


# ============== Bot-Loop ==============

def loop_once():
    # Preise/Position
    last = get_last_price(SYMBOL)
    pos_qty = get_position_qty(SYMBOL)

    # Range-Break Logging
    if last < GRID_LOW * (1 - BREAK_BUFFER_PCT / 100.0) or last > GRID_HIGH * (1 + BREAK_BUFFER_PCT / 100.0):
        log(f"[BOT] RANGE-BREAK! Preis={last} (Buffer={BREAK_BUFFER_PCT}%)")

    log(f"[BOT] Grid-Start • Symbol={SYMBOL} • Range={GRID_LOW}-{GRID_HIGH} • TP={TP_PCT}% • QTY/Level={QTY_PER_LEVEL}")
    log(f"[BOT] Last={last} • PosQty={pos_qty}")

    # BUY-Grid pflegen (nur innerhalb Range sinnvoll)
    grid_levels = compute_grid_levels(GRID_LOW, GRID_HIGH, GRID_LEVELS)
    # Optional: nur Levels unter aktuellem Preis nehmen
    levels_to_place = [p for p in grid_levels if p < last]
    ensure_buy_grid(SYMBOL, levels_to_place, QTY_PER_LEVEL)

    log("[BOT] Runde fertig.")


def main():
    log("==> Deploying...")
    log(f"[BOT] Grid-Start • Symbol={SYMBOL} • Range={GRID_LOW}-{GRID_HIGH} • TP={TP_PCT}% • QTY/Level={QTY_PER_LEVEL}")

    if REBUILD_ON_START:
        log("[BOT] REBUILD_ON_START aktiv → offene Orders canceln & Grid neu setzen.")
        n = cancel_all_open_orders(SYMBOL)
        if n > 0:
            log(f"[BOT] {n} offene Orders storniert.")

    # Initial einmal Grid anlegen
    try:
        last = get_last_price(SYMBOL)
        pos_qty = get_position_qty(SYMBOL)
        log(f"[BOT] Last={last} • PosQty={pos_qty}")
        grid_levels = compute_grid_levels(GRID_LOW, GRID_HIGH, GRID_LEVELS)
        levels_to_place = [p for p in grid_levels if p < last]
        ensure_buy_grid(SYMBOL, levels_to_place, QTY_PER_LEVEL)
    except Exception as e:
        log(f"[ERR] Initialisierung: {e}")

    log("     ==> Your service is live 🎉")

    # Endlosschleife
    while True:
        try:
            loop_once()
        except Exception as e:
            log(f"[ERR] Loop: {e}")
        time.sleep(max(1, LOOP_INTERVAL_SEC))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--loop", action="store_true", help="obssolet – läuft immer im Loop")
    args = parser.parse_args()

    log("==> Running 'python grid_bot.py --loop'")
    main()
