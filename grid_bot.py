#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Alpaca Grid/Range Trading Bot (LONG only, Crypto)

Features
- Legt Buy-Limit-Orders (Grid) im unteren Range-Bereich.
- Für jede gefüllte Buy-Order wird automatisch eine TP-Sell-Limit-Order angelegt.
- Range-Break mit Puffer: offene Orders canceln, optional Position liquidieren.
- Einzigartige client_order_id pro Order (verhindert 40010001-Fehler).
- Idempotent: setzt keine doppelten Buys, wenn die Preise schon im Orderbuch liegen.
- Läuft als Einmallauf (--once) oder im Loop (--loop).

ENV (Beispiele):
  APCA_API_KEY_ID=...
  APCA_API_SECRET_KEY=...
  APCA_API_BASE_URL=https://paper-api.alpaca.markets

  SYMBOL=ETH/USD
  GRID_LOW=4000
  GRID_HIGH=4400
  GRID_LEVELS=10
  QTY_PER_LEVEL=0.01
  TP_PCT=0.5
  BREAK_BUFFER_PCT=1
  LIQUIDATE_ON_BREAK=false
  REBUILD_ON_START=true
  LOOP_INTERVAL_SEC=30
  MAX_OPEN_BUYS=50
"""

import os
import time
import argparse
from decimal import Decimal, ROUND_DOWN
from typing import List, Dict, Optional
from uuid import uuid4

# Alpaca Trading (Orders/Positions)
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, OrderType, TimeInForce, QueryOrderStatus
from alpaca.trading.requests import GetOrdersRequest, MarketOrderRequest, LimitOrderRequest

# Alpaca Crypto Market Data
from alpaca.data.historical.crypto import CryptoHistoricalDataClient
from alpaca.data.requests import CryptoBarsRequest
from alpaca.data.timeframe import TimeFrame


# =============== Konfig & Helpers ===============

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
    except Exception:
        return default

def getenv_int(name: str, default: int) -> int:
    v = os.getenv(name)
    try:
        return int(v)
    except Exception:
        return default

APCA_API_KEY_ID     = getenv_str("APCA_API_KEY_ID")
APCA_API_SECRET_KEY = getenv_str("APCA_API_SECRET_KEY")
APCA_API_BASE_URL   = getenv_str("APCA_API_BASE_URL", "https://paper-api.alpaca.markets")

SYMBOL           = getenv_str("SYMBOL", "ETH/USD")   # Alpaca Crypto symbol – MUSS Slash enthalten!
GRID_LOW         = getenv_float("GRID_LOW", 4000.0)
GRID_HIGH        = getenv_float("GRID_HIGH", 4400.0)
GRID_LEVELS      = getenv_int("GRID_LEVELS", 10)
QTY_PER_LEVEL    = getenv_float("QTY_PER_LEVEL", 0.01)
TP_PCT           = getenv_float("TP_PCT", 0.5)       # Prozent

BREAK_BUFFER_PCT   = getenv_float("BREAK_BUFFER_PCT", 1.0)
LIQUIDATE_ON_BREAK = getenv_bool("LIQUIDATE_ON_BREAK", False)

REBUILD_ON_START = getenv_bool("REBUILD_ON_START", True)
LOOP_INTERVAL_SEC = getenv_int("LOOP_INTERVAL_SEC", 30)
MAX_OPEN_BUYS     = getenv_int("MAX_OPEN_BUYS", 50)

if not APCA_API_KEY_ID or not APCA_API_SECRET_KEY:
    raise RuntimeError("Fehlende Alpaca API Keys (APCA_API_KEY_ID / APCA_API_SECRET_KEY).")

def log(msg: str) -> None:
    print(msg, flush=True)

# Runden (USDT/USDC Preise auf 0.01, Krypto-Menge auf 6 Stellen)
def round_price(p: float) -> float:
    return float(Decimal(p).quantize(Decimal("0.01"), rounding=ROUND_DOWN))

def round_qty(q: float) -> float:
    return float(Decimal(q).quantize(Decimal("0.000001"), rounding=ROUND_DOWN))


# =============== Alpaca Clients ===============

trading = TradingClient(
    APCA_API_KEY_ID,
    APCA_API_SECRET_KEY,
    paper=APCA_API_BASE_URL.endswith("paper-api.alpaca.markets"),
)

data_client = CryptoHistoricalDataClient()


# =============== Marktpreis / Orders / Positionen ===============

def now_price(symbol: str) -> Optional[float]:
    """Letzter 1m Close als Proxy für aktuellen Preis."""
    try:
        req = CryptoBarsRequest(symbol_or_symbols=symbol, timeframe=TimeFrame.Minute, limit=1)
        bars = data_client.get_crypto_bars(req)
        if symbol in bars.data and bars.data[symbol]:
            return float(bars.data[symbol][-1].close)
    except Exception as e:
        log(f"[ERR] Preisabruf fehlgeschlagen: {e}")
    return None

def get_open_orders(symbol: str):
    try:
        req = GetOrdersRequest(status=QueryOrderStatus.OPEN, symbols=[symbol], nested=False)
        return trading.get_orders(filter=req)
    except Exception as e:
        log(f"[ERR] get_open_orders: {e}")
        return []

def cancel_all_open_orders(symbol: str) -> None:
    try:
        for o in get_open_orders(symbol):
            try:
                trading.cancel_order_by_id(o.id)
            except Exception as ce:
                log(f"[WARN] Cancel Order {o.id} failed: {ce}")
    except Exception as e:
        log(f"[ERR] cancel_all_open_orders: {e}")

def position_qty(symbol: str) -> float:
    """Anzahl Basis-Asset (0.0, falls keine Position)."""
    try:
        pos = trading.get_open_position(symbol)
        return float(pos.qty)
    except Exception:
        return 0.0


# =============== Grid-Logik ===============

def build_price_grid(low: float, high: float, levels: int) -> List[float]:
    """Preis-Liste inkl. LOW & HIGH mit gleichmäßigen Abständen."""
    if levels < 2:
        return [round_price(low), round_price(high)]
    step = (high - low) / levels
    prices = [round_price(low + i * step) for i in range(levels + 1)]
    # Doppelte entfernen
    return sorted(set(prices))

def ensure_buy_grid(symbol: str, grid_prices: List[float], qty_per_level: float, max_open: int) -> None:
    """Buy-Limits unter der Range-Mitte anlegen – ohne Dubletten/Spam."""
    mid = (GRID_LOW + GRID_HIGH) / 2.0
    desired = [p for p in grid_prices if p < mid]

    open_orders = get_open_orders(symbol)
    open_buys = [o for o in open_orders if o.side == OrderSide.BUY]

    if len(open_buys) >= max_open:
        log(f"[BOT] Schon {len(open_buys)} offene Buys (≥ MAX_OPEN_BUYS={max_open}).")
        return

    # Welche Preise liegen bereits als Buy-Limit im Orderbuch? (±0.01 Toleranz)
    existing_prices = []
    for o in open_buys:
        try:
            if o.limit_price is not None:
                existing_prices.append(float(o.limit_price))
        except Exception:
            pass

    def exists_price(target: float) -> bool:
        return any(abs(target - x) <= 0.01 for x in existing_prices)

    created = 0
    for p in desired:
        if exists_price(p):
            continue
        if len(open_buys) + created >= max_open:
            break
        # Eindeutige CID, damit Alpaca nie mit "client_order_id must be unique" antwortet
        cid = f"GRIDBUY-{p}-{uuid4().hex[:6]}"
        submit_limit_buy(symbol, qty_per_level, p, cid)
        created += 1

    if created == 0:
        log("[BOT] Kein neues Buy-Limit nötig (alles vorhanden).")

def list_filled_grid_buys(symbol: str):
    """Alle historischen gefüllten Buys, deren CID mit GRIDBUY- beginnt."""
    try:
        req = GetOrdersRequest(status=QueryOrderStatus.CLOSED, symbols=[symbol], nested=False)
        orders = trading.get_orders(filter=req)
        res = []
        for o in orders:
            if (
                o.side == OrderSide.BUY
                and o.client_order_id
                and o.client_order_id.startswith("GRIDBUY-")
                and str(o.filled_avg_price) != "None"
            ):
                res.append(o)
        return res
    except Exception as e:
        log(f"[ERR] list_filled_grid_buys: {e}")
        return []

def list_open_tp_orders(symbol: str) -> Dict[str, float]:
    """Map {buy_cid -> tp_price} aus offenen Orders vom Typ GRIDTP-<BUY_CID>."""
    mapping: Dict[str, float] = {}
    try:
        for o in get_open_orders(symbol):
            if o.client_order_id and o.client_order_id.startswith("GRIDTP-"):
                buy_cid = o.client_order_id.split("GRIDTP-")[-1]
                mapping[buy_cid] = float(o.limit_price) if o.limit_price else None
    except Exception as e:
        log(f"[ERR] list_open_tp_orders: {e}")
    return mapping

def ensure_tp_after_fills(symbol: str, tp_pct: float) -> None:
    """Für alle gefüllten GRIDBUY-Orders ohne offenen TP → TP-SELL anlegen."""
    filled = list_filled_grid_buys(symbol)
    if not filled:
        return
    open_tp = list_open_tp_orders(symbol)  # buy_cid -> tp_price

    created = 0
    for o in filled:
        buy_cid = o.client_order_id or ""
        if not buy_cid or buy_cid in open_tp:
            continue

        # Nur "wirklich gefüllte" Orders
        status = str(o.status).lower()
        if status not in ("filled", "partially_filled", "closed"):
            continue

        try:
            qty = float(o.filled_qty)
            fill = float(o.filled_avg_price)
        except Exception:
            continue
        if qty <= 0 or fill <= 0:
            continue

        tp_price = round_price(fill * (1.0 + tp_pct / 100.0))
        tp_cid = f"GRIDTP-{buy_cid}"
        submit_limit_sell(symbol, qty, tp_price, tp_cid)
        created += 1

    if created == 0:
        log("[BOT] Keine neuen TP-Orders benötigt.")

def check_range_break(last_price: float) -> bool:
    low_break  = GRID_LOW  * (1.0 - BREAK_BUFFER_PCT / 100.0)
    high_break = GRID_HIGH * (1.0 + BREAK_BUFFER_PCT / 100.0)
    return last_price < low_break or last_price > high_break

def handle_range_break(symbol: str) -> None:
    cancel_all_open_orders(symbol)
    if LIQUIDATE_ON_BREAK:
        qty = position_qty(symbol)
        if qty > 0:
            try:
                order = MarketOrderRequest(
                    symbol=symbol, side=OrderSide.SELL,
                    qty=str(round_qty(qty)), time_in_force=TimeInForce.GTC
                )
                trading.submit_order(order_data=order)
                log(f"[ORDER] LIQUIDATE {symbol} qty={round_qty(qty)} (Market)")
            except Exception as e:
                log(f"[ERR] liquidate: {e}")


# =============== Order Submit (mit Duplicate-CID-Schutz) ===============

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
        msg = str(e)
        if "client_order_id must be unique" in msg:
            # Harmlos: es existiert bereits eine Order mit genau dieser CID
            log(f"[WARN] BUY duplicate cid → ignoriert: {msg}")
        else:
            log(f"[ERR] submit_limit_buy: {e}")

def submit_limit_sell(symbol: str, qty: float, price: float, cid: str) -> None:
    try:
        order = LimitOrderRequest(
            symbol=symbol,
            qty=str(round_qty(qty)),
            side=OrderSide.SELL,
            time_in_force=TimeInForce.GTC,
            limit_price=Decimal(str(round_price(price))),
            client_order_id=cid,
        )
        trading.submit_order(order_data=order)
        log(f"[ORDER] TP-SELL {symbol} @ {round_price(price)} qty={round_qty(qty)} (cid={cid})")
    except Exception as e:
        msg = str(e)
        if "client_order_id must be unique" in msg:
            log(f"[WARN] SELL duplicate cid → ignoriert: {msg}")
        else:
            log(f"[ERR] submit_limit_sell: {e}")


# =============== Hauptlogik ===============

def trade_once() -> None:
    log(f"[BOT] Grid-Start • Symbol={SYMBOL} • Range={GRID_LOW}-{GRID_HIGH} • TP={TP_PCT}% • QTY/Level={QTY_PER_LEVEL}")
    price = now_price(SYMBOL)
    if price is None:
        log("[BOT] Kein Preis verfügbar – Runde übersprungen.")
        return

    posq = round_qty(position_qty(SYMBOL))
    log(f"[BOT] Last={round_price(price)} • PosQty={posq}")

    # Range-Break?
    if check_range_break(price):
        log(f"[BOT] RANGE-BREAK! Preis={round_price(price)} (Buffer={BREAK_BUFFER_PCT}%)")
        handle_range_break(SYMBOL)
        return

    # Grid bauen & Buys
