#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import uuid
from dataclasses import dataclass
from typing import List, Set

# ---------- Alpaca: versionstolerante Importe ----------
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, OrderType, TimeInForce
from alpaca.trading.requests import GetOrdersRequest, LimitOrderRequest

# Crypto-Datenclient: je nach alpaca-py-Version
try:
    # Neuere Versionen
    from alpaca.data.historical import CryptoHistoricalDataClient  # type: ignore
except Exception:
    # Ältere Versionen
    from alpaca.data.client import CryptoHistoricalDataClient  # type: ignore

try:
    from alpaca.data.requests import CryptoLatestTradeRequest  # type: ignore
except Exception as e:
    raise ImportError(
        "Alpaca: CryptoLatestTradeRequest nicht gefunden. "
        "Bitte stelle sicher, dass 'alpaca-py' installiert ist (z.B. >=0.21)."
    ) from e

# ---------- ENV / Konfiguration ----------
APCA_API_BASE_URL = os.getenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets")
APCA_API_KEY_ID = os.getenv("APCA_API_KEY_ID")
APCA_API_SECRET_KEY = os.getenv("APCA_API_SECRET_KEY")

SYMBOL = os.getenv("SYMBOL", "ETH/USD")

GRID_LOW = float(os.getenv("GRID_LOW", "3300"))
GRID_HIGH = float(os.getenv("GRID_HIGH", "3700"))
GRID_LEVELS = int(os.getenv("GRID_LEVELS", "10"))

TP_PCT = float(os.getenv("TP_PCT", "0.5"))        # % Take-Profit je Lot (nur Platzhalter)
QTY_PER_LEVEL = float(os.getenv("QTY_PER_LEVEL", "0.01"))

LOOP_INTERVAL_SEC = int(os.getenv("LOOP_INTERVAL_SEC", "30"))
REBUILD_ON_START = os.getenv("REBUILD_ON_START", "true").lower() == "true"
BREAK_BUFFER_PCT = float(os.getenv("BREAK_BUFFER_PCT", "1.0"))

# ---------- Clients ----------
trading = TradingClient(
    APCA_API_KEY_ID,
    APCA_API_SECRET_KEY,
    paper=("paper" in APCA_API_BASE_URL),
)

# der Datenclient braucht i.d.R. keine URL, nur Keys (funktioniert auch ohne)
crypto_data = CryptoHistoricalDataClient(APCA_API_KEY_ID, APCA_API_SECRET_KEY)

# ---------- Hilfsfunktionen ----------
def log(msg: str) -> None:
    print(msg, flush=True)

def get_last_price(symbol: str) -> float:
    """Hole den letzten Trade-Preis aus dem US Feed."""
    req = CryptoLatestTradeRequest(symbol_or_symbols=symbol, feed="us")
    res = crypto_data.get_latest_trade(req)
    # Rückgabe kann dict oder Objekt sein -> robust extrahieren
    trade = res
    if isinstance(res, dict):
        trade = next(iter(res.values()))
    return float(getattr(trade, "price"))

def list_open_orders() -> list:
    """Alle offenen Orders (API-Version tolerant)."""
    try:
        req = GetOrdersRequest(status="open")
        return trading.get_orders(filter=req)
    except Exception:
        # manche SDK-Versionen akzeptieren keyword-args
        try:
            return trading.get_orders(status="open")
        except Exception as e:
            log(f"[ERR] get_orders: {e}")
            return []

def get_open_order_prices(symbol: str, side: OrderSide) -> Set[float]:
    """Alle offenen Limit-Preise für gegebene Seite/Symbol."""
    prices: Set[float] = set()
    for o in list_open_orders():
        if (
            getattr(o, "symbol", None) == symbol
            and getattr(o, "side", None) == side
            and getattr(o, "type", None) == OrderType.LIMIT
            and getattr(o, "limit_price", None) is not None
        ):
            try:
                prices.add(float(o.limit_price))
            except Exception:
                pass
    return prices

def cancel_all_open_orders_for_symbol(symbol: str) -> int:
    """Storniert alle offenen Orders zum Symbol."""
    count = 0
    for o in list_open_orders():
        if getattr(o, "symbol", None) == symbol:
            try:
                trading.cancel_order_by_id(o.id)
                count += 1
            except Exception as e:
                log(f"[ERR] cancel_order: {e}")
    return count

def submit_limit_buy(symbol: str, price: float, qty: float) -> None:
    """Neue BUY-Limit-Order; client_order_id immer eindeutig."""
    client_order_id = f"GRIDBUY-{price}-{uuid.uuid4().hex[:8]}"
    req = LimitOrderRequest(
        symbol=symbol,
        qty=qty,
        limit_price=price,
        side=OrderSide.BUY,
        time_in_force=TimeInForce.GTC,
        client_order_id=client_order_id,
    )
    trading.submit_order(req)
    log(f"[ORDER] BUY-LIMIT {symbol} @ {price} qty={qty} (cid={client_order_id})")

# ---------- Grid ----------
@dataclass
class GridLevel:
    price: float

def build_grid(low: float, high: float, levels: int) -> List[GridLevel]:
    if levels < 2:
        raise ValueError("GRID_LEVELS muss >= 2 sein.")
    step = (high - low) / (levels - 1)
    return [GridLevel(price=round(low + i * step, 2)) for i in range(levels)]

# ---------- Main-Loop ----------
def main(loop: bool = False) -> None:
    if REBUILD_ON_START:
        log("[BOT] REBUILD_ON_START aktiv → offene Orders canceln & Grid neu setzen.")
        cancelled = cancel_all_open_orders_for_symbol(SYMBOL)
        log(f"[BOT] {cancelled} offene Orders storniert.")

    grid = build_grid(GRID_LOW, GRID_HIGH, GRID_LEVELS)

    while True:
        # Preis holen
        try:
            last = get_last_price(SYMBOL)
        except Exception as e:
            log(f"[ERR] Preisabruf fehlgeschlagen: {e}")
            if not loop:
                return
            time.sleep(LOOP_INTERVAL_SEC)
            continue

        log(f"[BOT] Grid-Start • Symbol={SYMBOL} • Range={GRID_LOW}-{GRID_HIGH} • TP={TP_PCT}% • QTY/Level={QTY_PER_LEVEL}")
        log(f"[BOT] Last={last} • PosQty=0.0")

        # Range-Break
        if last < GRID_LOW * (1.0 - BREAK_BUFFER_PCT / 100.0) or last > GRID_HIGH * (1.0 + BREAK_BUFFER_PCT / 100.0):
            log(f"[BOT] RANGE-BREAK! Preis={last} (Buffer={BREAK_BUFFER_PCT}%)")
            if not loop:
                return
            time.sleep(LOOP_INTERVAL_SEC)
            continue

        # Offene BUY-Orders je Preis sammeln (duplikatsicher)
        open_buy_prices = get_open_order_prices(SYMBOL, OrderSide.BUY)

        # Nur Level unterhalb des aktuellen Preises bestellen, die noch nicht offen sind
        for level in grid:
            if level.price < last and level.price not in open_buy_prices:
                try:
                    submit_limit_buy(SYMBOL, level.price, QTY_PER_LEVEL)
                except Exception as e:
                    log(f"[ERR] submit_limit_buy: {e}")

        # (optional) TP-Orders hier erzeugen – derzeit nicht aktiv
        log("[BOT] Keine neuen TP-Orders benötigt.")
        log("[BOT] Runde fertig.")

        if not loop:
            break
        time.sleep(LOOP_INTERVAL_SEC)

if __name__ == "__main__":
    import sys
    loop_flag = "--loop" in sys.argv
    if loop_flag:
        log("==> Running 'python grid_bot.py --loop'")
    main(loop=loop_flag)
