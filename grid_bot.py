#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import uuid
import math
from dataclasses import dataclass
from typing import List, Set

from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, OrderType, TimeInForce
from alpaca.trading.requests import GetOrdersRequest, LimitOrderRequest
from alpaca.data.client import StockHistoricalDataClient, CryptoHistoricalDataClient
from alpaca.data.requests import CryptoLatestTradeRequest
from alpaca.data.timeframe import TimeFrame

# ---------- Konfiguration aus ENV ----------

APCA_API_BASE_URL = os.getenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets")
APCA_API_KEY_ID = os.getenv("APCA_API_KEY_ID")
APCA_API_SECRET_KEY = os.getenv("APCA_API_SECRET_KEY")

SYMBOL = os.getenv("SYMBOL", "ETH/USD")

GRID_LOW = float(os.getenv("GRID_LOW", "3300"))
GRID_HIGH = float(os.getenv("GRID_HIGH", "3700"))
GRID_LEVELS = int(os.getenv("GRID_LEVELS", "10"))

TP_PCT = float(os.getenv("TP_PCT", "0.5"))  # in Prozent
QTY_PER_LEVEL = float(os.getenv("QTY_PER_LEVEL", "0.01"))

LOOP_INTERVAL_SEC = int(os.getenv("LOOP_INTERVAL_SEC", "30"))
REBUILD_ON_START = os.getenv("REBUILD_ON_START", "true").lower() == "true"

BREAK_BUFFER_PCT = float(os.getenv("BREAK_BUFFER_PCT", "1.0"))  # Range-Break Puffer

# ---------- Clients ----------

trading = TradingClient(
    APCA_API_KEY_ID,
    APCA_API_SECRET_KEY,
    paper=True if "paper" in APCA_API_BASE_URL else False,
)

crypto_data = CryptoHistoricalDataClient(APCA_API_KEY_ID, APCA_API_SECRET_KEY)

# ---------- Utils / Logging ----------

def log(msg: str) -> None:
    print(msg, flush=True)

def cid(prefix: str, price: float) -> str:
    # Stabiles Präfix pro Preis, aber mit Suffix nur wenn wirklich NEU bestellt wird
    return f"{prefix}-{price}"

# ---------- Markt / Orders ----------

def get_last_price(symbol: str) -> float:
    """Letzter Trade-Preis aus dem US-Crypto-Feed."""
    req = CryptoLatestTradeRequest(symbol_or_symbols=symbol, feed="us")
    res = crypto_data.get_latest_trade(req)
    # SDK kann dict oder Objekt zurückliefern
    trade = res
    if isinstance(res, dict):
        trade = next(iter(res.values()))
    return float(trade.price)

def get_open_order_prices(symbol: str, side: OrderSide) -> Set[float]:
    """Liest alle offenen Orders und gibt die Limit-Preise der gewünschten Seite zurück."""
    req = GetOrdersRequest(status="open", symbols=[symbol])
    orders = trading.get_orders(filter=req)
    prices: Set[float] = set()
    for o in orders:
        if o.side == side and o.type == OrderType.LIMIT and o.limit_price is not None:
            try:
                prices.add(float(o.limit_price))
            except Exception:
                pass
    return prices

def cancel_all_open_orders_for_symbol(symbol: str) -> int:
    """Storniert alle offenen Orders zu einem Symbol. Gibt Anzahl zurück."""
    req = GetOrdersRequest(status="open", symbols=[symbol])
    orders = trading.get_orders(filter=req)
    count = 0
    for o in orders:
        try:
            trading.cancel_order_by_id(o.id)
            count += 1
        except Exception as e:
            log(f"[ERR] cancel_order: {e}")
    return count

def submit_limit_buy(symbol: str, price: float, qty: float) -> None:
    """Reicht eine neue BUY-Limit-Order ein, falls noch keine offene Order bei diesem Preis existiert."""
    # Ein stabiler client_order_id Präfix je Preis – mit zusätzlichem zufälligen Suffix,
    # damit es *über die Zeit* eindeutig bleibt (Alpaca verlangt globale Eindeutigkeit).
    client_order_id = f"{cid('GRIDBUY', price)}-{uuid.uuid4().hex[:8]}"
    req = LimitOrderRequest(
        symbol=symbol,
        qty=qty,
        limit_price=price,
        side=OrderSide.BUY,
        time_in_force=TimeInForce.GTC,
        client_order_id=client_order_id,
    )
    order = trading.submit_order(req)
    log(f"[ORDER] BUY-LIMIT {symbol} @ {price} qty={qty} (cid={client_order_id})")

# ---------- Grid-Berechnung ----------

@dataclass
class GridLevel:
    price: float

def build_grid(low: float, high: float, levels: int) -> List[GridLevel]:
    if levels < 2:
        raise ValueError("GRID_LEVELS muss >= 2 sein.")
    step = (high - low) / (levels - 1)
    return [GridLevel(price=round(low + i * step, 2)) for i in range(levels)]

# ---------- Hauptlogik ----------

def main(loop: bool = False) -> None:
    # Optional: Offene Orders aufräumen
    if REBUILD_ON_START:
        log("[BOT] REBUILD_ON_START aktiv → offene Orders canceln & Grid neu setzen.")
        cancelled = cancel_all_open_orders_for_symbol(SYMBOL)
        log(f"[BOT] {cancelled} offene Orders storniert.")

    grid = build_grid(GRID_LOW, GRID_HIGH, GRID_LEVELS)
    tp_factor = 1.0 + TP_PCT / 100.0

    while True:
        try:
            last = get_last_price(SYMBOL)
        except Exception as e:
            log(f"[ERR] Preisabruf fehlgeschlagen: {e}")
            if not loop:
                return
            time.sleep(LOOP_INTERVAL_SEC)
            continue

        log(f"[BOT] Grid-Start • Symbol={SYMBOL} • Range={GRID_LOW}-{GRID_HIGH} • TP={TP_PCT}% • QTY/Level={QTY_PER_LEVEL}")
        log(f"[BOT] Last={last} • PosQty=0.0")  # Positionshandling optional – hier nicht verwendet

        # Range-Break: wenn Preis außerhalb der Range (+/- Buffer), nur melden, nichts setzen
        below = last < GRID_LOW * (1.0 - BREAK_BUFFER_PCT / 100.0)
        above = last > GRID_HIGH * (1.0 + BREAK_BUFFER_PCT / 100.0)
        if below or above:
            log(f"[BOT] RANGE-BREAK! Preis={last} (Buffer={BREAK_BUFFER_PCT}%)")
            if not loop:
                return
            time.sleep(LOOP_INTERVAL_SEC)
            continue

        # Bereits offene BUY-Limits nach Preis ermitteln → verhindert Duplikate je Preis
        open_buy_prices = get_open_order_prices(SYMBOL, OrderSide.BUY)

        # Nur Level unterhalb des aktuellen Preises belegen (klassische Grid-Logik)
        placed = 0
        for level in grid:
            if level.price < last and level.price not in open_buy_prices:
                try:
                    submit_limit_buy(SYMBOL, level.price, QTY_PER_LEVEL)
                    placed += 1
                except Exception as e:
                    # z.B. client_order_id must be unique → wird durch UUID minimiert;
                    # andere Fehler sauber loggen.
                    log(f"[ERR] submit_limit_buy: {e}")

        # (Optional) TP-Orders – hier bewusst ausgelassen, da Einstandspreise/Lots tracking nötig wäre.
        log("[BOT] Keine neuen TP-Orders benötigt.")

        log("[BOT] Runde fertig.")

        if not loop:
            break
        time.sleep(LOOP_INTERVAL_SEC)

if __name__ == "__main__":
    # Render CMD ruft i.d.R. mit --loop auf
    import sys
    loop_flag = "--loop" in sys.argv
    if loop_flag:
        log("==> Running 'python grid_bot.py --loop'")
    main(loop=loop_flag)
