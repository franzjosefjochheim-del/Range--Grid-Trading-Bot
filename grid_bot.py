#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Alpaca Grid/Range-Trading Bot (LONG-ONLY, Crypto)

Funktion:
- Baut ein Preis-Grid zwischen GRID_LOW und GRID_HIGH.
- Legt unterhalb der Range-Mitte BUY-Limit-Orders an (QTY_PER_LEVEL je Level).
- Für jede gefüllte BUY-Order wird genau eine TP-SELL-Limit-Order angelegt
  bei (Fill * (1 + TP_PCT/100)).
- Bei Range-Break (mit Puffer) -> alle offenen Orders canceln und optional
  komplette Position liquidieren.
- Laufmodi: --once (eine Runde) oder --loop (Dauerschleife).

Erforderliche ENV Variablen:
  APCA_API_KEY_ID
  APCA_API_SECRET_KEY
  APCA_API_BASE_URL           (z.B. https://paper-api.alpaca.markets)

  SYMBOL=ETH/USD              (Alpaca Crypto Symbol, unbedingt mit Slash!)
  GRID_LOW=4000
  GRID_HIGH=4400
  GRID_LEVELS=10
  QTY_PER_LEVEL=0.01
  TP_PCT=0.5
  REBUILD_ON_START=true
  BREAK_BUFFER_PCT=1.0
  LIQUIDATE_ON_BREAK=false
  LOOP_INTERVAL_SEC=30
  MAX_OPEN_BUYS=50
"""

import os
import time
import argparse
from uuid import uuid4
from decimal import Decimal, ROUND_DOWN
from typing import List, Dict, Optional

# Drittanbieter
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, OrderType, TimeInForce, QueryOrderStatus
from alpaca.trading.requests import GetOrdersRequest, MarketOrderRequest, LimitOrderRequest

from alpaca.data.historical.crypto import CryptoHistoricalDataClient
from alpaca.data.timeframe import TimeFrame
from alpaca.data.requests import CryptoBarsRequest


# ----------------------------- Helpers für ENV -----------------------------

def _get_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None or v.strip() == "" else v.strip()

def _get_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "t")

def _get_float(name: str, default: float) -> float:
    v = os.getenv(name)
    try:
        return float(v)
    except Exception:
        return default

def _get_int(name: str, default: int) -> int:
    v = os.getenv(name)
    try:
        return int(v)
    except Exception:
        return default


# ----------------------------- Konfiguration ------------------------------

APCA_API_KEY_ID     = _get_str("APCA_API_KEY_ID")
APCA_API_SECRET_KEY = _get_str("APCA_API_SECRET_KEY")
APCA_API_BASE_URL   = _get_str("APCA_API_BASE_URL", "https://paper-api.alpaca.markets")

SYMBOL           = _get_str("SYMBOL", "ETH/USD")  # Wichtig: mit Slash!
GRID_LOW         = _get_float("GRID_LOW", 4000.0)
GRID_HIGH        = _get_float("GRID_HIGH", 4400.0)
GRID_LEVELS      = _get_int("GRID_LEVELS", 10)
QTY_PER_LEVEL    = _get_float("QTY_PER_LEVEL", 0.01)
TP_PCT           = _get_float("TP_PCT", 0.5)

REBUILD_ON_START   = _get_bool("REBUILD_ON_START", True)
BREAK_BUFFER_PCT   = _get_float("BREAK_BUFFER_PCT", 1.0)
LIQUIDATE_ON_BREAK = _get_bool("LIQUIDATE_ON_BREAK", False)

LOOP_INTERVAL_SEC = _get_int("LOOP_INTERVAL_SEC", 30)
MAX_OPEN_BUYS     = _get_int("MAX_OPEN_BUYS", 50)

# Rundungen: USDT/USD ~ 2 Dezimalstellen, Krypto-Menge konservativ 6
def round_price(p: float) -> float:
    return float(Decimal(p).quantize(Decimal("0.01"), rounding=ROUND_DOWN))

def round_qty(q: float) -> float:
    return float(Decimal(q).quantize(Decimal("0.000001"), rounding=ROUND_DOWN))


# ----------------------------- Clients ------------------------------------

if not APCA_API_KEY_ID or not APCA_API_SECRET_KEY:
    raise RuntimeError("Fehlende Alpaca API Keys (APCA_API_KEY_ID / APCA_API_SECRET_KEY).")

trading = TradingClient(
    APCA_API_KEY_ID,
    APCA_API_SECRET_KEY,
    paper=APCA_API_BASE_URL.endswith("paper-api.alpaca.markets"),
)

data_client = CryptoHistoricalDataClient()


# ----------------------------- Utils --------------------------------------

def log(msg: str) -> None:
    print(msg, flush=True)

def now_price(symbol: str) -> Optional[float]:
    """Letzter 1m-Close als einfacher Preis-Proxy."""
    try:
        req = CryptoBarsRequest(symbol_or_symbols=symbol, timeframe=TimeFrame.Minute, limit=1)
        bars = data_client.get_crypto_bars(req)
        if symbol in bars.data and bars.data[symbol]:
            return float(bars.data[symbol][-1].close)
    except Exception as e:
        log(f"[ERR] Preisabruf fehlgeschlagen: {e}")
    return None

def open_orders_for_symbol(symbol: str):
    try:
        req = GetOrdersRequest(status=QueryOrderStatus.OPEN, symbols=[symbol], nested=False)
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
    try:
        pos = trading.get_open_position(symbol)
        return float(pos.qty)
    except Exception:
        return 0.0

def list_filled_grid_buys(symbol: str):
    """Gefüllte GRIDBUY-* Orders (geschlossen/filled)."""
    try:
        req = GetOrdersRequest(status=QueryOrderStatus.CLOSED, symbols=[symbol], nested=False)
        orders = trading.get_orders(filter=req)
        result = []
        for o in orders:
            if o.client_order_id and o.client_order_id.startswith("GRIDBUY-") and str(o.filled_avg_price) != "None":
                result.append(o)
        return result
    except Exception as e:
        log(f"[ERR] list_filled_grid_buys: {e}")
        return []

def list_open_tp_orders(symbol: str) -> Dict[str, float]:
    """Map {buy_client_id -> tp_price} aus offenen Orders: client_id beginnt mit GRIDTP-<BUY_CID>."""
    mapping: Dict[str, float] = {}
    try:
        for o in open_orders_for_symbol(symbol):
            if not o.client_order_id:
                continue
            cid = o.client_order_id
            if cid.startswith("GRIDTP-"):
                # Struktur: GRIDTP-<BUY_CID>-<suffix>
                # Wir extrahieren den Teil nach 'GRIDTP-' bis vor dem letzten '-' (falls vorhanden)
                rest = cid[len("GRIDTP-"):]
                buy_cid = rest.split("-")[0] if "-" in rest else rest
                mapping[buy_cid] = float(o.limit_price) if o.limit_price else None
    except Exception as e:
        log(f"[ERR] list_open_tp_orders: {e}")
    return mapping


# ----------------------------- Order-Submit --------------------------------

def submit_limit_buy(symbol: str, qty: float, price: float) -> Optional[str]:
    """Legt eine BUY-Limit an. Liefert die verwendete client_order_id zurück (oder None bei Fehler)."""
    cid = f"GRIDBUY-{round_price(price)}-{uuid4().hex[:8]}"
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
        return cid
    except Exception as e:
        log(f"[ERR] submit_limit_buy: {e}")
        return None

def submit_limit_sell(symbol: str, qty: float, price: float, buy_cid_ref: str) -> None:
    """Legt TP-SELL an. client_order_id referenziert die BUY-CID, bleibt aber eindeutig."""
    tp_cid = f"GRIDTP-{buy_cid_ref}-{uuid4().hex[:6]}"
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
    """Liquidiert komplette Long-Position (Market)."""
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


# ----------------------------- Grid-Logik ----------------------------------

def build_price_grid(low: float, high: float, levels: int) -> List[float]:
    """Erzeugt Preisliste (inkl. LOW & HIGH) mit gleichmäßigen Abständen."""
    if levels < 2 or high <= low:
        return [round_price(low), round_price(high)]
    step = (high - low) / levels
    prices = [round_price(low + i * step) for i in range(levels + 1)]
    # doppelte Werte entfernen & sortieren
    return sorted(set(prices))

def ensure_buy_grid(symbol: str, grid_prices: List[float], qty_per_level: float, max_open: int) -> None:
    """Lege BUY-Limits unterhalb der Range-Mitte an – vermeide Duplikate am gleichen Preis."""
    mid = (GRID_LOW + GRID_HIGH) / 2.0
    target_prices = [p for p in grid_prices if p < mid]

    # Offene BUY-Orders und deren Preise sammeln
    open_orders = open_orders_for_symbol(symbol)
    open_buys = [o for o in open_orders if o.side == OrderSide.BUY]
    already_prices = []
    for o in open_buys:
        if o.limit_price is not None:
            already_prices.append(round_price(float(o.limit_price)))

    # Toleranz für Preisvergleich
    def price_exists(p: float) -> bool:
        return any(abs(p - ap) <= 0.01 for ap in already_prices)

    created = 0
    for p in target_prices:
        if len(open_buys) + created >= max_open:
            break
        if price_exists(p):
            continue
        cid = submit_limit_buy(symbol, qty_per_level, p)
        if cid:
            created += 1

    if created == 0:
        log("[BOT] Kein neues Buy-Limit nötig (alles vorhanden).")

def ensure_tp_after_fills(symbol: str, tp_pct: float) -> None:
    """Für gefüllte GRIDBUY-Orders ohne TP -> eine TP-SELL anlegen."""
    filled_buys = list_filled_grid_buys(symbol)
    if not filled_buys:
        return

    open_tp_map = list_open_tp_orders(symbol)  # {buy_cid -> tp_price}
    created = 0

    for o in filled_buys:
        buy_cid = o.client_order_id
        if not buy_cid:
            continue

        # TP existiert bereits?
        if buy_cid in open_tp_map:
            continue

        # Nur für echte Fills
        try:
            status = str(o.status).lower()
            if status not in ("filled", "partially_filled", "closed"):
                continue
            qty = float(o.filled_qty)
            fill_price = float(o.filled_avg_price)
        except Exception:
            continue

        if qty <= 0 or fill_price <= 0:
            continue

        tp_price = round_price(fill_price * (1.0 + tp_pct / 100.0))
        submit_limit_sell(symbol, qty, tp_price, buy_cid)
        created += 1

    if created == 0:
        log("[BOT] Keine neuen TP-Orders benötigt.")

def check_range_break(price: float) -> bool:
    low_break = GRID_LOW * (1.0 - BREAK_BUFFER_PCT / 100.0)
    high_break = GRID_HIGH * (1.0 + BREAK_BUFFER_PCT / 100.0)
    return price < low_break or price > high_break

def range_break_action(symbol: str, price: float) -> None:
    cancel_all_open_orders(symbol)
    if LIQUIDATE_ON_BREAK:
        market_liquidate(symbol)


# ----------------------------- Hauptschleifen ------------------------------

def trade_once() -> None:
    log(f"[BOT] Grid-Start • Symbol={SYMBOL} • Range={GRID_LOW}-{GRID_HIGH} • TP={TP_PCT}% • QTY/Level={QTY_PER_LEVEL}")
    price = now_price(SYMBOL)
    pos_qty = get_position_qty(SYMBOL)
    if price is None:
        log("[BOT] Kein Preis verfügbar – Runde übersprungen.")
        return

    log(f"[BOT] Last={round_price(price)} • PosQty={round_qty(pos_qty)}")

    if check_range_break(price):
        log(f"[BOT] RANGE-BREAK! Preis={round_price(price)} (Buffer={BREAK_BUFFER_PCT}%)")
        range_break_action(SYMBOL, price)
        return

    grid = build_price_grid(GRID_LOW, GRID_HIGH, GRID_LEVELS)
    ensure_buy_grid(SYMBOL, grid, QTY_PER_LEVEL, MAX_OPEN_BUYS)
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


# ----------------------------- CLI ----------------------------------------

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
    # Default: eine Runde
    trade_once()

if __name__ == "__main__":
    main()
