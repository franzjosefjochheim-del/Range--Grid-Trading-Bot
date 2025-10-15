#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Range/Grid-Trading-Bot (Alpaca Crypto, LONG only)

Funktion:
- Legt gestaffelte BUY-Limit-Orders im unteren Range-Bereich.
- Für jede gefüllte BUY wird ein TP-Limit (TP_PCT) automatisch platziert.
- Bei Range-Break (mit Buffer) werden offene Orders gecancelt und (optional) Positionen liquidiert.
- Läuft als --once oder --loop.

ENV (Beispiele):
  APCA_API_KEY_ID
  APCA_API_SECRET_KEY
  APCA_API_BASE_URL=https://paper-api.alpaca.markets

  SYMBOL=ETH/USD                # mit Slash! (Wird notfalls automatisch normalisiert)
  GRID_LOW=4000                 # Range Low
  GRID_HIGH=4400                # Range High
  GRID_LEVELS=10                # Anzahl Teilstufen über die ganze Range (inkl. Enden)
  QTY_PER_LEVEL=0.01            # Basis-Asset Menge pro Level
  TP_PCT=0.5                    # Take Profit in Prozent
  BREAK_BUFFER_PCT=1.0          # Puffer für Range-Break in %
  REBUILD_ON_START=true         # Beim Start alle offenen Orders des Symbols löschen
  LIQUIDATE_ON_BREAK=false      # Bei Range-Break alles glattstellen
  LOOP_INTERVAL_SEC=30          # Poll-Intervall
  MAX_OPEN_BUYS=50              # Sicherheitslimit
"""

import os
import time
import argparse
from decimal import Decimal, ROUND_DOWN
from typing import List, Dict, Optional

# --- Ein-Instanz-Schutz -------------------------------------------------------
LOCK_FILE = "/tmp/gridbot.lock"
def single_instance_lock() -> None:
    """Verhindert parallele Instanzen im selben Container/Host."""
    if os.path.exists(LOCK_FILE):
        raise SystemExit("[EXIT] Another grid_bot.py instance is already running (lock).")
    # lock anlegen; bei sauberem Neustart ist /tmp leer
    open(LOCK_FILE, "w").close()

single_instance_lock()

# --- Helpers zum Lesen der ENV ------------------------------------------------
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

# --- Konfiguration ------------------------------------------------------------
APCA_API_KEY_ID = getenv_str("APCA_API_KEY_ID")
APCA_API_SECRET_KEY = getenv_str("APCA_API_SECRET_KEY")
APCA_API_BASE_URL = getenv_str("APCA_API_BASE_URL", "https://paper-api.alpaca.markets")

SYMBOL_RAW = getenv_str("SYMBOL", "ETH/USD")

GRID_LOW = getenv_float("GRID_LOW", 4000.0)
GRID_HIGH = getenv_float("GRID_HIGH", 4400.0)
GRID_LEVELS = getenv_int("GRID_LEVELS", 10)
QTY_PER_LEVEL = getenv_float("QTY_PER_LEVEL", 0.01)
TP_PCT = getenv_float("TP_PCT", 0.5)

BREAK_BUFFER_PCT = getenv_float("BREAK_BUFFER_PCT", 1.0)
REBUILD_ON_START = getenv_bool("REBUILD_ON_START", True)
LIQUIDATE_ON_BREAK = getenv_bool("LIQUIDATE_ON_BREAK", False)

LOOP_INTERVAL_SEC = getenv_int("LOOP_INTERVAL_SEC", 30)
MAX_OPEN_BUYS = getenv_int("MAX_OPEN_BUYS", 50)

# --- Logging ------------------------------------------------------------------
def log(msg: str) -> None:
    print(msg, flush=True)

# --- Symbol Normalisierung ----------------------------------------------------
def normalize_symbol(sym: str) -> str:
    """Akzeptiert ETH/USD oder ETHUSD und normalisiert auf 'BASE/QUOTE'."""
    s = sym.replace(" ", "").upper()
    if "/" in s:
        base, quote = s.split("/", 1)
        return f"{base}/{quote}"
    # Kein Slash -> versuchen zu erraten (Standard: USD)
    known_quotes = ("USDT", "USDC", "USD", "BTC")
    for q in known_quotes:
        if s.endswith(q):
            base = s[:-len(q)]
            return f"{base}/{q}"
    # fallback
    return f"{s}/USD"

SYMBOL = normalize_symbol(SYMBOL_RAW)

# --- Rundungen ----------------------------------------------------------------
def round_price(p: float) -> float:
    return float(Decimal(p).quantize(Decimal("0.01"), rounding=ROUND_DOWN))

def round_qty(q: float) -> float:
    return float(Decimal(q).quantize(Decimal("0.000001"), rounding=ROUND_DOWN))

# --- Alpaca SDK ---------------------------------------------------------------
if not APCA_API_KEY_ID or not APCA_API_SECRET_KEY:
    raise RuntimeError("Missing Alpaca API keys (APCA_API_KEY_ID / APCA_API_SECRET_KEY).")

from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, TimeInForce, OrderType, QueryOrderStatus
from alpaca.trading.requests import GetOrdersRequest, MarketOrderRequest, LimitOrderRequest

from alpaca.data.historical.crypto import CryptoHistoricalDataClient
from alpaca.data.timeframe import TimeFrame
from alpaca.data.requests import CryptoBarsRequest

trading = TradingClient(
    APCA_API_KEY_ID,
    APCA_API_SECRET_KEY,
    paper=APCA_API_BASE_URL.endswith("paper-api.alpaca.markets")
)
data_client = CryptoHistoricalDataClient()

# --- Market-Data: aktueller Preis --------------------------------------------
def now_price(symbol: str) -> Optional[float]:
    """Letzter Close der 1m-Bar als Preis-Proxy."""
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

# --- Trading Hilfen -----------------------------------------------------------
def open_orders_for_symbol(symbol: str) -> List:
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
                log(f"[WARN] Cancel {o.id} failed: {ce}")
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

def list_filled_grid_buys(symbol: str) -> List:
    """Gefüllte GRIDBUY-Orders (geschlossen/filled)."""
    try:
        req = GetOrdersRequest(status=QueryOrderStatus.CLOSED, symbols=[symbol], nested=False)
        orders = trading.get_orders(filter=req)
        out = []
        for o in orders:
            if o.client_order_id and o.client_order_id.startswith("GRIDBUY-") and str(o.filled_avg_price) != "None":
                out.append(o)
        return out
    except Exception as e:
        log(f"[ERR] list_filled_grid_buys: {e}")
        return []

def list_open_tp_orders(symbol: str) -> Dict[str, float]:
    """Map {buy_client_id -> tp_price} aus offenen Orders (CID = GRIDTP-<BUY_CID>)."""
    mapping: Dict[str, float] = {}
    try:
        for o in open_orders_for_symbol(symbol):
            if o.client_order_id and o.client_order_id.startswith("GRIDTP-"):
                buy_id = o.client_order_id.split("GRIDTP-")[-1]
                mapping[buy_id] = float(o.limit_price) if o.limit_price else None
    except Exception as e:
        log(f"[ERR] list_open_tp_orders: {e}")
    return mapping

def submit_limit_buy(symbol: str, qty: float, price: float, cid: str) -> None:
    """Sende BUY-Limit; Duplicate-Fehler (CID) wird geloggt, ist aber unkritisch (Mehrfachstart-Schutz)."""
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
        log(f"[ORDER] TP-SELL  {symbol} @ {round_price(price)} qty={round_qty(qty)} (cid={cid})")
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

# --- Grid-Erstellung ----------------------------------------------------------
def build_price_grid(low: float, high: float, levels: int) -> List[float]:
    """Gleichmäßige Levels inkl. Low & High; dedupliziert & gerundet."""
    if levels < 2:
        return [round_price(low), round_price(high)]
    step = (high - low) / levels
    prices = [round_price(low + i * step) for i in range(levels + 1)]
    return sorted(set(prices))

def ensure_buy_grid(symbol: str, grid_prices: List[float], qty_per_level: float, max_open: int) -> None:
    """BUY-Limits nur unterhalb der Range-Mitte; vermeidet Duplikate anhand Preisvergleich & CID."""
    mid = (GRID_LOW + GRID_HIGH) / 2.0
    wants = [p for p in grid_prices if p < mid]  # nur untere Hälfte
    oo = open_orders_for_symbol(symbol)
    open_buys = [o for o in oo if o.side == OrderSide.BUY]
    if len(open_buys) >= max_open:
        log(f"[BOT] Schon {len(open_buys)} offene Buys ≥ MAX_OPEN_BUYS={max_open}.")
        return

    def same_price(a: float, b: float) -> bool:
        return abs(a - b) <= 0.01

    existing_prices: List[float] = []
    existing_cids: List[str] = []
    for o in open_buys:
        if o.limit_price is not None:
            existing_prices.append(float(o.limit_price))
        if o.client_order_id:
            existing_cids.append(o.client_order_id)

    created = 0
    for p in wants:
        cid = f"GRIDBUY-{round_price(p):.2f}"
        # wenn bereits mit gleichem Preis oder gleicher CID offen -> überspringen
        if any(same_price(p, ep) for ep in existing_prices) or cid in existing_cids:
            continue
        if len(open_buys) + created >= max_open:
            break
        submit_limit_buy(symbol, qty_per_level, p, cid)
        created += 1

    if created == 0:
        log("[BOT] Kein neues BUY-Limit nötig (alles vorhanden).")

def ensure_tp_after_fills(symbol: str, tp_pct: float) -> None:
    """Lege TP-SELL für gefüllte GRIDBUYs an, sofern noch keiner offen ist."""
    filled_buys = list_filled_grid_buys(symbol)
    if not filled_buys:
        log("[BOT] Keine gefüllten BUYs ohne TP gefunden.")
        return

    open_tp_map = list_open_tp_orders(symbol)
    created = 0
    for o in filled_buys:
        buy_cid = o.client_order_id or ""
        if not buy_cid or buy_cid in open_tp_map:
            continue

        status = str(o.status).lower()
        if status not in ("filled", "partially_filled", "closed"):
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

# --- Range-Break --------------------------------------------------------------
def check_range_break(price: float) -> bool:
    lo = GRID_LOW * (1.0 - BREAK_BUFFER_PCT / 100.0)
    hi = GRID_HIGH * (1.0 + BREAK_BUFFER_PCT / 100.0)
    return price < lo or price > hi

def range_break_action(symbol: str) -> None:
    cancel_all_open_orders(symbol)
    if LIQUIDATE_ON_BREAK:
        market_liquidate(symbol)

# --- Kern-Logik ---------------------------------------------------------------
def trade_once() -> None:
    log(f"[BOT] Grid-Start • Symbol={SYMBOL} • Range={GRID_LOW}-{GRID_HIGH} • TP={TP_PCT}% • QTY/Level={QTY_PER_LEVEL}")
    price = now_price(SYMBOL)
    if price is None:
        log("[BOT] Kein Preis verfügbar – Runde übersprungen.")
        return

    pos_qty = round_qty(get_position_qty(SYMBOL))
    log(f"[BOT] Last={round_price(price)} • PosQty={pos_qty}")

    if check_range_break(price):
        log(f"[BOT] RANGE-BREAK! Preis={round_price(price)} (Buffer={BREAK_BUFFER_PCT}%)")
        range_break_action(SYMBOL)
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

# --- CLI ----------------------------------------------------------------------
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
