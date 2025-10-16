#!/usr/bin/env python3
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import List, Set

# ===== Alpaca Trading (Orders) =====
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import (
    GetOrdersRequest,
    LimitOrderRequest,
)
from alpaca.trading.enums import (
    OrderSide,
    OrderType,
    TimeInForce,
    QueryOrderStatus,
)

# ===== Alpaca Market Data (Preis) – robust mit Fallbacks =====
_last_price_clients_inited = False
_crypto_data_client = None
_requests_mod = None

def _init_price_clients():
    """Lazy-Init für Market-Data-Clients und request-Objekte (verschiedene SDK-Versionen möglich)."""
    global _last_price_clients_inited, _crypto_data_client, _requests_mod
    if _last_price_clients_inited:
        return
    try:
        # Neues alpaca-py API (v3+)
        from alpaca.data import CryptoDataClient  # type: ignore
        from alpaca.data import requests as data_requests  # type: ignore
        _crypto_data_client = CryptoDataClient()
        _requests_mod = data_requests
    except Exception:
        _crypto_data_client = None
        _requests_mod = None
    _last_price_clients_inited = True


# ========= Konfiguration =========
API_KEY = os.getenv("ALPACA_API_KEY") or os.getenv("APCA_API_KEY_ID") or ""
API_SECRET = os.getenv("ALPACA_SECRET_KEY") or os.getenv("APCA_API_SECRET_KEY") or ""
USE_PAPER = str(os.getenv("PAPER", "true")).lower() in ["1", "true", "yes", "y"]

SYMBOL = os.getenv("SYMBOL", "ETH/USD")

GRID_LOW = float(os.getenv("GRID_LOW", "4000"))
GRID_HIGH = float(os.getenv("GRID_HIGH", "4400"))
STEP = float(os.getenv("STEP", "40"))

TP_PCT = float(os.getenv("TP_PCT", "0.005"))  # 0.5% = 0.005
QTY = float(os.getenv("QTY", "0.01"))

MAX_ORDERS_PER_LOOP = int(os.getenv("MAX_ORDERS_PER_LOOP", "25"))
REBUILD_ON_START = str(os.getenv("REBUILD_ON_START", "true")).lower() in ["1", "true", "yes", "y"]
SLEEP_SEC = int(os.getenv("SLEEP_SEC", "20"))

trading = TradingClient(API_KEY, API_SECRET, paper=USE_PAPER)

# ========= Utilities =========
def unique_cid(prefix: str, price: float) -> str:
    # kurze, aber eindeutige CID – inkl. Preis und 8-stelliger UUID
    return f"{prefix}-{price:.1f}-{uuid.uuid4().hex[:8]}"

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def fmt(x: float) -> str:
    return f"{x:.2f}"

# ========= Preisabfrage =========
def get_last_price(symbol: str) -> float | None:
    """Versucht mehrere Wege, den letzten Preis zu holen. Gibt None, wenn alles fehlschlägt."""
    _init_price_clients()

    # 1) Neues alpaca.data: get_latest_trade
    if _crypto_data_client and _requests_mod:
        try:
            req = _requests_mod.CryptoLatestTradeRequest(symbol_or_symbols=symbol)
            res = _crypto_data_client.get_latest_trade(req)
            # Rückgabe kann dict-ähnlich sein (bei mehreren Symbolen) – abdecken
            price = None
            if hasattr(res, "price"):
                price = float(res.price)
            elif isinstance(res, dict):
                obj = res.get(symbol)
                if obj and hasattr(obj, "price"):
                    price = float(obj.price)
            if price:
                return price
        except Exception:
            pass

        # 2) Fallback: get_latest_quote (Midprice)
        try:
            req = _requests_mod.CryptoLatestQuoteRequest(symbol_or_symbols=symbol)
            res = _crypto_data_client.get_latest_quote(req)
            bid, ask = None, None
            if hasattr(res, "bid_price") and hasattr(res, "ask_price"):
                bid, ask = float(res.bid_price), float(res.ask_price)
            elif isinstance(res, dict):
                obj = res.get(symbol)
                if obj and hasattr(obj, "bid_price") and hasattr(obj, "ask_price"):
                    bid, ask = float(obj.bid_price), float(obj.ask_price)
            if bid and ask:
                return (bid + ask) / 2.0
        except Exception:
            pass

    # Letzter Fallback: keine Quelle verfügbar
    return None

# ========= Grid-Level Berechnung =========
def build_grid_levels(low: float, high: float, step: float) -> List[float]:
    if high <= low or step <= 0:
        return []
    levels = []
    p = low
    # mathematische Stabilität bei Gleitkomma
    while p <= high + 1e-9:
        levels.append(round(p, 1))
        p += step
    return levels

# ========= Order-Status Helpers =========
def get_open_grid_buy_prices(symbol: str) -> Set[float]:
    """Offene BUY-Limit-Orders unseres Grids (erkannt am CID-Prefix 'GRIDBUY-')."""
    req = GetOrdersRequest(status=QueryOrderStatus.OPEN, symbols=[symbol])
    prices: Set[float] = set()
    try:
        for o in trading.get_orders(filter=req):
            if o.side == OrderSide.BUY and o.type == OrderType.LIMIT and o.client_order_id and str(o.client_order_id).startswith("GRIDBUY-"):
                try:
                    prices.add(float(o.limit_price))
                except Exception:
                    pass
    except Exception as e:
        print(f"[WARN] get_open_grid_buy_prices: {e}", flush=True)
    return prices

def get_open_grid_tp_prices(symbol: str) -> Set[float]:
    """Offene SELL-Limit-Orders als TPs (CID-Prefix 'GRIDTP-') → liefert limit prices."""
    req = GetOrdersRequest(status=QueryOrderStatus.OPEN, symbols=[symbol])
    prices: Set[float] = set()
    try:
        for o in trading.get_orders(filter=req):
            if o.side == OrderSide.SELL and o.type == OrderType.LIMIT and o.client_order_id and str(o.client_order_id).startswith("GRIDTP-"):
                try:
                    prices.add(float(o.limit_price))
                except Exception:
                    pass
    except Exception as e:
        print(f"[WARN] get_open_grid_tp_prices: {e}", flush=True)
    return prices

def get_recent_filled_grid_buys(symbol: str, lookback_sec: int = 1800) -> List[tuple[float, float]]:
    """
    Liefert Liste (entry_price, filled_qty) kürzlich ERFOLGREICH ausgeführter Grid-BUY-Orders.
    Erkennung: CID startet mit 'GRIDBUY-'.
    """
    since = now_utc() - timedelta(seconds=lookback_sec)
    req = GetOrdersRequest(
        status=QueryOrderStatus.CLOSED,
        symbols=[symbol],
        side=OrderSide.BUY,
        after=since,
    )
    out: List[tuple[float, float]] = []
    try:
        for o in trading.get_orders(filter=req):
            if not (o.client_order_id and str(o.client_order_id).startswith("GRIDBUY-")):
                continue
            if not o.filled_qty or float(o.filled_qty) <= 0:
                continue
            try:
                entry = float(o.limit_price)
                qty = float(o.filled_qty)
                out.append((entry, qty))
            except Exception:
                pass
    except Exception as e:
        print(f"[WARN] get_recent_filled_grid_buys: {e}", flush=True)
    return out

# ========= Order-Platzierung =========
def submit_grid_buys(symbol: str, target_prices: List[float], qty: float, max_orders: int) -> int:
    """Platziert NUR fehlende Level (Duplikate werden übersprungen)."""
    already_open = get_open_grid_buy_prices(symbol)
    to_place = [p for p in target_prices if p not in already_open]
    placed = 0

    if not to_place:
        print("[BOT] Alle Grid-BUYs sind bereits offen. Nichts zu tun.", flush=True)
        return 0

    for p in to_place:
        if placed >= max_orders:
            break
        try:
            req = LimitOrderRequest(
                symbol=symbol,
                qty=qty,
                side=OrderSide.BUY,
                type=OrderType.LIMIT,
                time_in_force=TimeInForce.GTC,
                limit_price=p,
                client_order_id=unique_cid("GRIDBUY", p),
            )
            trading.submit_order(req)
            print(f"[ORDER] BUY-LIMIT {symbol} @ {p} qty={qty} (cid={req.client_order_id})", flush=True)
            placed += 1
        except Exception as e:
            print(f"[ERR] submit_limit_buy: {e}", flush=True)

    skipped = len(target_prices) - len(to_place)
    if skipped:
        print(f"[BOT] {skipped} Level bereits vorhanden → übersprungen.", flush=True)
    return placed

def submit_tp_sells_for_fills(symbol: str, tp_pct: float, max_orders: int) -> int:
    """
    Für kürzlich gefüllte Grid-BUYs die TP-SELL-Limit-Orders nachlegen.
    TP-Preis = entry*(1+tp_pct). Duplikate (gleicher Limit-Preis) werden übersprungen.
    """
    open_tp = get_open_grid_tp_prices(symbol)
    fills = get_recent_filled_grid_buys(symbol, lookback_sec=3600)
    placed = 0
    for entry_price, qty in fills:
        tp_price = round(entry_price * (1.0 + tp_pct), 2)
        if tp_price in open_tp:
            continue
        if placed >= max_orders:
            break
        try:
            req = LimitOrderRequest(
                symbol=symbol,
                qty=qty,
                side=OrderSide.SELL,
                type=OrderType.LIMIT,
                time_in_force=TimeInForce.GTC,
                limit_price=tp_price,
                client_order_id=unique_cid("GRIDTP", tp_price),
            )
            trading.submit_order(req)
            print(f"[TP] SELL-LIMIT {symbol} @ {fmt(tp_price)} qty={qty} (entry={fmt(entry_price)})", flush=True)
            placed += 1
        except Exception as e:
            print(f"[ERR] submit_tp_sell: {e}", flush=True)
    if placed == 0:
        print("[BOT] Keine neuen TP-Orders benötigt.", flush=True)
    return placed

# ========= Wartung =========
def cancel_open_grid_orders(symbol: str) -> int:
    """Storniert **nur** unsere Grid-Orders (BUY + TP), anhand CID-Präfix."""
    req = GetOrdersRequest(status=QueryOrderStatus.OPEN, symbols=[symbol])
    canceled = 0
    try:
        for o in trading.get_orders(filter=req):
            cid = str(o.client_order_id or "")
            if cid.startswith("GRIDBUY-") or cid.startswith("GRIDTP-"):
                try:
                    trading.cancel_order_by_id(o.id)
                    canceled += 1
                except Exception:
                    pass
    except Exception as e:
        print(f"[WARN] cancel_open_grid_orders: {e}", flush=True)
    return canceled

# ========= Main-Loop =========
def one_round():
    # Preis
    last = get_last_price(SYMBOL)
    last_txt = fmt(last) if last is not None else "—"

    # Grid
    levels = build_grid_levels(GRID_LOW, GRID_HIGH, STEP)
    print(f"[BOT] Grid-Start • Symbol={SYMBOL} • Range={GRID_LOW}-{GRID_HIGH} • TP={TP_PCT*100:.1f}% • QTY/Level={QTY}", flush=True)
    print(f"[BOT] Last={last_txt}", flush=True)

    # BUY-Levels nur innerhalb Range
    buys = [p for p in levels if p <= GRID_HIGH]
    submit_grid_buys(SYMBOL, buys, QTY, MAX_ORDERS_PER_LOOP)

    # TPs für neue Fills
    submit_tp_sells_for_fills(SYMBOL, TP_PCT, max_orders=MAX_ORDERS_PER_LOOP)

    print("[BOT] Runde fertig.", flush=True)

def main():
    if REBUILD_ON_START:
        print("[BOT] REBUILD_ON_START aktiv → offene Orders canceln & Grid neu setzen.", flush=True)
        n = cancel_open_grid_orders(SYMBOL)
        print(f"[BOT] {n} offene Orders storniert.", flush=True)

    # sofort eine Runde
    one_round()

    # Dauerloop (Render-Worker-Restyle)
    if any(arg in os.getenv("LOOP", "--loop") for arg in ["--loop", "1", "true"]):
        while True:
            try:
                one_round()
            except Exception as e:
                print(f"[ERR] Unerwarteter Fehler in Runde: {e}", flush=True)
            time.sleep(SLEEP_SEC)

if __name__ == "__main__":
    main()
