# balfund_renko_options_scalper.py
# ─────────────────────────────────────────────────────────────────────
# Balfund Renko Options Scalper v1.0
# ─────────────────────────────────────────────────────────────────────
# Strategy:
#   - Builds Renko bricks on NIFTY ATM CE & PE option premiums (live)
#   - Buys CE on green brick close (CE chart), buys PE on green brick
#     close (PE chart)
#   - Fixed TP = +8 pts, SL = -2 pts from entry (1:4 risk-reward)
#   - After exit, waits for next fresh green brick before re-entry
#   - ATM strike auto-selected from NIFTY spot; shifts when flat
#   - Session: 09:15–15:30 IST
#   - Max trades per day configurable (default 10)
#
# Data:
#   - Dhan WebSocket v2 for live ticks (CE, PE, NIFTY spot)
#   - Dhan REST API for order placement
#   - Dhan instrument master CSV for strike resolution
#
# Install:
#   pip install customtkinter requests pandas websocket-client python-dotenv
# ─────────────────────────────────────────────────────────────────────

import os
import sys
import time
import json
import math
import struct
import threading
import logging
from datetime import datetime, timezone, timedelta, date
from typing import Dict, Optional, List, Any, Tuple
from collections import deque
from io import StringIO

import requests
import pandas as pd
import websocket
from dotenv import load_dotenv

try:
    import customtkinter as ctk
except ImportError:
    print("ERROR: customtkinter not installed. Run: pip install customtkinter")
    sys.exit(1)

# ═══════════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════════

VERSION = "1.0"
APP_TITLE = f"Balfund Renko Options Scalper v{VERSION}"

# Dhan WebSocket
WS_URL_TEMPLATE = (
    "wss://api-feed.dhan.co?version=2"
    "&token={token}&clientId={client_id}&authType=2"
)
INSTRUMENT_CSV_URL = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"

# Dhan REST API
DHAN_BASE_URL = "https://api.dhan.co/v2"

# WS packet codes
REQ_SUB_TICKER = 15
RESP_TICKER = 2
RESP_PREV_CLOSE = 6
RESP_DISCONNECT = 50

EXCH_SEG_MAP = {
    0: "IDX_I", 1: "NSE_EQ", 2: "NSE_FNO",
    3: "NSE_CURRENCY", 4: "BSE_EQ", 5: "MCX_COMM",
}

# NIFTY spot index
NIFTY_SPOT_SEC_ID = "13"
NIFTY_SPOT_EXCHANGE = "IDX_I"

# IST offset
IST_OFFSET = timedelta(hours=5, minutes=30)

# Session times (IST)
SESSION_START_H, SESSION_START_M = 9, 15
SESSION_END_H, SESSION_END_M = 15, 30

# ═══════════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════════

LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

log_file = os.path.join(LOG_DIR, f"renko_scalper_{date.today().isoformat()}.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("RenkoScalper")


# ═══════════════════════════════════════════════════════════════════
# TIME HELPERS
# ═══════════════════════════════════════════════════════════════════

def now_ist() -> datetime:
    """Current time in IST."""
    return datetime.now(timezone.utc) + IST_OFFSET


def ist_hhmm(dt: datetime) -> str:
    return dt.strftime("%H:%M:%S")


def is_in_session(dt: Optional[datetime] = None) -> bool:
    """Check if we're inside 09:15–15:30 IST."""
    if dt is None:
        dt = now_ist()
    t = dt.hour * 60 + dt.minute
    start = SESSION_START_H * 60 + SESSION_START_M
    end = SESSION_END_H * 60 + SESSION_END_M
    return start <= t < end


def _normalize_dhan_epoch(ts: int) -> int:
    """Normalize Dhan WS timestamp quirk (IST offset already baked in)."""
    now_ts = int(time.time())
    diff = ts - now_ts
    if 4.5 * 3600 <= diff <= 6.5 * 3600:
        ts -= 19800
    return ts


# ═══════════════════════════════════════════════════════════════════
# DHAN WS PARSERS (binary)
# ═══════════════════════════════════════════════════════════════════

def parse_header_8(msg: bytes) -> Optional[Dict]:
    if len(msg) < 8:
        return None
    return {
        "resp_code": msg[0],
        "msg_len": struct.unpack_from("<H", msg, 1)[0],
        "exch_seg_num": msg[3],
        "security_id": str(struct.unpack_from("<I", msg, 4)[0]),
        "payload": msg[8:],
    }


def parse_ticker(payload: bytes) -> Optional[Dict]:
    if len(payload) < 8:
        return None
    ltp = struct.unpack_from("<f", payload, 0)[0]
    ltt = struct.unpack_from("<I", payload, 4)[0]
    return {"ltp": float(ltp), "ltt_epoch": _normalize_dhan_epoch(int(ltt))}


def parse_prev_close(payload: bytes) -> Optional[Dict]:
    if len(payload) < 8:
        return None
    return {"prev_close": float(struct.unpack_from("<f", payload, 0)[0])}


# ═══════════════════════════════════════════════════════════════════
# RENKO ENGINE (pure in-memory, no file I/O)
# ═══════════════════════════════════════════════════════════════════

class RenkoBrick:
    """One Renko brick."""
    __slots__ = ("time_epoch", "open", "close", "is_green")

    def __init__(self, t: int, o: float, c: float):
        self.time_epoch = t
        self.open = o
        self.close = c
        self.is_green = c > o

    def __repr__(self):
        col = "G" if self.is_green else "R"
        return f"Brick({col} O={self.open:.2f} C={self.close:.2f})"


class RenkoEngine:
    """
    Pure in-memory Renko engine.
    Feed it tick prices; it emits bricks.
    """

    def __init__(self, brick_size: float = 1.0, reversal_bricks: int = 2,
                 max_history: int = 200):
        self.brick_size = brick_size
        self.reversal_bricks = reversal_bricks
        self.bricks: deque = deque(maxlen=max_history)

        self._last_close: Optional[float] = None
        self._direction: int = 0  # 0=none, 1=up, -1=down
        self._initialized = False

        self.lock = threading.Lock()

    def reset(self):
        """Clear all bricks and state."""
        with self.lock:
            self.bricks.clear()
            self._last_close = None
            self._direction = 0
            self._initialized = False

    def feed(self, price: float, epoch: int) -> List[RenkoBrick]:
        """
        Feed a tick price. Returns list of newly formed bricks (0 or more).
        Thread-safe.
        """
        with self.lock:
            return self._process(price, epoch)

    def _process(self, price: float, epoch: int) -> List[RenkoBrick]:
        new_bricks = []

        if not self._initialized:
            self._last_close = price
            self._initialized = True
            return new_bricks

        bs = self.brick_size
        while True:
            diff = price - self._last_close

            # No direction yet
            if self._direction == 0:
                if diff >= bs:
                    o, c = self._last_close, self._last_close + bs
                    b = RenkoBrick(epoch, o, c)
                    self.bricks.append(b)
                    new_bricks.append(b)
                    self._last_close = c
                    self._direction = 1
                    continue
                elif diff <= -bs:
                    o, c = self._last_close, self._last_close - bs
                    b = RenkoBrick(epoch, o, c)
                    self.bricks.append(b)
                    new_bricks.append(b)
                    self._last_close = c
                    self._direction = -1
                    continue
                break

            # Continuation up
            if self._direction == 1 and diff >= bs:
                o, c = self._last_close, self._last_close + bs
                b = RenkoBrick(epoch, o, c)
                self.bricks.append(b)
                new_bricks.append(b)
                self._last_close = c
                continue

            # Continuation down
            if self._direction == -1 and diff <= -bs:
                o, c = self._last_close, self._last_close - bs
                b = RenkoBrick(epoch, o, c)
                self.bricks.append(b)
                new_bricks.append(b)
                self._last_close = c
                continue

            # Reversal down (was up)
            if self._direction == 1 and diff <= -(bs * self.reversal_bricks):
                o = self._last_close - bs
                c = self._last_close - 2.0 * bs
                b = RenkoBrick(epoch, o, c)
                self.bricks.append(b)
                new_bricks.append(b)
                self._last_close = c
                self._direction = -1
                continue

            # Reversal up (was down)
            if self._direction == -1 and diff >= (bs * self.reversal_bricks):
                o = self._last_close + bs
                c = self._last_close + 2.0 * bs
                b = RenkoBrick(epoch, o, c)
                self.bricks.append(b)
                new_bricks.append(b)
                self._last_close = c
                self._direction = 1
                continue

            break

        return new_bricks

    @property
    def brick_count(self) -> int:
        return len(self.bricks)

    @property
    def last_brick(self) -> Optional[RenkoBrick]:
        if self.bricks:
            return self.bricks[-1]
        return None


# ═══════════════════════════════════════════════════════════════════
# INSTRUMENT RESOLVER (Dhan instrument master)
# ═══════════════════════════════════════════════════════════════════

class InstrumentResolver:
    """Resolves NIFTY ATM CE/PE security IDs from Dhan instrument master."""

    def __init__(self):
        self.df: Optional[pd.DataFrame] = None
        self.loaded = False
        self._lock = threading.Lock()

    def load(self) -> bool:
        """Download and parse instrument master. Returns True on success."""
        try:
            log.info("Downloading Dhan instrument master...")
            r = requests.get(INSTRUMENT_CSV_URL, timeout=30)
            r.raise_for_status()

            usecols = [
                "EXCH_ID", "SEGMENT", "SECURITY_ID", "INSTRUMENT",
                "SYMBOL_NAME", "DISPLAY_NAME", "SM_EXPIRY_DATE",
                "SM_STRIKE_PRICE", "SM_OPTION_TYPE",
            ]
            df = pd.read_csv(StringIO(r.text), usecols=usecols, low_memory=False)

            for c in ["EXCH_ID", "SEGMENT", "INSTRUMENT", "SYMBOL_NAME",
                       "DISPLAY_NAME", "SM_OPTION_TYPE"]:
                df[c] = df[c].astype(str).str.strip()

            df["SM_EXPIRY_DATE"] = pd.to_datetime(df["SM_EXPIRY_DATE"], errors="coerce")
            df["SM_STRIKE_PRICE"] = pd.to_numeric(df["SM_STRIKE_PRICE"], errors="coerce")
            df["SECURITY_ID"] = df["SECURITY_ID"].astype(str).str.strip()

            # Filter to NSE FNO NIFTY options only
            self.df = df[
                (df["EXCH_ID"] == "NSE") &
                (df["INSTRUMENT"] == "OPTIDX") &
                (df["SYMBOL_NAME"] == "NIFTY")
            ].copy()

            self.loaded = True
            log.info(f"Instrument master loaded | NIFTY options rows: {len(self.df)}")
            return True

        except Exception as e:
            log.error(f"Failed to load instrument master: {e}")
            return False

    def get_nearest_expiry(self) -> Optional[pd.Timestamp]:
        """Get the nearest expiry date from today."""
        if not self.loaded or self.df is None:
            return None
        today = pd.Timestamp.now().normalize()
        future = self.df[self.df["SM_EXPIRY_DATE"] >= today]
        if future.empty:
            return None
        return future["SM_EXPIRY_DATE"].min()

    def resolve_atm_strikes(self, spot_price: float) -> Optional[Dict]:
        """
        Given NIFTY spot price, find ATM CE and PE security IDs
        for the nearest expiry.
        Returns dict: {
            "strike": 24500,
            "expiry": "2026-05-07",
            "ce_sec_id": "12345",
            "pe_sec_id": "12346",
            "ce_display": "NIFTY 07MAY 24500 CE",
            "pe_display": "NIFTY 07MAY 24500 PE",
        }
        """
        if not self.loaded or self.df is None:
            return None

        expiry = self.get_nearest_expiry()
        if expiry is None:
            log.error("No future NIFTY expiry found in instrument master.")
            return None

        # ATM strike = nearest 50 multiple
        atm_strike = round(spot_price / 50) * 50

        exp_df = self.df[self.df["SM_EXPIRY_DATE"] == expiry]

        ce_row = exp_df[
            (exp_df["SM_STRIKE_PRICE"] == atm_strike) &
            (exp_df["SM_OPTION_TYPE"] == "CALL")
        ]
        pe_row = exp_df[
            (exp_df["SM_STRIKE_PRICE"] == atm_strike) &
            (exp_df["SM_OPTION_TYPE"] == "PUT")
        ]

        if ce_row.empty or pe_row.empty:
            log.error(f"Could not find ATM {atm_strike} CE/PE for expiry {expiry.date()}")
            return None

        ce_sec = str(int(float(ce_row.iloc[0]["SECURITY_ID"])))
        pe_sec = str(int(float(pe_row.iloc[0]["SECURITY_ID"])))
        ce_disp = str(ce_row.iloc[0]["DISPLAY_NAME"])
        pe_disp = str(pe_row.iloc[0]["DISPLAY_NAME"])

        result = {
            "strike": int(atm_strike),
            "expiry": expiry.date().isoformat(),
            "ce_sec_id": ce_sec,
            "pe_sec_id": pe_sec,
            "ce_display": ce_disp,
            "pe_display": pe_disp,
        }
        log.info(f"ATM resolved | Strike={atm_strike} | Expiry={expiry.date()} | "
                 f"CE={ce_sec} | PE={pe_sec}")
        return result


# ═══════════════════════════════════════════════════════════════════
# TRADE MANAGER
# ═══════════════════════════════════════════════════════════════════

class Trade:
    """Represents one trade."""
    __slots__ = ("side", "entry_price", "target", "stoploss", "entry_time",
                 "exit_price", "exit_time", "pnl", "status", "sec_id",
                 "display_name", "order_id")

    def __init__(self, side: str, entry_price: float, sec_id: str,
                 display_name: str, tp: float, sl: float):
        self.side = side  # "CE" or "PE"
        self.entry_price = entry_price
        self.target = entry_price + tp
        self.stoploss = entry_price - sl
        self.entry_time = now_ist()
        self.exit_price = 0.0
        self.exit_time: Optional[datetime] = None
        self.pnl = 0.0
        self.status = "OPEN"  # OPEN, TARGET, STOPLOSS, MANUAL
        self.sec_id = sec_id
        self.display_name = display_name
        self.order_id = ""


class TradeManager:
    """Manages CE and PE trades independently."""

    def __init__(self, tp_points: float = 8.0, sl_points: float = 2.0,
                 max_trades: int = 10, lot_size: int = 75,
                 paper_mode: bool = True):
        self.tp_points = tp_points
        self.sl_points = sl_points
        self.max_trades = max_trades
        self.lot_size = lot_size  # NIFTY lot size
        self.paper_mode = paper_mode

        # Current open trades (max 1 CE, max 1 PE)
        self.ce_trade: Optional[Trade] = None
        self.pe_trade: Optional[Trade] = None

        # Waiting for fresh green brick
        self.ce_waiting = False
        self.pe_waiting = False

        # Trade history
        self.trades: List[Trade] = []
        self.total_trades_today = 0

        # Dhan credentials
        self.client_id = ""
        self.access_token = ""

        self.lock = threading.Lock()

    @property
    def max_reached(self) -> bool:
        return self.total_trades_today >= self.max_trades

    def can_enter_ce(self) -> bool:
        with self.lock:
            return (self.ce_trade is None and
                    not self.ce_waiting and
                    not self.max_reached)

    def can_enter_pe(self) -> bool:
        with self.lock:
            return (self.pe_trade is None and
                    not self.pe_waiting and
                    not self.max_reached)

    def enter_ce(self, price: float, sec_id: str, display_name: str) -> Optional[Trade]:
        with self.lock:
            if self.ce_trade is not None or self.max_reached:
                return None
            trade = Trade("CE", price, sec_id, display_name,
                          self.tp_points, self.sl_points)
            self.ce_trade = trade
            self.ce_waiting = False
            self.total_trades_today += 1
            self.trades.append(trade)

            if not self.paper_mode:
                self._place_buy_order(trade)

            log.info(f"{'[PAPER]' if self.paper_mode else '[LIVE]'} "
                     f"CE BUY | {display_name} | Entry={price:.2f} | "
                     f"TP={trade.target:.2f} SL={trade.stoploss:.2f} | "
                     f"Trade #{self.total_trades_today}")
            return trade

    def enter_pe(self, price: float, sec_id: str, display_name: str) -> Optional[Trade]:
        with self.lock:
            if self.pe_trade is not None or self.max_reached:
                return None
            trade = Trade("PE", price, sec_id, display_name,
                          self.tp_points, self.sl_points)
            self.pe_trade = trade
            self.pe_waiting = False
            self.total_trades_today += 1
            self.trades.append(trade)

            if not self.paper_mode:
                self._place_buy_order(trade)

            log.info(f"{'[PAPER]' if self.paper_mode else '[LIVE]'} "
                     f"PE BUY | {display_name} | Entry={price:.2f} | "
                     f"TP={trade.target:.2f} SL={trade.stoploss:.2f} | "
                     f"Trade #{self.total_trades_today}")
            return trade

    def check_exit_ce(self, ltp: float) -> Optional[str]:
        """Check CE trade TP/SL. Returns exit reason or None."""
        with self.lock:
            if self.ce_trade is None:
                return None
            t = self.ce_trade
            if ltp >= t.target:
                self._close_trade(t, ltp, "TARGET")
                self.ce_trade = None
                self.ce_waiting = True
                return "TARGET"
            if ltp <= t.stoploss:
                self._close_trade(t, ltp, "STOPLOSS")
                self.ce_trade = None
                self.ce_waiting = True
                return "STOPLOSS"
            return None

    def check_exit_pe(self, ltp: float) -> Optional[str]:
        """Check PE trade TP/SL. Returns exit reason or None."""
        with self.lock:
            if self.pe_trade is None:
                return None
            t = self.pe_trade
            if ltp >= t.target:
                self._close_trade(t, ltp, "TARGET")
                self.pe_trade = None
                self.pe_waiting = True
                return "TARGET"
            if ltp <= t.stoploss:
                self._close_trade(t, ltp, "STOPLOSS")
                self.pe_trade = None
                self.pe_waiting = True
                return "STOPLOSS"
            return None

    def on_green_brick_ce(self):
        """Called when a green brick forms on CE chart — clears waiting flag."""
        with self.lock:
            if self.ce_waiting:
                self.ce_waiting = False
                log.info("CE waiting cleared — fresh green brick, ready for entry")

    def on_green_brick_pe(self):
        """Called when a green brick forms on PE chart — clears waiting flag."""
        with self.lock:
            if self.pe_waiting:
                self.pe_waiting = False
                log.info("PE waiting cleared — fresh green brick, ready for entry")

    def _close_trade(self, trade: Trade, exit_price: float, reason: str):
        trade.exit_price = exit_price
        trade.exit_time = now_ist()
        trade.pnl = (exit_price - trade.entry_price) * self.lot_size
        trade.status = reason

        if not self.paper_mode:
            self._place_sell_order(trade)

        log.info(f"{'[PAPER]' if self.paper_mode else '[LIVE]'} "
                 f"{trade.side} EXIT | {reason} | {trade.display_name} | "
                 f"Entry={trade.entry_price:.2f} Exit={exit_price:.2f} | "
                 f"PnL={trade.pnl:+.2f}")

    def _place_buy_order(self, trade: Trade):
        """Place a market buy order via Dhan REST API."""
        try:
            url = f"{DHAN_BASE_URL}/orders"
            headers = {
                "Content-Type": "application/json",
                "access-token": self.access_token,
            }
            payload = {
                "dhanClientId": self.client_id,
                "transactionType": "BUY",
                "exchangeSegment": "NSE_FNO",
                "productType": "INTRADAY",
                "orderType": "MARKET",
                "validity": "DAY",
                "securityId": trade.sec_id,
                "quantity": self.lot_size,
                "price": 0,
                "triggerPrice": 0,
            }
            resp = requests.post(url, headers=headers, json=payload, timeout=5)
            data = resp.json()
            trade.order_id = str(data.get("orderId", ""))
            log.info(f"BUY order placed | orderId={trade.order_id} | resp={data}")
        except Exception as e:
            log.error(f"BUY order FAILED: {e}")

    def _place_sell_order(self, trade: Trade):
        """Place a market sell order via Dhan REST API."""
        try:
            url = f"{DHAN_BASE_URL}/orders"
            headers = {
                "Content-Type": "application/json",
                "access-token": self.access_token,
            }
            payload = {
                "dhanClientId": self.client_id,
                "transactionType": "SELL",
                "exchangeSegment": "NSE_FNO",
                "productType": "INTRADAY",
                "orderType": "MARKET",
                "validity": "DAY",
                "securityId": trade.sec_id,
                "quantity": self.lot_size,
                "price": 0,
                "triggerPrice": 0,
            }
            resp = requests.post(url, headers=headers, json=payload, timeout=5)
            data = resp.json()
            log.info(f"SELL order placed | resp={data}")
        except Exception as e:
            log.error(f"SELL order FAILED: {e}")

    def get_day_pnl(self) -> float:
        return sum(t.pnl for t in self.trades if t.status != "OPEN")

    def reset_day(self):
        with self.lock:
            self.ce_trade = None
            self.pe_trade = None
            self.ce_waiting = False
            self.pe_waiting = False
            self.trades.clear()
            self.total_trades_today = 0


# ═══════════════════════════════════════════════════════════════════
# CORE ENGINE — orchestrates WS, Renko, Trades
# ═══════════════════════════════════════════════════════════════════

class CoreEngine:
    """
    Main engine:
    1. Subscribes to NIFTY spot + CE + PE via Dhan WebSocket
    2. Feeds CE/PE ticks into their respective Renko engines
    3. On green brick close → enters trade (if conditions met)
    4. Monitors TP/SL tick-by-tick
    5. Auto-shifts ATM strike when flat
    """

    def __init__(self, client_id: str, access_token: str, brick_size: float,
                 tp_points: float, sl_points: float, max_trades: int,
                 lot_size: int, paper_mode: bool):
        self.client_id = client_id
        self.access_token = access_token
        self.brick_size = brick_size

        # Renko engines
        self.ce_renko = RenkoEngine(brick_size=brick_size)
        self.pe_renko = RenkoEngine(brick_size=brick_size)

        # Trade manager
        self.trade_mgr = TradeManager(
            tp_points=tp_points, sl_points=sl_points,
            max_trades=max_trades, lot_size=lot_size,
            paper_mode=paper_mode,
        )
        self.trade_mgr.client_id = client_id
        self.trade_mgr.access_token = access_token

        # Instrument resolver
        self.resolver = InstrumentResolver()

        # Current ATM info
        self.atm_info: Optional[Dict] = None
        self.nifty_spot: float = 0.0
        self.ce_ltp: float = 0.0
        self.pe_ltp: float = 0.0
        self.current_atm_strike: int = 0

        # WS
        self.ws: Optional[websocket.WebSocketApp] = None
        self.ws_thread: Optional[threading.Thread] = None
        self.ws_connected = False
        self.ws_error: Optional[str] = None

        # State
        self.running = False
        self.stop_event = threading.Event()
        self._lock = threading.Lock()
        self._last_reset_date: Optional[date] = None

        # Callbacks for GUI updates
        self.on_brick_callback = None  # (side: str, brick: RenkoBrick)
        self.on_trade_callback = None  # (trade: Trade, action: str)
        self.on_status_callback = None  # (msg: str)
        self.on_ltp_callback = None    # (spot, ce_ltp, pe_ltp)

    def start(self):
        """Load instruments and start WS connection."""
        self.running = True
        self.stop_event.clear()

        # Load instrument master
        if not self.resolver.load():
            self._status("ERROR: Failed to load instrument master")
            return

        # Start WS in background
        self.ws_thread = threading.Thread(target=self._ws_loop, daemon=True)
        self.ws_thread.start()

        # Start monitor thread
        threading.Thread(target=self._monitor_loop, daemon=True).start()

        self._status("Engine started — waiting for NIFTY spot to resolve ATM...")

    def stop(self):
        """Stop everything."""
        self.running = False
        self.stop_event.set()
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass
        self._status("Engine stopped")

    def _status(self, msg: str):
        log.info(msg)
        if self.on_status_callback:
            try:
                self.on_status_callback(msg)
            except Exception:
                pass

    # ── WebSocket ───────────────────────────────────────────────

    def _ws_loop(self):
        while not self.stop_event.is_set():
            try:
                ws_url = WS_URL_TEMPLATE.format(
                    token=self.access_token, client_id=self.client_id
                )
                self.ws = websocket.WebSocketApp(
                    ws_url,
                    on_open=self._on_ws_open,
                    on_message=self._on_ws_message,
                    on_error=self._on_ws_error,
                    on_close=self._on_ws_close,
                )
                self.ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                self.ws_error = str(e)
                log.error(f"WS exception: {e}")
            finally:
                self.ws_connected = False
                if not self.stop_event.is_set():
                    time.sleep(2)

    def _on_ws_open(self, ws):
        self.ws_connected = True
        self.ws_error = None
        log.info("WebSocket connected")
        self._subscribe_spot()
        # If ATM already resolved, subscribe options too
        if self.atm_info:
            self._subscribe_options()

    def _on_ws_close(self, ws, status_code, msg):
        self.ws_connected = False
        log.info(f"WebSocket closed: {status_code} {msg}")

    def _on_ws_error(self, ws, error):
        self.ws_error = str(error)
        log.error(f"WebSocket error: {error}")

    def _on_ws_message(self, ws, message):
        if isinstance(message, str):
            return
        hdr = parse_header_8(bytes(message))
        if not hdr:
            return

        code = hdr["resp_code"]
        sec_id = hdr["security_id"]

        if code != RESP_TICKER:
            return

        t = parse_ticker(hdr["payload"])
        if not t:
            return

        ltp = t["ltp"]
        epoch = t["ltt_epoch"]

        # ── NIFTY spot tick ──
        if sec_id == NIFTY_SPOT_SEC_ID:
            self.nifty_spot = ltp
            self._check_atm_shift()
            if self.on_ltp_callback:
                try:
                    self.on_ltp_callback(self.nifty_spot, self.ce_ltp, self.pe_ltp)
                except Exception:
                    pass
            return

        if self.atm_info is None:
            return

        # ── CE tick ──
        if sec_id == self.atm_info["ce_sec_id"]:
            self.ce_ltp = ltp
            # Check exit tick-by-tick
            exit_reason = self.trade_mgr.check_exit_ce(ltp)
            if exit_reason and self.on_trade_callback and self.trade_mgr.trades:
                self.on_trade_callback(self.trade_mgr.trades[-1], exit_reason)

            # Feed into Renko
            new_bricks = self.ce_renko.feed(ltp, epoch)
            for brick in new_bricks:
                if self.on_brick_callback:
                    self.on_brick_callback("CE", brick)
                if brick.is_green:
                    self._handle_green_brick_ce(ltp)

            if self.on_ltp_callback:
                try:
                    self.on_ltp_callback(self.nifty_spot, self.ce_ltp, self.pe_ltp)
                except Exception:
                    pass
            return

        # ── PE tick ──
        if sec_id == self.atm_info["pe_sec_id"]:
            self.pe_ltp = ltp
            # Check exit tick-by-tick
            exit_reason = self.trade_mgr.check_exit_pe(ltp)
            if exit_reason and self.on_trade_callback and self.trade_mgr.trades:
                self.on_trade_callback(self.trade_mgr.trades[-1], exit_reason)

            # Feed into Renko
            new_bricks = self.pe_renko.feed(ltp, epoch)
            for brick in new_bricks:
                if self.on_brick_callback:
                    self.on_brick_callback("PE", brick)
                if brick.is_green:
                    self._handle_green_brick_pe(ltp)

            if self.on_ltp_callback:
                try:
                    self.on_ltp_callback(self.nifty_spot, self.ce_ltp, self.pe_ltp)
                except Exception:
                    pass
            return

    # ── Signal handling ─────────────────────────────────────────

    def _handle_green_brick_ce(self, ltp: float):
        """Green brick closed on CE chart."""
        if not is_in_session():
            return

        # Clear waiting flag (fresh green brick arrived)
        self.trade_mgr.on_green_brick_ce()

        # Enter if we can
        if self.trade_mgr.can_enter_ce() and self.atm_info:
            trade = self.trade_mgr.enter_ce(
                ltp, self.atm_info["ce_sec_id"], self.atm_info["ce_display"]
            )
            if trade and self.on_trade_callback:
                self.on_trade_callback(trade, "ENTRY")

    def _handle_green_brick_pe(self, ltp: float):
        """Green brick closed on PE chart."""
        if not is_in_session():
            return

        # Clear waiting flag (fresh green brick arrived)
        self.trade_mgr.on_green_brick_pe()

        # Enter if we can
        if self.trade_mgr.can_enter_pe() and self.atm_info:
            trade = self.trade_mgr.enter_pe(
                ltp, self.atm_info["pe_sec_id"], self.atm_info["pe_display"]
            )
            if trade and self.on_trade_callback:
                self.on_trade_callback(trade, "ENTRY")

    # ── ATM management ──────────────────────────────────────────

    def _check_atm_shift(self):
        """Check if ATM strike needs to shift based on current NIFTY spot."""
        if self.nifty_spot <= 0:
            return

        new_atm = round(self.nifty_spot / 50) * 50

        # First-time resolution
        if self.atm_info is None:
            self._resolve_and_subscribe(new_atm)
            return

        # Shift only when flat (no open trades)
        if new_atm != self.current_atm_strike:
            if (self.trade_mgr.ce_trade is None and
                    self.trade_mgr.pe_trade is None):
                log.info(f"ATM shift: {self.current_atm_strike} → {new_atm}")
                self._resolve_and_subscribe(new_atm)

    def _resolve_and_subscribe(self, strike: int = 0):
        """Resolve ATM strike and subscribe to CE/PE ticks."""
        info = self.resolver.resolve_atm_strikes(
            self.nifty_spot if strike == 0 else float(strike)
        )
        if info is None:
            self._status(f"Failed to resolve ATM strike {strike}")
            return

        # Reset Renko engines for new strikes
        self.ce_renko.reset()
        self.pe_renko.reset()
        self.ce_ltp = 0.0
        self.pe_ltp = 0.0

        self.atm_info = info
        self.current_atm_strike = info["strike"]

        self._status(f"ATM={info['strike']} | CE={info['ce_display']} | "
                     f"PE={info['pe_display']} | Exp={info['expiry']}")

        # Subscribe to options via WS
        self._subscribe_options()

    def _subscribe_spot(self):
        """Subscribe to NIFTY spot index."""
        if not self.ws or not self.ws_connected:
            return
        msg = {
            "RequestCode": REQ_SUB_TICKER,
            "InstrumentCount": 1,
            "InstrumentList": [{
                "ExchangeSegment": NIFTY_SPOT_EXCHANGE,
                "SecurityId": NIFTY_SPOT_SEC_ID,
            }],
        }
        self.ws.send(json.dumps(msg))
        log.info("Subscribed to NIFTY spot")

    def _subscribe_options(self):
        """Subscribe to CE and PE ticks."""
        if not self.ws or not self.ws_connected or not self.atm_info:
            return
        msg = {
            "RequestCode": REQ_SUB_TICKER,
            "InstrumentCount": 2,
            "InstrumentList": [
                {
                    "ExchangeSegment": "NSE_FNO",
                    "SecurityId": self.atm_info["ce_sec_id"],
                },
                {
                    "ExchangeSegment": "NSE_FNO",
                    "SecurityId": self.atm_info["pe_sec_id"],
                },
            ],
        }
        self.ws.send(json.dumps(msg))
        log.info(f"Subscribed to CE={self.atm_info['ce_sec_id']} "
                 f"PE={self.atm_info['pe_sec_id']}")

    # ── Monitor loop ────────────────────────────────────────────

    def _monitor_loop(self):
        """Background loop for session management and daily reset."""
        while not self.stop_event.is_set():
            try:
                today = now_ist().date()
                if self._last_reset_date != today:
                    self._last_reset_date = today
                    self.trade_mgr.reset_day()
                    self.ce_renko.reset()
                    self.pe_renko.reset()
                    log.info(f"Daily reset done for {today}")
            except Exception as e:
                log.error(f"Monitor error: {e}")
            time.sleep(5)


# ═══════════════════════════════════════════════════════════════════
# GUI — CustomTkinter
# ═══════════════════════════════════════════════════════════════════

class RenkoCanvas(ctk.CTkCanvas):
    """Canvas widget that draws Renko bricks."""

    def __init__(self, master, side: str = "CE", **kwargs):
        super().__init__(master, **kwargs)
        self.side = side
        self.bricks: List[RenkoBrick] = []
        self.brick_width = 12
        self.brick_gap = 2
        self._trade_lines: List[Tuple[float, str]] = []  # (price, type)

    def set_bricks(self, bricks: List[RenkoBrick]):
        self.bricks = list(bricks)
        self._draw()

    def add_trade_line(self, price: float, line_type: str):
        """line_type: 'entry', 'tp', 'sl'"""
        self._trade_lines.append((price, line_type))
        self._draw()

    def clear_trade_lines(self):
        self._trade_lines.clear()
        self._draw()

    def _draw(self):
        self.delete("all")

        w = self.winfo_width()
        h = self.winfo_height()
        if w < 10 or h < 10 or not self.bricks:
            self.create_text(
                w // 2, h // 2,
                text=f"{self.side} Renko — waiting for bricks...",
                fill="#888888", font=("Consolas", 11)
            )
            return

        # Calculate price range
        all_prices = []
        for b in self.bricks:
            all_prices.extend([b.open, b.close])
        for price, _ in self._trade_lines:
            all_prices.append(price)

        min_p = min(all_prices)
        max_p = max(all_prices)
        price_range = max_p - min_p
        if price_range < 0.01:
            price_range = 1.0

        margin_top = 25
        margin_bottom = 25
        margin_left = 55
        margin_right = 10
        chart_h = h - margin_top - margin_bottom
        chart_w = w - margin_left - margin_right

        def price_to_y(p):
            return margin_top + chart_h - ((p - min_p) / price_range * chart_h)

        # How many bricks fit
        step = self.brick_width + self.brick_gap
        max_visible = max(1, chart_w // step)
        visible = self.bricks[-max_visible:]

        # Draw grid lines
        steps = 5
        for i in range(steps + 1):
            p = min_p + price_range * i / steps
            y = price_to_y(p)
            self.create_line(margin_left, y, w - margin_right, y,
                             fill="#333333", dash=(2, 4))
            self.create_text(margin_left - 5, y, text=f"{p:.1f}",
                             anchor="e", fill="#888888", font=("Consolas", 8))

        # Draw bricks
        for i, brick in enumerate(visible):
            x1 = margin_left + i * step
            x2 = x1 + self.brick_width
            y_open = price_to_y(brick.open)
            y_close = price_to_y(brick.close)

            if brick.is_green:
                color = "#00C853"
                outline = "#00E676"
            else:
                color = "#FF1744"
                outline = "#FF5252"

            self.create_rectangle(x1, min(y_open, y_close), x2, max(y_open, y_close),
                                  fill=color, outline=outline, width=1)

        # Draw trade lines
        line_colors = {"entry": "#FFAB00", "tp": "#00E5FF", "sl": "#FF1744"}
        for price, lt in self._trade_lines:
            y = price_to_y(price)
            col = line_colors.get(lt, "#FFFFFF")
            self.create_line(margin_left, y, w - margin_right, y,
                             fill=col, dash=(4, 2), width=1)
            label = lt.upper()
            self.create_text(w - margin_right - 5, y - 8, text=f"{label} {price:.2f}",
                             anchor="e", fill=col, font=("Consolas", 8))

        # Title
        last = visible[-1] if visible else None
        title_text = f"{self.side} Renko | Bricks: {len(self.bricks)}"
        if last:
            title_text += f" | Last: {'GREEN' if last.is_green else 'RED'} C={last.close:.2f}"
        self.create_text(w // 2, 10, text=title_text,
                         fill="#CCCCCC", font=("Consolas", 10, "bold"))


class App(ctk.CTk):
    """Main application window."""

    def __init__(self):
        super().__init__()

        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")

        self.title(APP_TITLE)
        self.geometry("1280x820")
        self.minsize(1000, 700)

        # State
        self.engine: Optional[CoreEngine] = None
        self._build_ui()

        # Load .env defaults
        load_dotenv()
        env_cid = os.getenv("DHAN_CLIENT_ID", "")
        env_tok = os.getenv("DHAN_ACCESS_TOKEN", "")
        if env_cid:
            self.entry_client_id.insert(0, env_cid)
        if env_tok:
            self.entry_token.insert(0, env_tok)

        self.protocol("WM_DELETE_WINDOW", self._on_close)

    def _build_ui(self):
        # ── Top bar: settings ───────────────────────────────────
        top = ctk.CTkFrame(self, height=50)
        top.pack(fill="x", padx=8, pady=(8, 4))

        ctk.CTkLabel(top, text="Client ID:").pack(side="left", padx=(8, 2))
        self.entry_client_id = ctk.CTkEntry(top, width=120, placeholder_text="Dhan Client ID")
        self.entry_client_id.pack(side="left", padx=2)

        ctk.CTkLabel(top, text="Token:").pack(side="left", padx=(8, 2))
        self.entry_token = ctk.CTkEntry(top, width=200, placeholder_text="Access Token", show="*")
        self.entry_token.pack(side="left", padx=2)

        ctk.CTkLabel(top, text="Brick:").pack(side="left", padx=(12, 2))
        self.entry_brick = ctk.CTkEntry(top, width=50)
        self.entry_brick.insert(0, "1")
        self.entry_brick.pack(side="left", padx=2)

        ctk.CTkLabel(top, text="TP:").pack(side="left", padx=(12, 2))
        self.entry_tp = ctk.CTkEntry(top, width=40)
        self.entry_tp.insert(0, "8")
        self.entry_tp.pack(side="left", padx=2)

        ctk.CTkLabel(top, text="SL:").pack(side="left", padx=(8, 2))
        self.entry_sl = ctk.CTkEntry(top, width=40)
        self.entry_sl.insert(0, "2")
        self.entry_sl.pack(side="left", padx=2)

        ctk.CTkLabel(top, text="Max Trades:").pack(side="left", padx=(12, 2))
        self.entry_max_trades = ctk.CTkEntry(top, width=40)
        self.entry_max_trades.insert(0, "10")
        self.entry_max_trades.pack(side="left", padx=2)

        ctk.CTkLabel(top, text="Lot Size:").pack(side="left", padx=(12, 2))
        self.entry_lot_size = ctk.CTkEntry(top, width=50)
        self.entry_lot_size.insert(0, "75")
        self.entry_lot_size.pack(side="left", padx=2)

        self.paper_var = ctk.BooleanVar(value=True)
        self.switch_paper = ctk.CTkSwitch(top, text="Paper", variable=self.paper_var,
                                          onvalue=True, offvalue=False)
        self.switch_paper.pack(side="left", padx=(12, 4))

        self.btn_start = ctk.CTkButton(top, text="▶  START", width=100,
                                       fg_color="#00C853", hover_color="#00E676",
                                       text_color="#000000", command=self._on_start)
        self.btn_start.pack(side="right", padx=8)

        self.btn_stop = ctk.CTkButton(top, text="■  STOP", width=80,
                                      fg_color="#FF1744", hover_color="#FF5252",
                                      text_color="#FFFFFF", command=self._on_stop,
                                      state="disabled")
        self.btn_stop.pack(side="right", padx=4)

        # ── Info bar ───────────────────────────────────────────
        info_bar = ctk.CTkFrame(self, height=35)
        info_bar.pack(fill="x", padx=8, pady=2)

        self.lbl_spot = ctk.CTkLabel(info_bar, text="NIFTY: --",
                                     font=("Consolas", 13, "bold"))
        self.lbl_spot.pack(side="left", padx=10)

        self.lbl_atm = ctk.CTkLabel(info_bar, text="ATM: --",
                                    font=("Consolas", 12))
        self.lbl_atm.pack(side="left", padx=10)

        self.lbl_ce_ltp = ctk.CTkLabel(info_bar, text="CE LTP: --",
                                       font=("Consolas", 12), text_color="#00E676")
        self.lbl_ce_ltp.pack(side="left", padx=10)

        self.lbl_pe_ltp = ctk.CTkLabel(info_bar, text="PE LTP: --",
                                       font=("Consolas", 12), text_color="#FF5252")
        self.lbl_pe_ltp.pack(side="left", padx=10)

        self.lbl_session = ctk.CTkLabel(info_bar, text="Session: --",
                                        font=("Consolas", 11))
        self.lbl_session.pack(side="left", padx=10)

        self.lbl_pnl = ctk.CTkLabel(info_bar, text="Day P&L: ₹0.00",
                                    font=("Consolas", 13, "bold"))
        self.lbl_pnl.pack(side="right", padx=10)

        self.lbl_trades = ctk.CTkLabel(info_bar, text="Trades: 0/10",
                                       font=("Consolas", 11))
        self.lbl_trades.pack(side="right", padx=10)

        # ── Middle: Renko charts side by side ─────────────────
        chart_frame = ctk.CTkFrame(self)
        chart_frame.pack(fill="both", expand=True, padx=8, pady=4)
        chart_frame.columnconfigure(0, weight=1)
        chart_frame.columnconfigure(1, weight=1)
        chart_frame.rowconfigure(0, weight=1)

        ce_frame = ctk.CTkFrame(chart_frame, border_width=1, border_color="#333333")
        ce_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 2))

        pe_frame = ctk.CTkFrame(chart_frame, border_width=1, border_color="#333333")
        pe_frame.grid(row=0, column=1, sticky="nsew", padx=(2, 0))

        self.ce_canvas = RenkoCanvas(ce_frame, side="CE", bg="#1A1A2E",
                                     highlightthickness=0)
        self.ce_canvas.pack(fill="both", expand=True)

        self.pe_canvas = RenkoCanvas(pe_frame, side="PE", bg="#1A1A2E",
                                     highlightthickness=0)
        self.pe_canvas.pack(fill="both", expand=True)

        # ── Bottom: trade log + status ────────────────────────
        bottom = ctk.CTkFrame(self, height=200)
        bottom.pack(fill="x", padx=8, pady=(4, 8))

        # Trade log (left)
        log_frame = ctk.CTkFrame(bottom)
        log_frame.pack(side="left", fill="both", expand=True, padx=(0, 4))

        ctk.CTkLabel(log_frame, text="Trade Log",
                     font=("Consolas", 11, "bold")).pack(anchor="w", padx=5, pady=2)
        self.trade_log = ctk.CTkTextbox(log_frame, height=140,
                                        font=("Consolas", 10),
                                        state="disabled")
        self.trade_log.pack(fill="both", expand=True, padx=2, pady=2)

        # Status log (right)
        status_frame = ctk.CTkFrame(bottom)
        status_frame.pack(side="right", fill="both", expand=True, padx=(4, 0))

        ctk.CTkLabel(status_frame, text="Status Log",
                     font=("Consolas", 11, "bold")).pack(anchor="w", padx=5, pady=2)
        self.status_log = ctk.CTkTextbox(status_frame, height=140,
                                         font=("Consolas", 10),
                                         state="disabled")
        self.status_log.pack(fill="both", expand=True, padx=2, pady=2)

        # Periodic GUI refresh
        self._refresh_timer()

    # ── GUI callbacks ───────────────────────────────────────────

    def _on_start(self):
        cid = self.entry_client_id.get().strip()
        tok = self.entry_token.get().strip()
        if not cid or not tok:
            self._append_status("ERROR: Enter Dhan Client ID and Access Token")
            return

        try:
            brick = float(self.entry_brick.get().strip())
            tp = float(self.entry_tp.get().strip())
            sl = float(self.entry_sl.get().strip())
            max_t = int(self.entry_max_trades.get().strip())
            lot = int(self.entry_lot_size.get().strip())
        except ValueError:
            self._append_status("ERROR: Invalid numeric input")
            return

        paper = self.paper_var.get()

        self.engine = CoreEngine(
            client_id=cid, access_token=tok,
            brick_size=brick, tp_points=tp, sl_points=sl,
            max_trades=max_t, lot_size=lot, paper_mode=paper,
        )

        # Wire callbacks
        self.engine.on_brick_callback = self._on_brick
        self.engine.on_trade_callback = self._on_trade
        self.engine.on_status_callback = self._on_status
        self.engine.on_ltp_callback = self._on_ltp

        self.engine.start()

        self.btn_start.configure(state="disabled")
        self.btn_stop.configure(state="normal")

        # Disable inputs while running
        for w in [self.entry_client_id, self.entry_token, self.entry_brick,
                  self.entry_tp, self.entry_sl, self.entry_max_trades,
                  self.entry_lot_size, self.switch_paper]:
            w.configure(state="disabled")

        mode = "PAPER" if paper else "LIVE"
        self._append_status(f"Started | {mode} | Brick={brick} | TP={tp} SL={sl} | "
                            f"MaxTrades={max_t} | Lot={lot}")

    def _on_stop(self):
        if self.engine:
            self.engine.stop()
            self.engine = None

        self.btn_start.configure(state="normal")
        self.btn_stop.configure(state="disabled")

        for w in [self.entry_client_id, self.entry_token, self.entry_brick,
                  self.entry_tp, self.entry_sl, self.entry_max_trades,
                  self.entry_lot_size, self.switch_paper]:
            w.configure(state="normal")

        self._append_status("Stopped")

    def _on_close(self):
        if self.engine:
            self.engine.stop()
        self.destroy()

    # ── Engine callbacks (called from background threads) ─────

    def _on_brick(self, side: str, brick: RenkoBrick):
        """Called when a new brick forms."""
        try:
            self.after(0, self._update_chart, side)
        except Exception:
            pass

    def _on_trade(self, trade: Trade, action: str):
        """Called on trade entry/exit."""
        try:
            self.after(0, self._log_trade, trade, action)
        except Exception:
            pass

    def _on_status(self, msg: str):
        try:
            self.after(0, self._append_status, msg)
        except Exception:
            pass

    def _on_ltp(self, spot: float, ce: float, pe: float):
        # Throttle LTP updates — handled by refresh timer
        pass

    # ── GUI updates (main thread) ─────────────────────────────

    def _update_chart(self, side: str):
        if not self.engine:
            return
        if side == "CE":
            bricks = list(self.engine.ce_renko.bricks)
            self.ce_canvas.set_bricks(bricks)
            # Draw trade lines
            self.ce_canvas.clear_trade_lines()
            t = self.engine.trade_mgr.ce_trade
            if t:
                self.ce_canvas.add_trade_line(t.entry_price, "entry")
                self.ce_canvas.add_trade_line(t.target, "tp")
                self.ce_canvas.add_trade_line(t.stoploss, "sl")
        else:
            bricks = list(self.engine.pe_renko.bricks)
            self.pe_canvas.set_bricks(bricks)
            self.pe_canvas.clear_trade_lines()
            t = self.engine.trade_mgr.pe_trade
            if t:
                self.pe_canvas.add_trade_line(t.entry_price, "entry")
                self.pe_canvas.add_trade_line(t.target, "tp")
                self.pe_canvas.add_trade_line(t.stoploss, "sl")

    def _log_trade(self, trade: Trade, action: str):
        if action == "ENTRY":
            msg = (f"[{ist_hhmm(trade.entry_time)}] {trade.side} BUY | "
                   f"{trade.display_name} | Entry={trade.entry_price:.2f} | "
                   f"TP={trade.target:.2f} SL={trade.stoploss:.2f}")
        else:
            pnl_str = f"₹{trade.pnl:+.2f}"
            msg = (f"[{ist_hhmm(trade.exit_time)}] {trade.side} {action} | "
                   f"{trade.display_name} | Exit={trade.exit_price:.2f} | "
                   f"PnL={pnl_str}")
        self._append_trade_log(msg)

        # Update charts after trade
        self._update_chart("CE")
        self._update_chart("PE")

    def _append_trade_log(self, msg: str):
        self.trade_log.configure(state="normal")
        self.trade_log.insert("end", msg + "\n")
        self.trade_log.see("end")
        self.trade_log.configure(state="disabled")

    def _append_status(self, msg: str):
        ts = ist_hhmm(now_ist())
        self.status_log.configure(state="normal")
        self.status_log.insert("end", f"[{ts}] {msg}\n")
        self.status_log.see("end")
        self.status_log.configure(state="disabled")

    def _refresh_timer(self):
        """Periodic GUI refresh — 500ms interval."""
        try:
            self._refresh_info_bar()
            self._refresh_charts()
        except Exception:
            pass
        self.after(500, self._refresh_timer)

    def _refresh_info_bar(self):
        if not self.engine:
            return

        # Spot
        if self.engine.nifty_spot > 0:
            self.lbl_spot.configure(text=f"NIFTY: {self.engine.nifty_spot:.2f}")

        # ATM
        if self.engine.atm_info:
            self.lbl_atm.configure(
                text=f"ATM: {self.engine.current_atm_strike} | "
                     f"Exp: {self.engine.atm_info['expiry']}"
            )

        # CE/PE LTP
        if self.engine.ce_ltp > 0:
            self.lbl_ce_ltp.configure(text=f"CE: {self.engine.ce_ltp:.2f}")
        if self.engine.pe_ltp > 0:
            self.lbl_pe_ltp.configure(text=f"PE: {self.engine.pe_ltp:.2f}")

        # Session
        if is_in_session():
            self.lbl_session.configure(text="Session: ACTIVE", text_color="#00E676")
        else:
            self.lbl_session.configure(text="Session: CLOSED", text_color="#FF5252")

        # Trades
        tm = self.engine.trade_mgr
        self.lbl_trades.configure(
            text=f"Trades: {tm.total_trades_today}/{tm.max_trades}"
        )

        # P&L
        pnl = tm.get_day_pnl()
        col = "#00E676" if pnl >= 0 else "#FF5252"
        self.lbl_pnl.configure(text=f"Day P&L: ₹{pnl:+.2f}", text_color=col)

    def _refresh_charts(self):
        """Refresh charts periodically (handles resize etc)."""
        if not self.engine:
            return
        self._update_chart("CE")
        self._update_chart("PE")


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════

def main():
    app = App()
    app.mainloop()


if __name__ == "__main__":
    main()
