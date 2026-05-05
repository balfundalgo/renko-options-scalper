# balfund_renko_options_scalper.py
# ─────────────────────────────────────────────────────────────────────
# Balfund Renko Scalper v2.0
# ─────────────────────────────────────────────────────────────────────
# Strategy:
#   - Builds Renko bricks on instrument LTP (live tick-by-tick)
#   - Buys on green brick close
#   - Fixed TP / SL (default +8 / -2, 1:4 risk-reward)
#   - After exit, waits for next fresh green brick before re-entry
#
# Instruments:
#   NIFTY OPTIONS  — ATM CE and PE (auto-selected, auto-shift)
#                    Session: 09:15-15:30 IST | Exchange: NSE_FNO
#   MCX GOLDTEN    — Near-month futures (10 grams)
#                    Session: 09:00-23:30 IST | Exchange: MCX_COMM
#   MCX SILVERM    — Near-month futures (Silver Micro, 1 kg)
#                    Session: 09:00-23:30 IST | Exchange: MCX_COMM
#
# Data:
#   - Dhan WebSocket v2 for live ticks
#   - Dhan REST API v2 for order placement
#   - Dhan instrument master CSV for security ID resolution
#
# Install:
#   pip install customtkinter requests pandas websocket-client python-dotenv
# ─────────────────────────────────────────────────────────────────────

import os, sys, time, json, math, struct, threading, logging
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

VERSION = "2.0"
APP_TITLE = f"Balfund Renko Scalper v{VERSION}"

WS_URL_TEMPLATE = (
    "wss://api-feed.dhan.co?version=2"
    "&token={token}&clientId={client_id}&authType=2"
)
INSTRUMENT_CSV_URL = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"
DHAN_BASE_URL = "https://api.dhan.co/v2"

REQ_SUB_TICKER = 15
RESP_TICKER = 2

NIFTY_SPOT_SEC_ID = "13"
NIFTY_SPOT_EXCHANGE = "IDX_I"
IST_OFFSET = timedelta(hours=5, minutes=30)

INSTRUMENT_DEFS = {
    "NIFTY_CE": {
        "key": "NIFTY_CE", "label": "NIFTY CE", "side": "CE",
        "exchange_order": "NSE_FNO", "exchange_ws": "NSE_FNO",
        "default_lot": 75, "session_start": (9, 15), "session_end": (15, 30),
        "resolver": "nifty_options",
    },
    "NIFTY_PE": {
        "key": "NIFTY_PE", "label": "NIFTY PE", "side": "PE",
        "exchange_order": "NSE_FNO", "exchange_ws": "NSE_FNO",
        "default_lot": 75, "session_start": (9, 15), "session_end": (15, 30),
        "resolver": "nifty_options",
    },
    "GOLDTEN": {
        "key": "GOLDTEN", "label": "GOLDTEN", "side": "GOLDTEN",
        "exchange_order": "MCX_COMM", "exchange_ws": "MCX_COMM",
        "default_lot": 10, "session_start": (9, 0), "session_end": (23, 30),
        "resolver": "mcx_futures", "mcx_symbol": "GOLDTEN",
    },
    "SILVERM": {
        "key": "SILVERM", "label": "SILVERM", "side": "SILVERM",
        "exchange_order": "MCX_COMM", "exchange_ws": "MCX_COMM",
        "default_lot": 1, "session_start": (9, 0), "session_end": (23, 30),
        "resolver": "mcx_futures", "mcx_symbol": "SILVERM",
    },
}

LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
os.makedirs(LOG_DIR, exist_ok=True)
log_file = os.path.join(LOG_DIR, f"renko_scalper_{date.today().isoformat()}.log")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    handlers=[logging.FileHandler(log_file, encoding="utf-8"), logging.StreamHandler(sys.stdout)])
log = logging.getLogger("RenkoScalper")

def now_ist(): return datetime.now(timezone.utc) + IST_OFFSET
def ist_hhmm(dt): return dt.strftime("%H:%M:%S")
def is_in_session_for(idef, dt=None):
    if dt is None: dt = now_ist()
    t = dt.hour * 60 + dt.minute
    sh, sm = idef["session_start"]; eh, em = idef["session_end"]
    return (sh*60+sm) <= t < (eh*60+em)
def _norm_epoch(ts):
    diff = ts - int(time.time())
    if 4.5*3600 <= diff <= 6.5*3600: ts -= 19800
    return ts

def parse_header_8(msg):
    if len(msg) < 8: return None
    return {"resp_code": msg[0], "security_id": str(struct.unpack_from("<I", msg, 4)[0]), "payload": msg[8:]}
def parse_ticker(payload):
    if len(payload) < 8: return None
    return {"ltp": float(struct.unpack_from("<f", payload, 0)[0]),
            "ltt_epoch": _norm_epoch(int(struct.unpack_from("<I", payload, 4)[0]))}

# ═══════════════════════════════════════════════════════════════════
class RenkoBrick:
    __slots__ = ("time_epoch", "open", "close", "is_green")
    def __init__(self, t, o, c):
        self.time_epoch = t; self.open = o; self.close = c; self.is_green = c > o

class RenkoEngine:
    def __init__(self, brick_size=1.0, reversal_bricks=2, max_history=200):
        self.brick_size = brick_size; self.reversal_bricks = reversal_bricks
        self.bricks = deque(maxlen=max_history)
        self._last_close = None; self._direction = 0; self._initialized = False
        self.lock = threading.Lock()
    def reset(self):
        with self.lock:
            self.bricks.clear(); self._last_close = None; self._direction = 0; self._initialized = False
    def feed(self, price, epoch):
        with self.lock: return self._process(price, epoch)
    def _process(self, price, epoch):
        nb = []
        if not self._initialized:
            self._last_close = price; self._initialized = True; return nb
        bs = self.brick_size
        while True:
            diff = price - self._last_close
            if self._direction == 0:
                if diff >= bs:
                    o, c = self._last_close, self._last_close + bs
                    b = RenkoBrick(epoch, o, c); self.bricks.append(b); nb.append(b)
                    self._last_close = c; self._direction = 1; continue
                elif diff <= -bs:
                    o, c = self._last_close, self._last_close - bs
                    b = RenkoBrick(epoch, o, c); self.bricks.append(b); nb.append(b)
                    self._last_close = c; self._direction = -1; continue
                break
            if self._direction == 1 and diff >= bs:
                o, c = self._last_close, self._last_close + bs
                b = RenkoBrick(epoch, o, c); self.bricks.append(b); nb.append(b)
                self._last_close = c; continue
            if self._direction == -1 and diff <= -bs:
                o, c = self._last_close, self._last_close - bs
                b = RenkoBrick(epoch, o, c); self.bricks.append(b); nb.append(b)
                self._last_close = c; continue
            if self._direction == 1 and diff <= -(bs * self.reversal_bricks):
                o = self._last_close - bs; c = self._last_close - 2.0 * bs
                b = RenkoBrick(epoch, o, c); self.bricks.append(b); nb.append(b)
                self._last_close = c; self._direction = -1; continue
            if self._direction == -1 and diff >= (bs * self.reversal_bricks):
                o = self._last_close + bs; c = self._last_close + 2.0 * bs
                b = RenkoBrick(epoch, o, c); self.bricks.append(b); nb.append(b)
                self._last_close = c; self._direction = 1; continue
            break
        return nb
    @property
    def brick_count(self): return len(self.bricks)
    @property
    def last_brick(self): return self.bricks[-1] if self.bricks else None

# ═══════════════════════════════════════════════════════════════════
class InstrumentResolver:
    def __init__(self):
        self.nifty_df = None; self.mcx_df = None; self.loaded = False
    def load(self):
        try:
            log.info("Downloading Dhan instrument master...")
            r = requests.get(INSTRUMENT_CSV_URL, timeout=30); r.raise_for_status()
            usecols = ["EXCH_ID","SEGMENT","SECURITY_ID","INSTRUMENT","SYMBOL_NAME",
                       "DISPLAY_NAME","SM_EXPIRY_DATE","SM_STRIKE_PRICE","SM_OPTION_TYPE"]
            df = pd.read_csv(StringIO(r.text), usecols=usecols, low_memory=False)
            for c in ["EXCH_ID","SEGMENT","INSTRUMENT","SYMBOL_NAME","DISPLAY_NAME","SM_OPTION_TYPE"]:
                df[c] = df[c].astype(str).str.strip()
            df["SM_EXPIRY_DATE"] = pd.to_datetime(df["SM_EXPIRY_DATE"], errors="coerce")
            df["SM_STRIKE_PRICE"] = pd.to_numeric(df["SM_STRIKE_PRICE"], errors="coerce")
            df["SECURITY_ID"] = df["SECURITY_ID"].astype(str).str.strip()
            self.nifty_df = df[(df["EXCH_ID"]=="NSE")&(df["INSTRUMENT"]=="OPTIDX")&(df["SYMBOL_NAME"]=="NIFTY")].copy()
            self.mcx_df = df[(df["EXCH_ID"]=="MCX")&(df["INSTRUMENT"]=="FUTCOM")].copy()
            self.loaded = True
            log.info(f"Master loaded | NIFTY opts: {len(self.nifty_df)} | MCX futs: {len(self.mcx_df)}")
            return True
        except Exception as e:
            log.error(f"Failed to load master: {e}"); return False

    def resolve_atm_strikes(self, spot_price):
        if not self.loaded or self.nifty_df is None: return None
        today = pd.Timestamp.now().normalize()
        future = self.nifty_df[self.nifty_df["SM_EXPIRY_DATE"] >= today]
        if future.empty: return None
        expiry = future["SM_EXPIRY_DATE"].min()
        atm = round(spot_price / 50) * 50
        exp_df = self.nifty_df[self.nifty_df["SM_EXPIRY_DATE"] == expiry]
        ce = exp_df[(exp_df["SM_STRIKE_PRICE"]==atm)&(exp_df["SM_OPTION_TYPE"]=="CALL")]
        pe = exp_df[(exp_df["SM_STRIKE_PRICE"]==atm)&(exp_df["SM_OPTION_TYPE"]=="PUT")]
        if ce.empty or pe.empty:
            log.error(f"ATM {atm} CE/PE not found for {expiry.date()}"); return None
        return {"strike": int(atm), "expiry": expiry.date().isoformat(),
                "ce_sec_id": str(int(float(ce.iloc[0]["SECURITY_ID"]))),
                "pe_sec_id": str(int(float(pe.iloc[0]["SECURITY_ID"]))),
                "ce_display": str(ce.iloc[0]["DISPLAY_NAME"]),
                "pe_display": str(pe.iloc[0]["DISPLAY_NAME"])}

    def resolve_mcx_near_month(self, symbol_name):
        if not self.loaded or self.mcx_df is None: return None
        today = pd.Timestamp.now().normalize()
        filt = self.mcx_df[(self.mcx_df["SYMBOL_NAME"]==symbol_name)&(self.mcx_df["SM_EXPIRY_DATE"]>=today)]
        if filt.empty:
            log.error(f"No MCX contract for {symbol_name}"); return None
        row = filt.loc[filt["SM_EXPIRY_DATE"].idxmin()]
        sec_id = str(int(float(row["SECURITY_ID"])))
        exp = row["SM_EXPIRY_DATE"]
        return {"sec_id": sec_id, "display": str(row["DISPLAY_NAME"]),
                "expiry": exp.date().isoformat() if pd.notna(exp) else "?"}

# ═══════════════════════════════════════════════════════════════════
class Trade:
    __slots__ = ("side","entry_price","target","stoploss","entry_time","exit_price",
                 "exit_time","pnl","status","sec_id","display_name","order_id",
                 "exchange_seg","lot_size")
    def __init__(self, side, entry_price, sec_id, display_name, tp, sl, exchange_seg="NSE_FNO", lot_size=75):
        self.side = side; self.entry_price = entry_price
        self.target = entry_price + tp; self.stoploss = entry_price - sl
        self.entry_time = now_ist(); self.exit_price = 0.0; self.exit_time = None
        self.pnl = 0.0; self.status = "OPEN"; self.sec_id = sec_id
        self.display_name = display_name; self.order_id = ""
        self.exchange_seg = exchange_seg; self.lot_size = lot_size

def _place_order(client_id, access_token, txn_type, exchange_seg, sec_id, qty, trade):
    try:
        headers = {"Content-Type": "application/json", "access-token": access_token}
        payload = {"dhanClientId": client_id, "transactionType": txn_type,
                   "exchangeSegment": exchange_seg, "productType": "INTRADAY",
                   "orderType": "MARKET", "validity": "DAY", "securityId": sec_id,
                   "quantity": qty, "price": 0, "triggerPrice": 0}
        resp = requests.post(f"{DHAN_BASE_URL}/orders", headers=headers, json=payload, timeout=5)
        data = resp.json()
        if txn_type == "BUY": trade.order_id = str(data.get("orderId", ""))
        log.info(f"{txn_type} order | {exchange_seg} {sec_id} qty={qty} | resp={data}")
    except Exception as e:
        log.error(f"{txn_type} order FAILED: {e}")

# ═══════════════════════════════════════════════════════════════════
class Channel:
    def __init__(self, key, inst_def, brick_size, tp_points, sl_points, lot_size):
        self.key = key; self.inst_def = inst_def; self.label = inst_def["label"]
        self.renko = RenkoEngine(brick_size=brick_size)
        self.tp_points = tp_points; self.sl_points = sl_points; self.lot_size = lot_size
        self.exchange_order = inst_def["exchange_order"]; self.ws_exchange = inst_def["exchange_ws"]
        self.sec_id = ""; self.display_name = ""
        self.trade = None; self.waiting = False; self.ltp = 0.0; self.lock = threading.Lock()
    def is_in_session(self): return is_in_session_for(self.inst_def)
    def can_enter(self):
        with self.lock: return self.trade is None and not self.waiting
    def enter(self, price, paper, cid, tok):
        with self.lock:
            if self.trade is not None: return None
            t = Trade(self.label, price, self.sec_id, self.display_name,
                      self.tp_points, self.sl_points, self.exchange_order, self.lot_size)
            self.trade = t; self.waiting = False
            if not paper: _place_order(cid, tok, "BUY", self.exchange_order, self.sec_id, self.lot_size, t)
            log.info(f"{'[PAPER]' if paper else '[LIVE]'} {self.label} BUY | {self.display_name} | "
                     f"Entry={price:.2f} TP={t.target:.2f} SL={t.stoploss:.2f}")
            return t
    def check_exit(self, ltp, paper, cid, tok):
        with self.lock:
            if self.trade is None: return None
            t = self.trade; reason = None
            if ltp >= t.target: reason = "TARGET"
            elif ltp <= t.stoploss: reason = "STOPLOSS"
            if reason:
                t.exit_price = ltp; t.exit_time = now_ist()
                t.pnl = (ltp - t.entry_price) * t.lot_size; t.status = reason
                if not paper: _place_order(cid, tok, "SELL", self.exchange_order, self.sec_id, self.lot_size, t)
                log.info(f"{'[PAPER]' if paper else '[LIVE]'} {self.label} {reason} | "
                         f"{self.display_name} | Exit={ltp:.2f} PnL={t.pnl:+.2f}")
                self.trade = None; self.waiting = True; return reason
            return None
    def on_green_brick(self):
        with self.lock:
            if self.waiting: self.waiting = False; log.info(f"{self.label} waiting cleared")
    def reset(self):
        with self.lock: self.trade = None; self.waiting = False; self.ltp = 0.0
        self.renko.reset()

# ═══════════════════════════════════════════════════════════════════
class CoreEngine:
    def __init__(self, client_id, access_token, brick_size, tp_points, sl_points,
                 max_trades, lot_sizes, active_instruments, paper_mode):
        self.client_id = client_id; self.access_token = access_token
        self.brick_size = brick_size; self.max_trades = max_trades; self.paper_mode = paper_mode
        self.resolver = InstrumentResolver()
        self.channels = {}
        for k in active_instruments:
            idef = INSTRUMENT_DEFS[k]
            self.channels[k] = Channel(k, idef, brick_size, tp_points, sl_points,
                                       lot_sizes.get(k, idef["default_lot"]))
        self._secid_map = {}
        self.nifty_spot = 0.0; self.atm_info = None; self.current_atm_strike = 0
        self._needs_nifty = ("NIFTY_CE" in self.channels or "NIFTY_PE" in self.channels)
        self.total_trades_today = 0; self.trades = []
        self.ws = None; self.ws_connected = False; self.ws_error = None
        self.running = False; self.stop_event = threading.Event(); self._last_reset_date = None
        self.on_brick_callback = None; self.on_trade_callback = None; self.on_status_callback = None

    @property
    def max_reached(self): return self.total_trades_today >= self.max_trades

    def start(self):
        self.running = True; self.stop_event.clear()
        if not self.resolver.load(): self._status("ERROR: Failed to load instrument master"); return
        for k, ch in self.channels.items():
            if ch.inst_def["resolver"] == "mcx_futures":
                info = self.resolver.resolve_mcx_near_month(ch.inst_def["mcx_symbol"])
                if info:
                    ch.sec_id = info["sec_id"]; ch.display_name = info["display"]
                    self._secid_map[info["sec_id"]] = k
                    self._status(f"{ch.inst_def['mcx_symbol']} -> {info['display']} (Exp {info['expiry']})")
                else: self._status(f"WARNING: Could not resolve {ch.inst_def['mcx_symbol']}")
        threading.Thread(target=self._ws_loop, daemon=True).start()
        threading.Thread(target=self._monitor_loop, daemon=True).start()
        self._status("Engine started" + (" - waiting for NIFTY spot..." if self._needs_nifty else ""))

    def stop(self):
        self.running = False; self.stop_event.set()
        if self.ws:
            try: self.ws.close()
            except: pass
        self._status("Engine stopped")

    def get_day_pnl(self): return sum(t.pnl for t in self.trades if t.status != "OPEN")

    def _status(self, msg):
        log.info(msg)
        if self.on_status_callback:
            try: self.on_status_callback(msg)
            except: pass

    def _ws_loop(self):
        while not self.stop_event.is_set():
            try:
                self.ws = websocket.WebSocketApp(
                    WS_URL_TEMPLATE.format(token=self.access_token, client_id=self.client_id),
                    on_open=self._on_ws_open, on_message=self._on_ws_message,
                    on_error=self._on_ws_error, on_close=self._on_ws_close)
                self.ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                self.ws_error = str(e); log.error(f"WS exception: {e}")
            finally:
                self.ws_connected = False
                if not self.stop_event.is_set(): time.sleep(2)

    def _on_ws_open(self, ws):
        self.ws_connected = True; self.ws_error = None; log.info("WebSocket connected")
        if self._needs_nifty:
            self._ws_sub(NIFTY_SPOT_EXCHANGE, NIFTY_SPOT_SEC_ID); log.info("Subscribed NIFTY spot")
        if self.atm_info: self._sub_nifty_opts()
        for k, ch in self.channels.items():
            if ch.inst_def["resolver"] == "mcx_futures" and ch.sec_id:
                self._ws_sub(ch.ws_exchange, ch.sec_id); log.info(f"Subscribed {ch.label} ({ch.sec_id})")

    def _on_ws_close(self, ws, sc, msg): self.ws_connected = False
    def _on_ws_error(self, ws, err): self.ws_error = str(err); log.error(f"WS error: {err}")

    def _on_ws_message(self, ws, message):
        if isinstance(message, str): return
        hdr = parse_header_8(bytes(message))
        if not hdr or hdr["resp_code"] != RESP_TICKER: return
        t = parse_ticker(hdr["payload"])
        if not t: return
        sec_id = hdr["security_id"]; ltp = t["ltp"]; epoch = t["ltt_epoch"]
        if sec_id == NIFTY_SPOT_SEC_ID:
            self.nifty_spot = ltp; self._check_atm_shift(); return
        ch_key = self._secid_map.get(sec_id)
        if ch_key is None: return
        ch = self.channels.get(ch_key)
        if ch is None: return
        ch.ltp = ltp
        exit_r = ch.check_exit(ltp, self.paper_mode, self.client_id, self.access_token)
        if exit_r and self.on_trade_callback:
            for tr in reversed(self.trades):
                if tr.status == exit_r and tr.side == ch.label:
                    self.on_trade_callback(tr, exit_r); break
        for brick in ch.renko.feed(ltp, epoch):
            if self.on_brick_callback: self.on_brick_callback(ch_key, brick)
            if brick.is_green: self._handle_green(ch_key, ch, ltp)

    def _handle_green(self, ch_key, ch, ltp):
        if not ch.is_in_session(): return
        ch.on_green_brick()
        if ch.can_enter() and not self.max_reached and ch.sec_id:
            trade = ch.enter(ltp, self.paper_mode, self.client_id, self.access_token)
            if trade:
                self.trades.append(trade); self.total_trades_today += 1
                if self.on_trade_callback: self.on_trade_callback(trade, "ENTRY")

    def _check_atm_shift(self):
        if self.nifty_spot <= 0 or not self._needs_nifty: return
        new_atm = round(self.nifty_spot / 50) * 50
        if self.atm_info is None: self._resolve_nifty(new_atm); return
        if new_atm != self.current_atm_strike:
            ce_flat = self.channels.get("NIFTY_CE") is None or self.channels["NIFTY_CE"].trade is None
            pe_flat = self.channels.get("NIFTY_PE") is None or self.channels["NIFTY_PE"].trade is None
            if ce_flat and pe_flat: self._resolve_nifty(new_atm)

    def _resolve_nifty(self, strike=0):
        info = self.resolver.resolve_atm_strikes(self.nifty_spot if strike == 0 else float(strike))
        if not info: self._status(f"Failed ATM {strike}"); return
        self._secid_map = {k: v for k, v in self._secid_map.items() if v not in ("NIFTY_CE","NIFTY_PE")}
        for key in ("NIFTY_CE","NIFTY_PE"):
            ch = self.channels.get(key)
            if ch: ch.renko.reset(); ch.ltp = 0.0
        ce_ch = self.channels.get("NIFTY_CE"); pe_ch = self.channels.get("NIFTY_PE")
        if ce_ch: ce_ch.sec_id = info["ce_sec_id"]; ce_ch.display_name = info["ce_display"]; self._secid_map[info["ce_sec_id"]] = "NIFTY_CE"
        if pe_ch: pe_ch.sec_id = info["pe_sec_id"]; pe_ch.display_name = info["pe_display"]; self._secid_map[info["pe_sec_id"]] = "NIFTY_PE"
        self.atm_info = info; self.current_atm_strike = info["strike"]
        self._status(f"ATM={info['strike']} | CE={info['ce_display']} | PE={info['pe_display']} | Exp={info['expiry']}")
        self._sub_nifty_opts()

    def _sub_nifty_opts(self):
        if not self.atm_info: return
        for key in ("NIFTY_CE","NIFTY_PE"):
            ch = self.channels.get(key)
            if ch and ch.sec_id: self._ws_sub(ch.ws_exchange, ch.sec_id)

    def _ws_sub(self, exchange, sec_id):
        if not self.ws or not self.ws_connected: return
        self.ws.send(json.dumps({"RequestCode": REQ_SUB_TICKER, "InstrumentCount": 1,
                                 "InstrumentList": [{"ExchangeSegment": exchange, "SecurityId": sec_id}]}))

    def _monitor_loop(self):
        while not self.stop_event.is_set():
            try:
                today = now_ist().date()
                if self._last_reset_date != today:
                    self._last_reset_date = today
                    for ch in self.channels.values(): ch.reset()
                    self.trades.clear(); self.total_trades_today = 0
                    log.info(f"Daily reset {today}")
            except Exception as e: log.error(f"Monitor: {e}")
            time.sleep(5)

# ═══════════════════════════════════════════════════════════════════
# GUI
# ═══════════════════════════════════════════════════════════════════
class RenkoCanvas(ctk.CTkCanvas):
    def __init__(self, master, side="CE", **kw):
        super().__init__(master, **kw); self.side = side; self.bricks = []; self.brick_width = 12
        self.brick_gap = 2; self._tlines = []
    def set_bricks(self, b): self.bricks = list(b); self._draw()
    def add_trade_line(self, p, lt): self._tlines.append((p, lt)); self._draw()
    def clear_trade_lines(self): self._tlines.clear(); self._draw()
    def _draw(self):
        self.delete("all"); w = self.winfo_width(); h = self.winfo_height()
        if w < 10 or h < 10 or not self.bricks:
            self.create_text(w//2, h//2, text=f"{self.side} - waiting...", fill="#888888", font=("Consolas",11)); return
        ap = []
        for b in self.bricks: ap.extend([b.open, b.close])
        for p, _ in self._tlines: ap.append(p)
        mn, mx = min(ap), max(ap); pr = mx - mn
        if pr < 0.01: pr = 1.0
        mt, mb, ml, mr = 25, 25, 55, 10; ch = h-mt-mb; cw = w-ml-mr
        def p2y(p): return mt + ch - ((p-mn)/pr*ch)
        step = self.brick_width + self.brick_gap; vis = self.bricks[-max(1,cw//step):]
        for i in range(6):
            p = mn + pr*i/5; y = p2y(p)
            self.create_line(ml,y,w-mr,y,fill="#333333",dash=(2,4))
            self.create_text(ml-5,y,text=f"{p:.1f}",anchor="e",fill="#888888",font=("Consolas",8))
        for i, brick in enumerate(vis):
            x1 = ml+i*step; x2 = x1+self.brick_width; yo, yc = p2y(brick.open), p2y(brick.close)
            c = "#00C853" if brick.is_green else "#FF1744"; o = "#00E676" if brick.is_green else "#FF5252"
            self.create_rectangle(x1,min(yo,yc),x2,max(yo,yc),fill=c,outline=o,width=1)
        lc = {"entry":"#FFAB00","tp":"#00E5FF","sl":"#FF1744"}
        for p, lt in self._tlines:
            y = p2y(p); cl = lc.get(lt,"#FFF")
            self.create_line(ml,y,w-mr,y,fill=cl,dash=(4,2),width=1)
            self.create_text(w-mr-5,y-8,text=f"{lt.upper()} {p:.2f}",anchor="e",fill=cl,font=("Consolas",8))
        last = vis[-1] if vis else None
        tt = f"{self.side} | Bricks: {len(self.bricks)}"
        if last: tt += f" | {'GREEN' if last.is_green else 'RED'} C={last.close:.2f}"
        self.create_text(w//2,10,text=tt,fill="#CCCCCC",font=("Consolas",10,"bold"))

class App(ctk.CTk):
    def __init__(self):
        super().__init__()
        ctk.set_appearance_mode("dark"); ctk.set_default_color_theme("blue")
        self.title(APP_TITLE); self.geometry("1400x900"); self.minsize(1100,750)
        self.engine = None; self._canvases = {}; self._build_ui()
        load_dotenv()
        ec = os.getenv("DHAN_CLIENT_ID",""); et = os.getenv("DHAN_ACCESS_TOKEN","")
        if ec: self.entry_client_id.insert(0, ec)
        if et: self.entry_token.insert(0, et)
        self.protocol("WM_DELETE_WINDOW", self._on_close)

    def _build_ui(self):
        r1 = ctk.CTkFrame(self, height=40); r1.pack(fill="x", padx=8, pady=(8,2))
        ctk.CTkLabel(r1, text="Client ID:").pack(side="left", padx=(8,2))
        self.entry_client_id = ctk.CTkEntry(r1, width=120, placeholder_text="Dhan Client ID"); self.entry_client_id.pack(side="left", padx=2)
        ctk.CTkLabel(r1, text="Token:").pack(side="left", padx=(8,2))
        self.entry_token = ctk.CTkEntry(r1, width=200, placeholder_text="Access Token", show="*"); self.entry_token.pack(side="left", padx=2)
        self.paper_var = ctk.BooleanVar(value=True)
        self.switch_paper = ctk.CTkSwitch(r1, text="Paper", variable=self.paper_var); self.switch_paper.pack(side="left", padx=(12,4))
        self.btn_start = ctk.CTkButton(r1, text="START", width=100, fg_color="#00C853", hover_color="#00E676", text_color="#000", command=self._on_start)
        self.btn_start.pack(side="right", padx=8)
        self.btn_stop = ctk.CTkButton(r1, text="STOP", width=80, fg_color="#FF1744", hover_color="#FF5252", text_color="#FFF", command=self._on_stop, state="disabled")
        self.btn_stop.pack(side="right", padx=4)

        r2 = ctk.CTkFrame(self, height=40); r2.pack(fill="x", padx=8, pady=2)
        ctk.CTkLabel(r2, text="Brick:").pack(side="left", padx=(8,2))
        self.entry_brick = ctk.CTkEntry(r2, width=50); self.entry_brick.insert(0,"1"); self.entry_brick.pack(side="left", padx=2)
        ctk.CTkLabel(r2, text="TP:").pack(side="left", padx=(10,2))
        self.entry_tp = ctk.CTkEntry(r2, width=40); self.entry_tp.insert(0,"8"); self.entry_tp.pack(side="left", padx=2)
        ctk.CTkLabel(r2, text="SL:").pack(side="left", padx=(6,2))
        self.entry_sl = ctk.CTkEntry(r2, width=40); self.entry_sl.insert(0,"2"); self.entry_sl.pack(side="left", padx=2)
        ctk.CTkLabel(r2, text="Max Trades:").pack(side="left", padx=(10,2))
        self.entry_max = ctk.CTkEntry(r2, width=40); self.entry_max.insert(0,"10"); self.entry_max.pack(side="left", padx=2)
        ctk.CTkLabel(r2, text="  |  Instruments:", font=("Consolas",11)).pack(side="left", padx=(14,4))
        self.chk_vars = {}
        for k, idef in INSTRUMENT_DEFS.items():
            v = ctk.BooleanVar(value=True); self.chk_vars[k] = v
            ctk.CTkCheckBox(r2, text=idef["label"], variable=v).pack(side="left", padx=4)
        ctk.CTkLabel(r2, text="  |  Lots:", font=("Consolas",11)).pack(side="left", padx=(10,4))
        self.lot_entries = {}
        for k in ("NIFTY_CE","GOLDTEN","SILVERM"):
            lbl = "NIFTY" if k=="NIFTY_CE" else INSTRUMENT_DEFS[k]["label"]
            ctk.CTkLabel(r2, text=f"{lbl}:").pack(side="left", padx=(4,1))
            e = ctk.CTkEntry(r2, width=40); e.insert(0, str(INSTRUMENT_DEFS[k]["default_lot"])); e.pack(side="left", padx=1)
            self.lot_entries[k] = e

        ib = ctk.CTkFrame(self, height=35); ib.pack(fill="x", padx=8, pady=2)
        self.lbl_spot = ctk.CTkLabel(ib, text="NIFTY: --", font=("Consolas",13,"bold")); self.lbl_spot.pack(side="left", padx=10)
        self.lbl_atm = ctk.CTkLabel(ib, text="ATM: --", font=("Consolas",12)); self.lbl_atm.pack(side="left", padx=10)
        self.lbl_gt = ctk.CTkLabel(ib, text="GOLDTEN: --", font=("Consolas",12), text_color="#FFD700"); self.lbl_gt.pack(side="left", padx=10)
        self.lbl_sm = ctk.CTkLabel(ib, text="SILVERM: --", font=("Consolas",12), text_color="#C0C0C0"); self.lbl_sm.pack(side="left", padx=10)
        self.lbl_pnl = ctk.CTkLabel(ib, text="Day P&L: Rs 0.00", font=("Consolas",13,"bold")); self.lbl_pnl.pack(side="right", padx=10)
        self.lbl_trades = ctk.CTkLabel(ib, text="Trades: 0/10", font=("Consolas",11)); self.lbl_trades.pack(side="right", padx=10)

        cf = ctk.CTkFrame(self); cf.pack(fill="both", expand=True, padx=8, pady=4)
        colors = {"NIFTY_CE":"#1A1A2E","NIFTY_PE":"#1A1A2E","GOLDTEN":"#2E2A1A","SILVERM":"#1A2E2A"}
        for i, k in enumerate(INSTRUMENT_DEFS.keys()):
            cf.columnconfigure(i, weight=1)
            frm = ctk.CTkFrame(cf, border_width=1, border_color="#333333")
            frm.grid(row=0, column=i, sticky="nsew", padx=2)
            canvas = RenkoCanvas(frm, side=INSTRUMENT_DEFS[k]["label"], bg=colors.get(k,"#1A1A2E"), highlightthickness=0)
            canvas.pack(fill="both", expand=True)
            self._canvases[k] = canvas
        cf.rowconfigure(0, weight=1)

        bot = ctk.CTkFrame(self, height=180); bot.pack(fill="x", padx=8, pady=(4,8))
        lf = ctk.CTkFrame(bot); lf.pack(side="left", fill="both", expand=True, padx=(0,4))
        ctk.CTkLabel(lf, text="Trade Log", font=("Consolas",11,"bold")).pack(anchor="w", padx=5, pady=2)
        self.trade_log = ctk.CTkTextbox(lf, height=130, font=("Consolas",10), state="disabled"); self.trade_log.pack(fill="both", expand=True, padx=2, pady=2)
        sf = ctk.CTkFrame(bot); sf.pack(side="right", fill="both", expand=True, padx=(4,0))
        ctk.CTkLabel(sf, text="Status Log", font=("Consolas",11,"bold")).pack(anchor="w", padx=5, pady=2)
        self.status_log = ctk.CTkTextbox(sf, height=130, font=("Consolas",10), state="disabled"); self.status_log.pack(fill="both", expand=True, padx=2, pady=2)
        self._refresh_timer()

    def _on_start(self):
        cid = self.entry_client_id.get().strip(); tok = self.entry_token.get().strip()
        if not cid or not tok: self._append_status("ERROR: Enter credentials"); return
        try:
            brick = float(self.entry_brick.get().strip()); tp = float(self.entry_tp.get().strip())
            sl = float(self.entry_sl.get().strip()); max_t = int(self.entry_max.get().strip())
        except ValueError: self._append_status("ERROR: Invalid numeric input"); return
        active = [k for k, v in self.chk_vars.items() if v.get()]
        if not active: self._append_status("ERROR: Select at least one instrument"); return
        try:
            nl = int(self.lot_entries["NIFTY_CE"].get().strip())
            lot_sizes = {"NIFTY_CE": nl, "NIFTY_PE": nl,
                         "GOLDTEN": int(self.lot_entries["GOLDTEN"].get().strip()),
                         "SILVERM": int(self.lot_entries["SILVERM"].get().strip())}
        except ValueError: self._append_status("ERROR: Invalid lot size"); return
        paper = self.paper_var.get()
        self.engine = CoreEngine(cid, tok, brick, tp, sl, max_t, lot_sizes, active, paper)
        self.engine.on_brick_callback = self._on_brick
        self.engine.on_trade_callback = self._on_trade
        self.engine.on_status_callback = lambda m: self.after(0, self._append_status, m)
        self.engine.start()
        self.btn_start.configure(state="disabled"); self.btn_stop.configure(state="normal")
        for w in [self.entry_client_id, self.entry_token, self.entry_brick, self.entry_tp,
                  self.entry_sl, self.entry_max, self.switch_paper] + list(self.lot_entries.values()):
            w.configure(state="disabled")
        self._append_status(f"Started | {'PAPER' if paper else 'LIVE'} | Brick={brick} TP={tp} SL={sl} | {', '.join(active)}")

    def _on_stop(self):
        if self.engine: self.engine.stop(); self.engine = None
        self.btn_start.configure(state="normal"); self.btn_stop.configure(state="disabled")
        for w in [self.entry_client_id, self.entry_token, self.entry_brick, self.entry_tp,
                  self.entry_sl, self.entry_max, self.switch_paper] + list(self.lot_entries.values()):
            w.configure(state="normal")
        self._append_status("Stopped")

    def _on_close(self):
        if self.engine: self.engine.stop()
        self.destroy()

    def _on_brick(self, ck, brick):
        try: self.after(0, self._update_chart, ck)
        except: pass
    def _on_trade(self, trade, action):
        try: self.after(0, self._log_trade, trade, action)
        except: pass

    def _update_chart(self, ck):
        if not self.engine: return
        ch = self.engine.channels.get(ck); canvas = self._canvases.get(ck)
        if not ch or not canvas: return
        canvas.set_bricks(list(ch.renko.bricks))
        canvas.clear_trade_lines()
        if ch.trade:
            canvas.add_trade_line(ch.trade.entry_price, "entry")
            canvas.add_trade_line(ch.trade.target, "tp")
            canvas.add_trade_line(ch.trade.stoploss, "sl")

    def _log_trade(self, trade, action):
        if action == "ENTRY":
            msg = f"[{ist_hhmm(trade.entry_time)}] {trade.side} BUY | {trade.display_name} | Entry={trade.entry_price:.2f} | TP={trade.target:.2f} SL={trade.stoploss:.2f}"
        else:
            msg = f"[{ist_hhmm(trade.exit_time)}] {trade.side} {action} | {trade.display_name} | Exit={trade.exit_price:.2f} | PnL=Rs {trade.pnl:+.2f}"
        self.trade_log.configure(state="normal"); self.trade_log.insert("end", msg+"\n"); self.trade_log.see("end"); self.trade_log.configure(state="disabled")
        if self.engine:
            for k in self.engine.channels: self._update_chart(k)

    def _append_status(self, msg):
        self.status_log.configure(state="normal"); self.status_log.insert("end", f"[{ist_hhmm(now_ist())}] {msg}\n")
        self.status_log.see("end"); self.status_log.configure(state="disabled")

    def _refresh_timer(self):
        try: self._refresh_info()
        except: pass
        self.after(500, self._refresh_timer)

    def _refresh_info(self):
        if not self.engine: return
        if self.engine.nifty_spot > 0: self.lbl_spot.configure(text=f"NIFTY: {self.engine.nifty_spot:.2f}")
        if self.engine.atm_info: self.lbl_atm.configure(text=f"ATM: {self.engine.current_atm_strike} | Exp: {self.engine.atm_info['expiry']}")
        gt = self.engine.channels.get("GOLDTEN")
        if gt and gt.ltp > 0: self.lbl_gt.configure(text=f"GOLDTEN: {gt.ltp:.2f}")
        sm = self.engine.channels.get("SILVERM")
        if sm and sm.ltp > 0: self.lbl_sm.configure(text=f"SILVERM: {sm.ltp:.2f}")
        self.lbl_trades.configure(text=f"Trades: {self.engine.total_trades_today}/{self.engine.max_trades}")
        pnl = self.engine.get_day_pnl(); col = "#00E676" if pnl >= 0 else "#FF5252"
        self.lbl_pnl.configure(text=f"Day P&L: Rs {pnl:+.2f}", text_color=col)
        for k in self.engine.channels: self._update_chart(k)

def main():
    app = App(); app.mainloop()

if __name__ == "__main__":
    main()
