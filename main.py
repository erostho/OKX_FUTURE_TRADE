"""
Phiên bản nâng cấp chuyên sâu cho trader giữ lệnh 1–6 giờ.

Tính năng chính:
✅ TP/SL thông minh theo swing
✅ Kiểm tra RR ≥ 1.2 và SL không quá hẹp
✅ Volume spike xác nhận tín hiệu top30
✅ Xác nhận đa chiều RSI/EMA/MACD/ADX/Bollinger
✅ Loại bỏ tín hiệu sideway (choppy filter)
✅ Mô hình giá: Flag, Wedge, Head & Shoulders (dạng đơn giản)
✅ Entry hợp lệ nếu nằm trong vùng Fibonacci retracement (0.5–0.618)
"""
# ==== STRUCTURED LOG HELPERS ====
import requests
import pandas as pd
import numpy as np
import time
import datetime as dt
import json
import os
import logging
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pytz import timezone
import pytz
from datetime import datetime, timedelta
from datetime import timezone
from datetime import datetime, timezone
from datetime import datetime, timezone, timedelta
from contextlib import contextmanager
import sys, io, logging


DEBUG_BACKTEST = True
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # luôn bật DEBUG/INFO
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
LOG_NAME = "SIGNAL"
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s")
logger = logging.getLogger(LOG_NAME)

def log_pass(stage: str, symbol: str, **kv):
    extras = " ".join(f"{k}={v}" for k,v in kv.items())
    logger.info(f"PASS|{stage}|{symbol}|{extras}")

def log_drop(stage: str, symbol: str, reason: str, **kv):
    extras = " ".join(f"{k}={v}" for k,v in kv.items())
    logger.info(f"DROP|{stage}|{symbol}|{reason} {extras}")

def drop_return():
    return None, None, None, None, False, None


# ========== CẤU HÌNH ==========
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
SHEET_CSV_URL = os.getenv("SHEET_CSV_URL")  # Đặt lại biến nếu chưa có
# Cấu hình scope
scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# Đọc file JSON credentials đã upload lên Render (tên phải là service_account.json)
creds = ServiceAccountCredentials.from_json_keyfile_name('/etc/secrets/service_account.json', scope)

# Authorize gspread
client = gspread.authorize(creds)

# Kết nối đến file Google Sheet
# ✅ Thêm 2 dòng debug này vào ngay sau khi tách sheet_id
try:
    sheet_id = SHEET_CSV_URL.split("/d/")[1].split("/")[0]
    print(f"[DEBUG] sheet_id = {sheet_id}")
    sheet = client.open_by_key(sheet_id).worksheet("DATA_FUTURE")
    time.sleep(1)
    print("[DEBUG] Đã mở sheet thành công.")
except Exception as e:
    print(f"[ERROR] Không mở được sheet: {e}")
    raise

# ========== THAM SỐ KỸ THUẬT ==========
TP_MULTIPLIER = 1.5
SL_MULTIPLIER = 1.0
ADX_THRESHOLD = 12
K_ATR_SL = 0.6   # tối thiểu SL = max(SL_MIN_PCT, 0.6*ATR/entry)
K_ATR_TP = 1.2   # tối thiểu TP = max(TP_MIN_{RELAX|STRICT}, 1.2*ATR/entry)
COINS_LIMIT = 300  # Số coin phân tích mỗi lượt
# ===== CONFIG =====
SL_MIN_PCT   = 0.006    # SL tối thiểu 0.6%
TP_MIN_RELAX = 0.02     # TP tối thiểu 2% (RELAX)
TP_MIN_STRICT= 0.05     # TP tối thiểu 5% (STRICT)
TOPN_PER_BATCH = 10   # tuỳ bạn, 5/10/15...
SL_MIN_PCT_BASE = SL_MIN_PCT
TP_MIN_RELAX_BASE = TP_MIN_RELAX
TP_MIN_STRICT_BASE = TP_MIN_STRICT
WICK_RATIO_LIMIT = 0.8  # Ngưỡng % chiều dài nến râu, TB 0.5

# ========================== NÂNG CẤP CHUYÊN SÂU ==========================
# ====== PRESET & HELPERS ======

STRICT_CFG = {
    "VOLUME_PERCENTILE": 80,   # top 20%
    "ADX_MIN_15M": 22,
    "BBW_MIN": 0.013,
    "RR_MIN": 1.5, # RR tối thiểu
    "NEWS_BLACKOUT_MIN": 60,   # phút
    "ATR_CLEARANCE_MIN": 0.8,  # >= 0.8 ATR
    "USE_VWAP": True,
    "RELAX_EXCEPT": False,
    "TAG": "STRICT",
    "RSI_LONG_MIN": 55,
    "RSI_SHORT_MAX": 45,
    "RSI_1H_LONG_MIN": 50,
    "RSI_1H_SHORT_MAX": 50,
    "MACD_DIFF_LONG_MIN": 0.05,
    "MACD_DIFF_SHORT_MIN": 0.001,
    "ALLOW_1H_NEUTRAL": False,
    "REQUIRE_RETEST": True,
    "REQ_EMA200_MULTI": True,
}
RELAX_CFG = {
    "VOLUME_PERCENTILE": 40,   # top 60%
    "ADX_MIN_15M": 18,
    "BBW_MIN": 0.048,
    "RR_MIN": 1.0,
    "NEWS_BLACKOUT_MIN": 0, # phút
    "ATR_CLEARANCE_MIN": 0.6, # >= 0.7ART
    "USE_VWAP": True,
    "RELAX_EXCEPT": True,      # cho phép ngoại lệ khi breakout + volume
    "TAG": "RELAX",
    "RSI_LONG_MIN": 45,
    "RSI_SHORT_MAX": 55,    # tăng là lỏng
    "RSI_1H_LONG_MIN": 45,
    "RSI_1H_SHORT_MAX": 55,    # tăng là lỏng
    "MACD_DIFF_LONG_MIN": 0.0003,
    "MACD_DIFF_SHORT_MIN": -0.0003,
    "ALLOW_1H_NEUTRAL": True,
    "REQUIRE_RETEST": False,
    "REQ_EMA200_MULTI": False,
    "SR_NEAR_K_ATR": 0.8,   # hệ số * ATR cho độ gần (từ 0.6 → 1.0 hoặc 1.2 để thoáng)
    "SR_NEAR_PCT":   1.2, # 1.2% khoảng cách tuyệt đối (tuỳ)
    "EARLY_ALERT": True,            # bật báo sớm
    "EARLY_USE_CURRENT_BAR": True,  # dùng nến đang chạy (khỏi chờ đóng)
    "EARLY_RATING_PENALTY": 1       # trừ 1 sao nếu là tín hiệu sớm
}

# log 1 dòng/coin/mode + tắt log tạm thời

_logged_once = {}
def log_once(mode, symbol, msg, level="info"):
    key = (mode, symbol)
    if _logged_once.get(key): return
    _logged_once[key] = True
    getattr(logging, level)(msg)

def reset_log_once_for_mode(mode, symbols):
    for s in symbols:
        _logged_once.pop((mode, s), None)

@contextmanager
def mute_logs():
    prev = logging.getLogger().level
    logging.getLogger().setLevel(logging.WARNING)
    try: yield
    finally: logging.getLogger().setLevel(prev)

@contextmanager
def suppress_output():
    _stdout, _stderr = sys.stdout, sys.stderr
    sys.stdout, sys.stderr = io.StringIO(), io.StringIO()
    try: yield
    finally: sys.stdout, sys.stderr = _stdout, _stderr

# ====== News blackout ======
def load_news_events():
    import json, os, requests
    url = os.getenv("NEWS_JSON_URL","").strip()
    if url:
        try:
            r = requests.get(url, timeout=8)
            if r.ok: return r.json()
        except: pass
    path = os.getenv("NEWS_JSON_FILE","news_events.json")
    try:
        with open(path,"r") as f: return json.load(f)
    except: return []


def in_news_blackout(window_min: int):
    now = dt.datetime.now(timezone.utc)
    for e in load_news_events():
        t = e.get("time") or e.get("time_utc")
        if not t: continue
        try:
            if t.endswith("Z"): tt = datetime.fromisoformat(t.replace("Z","+00:00"))
            else: tt = datetime.fromisoformat(t)
        except: continue
        if abs((tt - now).total_seconds()) <= window_min*60:
            return True
    return False

# ====== VWAP + ATR clearance ======

def _atr(df, n=14):
    tr = np.maximum.reduce([
        (df["high"] - df["low"]).abs(),
        (df["high"] - df["close"].shift()).abs(),
        (df["low"] - df["close"].shift()).abs()
    ])
    tr = pd.Series(tr)  # ✅ Ép về Series để dùng .rolling()
    return tr.rolling(n).mean()

def anchored_vwap(df, anchor_idx):
    sub = df.iloc[anchor_idx:]
    tp = (sub["high"] + sub["low"] + sub["close"]) / 3.0
    pv = (tp * sub["volume"]).cumsum()
    vv = sub["volume"].cumsum().replace(0, np.nan)
    vwap = pv / vv
    return float(vwap.iloc[-1])

def pick_anchor_index(df, side):
    win = df.iloc[-50:-5]
    return (win["low"].idxmin() if side=="LONG" else win["high"].idxmax())
def atr_clearance(df, side, mult):
    atr = float(_atr(df).iloc[-1])
    last = df.iloc[-1]
    z = df.iloc[-20:-1]
    if side=="LONG":
        obstacle = z["high"].max(); dist = obstacle - last["close"]
    else:
        obstacle = z["low"].min();  dist = last["close"] - obstacle
    if atr==0 or np.isnan(atr): return 0.0, False
    return dist/atr, (dist/atr) >= mult

def clean_missing_data(df, required_cols=["close", "high", "low", "volume"], max_missing=2):
    """Nếu thiếu 1-2 giá trị, loại bỏ. Nếu thiếu nhiều hơn, trả về None"""
    missing = df[required_cols].isnull().sum().sum()
    if missing > max_missing:
        return None
    return df.dropna(subset=required_cols)

def is_volume_spike(df):
    try:
        volumes = df["volume"].iloc[-20:]

        if len(volumes) < 10:
            logging.debug(f"[DEBUG][Volume FAIL] Không đủ dữ liệu volume: chỉ có {len(volumes)} nến")
            return False

        v_now = volumes.iloc[-1]
        threshold = np.percentile(volumes[:-1], 60) # TOP 40%

        if np.isnan(v_now) or np.isnan(threshold):
            logging.debug(f"[DEBUG][Volume FAIL] Dữ liệu volume bị NaN - v_now={v_now}, threshold={threshold}")
            return False

        logging.debug(f"[DEBUG][Volume Check] Volume hiện tại = {v_now:.0f}, Threshold 60% = {threshold:.0f}")

        if v_now <= threshold:
            logging.debug(f"[DEBUG][Volume FAIL] Volume chưa đủ spike")
            return False

        return True

    except Exception as e:
        logging.debug(f"[DEBUG][Volume FAIL] Lỗi khi kiểm tra volume: {e}")
        return False

def detect_breakout_pullback(df):
    df["ema20"] = df["close"].ewm(span=20).mean()
    recent_high = df["high"].iloc[-30:-10].max()
    ema = df["ema20"].iloc[-1]
    price = df["close"].iloc[-1]
    breakout = price > recent_high
    pullback = price < recent_high and price > ema
    return breakout and pullback

def find_support_resistance(df, window=30):
    highs = df["high"].iloc[-window:]
    lows = df["low"].iloc[-window:]
    return lows.min(), highs.max()

def rate_signal_strength(entry, sl, tp, short_trend, mid_trend):
    strength = 1
    if abs(tp - entry) / entry > 0.05:
        strength += 1
    if abs(entry - sl) / entry > 0.05:
        strength += 1
    if short_trend == mid_trend:
        strength += 1
    return "⭐️" * min(strength, 5)

def _bt_row_key(coin: str, side: str, when_vn: str, mode: str) -> str:
    # Khóa duy nhất cho 1 tín hiệu backtest
    return f"{coin.strip()}|{side.strip()}|{when_vn.strip()}|{mode.strip()}"

def load_existing_backtest_keys() -> set:
    """
    Đọc sheet BACKTEST_RESULT và trả về set các key đã tồn tại
    Cột: 0=Coin, 1=Side, 7=Ngày(VN), 8=Mode
    """
    try:
        rows = read_sheet_values("BACKTEST_RESULT") or []   # dùng hàm đọc sheet sẵn có của bạn
    except Exception:
        rows = []
    keys = set()
    for r in rows[0:]:  # bỏ header
        if len(r) >= 9:
            coin  = str(r[0])
            side  = str(r[1])
            when_ = str(r[7])
            mode  = str(r[8])
            keys.add(_bt_row_key(coin, side, when_, mode))
    return keys

def ts_to_str(ms_or_s):
    try:
        tz = pytz.timezone("Asia/Ho_Chi_Minh")
        if ms_or_s > 10**12:  # nano
            ms_or_s = ms_or_s / 10**6
        if ms_or_s > 10**10:  # mili
            dt_obj = dt.datetime.utcfromtimestamp(ms_or_s/1000.0)
        else:
            dt_obj = dt.datetime.utcfromtimestamp(ms_or_s)
        return tz.fromutc(dt_obj).strftime("%d/%m/%Y %H:%M")
    except Exception:
        return str(ms_or_s)
        
def _pivots(series: pd.Series, lookback=30, win=3):
    """Tìm 2 pivot highs & 2 pivot lows gần nhất trong lookback nến (win = cửa sổ local-extrema)."""
    s = series.tail(lookback).reset_index(drop=True)
    n = len(s)
    if n < win*2+3:
        return None, None, None, None  # thiếu dữ liệu
    # pivot high: s[i] là max trong [i-win, i+win]
    ph_idx = []
    pl_idx = []
    for i in range(win, n-win):
        seg = s[i-win:i+win+1]
        if s[i] == seg.max(): ph_idx.append(i)
        if s[i] == seg.min(): pl_idx.append(i)
    # lấy 2 cái gần nhất
    ph_idx = ph_idx[-2:] if len(ph_idx) >= 2 else (ph_idx if ph_idx else [])
    pl_idx = pl_idx[-2:] if len(pl_idx) >= 2 else (pl_idx if pl_idx else [])
    def _vals(idx_list):
        if len(idx_list) >= 2:
            return (idx_list[-2], s.iloc[idx_list[-2]]), (idx_list[-1], s.iloc[idx_list[-1]])
        return (None, None), (None, None)
    (i1h,v1h),(i2h,v2h) = _vals(ph_idx)
    (i1l,v1l),(i2l,v2l) = _vals(pl_idx)
    return (i1h, v1h, i2h, v2h, i1l, v1l, i2l, v2l)

def is_bear_div(price: pd.Series, osc: pd.Series, lb=30, win=3) -> bool:
    """Bearish divergence: price HH nhưng oscillator LH (veto LONG)."""
    if price is None or osc is None or len(price) < lb+5 or len(osc) < lb+5:
        return False
    i1h, p1h, i2h, p2h, *_ = _pivots(price, lb, win)
    _,  o1h, _,  o2h, *_ = _pivots(osc,   lb, win)
    if None in (i1h, p1h, i2h, p2h, o1h, o2h):
        return False
    # i1h < i2h đảm bảo trật tự thời gian
    return (i1h < i2h) and (p2h > p1h) and (o2h < o1h)

def is_bull_div(price: pd.Series, osc: pd.Series, lb=30, win=3) -> bool:
    """Bullish divergence: price LL nhưng oscillator HL (veto SHORT)."""
    if price is None or osc is None or len(price) < lb+5 or len(osc) < lb+5:
        return False
    *_, i1l, p1l, i2l, p2l = _pivots(price, lb, win)
    *_, o1l, o1v, o2l, o2v = _pivots(osc,   lb, win)
    if None in (i1l, p1l, i2l, p2l, o1l, o1v, o2l, o2v):
        return False
    return (i1l < i2l) and (p2l < p1l) and (o2v > o1v)

def _safe_dt(ts):
    """ts có thể là ms hoặc s; trả về datetime VN hoặc None."""
    try:
        import pandas as pd, pytz
        if ts is None: 
            return None
        ts = float(ts)
        if not (ts > 0):
            return None
        unit = 'ms' if ts > 1e12 else 's'  # ngưỡng thô: ms ~ 1e13, s ~ 1e9
        dt_utc = pd.to_datetime(ts, unit=unit, utc=True)
        return dt_utc.tz_convert('Asia/Ho_Chi_Minh')
    except Exception:
        return None
        
def fetch_ohlcv_okx(symbol: str, timeframe: str = "15m", limit: int = 1000):
    """
    Trả DataFrame cột: timestamp(ms), open, high, low, close, volume
    OKX trả data mới → cũ, timestamp là mili‑giây (string).
    """
    import requests, pandas as pd
    from datetime import datetime

    try:
        # Map alias timeframe về format OKX
        timeframe_map = {
            "1h": "1H", "4h": "4H", "1d": "1D",
            "15m": "15m", "5m": "5m", "1m": "1m"
        }
        tf_in = timeframe
        timeframe = timeframe_map.get(timeframe.lower(), timeframe)
        logging.debug(f"🕒 Timeframe input: {tf_in} => OKX dùng: {timeframe}")

        if timeframe not in ("1m","5m","15m","30m","1H","4H","1D"):
            logging.warning(f"⚠️ Timeframe không hợp lệ: {timeframe}")
            return None

        # OKX instId: BTC-USDT → BTC-USDT-SWAP (nếu thiếu -SWAP)
        inst_id = symbol.upper().replace("/", "-")
        if not inst_id.endswith("-SWAP"):
            inst_id += "-SWAP"

        url = "https://www.okx.com/api/v5/market/candles"
        params = {"instId": inst_id, "bar": timeframe, "limit": limit}
        logging.debug(f"🌐 Gửi request nến OKX: instId={inst_id}, bar={timeframe}, limit={limit}")
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        js = r.json()
        data = js.get("data", []) or []
        
        # Parse: [ts, o, h, l, c, vol]
        rows = []
        for it in data:
            ts = int(it[0]) if it and it[0] is not None else 0
            o, h, l, c = it[1], it[2], it[3], it[4]
            vol = it[5] if len(it) > 5 else None
            rows.append([ts, o, h, l, c, vol])
        
        df = pd.DataFrame(rows, columns=["timestamp","open","high","low","close","volume"])
        
        # Ép kiểu chuẩn
        df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce")  # ms hoặc s
        for col in ("open","high","low","close","volume"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        
        # Nếu timestamp đang là GIÂY thì đổi sang MILLISECOND
        if df["timestamp"].notna().any() and df["timestamp"].max() < 10**12:
            df["timestamp"] = df["timestamp"] * 1000.0
        
        # Loại rỗng + sort
        df = df.dropna(subset=["timestamp","open","high","low","close"]).copy()
        df = df.sort_values("timestamp").reset_index(drop=True)
        
        # (tùy) log phạm vi: chỉ dùng /1000 khi log, KHÔNG convert nếu NaN
        if len(df):
            t0 = int(df["timestamp"].iloc[0]); t1 = int(df["timestamp"].iloc[-1])
            from datetime import datetime, timezone
            logging.debug(f"[BT] RAW {symbol}: "
                          f"[{datetime.fromtimestamp(t0/1000, tz=timezone.utc)} -> "
                          f"{datetime.fromtimestamp(t1/1000, tz=timezone.utc)}] | rows={len(df)}")
        else:
            logging.debug(f"[BT] RAW {symbol}: dữ liệu trống.")
        
        return df

    except Exception as e:
        logging.warning(f"[OKX] fetch_ohlcv_okx lỗi với {symbol}: {e}")
        return None

def _ensure_ohlc(df):
    need = {"open","high","low","close"}
    if not need.issubset(set(df.columns)): 
        raise ValueError("Thiếu cột OHLC")
    return df.dropna()

def _dmi_adx(df, n=14):
    """Trả về Series: +DI, -DI, ADX (0..100). Nếu đã có cột adx/plus_di/minus_di thì dùng luôn."""
    d = _ensure_ohlc(df).copy()
    if {"adx","plus_di","minus_di"}.issubset(d.columns):
        return d["plus_di"], d["minus_di"], d["adx"]

    high, low, close = d["high"], d["low"], d["close"]
    up_move   = high.diff()
    down_move = -low.diff()

    plus_dm  = np.where((up_move > down_move) & (up_move > 0),  up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

    tr1 = (high - low)
    tr2 = (high - close.shift()).abs()
    tr3 = (low  - close.shift()).abs()
    tr  = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    atr = tr.ewm(span=n, adjust=False).mean()
    plus_di  = (pd.Series(plus_dm, index=d.index).ewm(span=n, adjust=False).mean()  / atr) * 100
    minus_di = (pd.Series(minus_dm, index=d.index).ewm(span=n, adjust=False).mean() / atr) * 100
    dx = ( (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan) ) * 100
    adx = dx.ewm(span=n, adjust=False).mean()

    return plus_di.fillna(0), minus_di.fillna(0), adx.fillna(0)

def _ema(series, span):
    return series.ewm(span=span, adjust=False).mean()

def trend_ema_adx(df, ema_period=50, adx_th=20):
    """
    Tăng:  close > EMA và EMA dốc lên, DI+ > DI-, ADX > ngưỡng
    Giảm:  close < EMA và EMA dốc xuống, DI- > DI+, ADX > ngưỡng
    Sideway: còn lại / ADX yếu
    """
    d = _ensure_ohlc(df).copy()
    ema = _ema(d["close"], ema_period)
    plus_di, minus_di, adx = _dmi_adx(d, n=14)

    close_now = float(d["close"].iloc[-1])
    ema_now   = float(ema.iloc[-1])
    ema_prev  = float(ema.iloc[-2]) if len(ema) > 1 else ema_now
    adx_now   = float(adx.iloc[-1])
    pdi_now   = float(plus_di.iloc[-1])
    mdi_now   = float(minus_di.iloc[-1])

    ema_up   = ema_now > ema_prev
    ema_down = ema_now < ema_prev

    if adx_now > adx_th and close_now > ema_now and ema_up and pdi_now > mdi_now:
        return "Tăng"
    if adx_now > adx_th and close_now < ema_now and ema_down and mdi_now > pdi_now:
        return "Giảm"
    return "Sideway"

def calculate_adx(df, period=14):
    high = pd.to_numeric(df["high"], errors="coerce")
    low = pd.to_numeric(df["low"], errors="coerce")
    close = pd.to_numeric(df["close"], errors="coerce")

    plus_dm = high.diff()
    minus_dm = low.diff()
    plus_dm[plus_dm < 0] = 0
    minus_dm[minus_dm > 0] = 0
    minus_dm = abs(minus_dm)

    tr1 = high - low
    tr2 = abs(high - close.shift())
    tr3 = abs(low - close.shift())
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    atr = tr.rolling(window=period).mean()
    plus_di = 100 * (plus_dm.rolling(window=period).mean() / atr)
    minus_di = 100 * (minus_dm.rolling(window=period).mean() / atr)
    dx = (abs(plus_di - minus_di) / (plus_di + minus_di)) * 100
    adx = dx.rolling(window=period).mean()

    df["adx"] = adx
    return df
    
def calculate_indicators(df):
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["ema20"] = df["close"].ewm(span=20).mean()
    df["ema50"] = df["close"].ewm(span=50).mean()
    df["ema100"] = df["close"].ewm(span=100).mean()

    # RSI
    delta = df["close"].diff()
    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)
    avg_gain = pd.Series(gain).rolling(window=14).mean()
    avg_loss = pd.Series(loss).rolling(window=14).mean()
    rs = avg_gain / avg_loss
    df["rsi"] = 100 - (100 / (1 + rs))

    # MACD
    exp1 = df["close"].ewm(span=12).mean()
    exp2 = df["close"].ewm(span=26).mean()
    df["macd"] = exp1 - exp2
    df["macd_signal"] = df["macd"].ewm(span=9).mean()

    df = calculate_adx(df)
    return df
    
def clean_missing_data(df, required_cols=["close", "high", "low", "volume"], max_missing=2):
    missing = df[required_cols].isnull().sum().sum()
    if missing > max_missing:
        return None
    return df.dropna(subset=required_cols)

# ==== EMA200 regime filter (1H/4H) ====
def _get_ec():
    return _ensure_cols if '_ensure_cols' in globals() else ensure_cols
    
def _ema200_up(df, span=200, slope_look=5):
    d = _get_ec()(df.copy()).dropna()
    if "ema200" not in d.columns:
        d["ema200"] = d["close"].ewm(span=span, adjust=False).mean()
    e_now  = float(d["ema200"].iloc[-1])
    e_prev = float(d["ema200"].iloc[-slope_look])
    px_now = float(d["close"].iloc[-1])
    return (px_now > e_now) and (e_now >= e_prev)
    
def _ema200_down(df, span=200, slope_look=5):
    d = _get_ec()(df.copy()).dropna()
    if "ema200" not in d.columns:
        d["ema200"] = d["close"].ewm(span=span, adjust=False).mean()
    e_now  = float(d["ema200"].iloc[-1])
    e_prev = float(d["ema200"].iloc[-slope_look])
    px_now = float(d["close"].iloc[-1])
    return (px_now < e_now) and (e_now <= e_prev)

def _clip01(x): return max(0.0, min(1.0, x))

def score_signal(rr, adx, clv, dist_ema, volp, atr_pct):
    # Chuẩn hóa thô, bạn có thể tinh chỉnh:
    s_rr   = _clip01((rr-1.0)/1.5)          # RR 1→2.5
    s_adx  = _clip01((adx-15)/20)           # ADX 15→35
    s_clv  = clv if 0<=clv<=1 else 0.5      # đã tính CLV 0..1
    s_dist = _clip01(1 - min(dist_ema / (2*atr_pct + 1e-9), 1))  # càng gần EMA/VWAP càng tốt
    s_vol  = _clip01((volp-50)/40)          # percentile 50→90
    return 0.30*s_rr + 0.25*s_adx + 0.20*s_clv + 0.15*s_dist + 0.10*s_vol

# ==== DEBUG BACKTEST ====

# DETECT_SIGNAL
def detect_signal(df_15m: pd.DataFrame,
                  df_1h: pd.DataFrame,
                  symbol: str,
                  cfg: dict = None,
                  silent: bool = True,
                  context: str = "LIVE",
                  return_reason: bool = False):
    fail = []
    sig_score = 0.0
    """
    Return:
      mặc định: (side, entry, sl, tp, ok)
      nếu return_reason=True: (side, entry, sl, tp, ok, reason_str)
    """
    cfg = cfg or STRICT_CFG
    tg_candidates = []
    # ---- tham số từ preset (có default để không vỡ) ----
    vol_p        = cfg.get("VOLUME_PERCENTILE", 70)
    adx_min      = cfg.get("ADX_MIN_15M", 20)
    bbw_min      = cfg.get("BBW_MIN", 0.010)
    rr_min       = cfg.get("RR_MIN", 1.5)
    news_win     = cfg.get("NEWS_BLACKOUT_MIN", 60)
    atr_need     = cfg.get("ATR_CLEARANCE_MIN", 0.8)
    use_vwap     = cfg.get("USE_VWAP", True)
    allow_adx_ex = cfg.get("RELAX_EXCEPT", False)

    # nới/chặt theo preset (nếu đã thêm)
    rsi_long_min   = cfg.get("RSI_LONG_MIN", 55)
    rsi_short_max  = cfg.get("RSI_SHORT_MAX", 45)
    rsi1h_long_min = cfg.get("RSI_1H_LONG_MIN", 50)
    rsi1h_short_max= cfg.get("RSI_1H_SHORT_MAX", 50)
    macd_long_min  = cfg.get("MACD_DIFF_LONG_MIN", 0.05)
    macd_short_min = cfg.get("MACD_DIFF_SHORT_MIN", 0.001)
    allow_1h_neu   = cfg.get("ALLOW_1H_NEUTRAL", False)
    body_atr_k     = cfg.get("BREAKOUT_BODY_ATR", 0.6)
    sr_near_pct    = cfg.get("SR_NEAR_PCT", 0.04)  # 4%

    fail = []  # gom lý do rớt

    def _ret(side, entry, sl, tp, ok):
        if return_reason:
            reason = ", ".join(fail) if fail else "PASS"
            # Add EARLY tag for RELAX mode using current bar so downstream can show badge
            try:
                if cfg.get("TAG", "RELAX") == "RELAX" and cfg.get("EARLY_ALERT", False) and cfg.get("EARLY_USE_CURRENT_BAR", False):
                    reason = "[EARLY] " + str(reason)
            except Exception:
                pass
            return side, entry, sl, tp, ok, reason, sig_score
        else:
            return side, entry, sl, tp, ok

    # ---------- dữ liệu & chỉ báo cơ bản ----------
    if df_15m is None or len(df_15m) < 60:
        fail.append("DATA: thiếu 15m")
        return _ret(None, None, None, None, False)
    df = df_15m.copy()

    # đảm bảo các cột cần thiết (không phá code cũ)
    def _ensure_cols(dfx):
        if "ema20" not in dfx.columns: dfx["ema20"] = dfx["close"].ewm(span=20).mean()
        if "ema50" not in dfx.columns: dfx["ema50"] = dfx["close"].ewm(span=50).mean()
        if "rsi" not in dfx.columns:
            delta = dfx["close"].diff()
            gain = delta.clip(lower=0).rolling(14).mean()
            loss = (-delta.clip(upper=0)).rolling(14).mean().replace(0, np.nan)
            rs = gain / loss
            dfx["rsi"] = 100 - (100/(1+rs))
        if "macd" not in dfx.columns or "macd_signal" not in dfx.columns:
            ema12 = dfx["close"].ewm(span=12).mean()
            ema26 = dfx["close"].ewm(span=26).mean()
            dfx["macd"] = ema12 - ema26
            dfx["macd_signal"] = dfx["macd"].ewm(span=9).mean()
        if "bb_width" not in dfx.columns:
            ma20 = dfx["close"].rolling(20).mean()
            std20= dfx["close"].rolling(20).std()
            dfx["bb_upper"] = ma20 + 2*std20
            dfx["bb_lower"] = ma20 - 2*std20
            dfx["bb_width"] = (dfx["bb_upper"] - dfx["bb_lower"]) / dfx["close"]
        if "adx" not in dfx.columns:
            dfx["adx"] = 25.0  # fallback nhẹ nếu thiếu
        return dfx

    df = _ensure_cols(df).dropna()
    early   = bool(cfg.get("EARLY_ALERT", False))
    use_cur = bool(cfg.get("EARLY_USE_CURRENT_BAR", False))
    # --- EARLY: lấy giá trị nến nào & nới tiêu chí ---
    # vị trí nến dùng để lọc ( -1: nến đang chạy, -2: nến đã đóng )
    idx_use = -1 if (early and use_cur) else -2
    dfx = df_1h.copy()
    dfx = _ensure_cols(dfx).dropna()
    side = None
    entry = sl = tp = None
    oke = False
    reason = ""
    sig_score = 0.0
    # lấy giá trị chỉ báo ở đúng nến dùng
    rsi6  = float(dfx["rsi6"].iloc[idx_use])   if "rsi6"  in dfx.columns  else None
    rsi12 = float(dfx["rsi12"].iloc[idx_use])  if "rsi12" in dfx.columns  else None
    rsi24 = float(dfx["rsi24"].iloc[idx_use])  if "rsi24" in dfx.columns  else None
    macd  = float(dfx["macd"].iloc[idx_use])   if "macd"  in dfx.columns  else None
    clv   = float(dfx["clv"].iloc[idx_use])    if "clv"   in dfx.columns  else None
    adx   = float(dfx["adx"].iloc[idx_use])    if "adx"   in dfx.columns  else 25.0
    
    # ngưỡng gốc (đã có trong cfg của bạn)
    RSI_LONG_MIN   = cfg.get("RSI_1H_LONG_MIN", 50)
    RSI_SHORT_MAX  = cfg.get("RSI_1H_SHORT_MAX", 50)
    MACD_LONG_MIN  = cfg.get("MACD_DIFF_LONG_MIN", 0.05)
    MACD_SHORT_MIN = cfg.get("MACD_DIFF_SHORT_MIN", 0.01)
    CLV_MIN        = cfg.get("CLV_MIN", 0.60)
    ADX_MIN        = cfg.get("ADX_MIN", 18)
    
    # nới tiêu chí khi EARLY + dùng nến hiện tại
    if early and use_cur:
        # giảm yêu cầu RSI/MACD/CLV ~10% và hạ ADX 2 điểm
        RSI_LONG_MIN   = max(0, RSI_LONG_MIN - 3)           # vd 50 -> 47
        RSI_SHORT_MAX  = min(100, RSI_SHORT_MAX + 3)        # vd 50 -> 53
        MACD_LONG_MIN  = MACD_LONG_MIN * 0.9                # vd 0.05 -> 0.045
        MACD_SHORT_MIN = MACD_SHORT_MIN * 0.9               # vd 0.01 -> 0.009
        CLV_MIN        = CLV_MIN * 0.9                      # vd 0.60 -> 0.54
        ADX_MIN        = max(0, ADX_MIN - 2)
    
    # quyết định chiều theo side hiện tại
    def pass_filters(side: str) -> (bool, str):
        reasons = []
        if side == "LONG":
            if rsi12 is not None and rsi12 < RSI_LONG_MIN:
                reasons.append(f"RSI12<{RSI_LONG_MIN}")
            if macd is not None and macd < MACD_LONG_MIN:
                reasons.append(f"MACD<{MACD_LONG_MIN:.3f}")
            if clv is not None and clv < CLV_MIN:
                reasons.append(f"CLV<{CLV_MIN:.2f}")
            if adx is not None and adx < ADX_MIN:
                reasons.append(f"ADX<{ADX_MIN}")
        else:  # SHORT
            if rsi12 is not None and rsi12 > RSI_SHORT_MAX:
                reasons.append(f"RSI12>{RSI_SHORT_MAX}")
            if macd is not None and macd > -MACD_SHORT_MIN:  # macd âm đủ mạnh
                reasons.append(f"MACD>-{MACD_SHORT_MIN:.3f}")
            if clv is not None and clv > (1 - CLV_MIN):
                reasons.append(f"CLV>{1-CLV_MIN:.2f}")
            if adx is not None and adx < ADX_MIN:
                reasons.append(f"ADX<{ADX_MIN}")
    
        return (len(reasons) == 0, "PASS" if not reasons else ", ".join(reasons))
    
    # ví dụ dùng:
    # must have side before filtering
    if not side:
        return side, entry, sl, tp, False, "[SKIP] side not decided", sig_score

    ok, reason = pass_filters(side)
    # nếu bạn cần gắn tag EARLY để hiển thị:
    if early and use_cur:
        reason = "[EARLY] " + reason
    # nến dùng để ra quyết định
    bar_idx = -1 if (early and use_cur) else -2
    cur  = df.iloc[bar_idx]
    prev = df.iloc[bar_idx-1]
    
    # chống báo quá sớm khi nến vừa mở (ví dụ TF 1h: đòi ít nhất 20')
    min_age_min = int(cfg.get("EARLY_MIN_AGE_MIN", 20))
    if early and use_cur:
        # df nên có cột 'ts' (ms) hoặc DatetimeIndex; đổi ra phút đã trôi qua trong nến
        last_ts = int(cur.get("timestamp", getattr(cur, "ts", 0)))  # ms
        bar_sec = 60 * 60  # TF 1h (nếu TF khác, đổi theo)
        now_sec = int(time.time())
        bar_start = (last_ts // 1000 // bar_sec) * bar_sec
        age_min = (now_sec - bar_start) // 60
        if age_min < min_age_min:
            return _ret(side=None, entry=None, sl=None, tp=None, ok=False)  # đợi thêm
    last_idx = -1 if (early and use_cur) else -2   # -1: nến đang chạy, -2: nến đã đóng
    latest = df.iloc[last_idx]
    price  = float(latest["close"])
    # ---------- volume percentile ----------
    vols = df["volume"].iloc[-20:]
    if len(vols) < 10:
        fail.append("DATA: thiếu volume 15m")
        return _ret(None, None, None, None, False)
    
    v_now = float(vols.iloc[-1])
    v_thr = float(np.percentile(vols.iloc[:-1], vol_p))
    
    # LOG chi tiết ở mức INFO
    logging.info(f"[VOL] {symbol}: v_now={v_now:.0f}, v_thr(P{vol_p})={v_thr:.0f}")
    
    if not (v_now >= v_thr):
        # đưa số vào reason để hiện ngay ở log INFO của bạn
        fail.append(f"VOLUME < P{vol_p} (v_now={v_now:.0f} < {v_thr:.0f})")

    # ---------- choppy filter: ADX + BBWidth ----------
    adx = float(df["adx"].iloc[-1]); bbw = float(df["bb_width"].iloc[-1])
    if adx < adx_min and not allow_adx_ex:
        fail.append(f"ADX {adx:.1f}<{adx_min}")
    if bbw < bbw_min:
        fail.append(f"BBW {bbw:.4f}<{bbw_min}")

    # ngoại lệ ADX (RELAX): bắt buộc body >= k*ATR
    if adx < adx_min and allow_adx_ex:
        last = df.iloc[last_idx]
        body = abs(last["close"] - last["open"])
        atr  = float(_atr(df).iloc[-1])
        if not (atr > 0 and body >= body_atr_k * atr):
            fail.append("ADX-except: body<k*ATR")

    # ---------- xác nhận đa khung ----------
    ema_up_15 = latest["ema20"] > latest["ema50"]
    ema_dn_15 = latest["ema20"] < latest["ema50"]
    rsi_15    = float(latest["rsi"])
    macd_diff = abs(float(latest["macd"] - latest["macd_signal"])) / max(price, 1e-9)

    if df_1h is not None and len(df_1h) > 60:
        d1 = _ensure_cols(df_1h.copy()).dropna()
        l1 = d1.iloc[-1]
        ema_up_1h = bool(l1["ema20"] > l1["ema50"])
        rsi_1h    = float(l1["rsi"])
    else:
        ema_up_1h = True; rsi_1h = 50.0

    cond_1h_long_ok  = (ema_up_1h and rsi_1h > rsi1h_long_min) or (allow_1h_neu and ema_up_1h)
    cond_1h_short_ok = ((not ema_up_1h) and rsi_1h < rsi1h_short_max) or (allow_1h_neu and (not ema_up_1h))
    
    # Xác định mode & cờ multi‑timeframe từ chính cfg
    mode = cfg.get("TAG", "RELAX")                # "STRICT" hoặc "RELAX"
    require_multi = cfg.get("REQ_EMA200_MULTI", False)
    if require_multi:
        ok_1h = ok_4h = True
        try:
            ok_1h = _ema200_up(df_1h) if side == "LONG" else _ema200_down(df_1h)
        except Exception:
            pass
        try:
            ok_4h = _ema200_up(df_4h) if side == "LONG" else _ema200_down(df_4h)
        except Exception:
            pass
    
        if mode == "STRICT":
            if not (ok_1h and ok_4h):
                fail.append("REGIME: EMA200 1H/4H không đồng hướng")
                return _ret(None, None, None, None, False)
        else:  # RELAX
            if not (ok_1h or ok_4h):
                fail.append("REGIME: EMA200 thiếu xác nhận (RELAX)")
                return _ret(None, None, None, None, False)
            
    # ===== XÁC NHẬN HƯỚNG & ĐỒNG PHA (STRICT=3/3, RELAX>=2/3) =====
    side = None
    
    # vote theo 3 nhóm: EMA(+1), RSI(+1), MACD(+1)
    long_vote  = 0
    short_vote = 0
    
    # 1) EMA (15m + xác nhận 1h)
    if ema_up_15 and cond_1h_long_ok:
        long_vote += 1
    elif ema_dn_15 and cond_1h_short_ok:
        short_vote += 1
    
    # 2) RSI
    if rsi_15 > rsi_long_min:
        long_vote += 1
    elif rsi_15 < rsi_short_max:
        short_vote += 1
    
    # 3) MACD: dùng hướng thô (macd - signal) + ngưỡng độ lớn macd_diff
    macd_raw = float(latest["macd"]) - float(latest["macd_signal"])
    if macd_raw > 0 and macd_diff > macd_long_min:
        long_vote += 1
    elif macd_raw < 0 and macd_diff > macd_short_min:
        short_vote += 1
    
    # yêu cầu đồng pha: RELAX=2/3, STRICT=3/3
    need_align = 2 if cfg.get("TAG", "STRICT") == "RELAX" else 3
    
    # xét ADX cùng lúc (giữ nguyên ngưỡng bạn đã đặt)
    if long_vote >= need_align and adx >= adx_min:
        side = "LONG"
    elif short_vote >= need_align and adx >= adx_min:
        side = "SHORT"
    else:
        # báo lý do rớt chi tiết
        best = max(long_vote, short_vote)
        fail.append(f"ALIGN {best}/3 (y/c {need_align}/3)")
        if adx < adx_min:
            fail.append(f"ADX {adx:.1f} < {adx_min}")
    
    # nếu đã có lỗi ở trên, trả sớm
    if fail:
        return _ret(None, None, None, None, False)


    # ---------- ép vị trí near S/R theo hướng ----------
    try:
        low_sr, high_sr = find_support_resistance(df, lookback=40)
    except Exception:
        low_sr = float(df["low"].iloc[-40:-1].min())
        high_sr= float(df["high"].iloc[-40:-1].max())
    px = price
    near_sup = abs(px - low_sr)/max(low_sr,1e-9) <= sr_near_pct
    near_res = abs(px - high_sr)/max(high_sr,1e-9) <= sr_near_pct
    if side == "LONG" and not near_sup:   fail.append("SR: không near support")
    if side == "SHORT" and not near_res:  fail.append("SR: không near resistance")
    if fail: return _ret(None, None, None, None, False)

    # ===== EXTRA PRO FILTERS (veto nếu không đạt) =====
    try:
        # 1) Divergence veto (dùng RSI nếu có, fallback MACD histogram)
        osc = df["rsi"] if "rsi" in df.columns else (df["macd"] - df["macd_signal"] if "macd" in df.columns and "macd_signal" in df.columns else None)
        if osc is not None:
            if side == "LONG"  and is_bear_div(df["close"], osc, lb=30, win=3):
                fail.append("DIVERGENCE (bear)");  return _ret(None, None, None, None, False)
            if side == "SHORT" and is_bull_div(df["close"], osc, lb=30, win=3):
                fail.append("DIVERGENCE (bull)");  return _ret(None, None, None, None, False)
    
        # 2) Wick quality (tránh râu ngược quá lớn)
        last = df.iloc[last_idx]
        rng   = max(float(last["high"]) - float(last["low"]), 1e-9)
        body  = abs(float(last["close"]) - float(last["open"]))
        upper = float(last["high"]) - max(float(last["close"]), float(last["open"]))
        lower = min(float(last["close"]), float(last["open"])) - float(last["low"])
        if side == "LONG"  and upper >= WICK_RATIO_LIMIT * rng:
            fail.append("UPPER-WICK");  return _ret(None, None, None, None, False)
        if side == "SHORT" and lower >= WICK_RATIO_LIMIT * rng:
            fail.append("LOWER-WICK");  return _ret(None, None, None, None, False)
    
        # 3) CLV (Close near extreme của nến tín hiệu)
        clv = (float(last["close"]) - float(last["low"])) / rng  # 0..1
        if side == "LONG"  and clv < 0.60:
            fail.append("CLV<0.60");   return _ret(None, None, None, None, False)
        if side == "SHORT" and clv > 0.40:
            fail.append("CLV>0.40");   return _ret(None, None, None, None, False)
    
        # 4) ATR expansion (ATR hiện tại > median ATR 20 nến trước * 1.2)
        atr_s   = _atr(df, n=14)
        atr_now = float(atr_s.iloc[-1]) if atr_s is not None and len(atr_s) else None
        px      = float(last["close"])
        
        k_atr = cfg.get("SR_NEAR_K_ATR", 0.6)   # cũ là 0.6
        pct   = cfg.get("SR_NEAR_PCT", 0.008)   # cũ ~ 0.8%
        
        tol_abs = 0.0
        if atr_now:
            tol_abs = max(tol_abs, k_atr * atr_now)
        tol_abs = max(tol_abs, pct * px)
        
        if side == "LONG":
            # cần gần KHÁNG CỰ (high_sr)
            if "high_sr" in locals() and high_sr is not None:
                if abs(px - float(high_sr)) > tol_abs:
                    fail.append("SR: không near resistance")
                    return _ret(None,None,None,None,False)
        elif side == "SHORT":
            # cần gần HỖ TRỢ (low_sr)
            if "low_sr" in locals() and low_sr is not None:
                if abs(px - float(low_sr)) > tol_abs:
                    fail.append("SR: không near support")
                    return _ret(None,None,None,None,False)
    except Exception as _e:
        # An toàn: nếu filter phụ lỗi thì bỏ qua (không làm hỏng luồng chính)
        logging.debug(f"[EXTRA-FILTER] skip due to error: {_e}")
    
    # ---------- Divergence veto (RSI/MACD) ----------
    osc = df["rsi"] if "rsi" in df.columns else (df["macd"]-df["macd_signal"] if {"macd","macd_signal"}<=set(df.columns) else None)
    if osc is not None:
        if side=="LONG"  and is_bear_div(df["close"], osc, lb=30, win=3):
            fail.append("DIVERGENCE"); return _ret(None,None,None,None,False)
        if side=="SHORT" and is_bull_div(df["close"], osc, lb=30, win=3):
            fail.append("DIVERGENCE"); return _ret(None,None,None,None,False)     
    
    # ---------- Wick ration (râu nến xấu) ----------
    last=df.iloc[-1]; rng=max(float(last.high)-float(last.low),1e-9)
    upper=float(last.high)-max(float(last.close),float(last.open))
    lower=min(float(last.close),float(last.open))-float(last.low)
    if side=="LONG"  and upper>=0.5*rng:  fail.append("UPPER-WICK");  return _ret(None,None,None,None,False)
    if side=="SHORT" and lower>=0.5*rng:  fail.append("LOWER-WICK");  return _ret(None,None,None,None,False)

    # ---------- CLV (Close gần cực trị nến) ----------
    clv=(float(last.close)-float(last.low))/rng
    if side=="LONG"  and clv<0.60: fail.append("CLV<0.60");  return _ret(None,None,None,None,False)
    if side=="SHORT" and clv>0.40: fail.append("CLV>0.40");  return _ret(None,None,None,None,False)

    # ---------- Breakout phải có retest nhẹ (>= 1 nến trước đó) ----------
    require_retest = cfg.get("REQUIRE_RETEST", True)
    brk_level = high_sr if side == "LONG" else low_sr
    pre = df.tail(4)[:-1]
    if np.isfinite(brk_level) and require_retest:
        if side == "LONG":
            ok = any(float(r.low)  <= brk_level <= float(r.close) for _, r in pre.iterrows())
        else:
            ok = any(float(r.close) <= brk_level <= float(r.high) for _, r in pre.iterrows())
        if not ok:
            fail.append("NO-RETEST")
            return _ret(None, None, None, None, False)
            
    # ---------- Bar exhaustion (nến quá dài -> dễ hụt hơi) ----------
    if float(last.high)-float(last.low) > 1.8*float(_atr(df,14).iloc[-1]):
        fail.append("EXHAUSTION-BAR"); return _ret(None,None,None,None,False)

    # ---------- Bar- since-break (tránh vào trễ, chỉ ghi nhận cú break đầu) ----------
    pre=df.tail(10)[:-1]
    if side=="LONG"  and any(float(r.close)>brk_level for _,r in pre.iterrows()):
        fail.append("LATE-BREAK"); return _ret(None,None,None,None,False)
    if side=="SHORT" and any(float(r.close)<brk_level for _,r in pre.iterrows()):
        fail.append("LATE-BREAK"); return _ret(None,None,None,None,False)
        
    # ---------- Away from mean (không đu quá xa EMA/VWAP) ----------
    ema20=df["ema20"].iloc[-1] if "ema20" in df.columns else None
    vwap =df["vwap"].iloc[-1]  if "vwap"  in df.columns else None
    atr14=float(_atr(df,14).iloc[-1])
    if side=="LONG":
        if ema20 and (float(last.close)-float(ema20))>1.0*atr14: fail.append("AWAY-EMA20"); return _ret(None,None,None,None,False)
        if vwap  and (float(last.close)-float(vwap)) >1.2*atr14: fail.append("AWAY-VWAP");  return _ret(None,None,None,None,False)
    else:
        if ema20 and (float(ema20)-float(last.close))>1.0*atr14: fail.append("AWAY-EMA20"); return _ret(None,None,None,None,False)
        if vwap  and (float(vwap) -float(last.close))>1.2*atr14: fail.append("AWAY-VWAP");  return _ret(None,None,None,None,False)
                      
    # ---------- Entry/SL/TP/RR ----------
    if side == "LONG":
        sl = float(df["low"].iloc[-10:-1].min())
        tp = float(df["high"].iloc[-10:-1].max())
    else:
        sl = float(df["high"].iloc[-10:-1].max())
        tp = float(df["low"].iloc[-10:-1].min())
    
    entry = px
    # --- Check SL/TP/RR động theo ATR ---
    atr_s  = _atr(df, n=14)
    atr_now = float(atr_s.iloc[-1]) if atr_s is not None and len(atr_s) else None
    atr_pct = (atr_now / max(entry, 1e-9)) if atr_now else 0.0
    
    sl_min_pct_base = SL_MIN_PCT_BASE
    tp_min_pct_base = TP_MIN_STRICT_BASE if mode == "STRICT" else TP_MIN_RELAX_BASE
    
    sl_min_pct_dyn  = max(sl_min_pct_base, K_ATR_SL * atr_pct)
    tp_min_pct_dyn  = max(tp_min_pct_base, K_ATR_TP * atr_pct)
    
    sl_pct = abs(entry - sl) / max(entry, 1e-9)
    if sl_pct < sl_min_pct_dyn:
        fail.append(f"SL<{sl_min_pct_dyn*100:.2f}%")
        return _ret(None, None, None, None, False)
    
    tp_pct = abs(tp - entry) / max(entry, 1e-9)
    if tp_pct < tp_min_pct_dyn:
        fail.append(f"TP<{tp_min_pct_dyn*100:.2f}%")
        return _ret(None, None, None, None, False)
    
    rr_min = (STRICT_CFG if mode == "STRICT" else RELAX_CFG)["RR_MIN"]
    risk = max(abs(entry - sl), 1e-9)
    rr = abs(tp - entry) / risk
    if rr < rr_min:
        fail.append(f"RR {rr:.2f}<{rr_min}")
        return _ret(None, None, None, None, False)
                      
    # ---------- news blackout ----------
    try:
        if in_news_blackout(news_win):
            fail.append("NEWS blackout")
    except Exception:
        pass
    if fail: return _ret(None, None, None, None, False)

    # ---------- anchored VWAP blocker ----------
    if use_vwap and len(df) > 55:
        try:
            aidx = pick_anchor_index(df, side)
            if not isinstance(aidx, int):
                try:
                    aidx = df.index.get_loc(aidx)
                except Exception:
                    aidx = max(0, len(df)-50)
            vwap = anchored_vwap(df, aidx)
            dist = abs(entry - vwap)/max(vwap,1e-9)
            if dist <= 0.001 and ((side=="LONG" and entry < vwap) or (side=="SHORT" and entry > vwap)):
                fail.append("VWAP chặn")
        except Exception:
            pass
    if fail: return _ret(None, None, None, None, False)

    # ---------- ATR clearance (cản gần) ----------
    try:
        clr_ratio, clr_ok = atr_clearance(df, side, atr_need)
        if not clr_ok:
            fail.append(f"ATR clearance {clr_ratio:.2f}<{atr_need}")
    except Exception:
        pass
    if fail: return _ret(None, None, None, None, False)

    # ---------- Đồng pha BTC ----------
    try:
        btc_1h = fetch_ohlcv_okx("BTC-USDT-SWAP", "1h")
        btc_up_1h = True
        if isinstance(btc_1h, pd.DataFrame) and len(btc_1h) > 60:
            b1 = calculate_indicators(btc_1h.copy()).dropna().iloc[-1]
            btc_up_1h = bool(b1["ema20"] > b1["ema50"])
        if side == "LONG" and not btc_up_1h:  fail.append("BTC ngược hướng 1h")
        if side == "SHORT" and btc_up_1h:     fail.append("BTC ngược hướng 1h")
        # biến động 15m ngắn hạn
        btc_15 = fetch_ohlcv_okx("BTC-USDT-SWAP", "15m")
        if isinstance(btc_15, pd.DataFrame) and len(btc_15) > 5:
            c0 = float(btc_15["close"].iloc[-1]); c3 = float(btc_15["close"].iloc[-4])
            chg = (c0 - c3)/max(c3,1e-9)
            if side == "LONG" and chg < -0.01: fail.append("BTC -1%/45m")
            if side == "SHORT" and chg >  0.01: fail.append("BTC +1%/45m")
    except Exception:
        pass
    if fail: return _ret(None, None, None, None, False)

    # ---------- PASS ----------
    if not silent:
        logging.info(f"✅ {symbol} VƯỢT QUA HẾT BỘ LỌC ({side})")
    # --- ATR% trên giá hiện tại (dùng để chuẩn hoá khoảng cách EMA/VWAP) ---
    price  = float(df_15m["close"].iloc[-1])
    atr14  = float(_atr(df_15m, 14).iloc[-1])          # bạn đã có _atr(df, n) ở chỗ khác
    atr_pct = atr14 / max(price, 1e-9)                 # ví dụ: 0.012 = 1.2%
    # an toàn: nếu NaN/inf thì gán mặc định
    if not (0 < atr_pct < 1e3):
        atr_pct = 0.01    
    # ---------- Tính score tín hiệu ----------
    adx_val  = float(df["adx"].iloc[-1]) if "adx" in df.columns else 20.0
    clv      = (float(last["close"])-float(last["low"])) / max(float(last["high"])-float(last["low"]),1e-9)
    dist_ema = abs(float(last["close"]) - float(last.get("ema20", float(last["close"]))))
    volp     = vol_p if 'vol_p' in locals() else 60
    
    sig_score = score_signal(rr, adx_val, clv, dist_ema, volp, atr_pct)
    
    # Lưu thêm score vào tg_candidates               
    return _ret(side, float(entry), float(sl), float(tp), True)

def analyze_trend_multi(symbol):
    tf_map = {
        'short': ['1H', '4H', '1D', '1W'],
        'mid':   ['1D', '1W', '1W', '1M']
    }

    def get_score(tf):
        try:
            df = fetch_ohlcv(symbol, tf.lower(), 50)
            df = calculate_indicators(df)
            rsi = df['rsi'].iloc[-1]
            ema20 = df['ema20'].iloc[-1]
            ema50 = df['ema50'].iloc[-1]
            return 2 if (rsi > 60 and ema20 > ema50) else 1 if (rsi > 50 and ema20 > ema50) else 0
        except:
            return 0

    short_score = sum([get_score(tf) for tf in tf_map['short']])
    mid_score = sum([get_score(tf) for tf in tf_map['mid']])

    def to_text(score):
        return "Tăng (★★★)" if score >= 6 else "Không rõ (★)" if score >= 2 else "Giảm (✖)"

    return to_text(short_score), to_text(mid_score)
 

def calculate_signal_rating(signal, short_trend, mid_trend, volume_ok):
    if signal == "LONG" and short_trend.startswith("Tăng") and mid_trend.startswith("Tăng") and volume_ok:
        return 5
    elif signal == "SHORT" and short_trend.startswith("Giảm") and mid_trend.startswith("Giảm"):
        return 5
    elif short_trend.startswith("Tăng") and mid_trend.startswith("Tăng"):
        return 4
    elif short_trend.startswith("Giảm") and mid_trend.startswith("Giảm"):
        return 4
    elif signal in ["LONG", "SHORT"]:
        return 3
    else:
        return 2

def _to_user_entered(x):
    if x is None:
        return ""
    if isinstance(x, float):
        s = f"{x:.8f}".rstrip("0").rstrip(".")
        return s if s else "0"
    return str(x)


VN_TZ = pytz.timezone("Asia/Ho_Chi_Minh")
def parse_vn_time(s):
    """Trả về datetime có tz; hỗ trợ nhiều format từ Google Sheets."""
    if s is None or s == "":
        return None
    # Nếu đã là datetime
    if isinstance(s, dt.datetime):
        return s if s.tzinfo else VN_TZ.localize(s)

    s = str(s).strip().replace("Z", "+00:00")  # ISO 'Z' -> +00:00

    formats = [
        "%d/%m/%Y %H:%M",          # 11/08/2025 16:10
        "%Y-%m-%d %H:%M:%S%z",     # 2025-08-11 16:10:00+07:00  <-- QUAN TRỌNG
        "%Y-%m-%dT%H:%M:%S%z",     # 2025-08-11T16:10:00+07:00
        "%Y-%m-%d %H:%M:%S",       # 2025-08-11 16:10:00 (naive)
        "%Y-%m-%dT%H:%M:%S",       # 2025-08-11T16:10:00 (naive)
        "%Y-%m-%d %H:%M",          # 2025-08-11 16:10
        "%Y-%m-%dT%H:%M",          # 2025-08-11T16:10
    ]
    for fmt in formats:
        try:
            d = dt.datetime.strptime(s, fmt)
            return d if d.tzinfo else VN_TZ.localize(d)
        except Exception:
            pass
    return None

def prepend_with_retention(ws, new_rows, keep_days=3):
    """
    Chèn new_rows lên đầu Google Sheet ws, giữ lại dữ liệu cũ trong vòng keep_days ngày.
    new_rows: list of lists (mỗi list là 1 dòng)
    """
    try:
        # Lấy toàn bộ dữ liệu hiện tại
        existing_data = ws.get_all_values()

        # Nếu sheet đang trống → thêm header trước
        if not existing_data:
            headers = ["Coin", "Tín hiệu", "Entry", "SL", "TP", "Xu hướng ngắn", "Xu hướng trung", "Ngày", "Mode"]
            ws.insert_row(headers, 1)
            existing_data = [headers]

        headers = existing_data[0]
        old_rows = existing_data[1:]

        # Lọc dữ liệu cũ theo ngày
        today = datetime.now(pytz.timezone("Asia/Ho_Chi_Minh")).date()
        retained_rows = []
        for row in old_rows:
            try:
                date_str = row[7]  # Cột Ngày (index 7)
                if date_str.strip():
                    row_date = datetime.strptime(date_str, "%d/%m/%Y %H:%M").date()
                    if (today - row_date).days <= keep_days:
                        retained_rows.append(row)
                    else:
                        pass  # quá hạn → bỏ
                else:
                    retained_rows.append(row)  # nếu không có ngày → giữ nguyên
            except Exception:
                retained_rows.append(row)  # lỗi parse ngày → giữ nguyên

        # Đảm bảo mỗi dòng đều có 9 cột
        def normalize_row(r):
            r = list(r)
            while len(r) < 9:
                r.append("")
            return r[:9]

        new_rows_norm = [normalize_row(r) for r in new_rows]
        retained_rows_norm = [normalize_row(r) for r in retained_rows]

        # Gộp lại: header + 5 dòng mới + dữ liệu cũ (đã lọc)
        final_data = [headers] + new_rows_norm + retained_rows_norm

        # Xóa dữ liệu cũ rồi ghi lại
        ws.clear()
        ws.update("A1", final_data)

        logging.info(f"[SHEET] ✅ Prepend {len(new_rows_norm)} dòng mới, giữ lại {len(retained_rows_norm)} dòng cũ (≤ {keep_days} ngày)")

    except Exception as e:
        logging.error(f"[SHEET] Lỗi khi prepend_with_retention: {e}")
# === tính sao ===
def stars(n:int) -> str:
    n = max(0, min(5, int(n)))
    return "⭐" * n
    
# === GỬI TELEGRAM ===
def send_telegram_message(message):
    try:
        token = TELEGRAM_TOKEN
        chat_id = TELEGRAM_CHAT_ID
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "HTML"
        }
        response = requests.post(url, data=payload)
        if response.status_code != 200:
            logging.warning(f"Telegram error: {response.text}")
    except Exception as e:
        logging.error(f"❌ Lỗi gửi Telegram: {e}")
        
def run_bot():
    logging.basicConfig(level=logging.INFO)
    coin_list = get_top_usdt_pairs(limit=COINS_LIMIT)

    # bộ nhớ tạm để GỘP kết quả
    sheet_rows = []          # mỗi phần tử = row prepend_to_sheet([...]) theo format gốc của bạn
    tg_candidates = []       # (mode, symbol, side, entry, sl, tp, rating, sig_score)

    # -------- STRICT pass --------
    reset_log_once_for_mode("STRICT", coin_list)
    strict_hits = set()

    for symbol in coin_list:
        ok = False; side = entry = sl = tp = None; rating = 0

        with mute_logs():
            inst_id = symbol.upper().replace("/", "-") + "-SWAP"
            df_15m = fetch_ohlcv_okx(inst_id, "15m")
            df_1h  = fetch_ohlcv_okx(inst_id, "1h")
            if isinstance(df_15m, pd.DataFrame) and isinstance(df_1h, pd.DataFrame):
                df_15m = calculate_indicators(df_15m).dropna()
                df_1h  = calculate_indicators(df_1h ).dropna()
                side, entry, sl, tp, ok, reason, sig_score = detect_signal(
                    df_15m, df_1h, symbol,
                    cfg=STRICT_CFG, silent=True, context="LIVE-STRICT",
                    return_reason=True
                )
          
                if ok:
                    strict_hits.add(symbol)
                
                    # ---- trends (dùng cho rating) ----
                    try:
                        trend_s = trend_ema_adx(df_15m, ema_period=50, adx_th=20)   # 'Tăng' / 'Giảm' / 'Trung lập'
                    except Exception:
                        trend_s = "?"
                
                    try:
                        trend_m = trend_ema_adx(df_1h, ema_period=100, adx_th=20)
                    except Exception:
                        trend_m = "?"
                
                    # ---- volume_ok lấy từ filter volume ở trên ----
                    volume_ok = True
                    try:
                        volume_ok = (v_now >= v_thr)
                    except NameError:
                        volume_ok = True
                
                    # ---- rating ----
                    try:
                        if 'calculate_signal_rating' in globals():
                            rating = int(calculate_signal_rating(side, trend_s, trend_m, volume_ok))
                        else:
                            rating = 3
                            if side == "LONG" and trend_s.startswith("Tăng") and trend_m.startswith("Tăng"):
                                rating = 5 if volume_ok else 4
                            elif side == "SHORT" and trend_s.startswith("Giảm") and trend_m.startswith("Giảm"):
                                rating = 5 if volume_ok else 4
                            elif (trend_s[:1] == trend_m[:1]) and trend_s[:1] in ("T", "G"):
                                rating = 4
                    except Exception:
                        rating = 3
                    now_vn = dt.datetime.now(pytz.timezone("Asia/Ho_Chi_Minh")).strftime("%d/%m/%Y %H:%M")
                    # giữ ĐÚNG format prepend_to_sheet gốc của bạn:
                    side_with_stars = f"{side}{'⭐'*max(1, min(rating, 5))}"
                    sheet_rows.append([symbol, side_with_stars, entry, sl, tp, trend_s, trend_m, now_vn, "STRICT"])
                    tg_candidates.append(("STRICT", symbol, side, entry, sl, tp, rating, sig_score))

        # log tóm tắt 1 dòng/coin
        if ok:
            log_once("STRICT", symbol, f"[STRICT] {symbol}: ✅ PASS", "info")
        else:
            log_once("STRICT", symbol, f"[STRICT] {symbol}: ❌ rớt filter – {reason}", "info")

    # -------- RELAX pass (bỏ symbol đã ra ở STRICT) --------
    relax_list = [s for s in coin_list if s not in strict_hits]
    reset_log_once_for_mode("RELAX", relax_list)

    for symbol in relax_list:
        ok = False; side = entry = sl = tp = None; rating = 0

        with mute_logs():
            inst_id = symbol.upper().replace("/", "-") + "-SWAP"
            df_15m = fetch_ohlcv_okx(inst_id, "15m")
            df_1h  = fetch_ohlcv_okx(inst_id, "1h")
            if isinstance(df_15m, pd.DataFrame) and isinstance(df_1h, pd.DataFrame):
                df_15m = calculate_indicators(df_15m).dropna()
                df_1h  = calculate_indicators(df_1h ).dropna()
                side, entry, sl, tp, ok, reason, sig_score = detect_signal(
                    df_15m, df_1h, symbol,
                    cfg=RELAX_CFG, silent=True, context="LIVE-RELAX",
                    return_reason=True
                )
            
                if ok:                    
                    # ---- trends (phục vụ rating) ----
                    try:
                        trend_s = trend_ema_adx(df_15m, ema_period=50, adx_th=20)
                    except Exception:
                        trend_s = "?"
                    try:
                        trend_m = trend_ema_adx(df_1h, ema_period=100, adx_th=20)
                    except Exception:
                        trend_m = "?"
                    
                    # ---- rating ----
                    rating = 3
                    try:
                        # volume_ok nếu có v_now/v_thr; nếu không có thì coi như True để không crash
                        try:
                            volume_ok = (v_now >= v_thr)
                        except Exception:
                            volume_ok = True
                    
                        if 'calculate_signal_rating' in globals():
                            rating = int(calculate_signal_rating(side, trend_s, trend_m, volume_ok))
                        else:
                            # fallback đơn giản
                            if side == "LONG"  and trend_s.startswith("T") and trend_m.startswith("T"):
                                rating = 5 if volume_ok else 4
                            elif side == "SHORT" and trend_s.startswith("G") and trend_m.startswith("G"):
                                rating = 5 if volume_ok else 4
                            elif trend_s[:1] == trend_m[:1] and trend_s[:1] in ("T", "G"):
                                rating = 4
                            else:
                                rating = 3
                    except Exception:
                        rating = 3
                    
                    # ---- bảo vệ sig_score ----
                    if sig_score is None:
                        sig_score = 0.0
                    
                    # ---- ghi ra sheet & lưu candidate ----
                    now_vn = dt.datetime.now(pytz.timezone("Asia/Ho_Chi_Minh")).strftime("%d/%m/%Y %H:%M")
                    early_tag = " EARLY" if "[EARLY]" in str(reason) else ""
                    side_with_stars = f"{side}{early_tag} : ⭐{max(1, min(int(rating), 5))}"
                    sheet_rows.append([symbol, side_with_stars, entry, sl, tp, trend_s, trend_m, now_vn, "RELAX"])
                    tg_candidates.append(("RELAX", symbol, f"{side}{early_tag}", entry, sl, tp, rating, sig_score))

        if ok:
            log_once("RELAX", symbol, f"[RELAX] {symbol}: ✅ PASS", "info")
        else:
            log_once("RELAX", symbol, f"[RELAX] {symbol}: ❌ rớt filter – {reason}", "info")

    # ======= GỘP GHI GOOGLE SHEET MỘT LẦN =======
    try:
        if sheet_rows:  # list các dòng đã gom
            ws = client.open_by_key(sheet_id).worksheet("DATA_FUTURE")  # đúng tên sheet bạn đang dùng
            prepend_with_retention(ws, sheet_rows, keep_days=3)
        else:
            logging.info("[SHEET] Không có dòng nào để ghi.")
    except Exception as e:
        logging.error(f"[SHEET] ghi batch lỗi: {e}")

    # ===== GỬI TELEGRAM 1 LẦN  =====
    try:
        TOPN = TOPN_PER_BATCH  # nhớ đã khai báo biến này ở phần CONFIG
    
        if tg_candidates:
            # Chuẩn hoá: nếu phần tử chưa có score (7 phần tử) thì gán score=0.0
            cand = []
            for t in tg_candidates:
                if len(t) == 8:
                    cand.append(t)  # (mode, sym, side, entry, sl, tp, rating, sig_score)
                else:
                    mode, sym, side, entry, sl, tp, rating = t
                    cand.append((mode, sym, side, entry, sl, tp, rating, sig_score))
    
            # Sắp xếp theo score giảm dần và lấy Top‑N
            cand.sort(key=lambda x: x[-1], reverse=True)
            pick = cand[:TOPN]
    
            # Gộp message để gửi
            msgs = []
            for mode, sym, side, entry, sl, tp, rating, sc in pick:
                # Build message with size advice by mode
                size_advice = "Early 0.5× (scout)" if mode == "RELAX" else "ADD-ON/Full 1.0×"
                msgs.append(
                    f"[{mode}] | {sym} | {side}\n"
                    f"Entry: {entry}\nSL: {sl}\nTP: {tp}\n"
                    f"⭐ {rating}/5 | Score: {sc:.2f}\n"
                    f"Khuyến nghị: {size_advice}"
                )
    
            if msgs and 'send_telegram_message' in globals():
                send_telegram_message("📌 TOP tín hiệu mạnh\n\n" + "\n\n".join(msgs))
                logging.info(f"[TG] Đã gửi {len(msgs)}/{len(cand)} tín hiệu (Top‑N).")
        else:
            logging.info("[TG] Không có tín hiệu nào để gửi.")
    except Exception as e:
        logging.error(f"[TG] Gửi Top‑N lỗi: {e}")
    
def clean_old_rows():
    try:
        data = sheet.get_all_values()
        headers = data[0]
        rows = data[1:]
        today = dt.datetime.now(pytz.timezone("Asia/Ho_Chi_Minh")).date()

        new_rows = []
        for row in rows:
            try:
                row_date = dt.datetime.strptime(row[7], "%d/%m/%Y %H:%M").date()
                if (today - row_date).days < 3:
                    new_rows.append(row)
            except:
                new_rows.append(row)  # Nếu lỗi parse date thì giữ lại
        time.sleep(1)
        # Ghi lại: headers + rows mới
        sheet.clear()
        sheet.update([headers] + new_rows)
        logging.info(f"🧹 Đã xoá những dòng quá 3 ngày (giữ lại {len(new_rows)} dòng)")

    except Exception as e:
        logging.warning(f"❌ Lỗi khi xoá dòng cũ: {e}")

def get_top_usdt_pairs(limit=300):
    url = "https://www.okx.com/api/v5/public/instruments?instType=SPOT"
    try:
        res = requests.get(url)
        data = res.json()['data']
        usdt_pairs = [item['instId'] for item in data if item['quoteCcy'] == 'USDT']
        return usdt_pairs[:limit]
    except Exception as e:
        logging.error(f"Lỗi lấy danh sách coin: {e}")
        return []
if __name__ == "__main__":
    run_bot()

        
# ===== BACKTEST: đọc danh sách từ sheet THEO DÕI & ghi về BACKTEST_RESULT =====
# ==== PARSE & SHEET HELPERS (THEO DÕI -> BACKTEST_RESULT) ====

def _to_user_entered(x):
    if x is None: return ""
    if isinstance(x, float):
        s = f"{x:.8f}".rstrip("0").rstrip(".")
        return s if s else "0"
    return str(x)

    
def _okx_inst_id(sym: str) -> str:
    s = sym.upper().replace("/", "-")
    return s if s.endswith("-SWAP") else s + "-SWAP"

def _ensure_timestamp_ms(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    if "timestamp" not in d.columns:
        ts = pd.to_numeric(d.index, errors="coerce")
        if ts.notna().all():
            # nếu index đang là giây thì nhân 1000 -> ms
            if ts.max() < 10**12:
                ts = ts * 1000.0
            d["timestamp"] = ts.astype("int64")
    if "timestamp" in d.columns:
        d = d.sort_values("timestamp").reset_index(drop=True)  # quan trọng: sort tăng dần
    return d
    
def _ensure_ts_col(df):
    """Đảm bảo có cột timestamp (ms) và sort tăng dần."""
    import pandas as pd, numpy as np
    if df is None or len(df) == 0:
        return df, None, None

    cols = [c.lower() for c in df.columns]
    df2 = df.copy()

    # OKX thường trả 'ts' (ms). Nếu chưa có 'timestamp' thì tạo.
    if "timestamp" not in cols:
        if "ts" in df2.columns:
            df2["timestamp"] = pd.to_numeric(df2["ts"], errors="coerce")
        else:
            # thử lấy từ index
            try:
                ts = pd.to_numeric(df2.index, errors="coerce")
                if np.isfinite(ts).all():
                    df2["timestamp"] = ts
            except Exception:
                pass

    if "timestamp" not in [c.lower() for c in df2.columns]:
        return df2, None, None

    # sort theo thời gian tăng dần
    df2 = df2.sort_values(by=[col for col in df2.columns if col.lower()=="timestamp"][0])
    tmin = int(df2[[c for c in df2.columns if c.lower()=="timestamp"][0]].min())
    tmax = int(df2[[c for c in df2.columns if c.lower()=="timestamp"][0]].max())
    return df2, tmin, tmax


def _log_frame_span(tag, inst_id, when_utc, tmin, tmax):
    import datetime as dt, pytz
    def _fmt(ms):
        try:
            return dt.datetime.fromtimestamp(ms/1000, tz=pytz.utc).strftime("%Y-%m-%d %H:%M:%S%z")
        except Exception:
            return str(ms)
    if tmin is None:
        logging.debug(f"[BT] {tag} {inst_id}: ❌ không có cột timestamp.")
    else:
        logging.debug(
            f"[BT] {tag} {inst_id}: ts_entry={when_utc.strftime('%Y-%m-%d %H:%M:%S%z')} "
            f"| span=[{_fmt(tmin)} .. {_fmt(tmax)}]"
        )


def read_watchlist_from_sheet(sheet_name="THEO DÕI"):
    """
    Đọc sheet THEO DÕI -> list tuple:
    (symbol, side, entry, sl, tp, trend_s, trend_m, when_vn, mode)
    """
    try:
        logging.info(f"[BACKTEST] 👉 mở sheet '{sheet_name}' (id={sheet_id})")
        ws = client.open_by_key(sheet_id).worksheet(sheet_name)
    except Exception as e:
        logging.error(f"[BACKTEST] ❌ Không mở được worksheet '{sheet_name}': {e}")
        return []

    try:
        rows = ws.get_all_values()
        logging.info(f"[BACKTEST] Số dòng đọc được (kể cả header): {len(rows)}")
        if not rows or len(rows) < 2:
            logging.info("[BACKTEST] THEO DÕI rỗng (không có dòng dữ liệu dưới header).")
            return []
    except Exception as e:
        logging.error(f"[BACKTEST] ❌ Lỗi get_all_values: {e}")
        return []

    # Header + map cột
    head = rows[0]
    logging.debug(f"[BACKTEST] Header: {head}")
    col = {name.strip(): i for i, name in enumerate(head)}
    logging.debug(f"[BACKTEST] Map cột: {col}")

    # Kiểm tra đủ cột
    need = ["Coin","Tín hiệu","Entry","SL","TP","Xu hướng ngắn","Xu hướng trung","Ngày","Mode"]
    missing = [n for n in need if n not in col]
    if missing:
        logging.warning(f"[BACKTEST] Thiếu cột trong THEO DÕI: {missing}")
        return []

    out = []
    parsed = 0

    for idx, r in enumerate(rows[1:] , start=2):  # bắt đầu từ dòng 2 (1-based)
        try:
            raw_when = (r[col["Ngày"]] or "").strip()
            when_vn  = parse_vn_time(raw_when)
            sym      = (r[col["Coin"]] or "").strip()
            side     = ((r[col["Tín hiệu"]] or "").strip().split()[0]).upper()   # lấy LONG/SHORT
            entry    = float(str(r[col["Entry"]]).replace(",", "")) if r[col["Entry"]] else None
            sl       = float(str(r[col["SL"]]).replace(",", ""))     if r[col["SL"]]    else None
            tp       = float(str(r[col["TP"]]).replace(",", ""))     if r[col["TP"]]    else None
            trend_s  = (r[col["Xu hướng ngắn"]]  or "").strip()
            trend_m  = (r[col["Xu hướng trung"]] or "").strip()
            mode     = (r[col["Mode"]] or "RELAX").strip().upper()

            # Log mẫu vài dòng đầu
            if idx <= 5:
                logging.debug(f"[BACKTEST] Row{idx}: sym={sym}, side={side}, entry={entry}, "
                              f"sl={sl}, tp={tp}, trend_s='{trend_s}', trend_m='{trend_m}', "
                              f"ngày='{raw_when}' -> when_vn={when_vn}, mode={mode}")

            # Validate tối thiểu
            if not sym or side not in ("LONG","SHORT") or when_vn is None:
                logging.warning(f"[BACKTEST] Bỏ dòng {idx}: sym/side/ngày không hợp lệ "
                                f"(sym='{sym}', side='{side}', raw_when='{raw_when}')")
                continue
            if entry is None or sl is None or tp is None:
                logging.warning(f"[BACKTEST] Bỏ dòng {idx}: thiếu Entry/SL/TP "
                                f"(entry={entry}, sl={sl}, tp={tp})")
                continue

            out.append((sym, side, entry, sl, tp, trend_s, trend_m, when_vn, mode))
            parsed += 1

        except Exception as e:
            logging.warning(f"[BACKTEST] Lỗi parse dòng {idx}: {e} | raw={r}")

    logging.info(f"[BACKTEST] ✅ Parse xong: {parsed} dòng hợp lệ / {len(rows)-1} dữ liệu.")
    return out


def write_backtest_row(row):
    """row = [Coin, Tín hiệu, Entry, SL, TP, Xu hướng ngắn, Xu hướng trung, Ngày, Mode, Kết quả]"""
    ws = client.open_by_key(sheet_id).worksheet("BACKTEST_RESULT")
    ws.append_row([_to_user_entered(x) for x in row], value_input_option="USER_ENTERED")
    time.sleep(1)


def ts_to_str(ms):
    """ms -> 'dd/MM/YYYY HH:MM' (UTC); trả '—' nếu None/lỗi."""
    try:
        if ms is None or int(ms) <= 0:
            return "—"
        return datetime.fromtimestamp(int(ms)/1000, tz=timezone.utc).strftime("%d/%m/%Y %H:%M")
    except Exception:
        return "—"

def _first_touch_result(df, side, entry, sl, tp, sym=None, when_ts=None):
    """Quy tắc: nến nào chạm SL/TP trước -> LOSS/WIN; hết cửa sổ mà chưa chạm -> OPEN."""
    if df is None or len(df) == 0:
        if DEBUG_BACKTEST:
            logging.debug(f"[BT] {sym or ''} {side} -> OPEN (no candles)")
        return "OPEN"

    hit_logged = False
    for i, row in df.iterrows():
        try:
            h = float(row["high"]); l = float(row["low"])
            ts = int(row["timestamp"]) if "timestamp" in row else None
        except Exception:
            continue

        if side == "LONG":
            if l <= sl:
                if DEBUG_BACKTEST and not hit_logged:
                    logging.debug(f"[BT] {sym or ''} LONG -> LOSS at {ts_to_str(ts)} "
                                  f"(low={l:.6g} <= SL={sl:.6g})")
                    hit_logged = True
                return "LOSS"
            if h >= tp:
                if DEBUG_BACKTEST and not hit_logged:
                    logging.debug(f"[BT] {sym or ''} LONG -> WIN at {ts_to_str(ts)} "
                                  f"(high={h:.6g} >= TP={tp:.6g})")
                    hit_logged = True
                return "WIN"
        else:  # SHORT
            if h >= sl:
                if DEBUG_BACKTEST and not hit_logged:
                    logging.debug(f"[BT] {sym or ''} SHORT -> LOSS at {ts_to_str(ts)} "
                                  f"(high={h:.6g} >= SL={sl:.6g})")
                    hit_logged = True
                return "LOSS"
            if l <= tp:
                if DEBUG_BACKTEST and not hit_logged:
                    logging.debug(f"[BT] {sym or ''} SHORT -> WIN at {ts_to_str(ts)} "
                                  f"(low={l:.6g} <= TP={tp:.6g})")
                    hit_logged = True
                return "WIN"

    # Không chạm: log khoảng cách gần nhất để biết "vì sao OPEN"
    if DEBUG_BACKTEST:
        try:
            import numpy as _np
            highs = _np.array(df["high"], dtype=float)
            lows  = _np.array(df["low"],  dtype=float)
            if side == "LONG":
                gap_sl = float(_np.min(_np.maximum(lows - sl, 0.0)))   # >=0
                gap_tp = float(_np.min(_np.maximum(tp - highs, 0.0))) # >=0
            else:
                gap_sl = float(_np.min(_np.maximum(highs - sl, 0.0)))
                gap_tp = float(_np.min(_np.maximum(lows - tp, 0.0)))
            t0 = _ts_to_str(int(df["timestamp"].iloc[0])) if "timestamp" in df.columns else "N/A"
            t1 = _ts_to_str(int(df["timestamp"].iloc[-1])) if "timestamp" in df.columns else "N/A"
            logging.debug(f"[BT] {sym or ''} {side} -> OPEN (no touch) | "
                          f"nearest gaps: SL={gap_sl:.6g}, TP={gap_tp:.6g} | "
                          f"window={t0}→{t1}, candles={len(df)}")
        except Exception:
            logging.debug(f"[BT] {sym or ''} {side} -> OPEN (no touch; gap calc failed)")
    return "OPEN"


def backtest_from_watchlist():
    """
    Đọc sheet THEO DÕI và ghi kết quả về BACKTEST_RESULT.
    - timeframe: 15m
    - chỉ kiểm tra nến có timestamp >= thời điểm tín hiệu
    - kết quả: WIN/LOSS/OPEN theo rule chạm SL/TP cái nào trước
    """

    # --- util nhỏ: key duy nhất cho 1 tín hiệu ---
    def _make_key(row):
        sym      = str(row[0]).strip().upper()
        side     = str(row[1]).split()[0].upper()      # bỏ phần ⭐
        entry    = f"{float(row[2]):.8f}"
        sl       = f"{float(row[3]):.8f}"
        tp       = f"{float(row[4]):.8f}"
        date_str = str(row[7]).strip()
        mode     = str(row[8]).strip().upper() if len(row) > 8 else ""
        return f"{sym}|{side}|{entry}|{sl}|{tp}|{date_str}|{mode}"

    # --- đọc THEO DÕI ---
    items = read_watchlist_from_sheet("THEO DÕI")  # trả về list[list]
    if not items or len(items) <= 1:
        logging.info("[BACKTEST] THEO DÕI rỗng (không có dòng dữ liệu dưới header).")
        return
    header = items[0]
    rows   = items[0:]  # bỏ header

    logging.debug(f"[BACKTEST] Header: {header}")
    logging.info(f"[BACKTEST] 👍 Parse xong: {len(rows)} dòng hợp lệ / {len(items)-1} dữ liệu.")

    # --- lấy các key đã có ở BACKTEST_RESULT ---
    existing_keys = set()
    try:
        rows_bt = read_watchlist_from_sheet("BACKTEST_RESULT")  # dùng chung reader cho đồng nhất
        if rows_bt and len(rows_bt) > 1:
            for r in rows_bt[0:]:
                try:
                    k = _make_key(r)
                    existing_keys.add(k)
                except Exception:
                    continue
        logging.info(f"[BACKTEST] Số key đang có (BACKTEST_RESULT): {len(existing_keys)}")
    except Exception as e:
        logging.warning(f"[BACKTEST] Không đọc được BACKTEST_RESULT: {e}")

    # --- chạy backtest cho các dòng CHƯA có key ---
    seen_in_run = set()
    tf = "15m"
    max_after = 700
    written = 0

    for r in rows:
        try:
            if len(r) < 8:
                continue

            key = _make_key(r)
            if key in existing_keys or key in seen_in_run:
                # đã có rồi -> bỏ qua
                continue

            sym  = str(r[0]).strip().upper()
            side = str(r[1]).split()[0].upper()
            entry = float(r[2]); sl = float(r[3]); tp = float(r[4])
            trend_s = str(r[5]).strip()
            trend_m = str(r[6]).strip()
            date_str = str(r[7]).strip()
            mode = str(r[8]).strip().upper() if len(r) > 8 else "RELAX"

            # thời điểm tín hiệu (VN) -> dt
            when_vn = parse_vn_time(date_str)  # bạn đã có hàm này
            if not when_vn:
                logging.warning(f"[BACKTEST] Bỏ {sym}: when_vn không hợp lệ: {date_str}")
                continue

            # đổi VN -> UTC ms (OKX cần UTC)
            when_utc = when_vn.astimezone(pytz.utc)
            bar_sec  = 15 * 60
            ts_floor = int((when_utc.timestamp() // bar_sec) * bar_sec)
            ts_cut   = ts_floor * 1000  # ms
            
            # chuẩn hoá inst_id OKX
            inst_id = _okx_inst_id(sym)  # bạn đã có hàm này (upper + thêm '-SWAP' nếu thiếu)

            # lấy OHLCV 15m sau thời điểm tín hiệu 
            df = fetch_ohlcv_okx(inst_id, "15m", limit=1000)            

            if df is None or len(df) == 0:
                logging.debug(f"[BT] {sym} -> OPEN (no candles)")
                res = "OPEN"
            else:
                # đảm bảo có cột timestamp (ms)
                if "timestamp" not in df.columns:
                    try:
                        ts = pd.to_numeric(df.index, errors="coerce")
                        if ts.notna().all():
                            if ts.max() < 10**12:   # giây -> đổi sang ms
                                ts = ts * 1000.0
                            df = df.copy()
                            df["timestamp"] = ts
                    except Exception:
                        pass

                # cắt phần sau tín hiệu
                logging.debug(f"[BACKTEST] {sym}: ts_cut={ts_cut}, tổng nến={len(df)}, min_ts={df['timestamp'].min()}, max_ts={df['timestamp'].max()}")
                df_after = df[df["timestamp"] >= ts_cut] if "timestamp" in df.columns else df.copy()
                if df_after is None or len(df_after) == 0:
                    logging.warning(f"[BACKTEST] Bỏ {sym}: không có nến sau {when_vn} (ts_cut={ts_cut})")
                    continue
                if len(df_after) > max_after:
                    df_after = df_after.iloc[:max_after]
                res = _first_touch_result(df_after, side, entry, sl, tp)  # bạn đã có hàm này

            row_out = [
                sym, side, entry, sl, tp,
                trend_s, trend_m,
                when_vn.strftime("%d/%m/%Y %H:%M"),
                mode, res
            ]
            write_backtest_row(row_out)      # ghi 1 dòng
            seen_in_run.add(key)
            written += 1
            time.sleep(1)

        except Exception as e:
            logging.warning(f"[BACKTEST] Lỗi với {r}: {e}")

    logging.info(f"[BACKTEST] Ghi {written} dòng vào sheet BACKTEST_RESULT xong.")


# ====== CẤU HÌNH ======
RUN_BACKTEST = True   # bật để chạy, tắt nếu không cần
if RUN_BACKTEST:
    logging.info("🚀 Bắt đầu backtest từ sheet THEO DÕI…")
    try:
        backtest_from_watchlist()
        logging.info("✅ Hoàn thành backtest.")
    except Exception as e:
        logging.error(f"❌ Lỗi khi backtest: {e}")
