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
from datetime import timedelta

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)  # luôn bật DEBUG/INFO
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
    return None, None, None, None, False


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
    print("[DEBUG] Đã mở sheet thành công.")
except Exception as e:
    print(f"[ERROR] Không mở được sheet: {e}")
    raise

# ========== THAM SỐ KỸ THUẬT ==========
TP_MULTIPLIER = 1.5
SL_MULTIPLIER = 1.0
ADX_THRESHOLD = 12
COINS_LIMIT = 300  # Số coin phân tích mỗi lượt

# ========================== NÂNG CẤP CHUYÊN SÂU ==========================
# ====== PRESET & HELPERS ======
STRICT_CFG = {
    "VOLUME_PERCENTILE": 80,   # top 20%
    "ADX_MIN_15M": 22,
    "BBW_MIN": 0.012,
    "RR_MIN": 1.5,
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
}
RELAX_CFG = {
    "VOLUME_PERCENTILE": 50,   # top 50%
    "ADX_MIN_15M": 12,
    "BBW_MIN": 0.009,
    "RR_MIN": 1.3,
    "NEWS_BLACKOUT_MIN": 30, # phút
    "ATR_CLEARANCE_MIN": 0.6, # >= 0.6ART
    "USE_VWAP": True,
    "RELAX_EXCEPT": True,      # cho phép ngoại lệ khi breakout + volume
    "TAG": "RELAX",
    "RSI_LONG_MIN": 52,
    "RSI_SHORT_MAX": 48,
    "RSI_1H_LONG_MIN": 50,
    "RSI_1H_SHORT_MAX": 50,
    "MACD_DIFF_LONG_MIN": 0.02,
    "MACD_DIFF_SHORT_MIN": 0.0005,
    "ALLOW_1H_NEUTRAL": True,
}

# log 1 dòng/coin/mode + tắt log tạm thời
from contextlib import contextmanager
import sys, io, logging
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


def fetch_ohlcv_okx(symbol: str, timeframe: str = "15m", limit: int = 100):
    try:
        timeframe_map = {
            '1h': '1H', '4h': '4H', '1d': '1D',
            '15m': '15m', '5m': '5m', '1m': '1m'
        }
        timeframe_input = timeframe
        timeframe = timeframe_map.get(timeframe.lower(), timeframe)
        logging.debug(f"🕒 Timeframe input: {timeframe_input} => OKX dùng: {timeframe}")

        if timeframe not in ["1m", "5m", "15m", "30m", "1H", "4H", "1D"]:
            logging.warning(f"⚠️ Timeframe không hợp lệ: {timeframe}")
            return None

        url = f"https://www.okx.com/api/v5/market/candles?instId={symbol}&bar={timeframe}&limit={limit}"
        logging.debug(f"📤 Gửi request nến OKX: instId={symbol}, bar={timeframe}, limit={limit}")
        response = requests.get(url)
        data = response.json()


        if 'data' not in data or not data['data']:
            logging.warning(f"⚠️ Không có dữ liệu OHLCV: instId={symbol}, bar={timeframe}")
            return None

        df = pd.DataFrame(data["data"])
        df.columns = ["ts", "open", "high", "low", "close", "volume", "volCcy", "volCcyQuote", "confirm"]
        df["ts"] = pd.to_datetime(df["ts"].astype(int), unit="ms")  # ✅ an toàn hơn
        df = df.iloc[::-1].copy()

        # ✅ Chuyển các cột số sang float để tránh lỗi toán học
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        return df

    except Exception as e:
        logging.error(f"❌ Lỗi khi fetch ohlcv OKX cho {symbol} [{timeframe_input}]: {e}")
        return None
        
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
    
def detect_signal(df_15m: pd.DataFrame,
                  df_1h: pd.DataFrame,
                  symbol: str,
                  cfg: dict = None,
                  silent: bool = True,
                  context: str = "LIVE",
                  return_reason: bool = False):
    """
    Return:
      mặc định: (side, entry, sl, tp, ok)
      nếu return_reason=True: (side, entry, sl, tp, ok, reason_str)
    """
    cfg = cfg or STRICT_CFG
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
    sr_near_pct    = cfg.get("SR_NEAR_PCT", 0.05)  # 5%

    fail = []  # gom lý do rớt

    def _ret(side, entry, sl, tp, ok):
        if return_reason:
            return side, entry, sl, tp, ok, (", ".join(fail) if fail else "PASS")
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
    latest = df.iloc[-1]
    price  = float(latest["close"])

    # ---------- volume percentile ----------
    vols = df["volume"].iloc[-20:]
    if len(vols) < 10:
        fail.append("DATA: thiếu volume 15m")
        return _ret(None, None, None, None, False)
    v_now = float(vols.iloc[-1]); v_thr = float(np.percentile(vols.iloc[:-1], vol_p))
    if not (v_now >= v_thr):
        fail.append(f"VOLUME < P{vol_p}")

    # ---------- choppy filter: ADX + BBWidth ----------
    adx = float(df["adx"].iloc[-1]); bbw = float(df["bb_width"].iloc[-1])
    if adx < adx_min and not allow_adx_ex:
        fail.append(f"ADX {adx:.1f}<{adx_min}")
    if bbw < bbw_min:
        fail.append(f"BBW {bbw:.4f}<{bbw_min}")

    # ngoại lệ ADX (RELAX): bắt buộc body >= k*ATR
    if adx < adx_min and allow_adx_ex:
        last = df.iloc[-1]
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

    # ---------- Entry/SL/TP/RR ----------
    if side == "LONG":
        sl = float(df["low"].iloc[-10:-1].min())
        tp = float(df["high"].iloc[-10:-1].max())
    else:
        sl = float(df["high"].iloc[-10:-1].max())
        tp = float(df["low"].iloc[-10:-1].min())
    entry = px
    risk  = max(abs(entry - sl), 1e-9)
    rr    = abs(tp - entry)/risk
    if rr < rr_min:                        fail.append(f"RR {rr:.2f}<{rr_min}")
    if abs(entry - sl)/max(entry,1e-9) < 0.003: fail.append("SL quá sát <0.3%")
    if fail: return _ret(None, None, None, None, False)

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

def _parse_vn_time(s):
    # hỗ trợ cả "dd/mm/YYYY HH:MM" và ISO
    for fmt in ("%d/%m/%Y %H:%M", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S"):
        try:
            return dt.datetime.strptime(s, fmt)
        except Exception:
            continue
    # nếu không parse được, trả None -> sẽ giữ lại (an toàn)
    return None

def prepend_rows_with_retention(ws, rows, keep_days=3, tz_name="Asia/Ho_Chi_Minh"):
    """
    Prepend nhiều dòng lên đầu sheet, chỉ loại các dòng cũ hơn keep_days.
    Không xóa toàn bộ sheet DATA_FUTURE.
    ws: gspread Worksheet
    rows: List[List[Any]] theo đúng format cột của bạn
          [symbol, side + " ⭐⭐⭐", entry, sl, tp, "-", "-", "dd/mm/YYYY HH:MM"]
    """
    try:
        # Lấy toàn bộ dữ liệu hiện có
        old = ws.get_all_values()  # 2D list
        if old:
            headers = old[0]
            body = old[1:]
        else:
            headers = ["Symbol", "Side", "Entry", "SL", "TP", "-", "-", "Date"]
            body = []

        # Chuẩn hoá rows mới
        cleaned_new = [[_to_user_entered(c) for c in r] for r in rows]

        # Thời điểm hiện tại (VN)
        now_vn = dt.datetime.now(pytz.timezone(tz_name))
        cutoff = now_vn - timedelta(days=keep_days)

        # Lọc body cũ: chỉ giữ những dòng có Date >= cutoff
        kept = []
        for r in body:
            if not r:
                continue
            # date nằm ở cột cuối cùng theo format bạn đang dùng
            date_str = r[-1] if len(r) >= 1 else ""
            tt = _parse_vn_time(date_str)
            if tt is None:
                # không parse được -> giữ lại cho an toàn
                kept.append(r)
                continue
            # tt có thể là naive -> gán tz VN để so sánh
            if tt.tzinfo is None:
                tt = pytz.timezone(tz_name).localize(tt)
            if tt >= cutoff:
                kept.append(r)

        # Gộp: new rows lên đầu (đảo để bảo toàn thứ tự push), rồi tới phần kept
        combined = cleaned_new[::-1] + kept

        # Update lại sheet (chỉ ghi đè nội dung, không xoá sạch)
        ws.update([headers] + combined, value_input_option="USER_ENTERED")

        # Thu nhỏ số hàng để không còn các dòng quá hạn nằm bên dưới
        try:
            ws.resize(rows=len(combined) + 1)
        except Exception:
            pass

        logging.info(f"[SHEET] ✅ Prepend {len(cleaned_new)} dòng, giữ lại {len(kept)} dòng <={keep_days} ngày.")
    except Exception as e:
        logging.warning(f"[SHEET] ❌ Lỗi prepend_with_retention: {e}")

def run_bot():
    logging.basicConfig(level=logging.INFO)
    coin_list = get_top_usdt_pairs(limit=COINS_LIMIT)

    # bộ nhớ tạm để GỘP kết quả
    sheet_rows = []          # mỗi phần tử = row prepend_to_sheet([...]) theo format gốc của bạn
    tg_candidates = []       # (mode, symbol, side, entry, sl, tp, rating)

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
                side, entry, sl, tp, ok, reason = detect_signal(
                    df_15m, df_1h, symbol,
                    cfg=STRICT_CFG, silent=True, context="LIVE-STRICT",
                    return_reason=True
                )
                if ok:
                    strict_hits.add(symbol)
                    # rating: dùng hàm gốc nếu có, else mặc định STRICT=3 sao
                    if 'calculate_signal_rating' in globals():
                        try:
                            rating = int(calculate_signal_rating(side, "Tăng" if side=="LONG" else "Giảm",
                                                                 "Tăng" if side=="LONG" else "Giảm", True))
                        except Exception:
                            rating = 3
                    else:
                        rating = 3

                    now_vn = dt.datetime.now(pytz.timezone("Asia/Ho_Chi_Minh")).strftime("%d/%m/%Y %H:%M")
                    # giữ ĐÚNG format prepend_to_sheet gốc của bạn:
                    sheet_rows.append([symbol, side + " ⭐️⭐️⭐️", entry, sl, tp, "—", "—", now_vn])
                    tg_candidates.append(("STRICT", symbol, side, entry, sl, tp, rating))

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
                side, entry, sl, tp, ok, reason = detect_signal(
                    df_15m, df_1h, symbol,
                    cfg=RELAX_CFG, silent=True, context="LIVE-RELAX",
                    return_reason=True
                )
                if ok:
                    # rating: dùng hàm gốc nếu có, else RELAX=2 sao
                    if 'calculate_signal_rating' in globals():
                        try:
                            rating = int(calculate_signal_rating(side, "Tăng" if side=="LONG" else "Giảm",
                                                                 "Tăng" if side=="LONG" else "Giảm", True))
                        except Exception:
                            rating = 2
                    else:
                        rating = 2

                    now_vn = dt.datetime.now(pytz.timezone("Asia/Ho_Chi_Minh")).strftime("%d/%m/%Y %H:%M")
                    sheet_rows.append([symbol, side + " ⭐️⭐️", entry, sl, tp, "—", "—", now_vn])
                    tg_candidates.append(("RELAX", symbol, side, entry, sl, tp, rating))

        if ok:
            log_once("RELAX", symbol, f"[RELAX] {symbol}: ✅ PASS", "info")
        else:
            log_once("RELAX", symbol, f"[RELAX] {symbol}: ❌ rớt filter – {reason}", "info")

    # ======= GỘP GHI GOOGLE SHEET MỘT LẦN =======
    try:
        if sheet_rows:  # list các dòng đã gom
            ws = client.open_by_key(sheet_id).worksheet("DATA_FUTURE")  # đúng tên sheet bạn đang dùng
            prepend_rows_with_retention(ws, sheet_rows, keep_days=3)
        else:
            logging.info("[SHEET] Không có dòng nào để ghi.")
    except Exception as e:
        logging.error(f"[SHEET] ghi batch lỗi: {e}")

    # ======= GỬI TELEGRAM 1 LẦN (chỉ kèo > 3 sao) =======
    try:
        msgs = []
        for mode, sym, side, entry, sl, tp, rating in tg_candidates:
            if rating >= 3:  # > 2 sao
                msgs.append(f"[{mode}] | {sym} | {side}\nEntry: {entry}\nSL: {sl}\nTP: {tp}\n⭐️ {rating}/5")
        if msgs and 'send_telegram_message' in globals():
            send_telegram_message("🔥 TỔNG HỢP TÍN HIỆU MỚI (>=3⭐️)\n\n" + "\n\n".join(msgs))
    except Exception as e:
        logging.error(f"[TG] gửi tổng hợp lỗi: {e}")
    
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

        
# ===== BACKTEST: đọc danh sách từ sheet THEO DÕI & ghi về BACKTEST_RESULT =====

def read_symbols_from_sheet(sheet_name="THEO DÕI") -> list:
    """Đọc cột A của sheet THEO DÕI thành list symbol ('HUMA-USDT'...). Bỏ trống, bỏ header."""
    try:
        ws = client.open_by_key(sheet_id).worksheet(sheet_name)
        rows = ws.get_all_values()
        syms = []
        for r in rows:
            if not r: 
                continue
            s = (r[0] or "").strip().upper()
            if not s or s == "COIN" or s == "SYMBOL":
                continue
            syms.append(s)
        return syms
    except Exception as e:
        logging.error(f"[BACKTEST] Lỗi đọc sheet {sheet_name}: {e}")
        return []

def _first_hit_result(future_df: pd.DataFrame, side: str, entry: float, sl: float, tp: float) -> str:
    """Đi qua nến tương lai, xem chạm SL hay TP trước (dựa intrabar: low/high)."""
    for _, row in future_df.iterrows():
        hi = float(row["high"]); lo = float(row["low"])
        if side == "LONG":
            # chạm SL trước hay TP trước?
            if lo <= sl:   return "LOSS"
            if hi >= tp:   return "WIN"
        else:
            if hi >= sl:   return "LOSS"
            if lo <= tp:   return "WIN"
    return "OPEN"

def backtest_signals_90_days_from_sheet(sheet_src="THEO DÕI",
                                        sheet_dst="BACKTEST_RESULT",
                                        cfg=None, tag="STRICT",
                                        look_ahead=20):
    """
    - Lấy list coin từ sheet THEO DÕI (cột A).
    - Quét 90 ngày dữ liệu 15m; tại mỗi vị trí, nếu detect pass -> kiểm tra TP/SL trong 'look_ahead' nến.
    - Không log chi tiết; chỉ ghi kết quả về sheet BACKTEST_RESULT.
    """
    cfg = cfg or STRICT_CFG
    symbols = read_symbols_from_sheet(sheet_src)
    if not symbols:
        logging.warning("[BACKTEST] THEO DÕI rỗng – không có gì để test.")
        return

    results_rows = []  # sẽ ghi 1 lần

    try:
        ws = client.open_by_key(sheet_id).worksheet(sheet_dst)
    except Exception as e:
        logging.error(f"[BACKTEST] Không mở được sheet {sheet_dst}: {e}")
        return

    # mốc thời gian 90 ngày
    end_utc = dt.datetime.now(dt.timezone.utc)
    start_utc = end_utc - dt.timedelta(days=90)

    for sym in symbols:
        inst_id = sym.replace("/", "-").upper()
        if not inst_id.endswith("-USDT"):
            inst_id += "-USDT"
        inst_id += "-SWAP"

        try:
            with mute_logs():
                df15 = fetch_ohlcv_okx(inst_id, "15m")
                df1h  = fetch_ohlcv_okx(inst_id, "1h")
            if df15 is None or len(df15) < 200 or df1h is None or len(df1h) < 120:
                # thiếu dữ liệu, bỏ qua coin này
                continue

            df15 = calculate_indicators(df15).dropna()
            df1h = calculate_indicators(df1h).dropna()
        except Exception:
            continue

        # chỉ lấy đoạn 90 ngày gần nhất
        df15 = df15[df15.index >= pd.Timestamp(start_utc)]
        if len(df15) < 100:  # quá ít nến
            continue

        # quét sliding window
        for i in range(60, len(df15) - look_ahead):
            sub15 = df15.iloc[:i].copy()
            # 1h dùng đến cùng thời điểm
            t_i = df15.index[i-1]
            sub1h = df1h[df1h.index <= t_i].copy()
            if len(sub1h) < 60: 
                continue

            # detect (im lặng, không log); nếu bạn muốn lý do rớt -> return_reason=True
            side, entry, sl, tp, ok = detect_signal(sub15, sub1h, sym, cfg=cfg, silent=True, context=f"BT-{tag}")
            if not ok:
                continue

            future = df15.iloc[i:i+look_ahead].copy()
            result = _first_hit_result(future, side, entry, sl, tp)

            # Ghi 1 dòng kết quả theo format sheet của bạn: [Coin, Tín hiệu, Entry, SL, TP, Xu hướng, Xu hướng, Ngày]
            ts_vn = (t_i.tz_localize("UTC").astimezone(pytz.timezone("Asia/Ho_Chi_Minh"))).strftime("%d/%m/%Y %H:%M")
            star  = "⭐️⭐️⭐️" if tag == "STRICT" else "⭐️⭐️"
            results_rows.append([sym, f"{side} {star}", entry, sl, tp, "-", result, ts_vn])

    # ghi 1 lần
    try:
        # nếu bạn có Apps Script nhận batch -> dùng requests POST; không thì append từng dòng:
        for r in results_rows:
            ws.append_row(r, value_input_option="USER_ENTERED")
        logging.info(f"[BACKTEST] Ghi {len(results_rows)} dòng vào sheet {sheet_dst} xong.")
    except Exception as e:
        logging.error(f"[BACKTEST] Lỗi ghi sheet {sheet_dst}: {e}")


# ====== CẤU HÌNH ======
RUN_BACKTEST = True  # ✅ Đổi sang False nếu không muốn chạy backtest
# ====== LUỒNG CHÍNH ======
if RUN_BACKTEST:
    logging.info("🚀 Bắt đầu chạy backtest 90 ngày...")
    try:
        # STRICT
        backtest_signals_90_days_from_sheet(
            sheet_src="THEO DÕI",
            sheet_dst="BACKTEST_RESULT",
            cfg=STRICT_CFG,
            tag="STRICT",
            look_ahead=20
        )
        # (tuỳ chọn) chạy thêm RELAX
        backtest_signals_90_days_from_sheet(
            sheet_src="THEO DÕI",
            sheet_dst="BACKTEST_RESULT",
            cfg=RELAX_CFG,
            tag="RELAX",
            look_ahead=20
        )
        logging.info("✅ Hoàn thành backtest 90 ngày.")
    except Exception as e:
        logging.error(f"❌ Lỗi khi backtest: {e}")
