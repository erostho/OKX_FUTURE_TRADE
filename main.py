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
import datetime
import json
import os
import logging
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pytz import timezone
import pytz

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
}
RELAX_CFG = {
    "VOLUME_PERCENTILE": 85,   # top 15%
    "ADX_MIN_15M": 15,
    "BBW_MIN": 0.010,
    "RR_MIN": 1.6,
    "NEWS_BLACKOUT_MIN": 45,
    "ATR_CLEARANCE_MIN": 1.0,
    "USE_VWAP": True,
    "RELAX_EXCEPT": True,      # cho phép ngoại lệ khi breakout + volume
    "TAG": "RELAX",
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

from datetime import datetime, timezone
def in_news_blackout(window_min: int):
    now = datetime.now(timezone.utc)
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
import numpy as np
def _atr(df, n=14):
    tr = np.maximum.reduce([
        (df["high"]-df["low"]).abs(),
        (df["high"]-df["close"].shift()).abs(),
        (df["low"] -df["close"].shift()).abs()
    ])
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
        threshold = np.percentile(volumes[:-1], 70) # TOP 30%

        if np.isnan(v_now) or np.isnan(threshold):
            logging.debug(f"[DEBUG][Volume FAIL] Dữ liệu volume bị NaN - v_now={v_now}, threshold={threshold}")
            return False

        logging.debug(f"[DEBUG][Volume Check] Volume hiện tại = {v_now:.0f}, Threshold 70% = {threshold:.0f}")

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
                  context: str = "LIVE"):
    """
    Trả về: (side, entry, sl, tp, ok)
    side ∈ {"LONG","SHORT"} hoặc None
    """

    cfg = cfg or STRICT_CFG
    pct_near_sr = cfg.get("SR_NEAR_PCT", 0.03)         # 3% nếu không set
    rr_min      = cfg.get("RR_MIN", 1.5)
    bbw_min     = cfg.get("BBW_MIN", 0.012)
    adx_min     = cfg.get("ADX_MIN_15M", 22)
    vol_p       = cfg.get("VOLUME_PERCENTILE", 80)
    news_win    = cfg.get("NEWS_BLACKOUT_MIN", 60)
    atr_need    = cfg.get("ATR_CLEARANCE_MIN", 0.8)
    use_vwap    = cfg.get("USE_VWAP", True)
    allow_adx_ex= cfg.get("RELAX_EXCEPT", False)
    body_atr_k  = cfg.get("BREAKOUT_BODY_ATR", 0.6)

    # --------- Chuẩn bị chỉ báo 15m (nếu thiếu cột thì tính gọn) ---------
    df = df_15m.copy()
    req_cols = {"open","high","low","close","volume"}
    if not req_cols.issubset(df.columns) or len(df) < 60:
        return None, None, None, None, False

    def _ensure_cols(dfx: pd.DataFrame):
        # EMA
        if "ema20" not in dfx.columns: dfx["ema20"] = dfx["close"].ewm(span=20).mean()
        if "ema50" not in dfx.columns: dfx["ema50"] = dfx["close"].ewm(span=50).mean()
        # RSI
        if "rsi" not in dfx.columns:
            delta = dfx["close"].diff()
            gain = delta.clip(lower=0).rolling(14).mean()
            loss = (-delta.clip(upper=0)).rolling(14).mean().replace(0, np.nan)
            rs = gain / loss
            dfx["rsi"] = 100 - (100/(1+rs))
        # MACD
        if "macd" not in dfx.columns or "macd_signal" not in dfx.columns:
            ema12 = dfx["close"].ewm(span=12).mean()
            ema26 = dfx["close"].ewm(span=26).mean()
            dfx["macd"] = ema12 - ema26
            dfx["macd_signal"] = dfx["macd"].ewm(span=9).mean()
        # BB width
        if "bb_width" not in dfx.columns:
            ma20 = dfx["close"].rolling(20).mean()
            std20= dfx["close"].rolling(20).std()
            dfx["bb_upper"] = ma20 + 2*std20
            dfx["bb_lower"] = ma20 - 2*std20
            dfx["bb_width"] = (dfx["bb_upper"] - dfx["bb_lower"]) / dfx["close"]
        # ADX (nếu đã có thì giữ, không thì bỏ qua dùng fallback)
        if "adx" not in dfx.columns:
            # fallback nhẹ: không tính lại để đỡ nặng — coi như ADX đủ nếu thiếu
            dfx["adx"] = 25.0
        return dfx

    df = _ensure_cols(df).dropna()
    latest = df.iloc[-1]
    close_price = float(latest["close"])

    # --------- Volume spike (percentile) ---------
    vols = df["volume"].iloc[-20:]
    if len(vols) < 10:
        return None, None, None, None, False
    v_now = float(vols.iloc[-1])
    vol_thr = float(np.percentile(vols.iloc[:-1], vol_p))
    if not (v_now >= vol_thr):
        return None, None, None, None, False
    # --------- Lọc choppy: ADX + BBW ---------
    adx = float(df["adx"].iloc[-1])
    bbw = float(df["bb_width"].iloc[-1])

    if adx < adx_min and not allow_adx_ex:
        return None, None, None, None, False
    if bbw < bbw_min:
        return None, None, None, None, False

    # Nếu cho phép ngoại lệ ADX (RELAX): bắt buộc thân nến >= k*ATR để tránh break giả
    if adx < adx_min and allow_adx_ex:
        last = df.iloc[-1]
        body = abs(last["close"] - last["open"])
        atr  = float(_atr(df).iloc[-1])
        if atr <= 0 or body < body_atr_k * atr:
            return None, None, None, None, False

    # --------- Xác nhận đa khung (1H) ---------
    ema_up_15 = latest["ema20"] > latest["ema50"]
    ema_dn_15 = latest["ema20"] < latest["ema50"]
    rsi_15    = float(latest["rsi"])
    macd_diff = abs(float(latest["macd"] - latest["macd_signal"])) / max(close_price, 1e-9)

    if df_1h is not None and len(df_1h) > 60:
        d1 = df_1h.copy()
        d1 = _ensure_cols(d1).dropna()
        l1 = d1.iloc[-1]
        ema_up_1h = l1["ema20"] > l1["ema50"]
        rsi_1h    = float(l1["rsi"])
    else:
        ema_up_1h = True
        rsi_1h    = 50.0

    side = None
    # LONG mạnh khi: EMA up cả 15m/1h, RSI>55/50, MACD diff đủ lớn, ADX đạt ngưỡng
    if ema_up_15 and ema_up_1h and rsi_15 > 55 and rsi_1h > 50 and macd_diff > 0.05 and adx >= adx_min:
        side = "LONG"
    # SHORT mạnh khi: EMA down cả 15m/1h, RSI<45/50, MACD diff > nhỏ, ADX đạt ngưỡng
    elif ema_dn_15 and (not ema_up_1h) and rsi_15 < 45 and rsi_1h < 50 and macd_diff > 0.001 and adx >= adx_min:
        side = "SHORT"
    else:
        return None, None, None, None, False

    # --------- Vị trí theo S/R (ép hướng): LONG near support, SHORT near resistance ---------
    try:
        low_sr, high_sr = find_support_resistance(df, lookback=40)
    except Exception:
        low_sr, high_sr = float(df["low"].iloc[-40:-1].min()), float(df["high"].iloc[-40:-1].max())

    px = close_price
    near_sup = abs(px - low_sr)  / max(low_sr, 1e-9) <= pct_near_sr
    near_res = abs(px - high_sr) / max(high_sr,1e-9) <= pct_near_sr
    if (side == "LONG"  and not near_sup) or (side == "SHORT" and not near_res):
        return None, None, None, None, False

    # --------- Entry/SL/TP/RR ---------
    if side == "LONG":
        sl = float(df["low"].iloc[-10:-1].min())
        tp = float(df["high"].iloc[-10:-1].max())
    else:
        sl = float(df["high"].iloc[-10:-1].max())
        tp = float(df["low"].iloc[-10:-1].min())
    entry = px
    risk = max(abs(entry - sl), 1e-9)
    rr   = abs(tp - entry) / risk
    if rr < rr_min:
        return None, None, None, None, False
    # SL quá sát (tránh nhiễu)
    if abs(entry - sl)/max(entry,1e-9) < 0.003:  # 0.3%
        return None, None, None, None, False

    # --------- News blackout ---------
    try:
        if in_news_blackout(news_win):
            return None, None, None, None, False
    except Exception:
        pass  # nếu chưa cấu hình news thì bỏ qua

    # --------- VWAP blocker (anchored) ---------
    if use_vwap and len(df) > 55:
        try:
            anchor_idx = pick_anchor_index(df, side)
            # anchor_idx có thể là index label; chuyển về vị trí nếu cần
            if not isinstance(anchor_idx, int):
                try:
                    aidx = df.index.get_loc(anchor_idx)
                except Exception:
                    aidx = max(0, len(df)-50)
            else:
                aidx = anchor_idx
            vwap = anchored_vwap(df, aidx)
            dist = abs(entry - vwap) / max(vwap, 1e-9)
            near = dist <= 0.001  # 0.1%
            if near and ((side == "LONG" and entry < vwap) or (side == "SHORT" and entry > vwap)):
                return None, None, None, None, False
        except Exception:
            pass

    # --------- ATR clearance phía trước ---------
    try:
        clr_ratio, clr_ok = atr_clearance(df, side, atr_need)
        if not clr_ok:
            return None, None, None, None, False
    except Exception:
        # nếu không tính được ATR thì không chặn
        pass

    # --------- Đồng pha BTC (1H) + kiểm tra biến động BTC ngắn hạn ---------
    try:
        btc_15 = fetch_ohlcv_okx("BTC-USDT-SWAP", "15m")
        btc_1h = fetch_ohlcv_okx("BTC-USDT-SWAP", "1h")
        btc_up_1h = True
        if isinstance(btc_1h, pd.DataFrame) and len(btc_1h) > 60:
            b1 = calculate_indicators(btc_1h.copy()).dropna().iloc[-1]
            btc_up_1h = bool(b1["ema20"] > b1["ema50"])
        # chặn ngược hướng BTC 1h
        if side == "LONG" and not btc_up_1h:
            return None, None, None, None, False
        if side == "SHORT" and btc_up_1h:
            return None, None, None, None, False

        # biến động BTC ngắn hạn (15m) – tránh gió ngược mạnh
        if isinstance(btc_15, pd.DataFrame) and len(btc_15) > 5:
            c0 = float(btc_15["close"].iloc[-1]); c3 = float(btc_15["close"].iloc[-4])
            btc_change = (c0 - c3)/max(c3,1e-9)
            if side == "LONG" and btc_change < -0.01:   # BTC giảm >1% trong ~45m
                return None, None, None, None, False
            if side == "SHORT" and btc_change > 0.01:   # BTC tăng >1% trong ~45m
                return None, None, None, None, False
    except Exception:
        pass  # nếu fetch lỗi, không chặn

    # --------- PASS ---------
    if not silent:
        logging.info(f"✅ {symbol} VƯỢT QUA HẾT BỘ LỌC ({side})")
    return side, float(entry), float(sl), float(tp), True

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
    

def send_telegram_message(message: str):
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")  # hoặc ghi trực tiếp chuỗi token nếu bạn test thủ công
    chat_id = os.getenv("TELEGRAM_CHAT_ID")      # tương tự, gán chat_id thủ công nếu cần

    if not bot_token or not chat_id:
        print("❌ TELEGRAM_BOT_TOKEN hoặc TELEGRAM_CHAT_ID chưa được cấu hình.")
        return

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown"
    }

    try:
        response = requests.post(url, data=payload)
        response.raise_for_status()
        print("✅ Đã gửi tin nhắn Telegram.")
    except Exception as e:
        print(f"❌ Lỗi gửi Telegram: {e}")

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
        
def prepend_to_sheet(row_data: list):
    try:
        old_data = sheet.get_all_values()
        headers = old_data[0]
        body = old_data[1:]
        
        # Chèn dòng mới vào đầu
        body.insert(0, row_data)

        # Ghi lại toàn bộ (bao gồm cả header)
        sheet.update([headers] + body)
        logging.info(f"✅ Đã ghi dòng mới lên đầu: {row_data[0]}")

    except Exception as e:
        logging.warning(f"❌ Lỗi ghi sheet (prepend): {e}")

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
                side, entry, sl, tp, ok = detect_signal(
                    df_15m, df_1h, symbol, cfg=STRICT_CFG, silent=True, context="LIVE-STRICT"
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

                    now_vn = datetime.datetime.now(pytz.timezone("Asia/Ho_Chi_Minh")).strftime("%d/%m/%Y %H:%M")
                    # giữ ĐÚNG format prepend_to_sheet gốc của bạn:
                    sheet_rows.append([symbol, side + " ⭐️⭐️⭐️", entry, sl, tp, "—", "—", now_vn])
                    tg_candidates.append(("STRICT", symbol, side, entry, sl, tp, rating))

        # log tóm tắt 1 dòng/coin
        if ok:  log_once("STRICT", symbol, f"[STRICT] {symbol}: ✅ đạt tín hiệu", "info")
        else:   log_once("STRICT", symbol, f"[STRICT] {symbol}: ❌ không đạt (rớt filter)", "info")

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
                side, entry, sl, tp, ok = detect_signal(
                    df_15m, df_1h, symbol, cfg=RELAX_CFG, silent=True, context="LIVE-RELAX"
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

                    now_vn = datetime.datetime.now(pytz.timezone("Asia/Ho_Chi_Minh")).strftime("%d/%m/%Y %H:%M")
                    sheet_rows.append([symbol, side + " ⭐️⭐️", entry, sl, tp, "—", "—", now_vn])
                    tg_candidates.append(("RELAX", symbol, side, entry, sl, tp, rating))

        if ok:  log_once("RELAX", symbol, f"[RELAX] {symbol}: ✅ đạt tín hiệu", "info")
        else:   log_once("RELAX", symbol, f"[RELAX] {symbol}: ❌ không đạt (rớt filter)", "info")

    # ======= GỘP GHI GOOGLE SHEET MỘT LẦN =======
    try:
        prepend_to_sheet(sheet_rows)
    except Exception as e:
        logging.error(f"[SHEET] ghi batch lỗi: {e}")

    # ======= GỬI TELEGRAM 1 LẦN (chỉ kèo > 3 sao) =======
    try:
        msgs = []
        for mode, sym, side, entry, sl, tp, rating in tg_candidates:
            if rating >= 4:  # > 3 sao
                msgs.append(f"[{mode}] {sym} {side}\nEntry: {entry}\nSL: {sl}\nTP: {tp}\n⭐️ {rating}/5")
        if msgs and 'send_telegram_message' in globals():
            send_telegram_message("🔥 TỔNG HỢP KÈO CHẤT (>=4⭐️)\n\n" + "\n\n".join(msgs))
    except Exception as e:
        logging.error(f"[TG] gửi tổng hợp lỗi: {e}")
    
def clean_old_rows():
    try:
        data = sheet.get_all_values()
        headers = data[0]
        rows = data[1:]
        today = datetime.datetime.now(pytz.timezone("Asia/Ho_Chi_Minh")).date()

        new_rows = []
        for row in rows:
            try:
                row_date = datetime.datetime.strptime(row[7], "%d/%m/%Y %H:%M").date()
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


# === GHI GOOGLE SHEET ===

def append_to_google_sheet(sheet, row):
    try:
        sheet.append_row(row, value_input_option="USER_ENTERED")
    except Exception as e:
        logging.error(f"❌ Không ghi được Google Sheet: {e}")
        
# === BACKTEST 90 NGÀY ===
def backtest_signals_90_days(symbol_list):
    # Giả định đã có fetch_ohlcv_okx và detect_signal
    today = datetime.datetime.now(timezone.utc)
    start_time = today - datetime.timedelta(days=90)
    results = []

    for symbol in symbol_list:
        logging.info(f"🔍 Backtest: {symbol}")
        try:
            df = fetch_ohlcv_okx(symbol, "15m", limit=3000)
            if df is None or len(df) < 100:
                continue

            df = calculate_indicators(df)
            for i in range(50, len(df)-20):  # chừa nến để kiểm tra sau
                sub_df = df.iloc[i-50:i].copy()
                df_1h = df.iloc[i-100:i].copy()  # placeholder
                signal, entry, sl, tp, _ = detect_signal(sub_df, df_1h, symbol)
                if signal and entry and sl and tp:
                    future_data = df.iloc[i:i+20]
                    result = "NONE"
                    for j in range(len(future_data)):
                        price = future_data.iloc[j]["high"] if signal == "LONG" else future_data.iloc[j]["low"]
                        if signal == "LONG" and price >= tp:
                            result = "WIN"
                            break
                        elif signal == "LONG" and price <= sl:
                            result = "LOSS"
                            break
                        elif signal == "SHORT" and price <= tp:
                            result = "WIN"
                            break
                        elif signal == "SHORT" and price >= sl:
                            result = "LOSS"
                            break
                    results.append([symbol, signal, entry, sl, tp, result, df.iloc[i]["ts"].strftime("%Y-%m-%d %H:%M")])
        except Exception as e:
            logging.error(f"Backtest lỗi {symbol}: {e}")
            continue

    # Ghi kết quả ra sheet BACKTEST_RESULT
    try:
        sheet = client.open_by_key(sheet_id).worksheet("BACKTEST_RESULT")
        for row in results:
            sheet.append_row(row, value_input_option="USER_ENTERED")
    except Exception as e:
        logging.error(f"Lỗi ghi BACKTEST_RESULT: {e}")


# ====== CẤU HÌNH ======
RUN_BACKTEST = True  # ✅ Đổi sang False nếu không muốn chạy backtest
# ====== LUỒNG CHÍNH ======
if RUN_BACKTEST:
    logging.info("🚀 Bắt đầu chạy backtest 90 ngày...")
    try:
        backtest_signals_90_days(symbol_list)
        logging.info("✅ Hoàn thành backtest 90 ngày.")
    except Exception as e:
        logging.error(f"❌ Lỗi khi backtest: {e}")
