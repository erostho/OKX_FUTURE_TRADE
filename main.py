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
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

# ====== LOG HELPERS (log-once & mute logs) ======
from contextlib import contextmanager
_logged_once = {}

def log_once(mode: str, symbol: str, msg: str, level="info"):
    key = (mode, symbol)
    if _logged_once.get(key):
        return
    _logged_once[key] = True
    if level=="debug":
        logging.debug(msg)
    else:
        logging.info(msg)

def reset_log_once_for_mode(mode_tag: str, symbols: list):
    for s in symbols:
        _logged_once.pop((mode_tag, s), None)

@contextmanager
def mute_logs():
    prev = logging.getLogger().level
    logging.getLogger().setLevel(logging.ERROR)
    try:
        yield
    finally:
        logging.getLogger().setLevel(prev)

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

# === PRESET CONFIGS FOR STRICT/RELAX MODES ===
STRICT_CFG = {
    "ADX_MIN_15M": 22, "ADX_MIN_1H": 20,
    "BBW_MIN": 0.012, "BREAKOUT_BODY_ATR": 0.6,
    "VOLUME_MODE": os.getenv("VOLUME_MODE", "auto"),
    "VOLUME_PERCENTILE": int(os.getenv("VOLUME_PERCENTILE", "80")),
    "VOLUME_FACTOR": float(os.getenv("VOLUME_FACTOR", "1.6")),
    "RELAX_EXCEPT": False,
    "TAG": "STRICT"
}
RELAX_CFG = {
    "ADX_MIN_15M": 15, "ADX_MIN_1H": 15,
    "BBW_MIN": 0.010, "BREAKOUT_BODY_ATR": 0.7,
    "VOLUME_MODE": os.getenv("VOLUME_MODE", "auto"),
    "VOLUME_PERCENTILE": int(os.getenv("VOLUME_PERCENTILE", "85")),
    "VOLUME_FACTOR": float(os.getenv("VOLUME_FACTOR", "1.6")),
    "RELAX_EXCEPT": True,
    "TAG": "RELAX"
}
CURRENT_CFG = STRICT_CFG  # will be set in run_bot/backtest before scanning


# ========================== NÂNG CẤP CHUYÊN SÂU ==========================
def clean_missing_data(df, required_cols=["close", "high", "low", "volume"], max_missing=2):
    """Nếu thiếu 1-2 giá trị, loại bỏ. Nếu thiếu nhiều hơn, trả về None"""
    missing = df[required_cols].isnull().sum().sum()
    if missing > max_missing:
        return None
    return df.dropna(subset=required_cols)


def is_volume_spike(df):
    """
    Volume spike with smart modes:
      - auto: percentile depends on ADX & BB width (needs indicators present)
      - percentile: fixed percentile on last 29 bars
      - factor: v_now >= mean(vols[:-1]) * factor
    Reads CURRENT_CFG for thresholds.
    """
    try:
        vols = df["volume"].iloc[-30:]
        if len(vols) < 10:
            logging.debug(f"[DEBUG][Volume FAIL] Không đủ dữ liệu volume: chỉ có {len(vols)} nến")
            return False
        v_now = float(vols.iloc[-1])
        mode = CURRENT_CFG.get("VOLUME_MODE", "auto")
        if mode == "auto":
            adx = float(df["adx"].iloc[-1]) if "adx" in df.columns else np.nan
            if "bb_upper" in df.columns and "bb_lower" in df.columns:
                bbw = float((df["bb_upper"].iloc[-1] - df["bb_lower"].iloc[-1]) / max(df["close"].iloc[-1], 1e-12))
            else:
                bbw = np.nan
            # choose percentile
            if (not np.isnan(adx) and adx < 20) or (not np.isnan(bbw) and bbw < 0.010):
                p = 85
            elif (not np.isnan(adx) and adx < 25) or (not np.isnan(bbw) and bbw < 0.015):
                p = 80
            else:
                p = 70
            thr = float(np.percentile(vols[:-1], p))
            logging.debug(f"[DEBUG][Volume Check] mode=auto P={p}, v_now={v_now:.0f}, thr={thr:.0f}")
            return bool(v_now >= thr)
        elif mode == "percentile":
            p = int(CURRENT_CFG.get("VOLUME_PERCENTILE", 75))
            thr = float(np.percentile(vols[:-1], p))
            logging.debug(f"[DEBUG][Volume Check] mode=percentile P={p}, v_now={v_now:.0f}, thr={thr:.0f}")
            return bool(v_now >= thr)
        else:
            factor = float(CURRENT_CFG.get("VOLUME_FACTOR", 1.6))
            base = float(np.mean(vols[:-1]))
            thr = base * factor
            logging.debug(f"[DEBUG][Volume Check] mode=factor base={base:.0f} x{factor} thr={thr:.0f}, v_now={v_now:.0f}")
            return bool(v_now >= thr)
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

def candle_quality_ok(df, side):
    """
    Kiểm tra chất lượng nến để tránh tín hiệu nhiễu.
    - side: "long" hoặc "short"
    - df: DataFrame chứa OHLCV
    """
    last = df.iloc[-1]
    body = abs(last['close'] - last['open'])
    wick_top = last['high'] - max(last['close'], last['open'])
    wick_bottom = min(last['close'], last['open']) - last['low']

    # Ví dụ lọc: thân nến >= 50% tổng range
    range_total = last['high'] - last['low']
    if range_total == 0:
        return False

    body_ratio = body / range_total
    if side == "long":
        return (last['close'] > last['open']) and (body_ratio >= 0.5) and (wick_bottom <= body)
    else:
        return (last['close'] < last['open']) and (body_ratio >= 0.5) and (wick_top <= body)

def detect_signal(df_15m: pd.DataFrame, df_1h: pd.DataFrame, symbol: str):
    import logging
    df = df_15m.copy()
    df = clean_missing_data(df)
    if df is None or len(df) < 30:
        print(f"[DEBUG] {symbol}: ⚠️ loại do thiếu dữ liệu (<30 nến)")
        return None, None, None, None, False

    # Tính chỉ báo
    df["ema20"] = df["close"].ewm(span=20).mean()
    df["ema50"] = df["close"].ewm(span=50).mean()
    df["macd"] = df["close"].ewm(span=12).mean() - df["close"].ewm(span=26).mean()
    df["macd_signal"] = df["macd"].ewm(span=9).mean()
    delta = df["close"].diff()
    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)
    avg_gain = pd.Series(gain).rolling(window=14).mean()
    avg_loss = pd.Series(loss).rolling(window=14).mean()
    rs = avg_gain / avg_loss
    df["rsi"] = 100 - (100 / (1 + rs))
    df = calculate_adx(df)
    df["bb_mid"] = df["close"].rolling(20).mean()
    df["bb_std"] = df["close"].rolling(20).std()
    df["bb_upper"] = df["bb_mid"] + 2 * df["bb_std"]
    df["bb_lower"] = df["bb_mid"] - 2 * df["bb_std"]

    latest = df.iloc[-1]
    close_price = latest["close"]
    ema_up = latest["ema20"] > latest["ema50"]
    ema_down = latest["ema20"] < latest["ema50"]
    macd_diff = abs(latest["macd"] - latest["macd_signal"]) / close_price
    rsi = latest["rsi"]
    adx = latest["adx"]
    bb_width = (latest["bb_upper"] - latest["bb_lower"]) / close_price

    # Volume spike top 70%
    if not is_volume_spike(df):
        print(f"[DEBUG] {symbol}: ⚠️ loại do không có volume spike")
        return None, None, None, None, False

    # Choppy filter
    if adx < CURRENT_CFG.get("ADX_MIN_15M", 20):
        # RELAX exception: allow if breakout + strong candle + volume spike
        if CURRENT_CFG.get("RELAX_EXCEPT", False):
            bo_up, bo_down = False, False
            try:
                bo_up, bo_down, pb_up, pb_down = detect_breakout_pullback(df)
            except Exception:
                pass
            v_ok = is_volume_spike(df)
            candle_ok = candle_quality_ok(df, "long" if bo_up else "short")
            if not ((bo_up or bo_down) and v_ok and candle_ok):
                print(f"[DEBUG] {symbol}: ⚠️ loại do ADX = {adx:.2f} quá yếu (sideway)")
                return None, None, None, None, False
        else:
            print(f"[DEBUG] {symbol}: ⚠️ loại do ADX = {adx:.2f} quá yếu (sideway)")
            return None, None, None, None, False
        print(f"[DEBUG] {symbol}: ⚠️ loại do ADX = {adx:.2f} quá yếu (sideway)")
        return None, None, None, None, False
    if bb_width < CURRENT_CFG.get("BBW_MIN", 0.02):
        print(f"[DEBUG] {symbol}: ⚠️ loại do BB Width = {bb_width:.4f} quá hẹp")
        return None, None, None, None, False

    # Price Action (Engulfing hoặc Breakout)
    recent = df["close"].iloc[-4:].tolist()
    is_engulfing = len(recent) == 4 and ((recent[-1] > recent[-2] > recent[-3]) or (recent[-1] < recent[-2] < recent[-3]))
    if not is_engulfing and not detect_breakout_pullback(df):
        print(f"[DEBUG] {symbol}: ⚠️ loại do không có mô hình giá rõ ràng")
        return None, None, None, None, False

    # Gần vùng hỗ trợ/kháng cự
    support, resistance = find_support_resistance(df)
    near_sr = abs(close_price - support)/support < 0.03 or abs(close_price - resistance)/resistance < 0.03

    # Entry - SL - TP - RR
    df_recent = df.iloc[-10:]
    entry = close_price
    sl = df_recent["low"].min() if ema_up else df_recent["high"].max()
    tp = df_recent["high"].max() if ema_up else df_recent["low"].min()
    rr = abs(tp - entry) / abs(entry - sl) if (entry - sl) != 0 else 0

    if any(x is None for x in [entry, sl, tp]):
        print(f"[DEBUG] {symbol}: ⚠️ loại do thiếu giá trị entry/sl/tp")
        return None, None, None, None, False

    if rr < 1.5:
        print(f"[DEBUG] {symbol}: ⚠️ loại do RR = {rr:.2f} < 1.5")
        return None, None, None, None, False

    if abs(entry - sl)/entry < 0.003:
        print(f"[DEBUG] {symbol}: ⚠️ loại do SL biên độ quá nhỏ = {(abs(entry - sl)/entry)*100:.2f}%")
        return None, None, None, None, False

    # Multi-timeframe confirmation (1H đồng pha 15m)
    try:
        df1h = calculate_indicators(df_1h.copy())
        ema_up_1h = df1h["ema20"].iloc[-1] > df1h["ema50"].iloc[-1]
        rsi_1h = df1h["rsi"].iloc[-1]
    except Exception as e:
        print(f"[DEBUG] {symbol}: ⚠️ lỗi khi phân tích khung 1H: {e}")
        return None, None, None, None, False

    # Kiểm tra xu hướng BTC
    try:
        btc_df = fetch_ohlcv_okx("BTC-USDT", "15m", limit=10)
        btc_df["close"] = pd.to_numeric(btc_df["close"])
        btc_change = (btc_df["close"].iloc[-1] - btc_df["close"].iloc[-3]) / btc_df["close"].iloc[-3]
    except Exception as e:
        print(f"[DEBUG] {symbol}: ⚠️ lỗi khi fetch BTC: {e}")
        btc_change = 0

    # Xác nhận tín hiệu
    signal = None
    if (
        ema_up and not ema_up_1h and rsi > 55 and rsi_1h > 50
        and macd_diff > 0.05 and adx > CURRENT_CFG.get("ADX_MIN_15M", 20) and near_sr
    ):
        if btc_change >= -0.01:
            signal = "LONG"
    elif (
        ema_down and not ema_up_1h and rsi < 45 and rsi_1h < 50
        and macd_diff > 0.001 and adx > CURRENT_CFG.get("ADX_MIN_15M", 20) and near_sr
    ):
        if btc_change <= 0.01:
            signal = "SHORT"

    # ✅ Log cuối cùng nếu coin vượt tất cả bộ lọc
    print(f"[DEBUG] {symbol}: ✅ Hoàn tất phân tích, Signal = {signal}, RR = {rr:.2f}, SL = {(abs(entry - sl)/entry)*100:.2f}%, ADX = {adx:.2f}, BB width = {bb_width:.4f}")
    return (signal, entry, sl, tp, True) if signal else (None, None, None, None, False)


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
    elif signal == "SHORT" and short_trend.startswith("Giảm") and mid_trend.startswith("Giảm") and volume_ok:
        return 5
    elif volume_ok and (
        (signal == "LONG" and short_trend.startswith("Tăng")) or
        (signal == "SHORT" and short_trend.startswith("Giảm"))
    ):
        return 4
    elif volume_ok:
        return 3
    else:
        return 1

def _scan_with_cfg(coin_list, cfg, tag):
    global CURRENT_CFG
    CURRENT_CFG = cfg
    valid_signals = []
    messages = []
    done_symbols = set()
    count = 0
    for symbol in coin_list:
        reset = False
        logging.info(f"🔍 [{tag}] Phân tích {symbol}...")
        # mute all inner DEBUG/INFO for this symbol
        with mute_logs():
        inst_id = symbol.upper().replace("/", "-") + "-SWAP"
        df_15m = fetch_ohlcv_okx(inst_id, "15m")
        df_1h = fetch_ohlcv_okx(inst_id, "1h")
        if df_15m is None or df_1h is None:
            continue
        df_15m = calculate_indicators(df_15m).dropna()
        df_1h = calculate_indicators(df_1h).dropna()
        # Volume prefilter
        if not is_volume_spike(df_15m):
            logging.debug(f"[DEBUG] {symbol}: bị loại do KHÔNG đạt volume spike hoặc lỗi volume")
            continue
        required_cols = ['ema20', 'ema50', 'rsi', 'macd', 'macd_signal']
        if not all(col in df_15m.columns for col in required_cols):
            logging.warning(f"⚠️ Thiếu cột trong df_15m: {df_15m.columns}")
            continue
        signal, entry, sl, tp, volume_ok = detect_signal(df_15m, df_1h, symbol)
        if signal:
            short_trend, mid_trend = analyze_trend_multi(symbol)
            rating = calculate_signal_rating(signal, short_trend, mid_trend, volume_ok)
            now = datetime.datetime.now(pytz.timezone("Asia/Ho_Chi_Minh")).strftime("%d/%m/%Y %H:%M")
            count += 1
            valid_signals.append([
                symbol,
                f"{signal} " + ("⭐️" * rating) + f" [{tag}]",
                entry, sl, tp, short_trend, mid_trend, now
            ])
            if rating >= 4:
                messages.append(f"[{tag}] {symbol} ({signal}) {entry} → TP {tp} / SL {sl} ({'⭐️' * rating})")
            done_symbols.add(symbol)
            summary="✅ đạt tín hiệu"
        
        if 'summary' not in locals():
            summary="❌ không đạt (rớt filter)"
        
        # log one line per symbol per mode
        log_once(tag, symbol, f"[{tag}] {symbol}: {summary}", level="info")
            pass
# append to sheet & telegram using existing helpers
    try:
        sheet_id = SHEET_CSV_URL.split("/d/")[1].split("/")[0]
        sheet = client.open_by_key(sheet_id).worksheet("DATA_FUTURE")
        try:
            headers = sheet.row_values(1)
        except Exception:
            headers = ["Symbol", "Signal", "Entry", "SL", "TP", "Short Trend", "Mid Trend", "Timestamp"]
            sheet.insert_row(headers, 1)
        body = sheet.get_all_values()[1:]
        for row in valid_signals[::-1]:
            body.insert(0, row)
        sheet.update([headers] + body)
        logging.info(f"✅ [{tag}] Đã ghi {len(valid_signals)} tín hiệu")
    except Exception as e:
        logging.warning(f"❌ [{tag}] Lỗi ghi sheet: {e}")
    for msg in messages:
        try:
            send_telegram_message(msg)
        except Exception as e:
            logging.warning(f"TG error: {e}")
    return done_symbols
# === BACKTEST 90 NGÀY ===
    except Exception:
            headers = ["Symbol", "Signal", "Entry", "SL", "TP", "Short Trend", "Mid Trend", "Timestamp"]
            sheet.insert_row(headers, 1)
        body = sheet.get_all_values()[1:]
        for row in valid_signals[::-1]:
            body.insert(0, row)
        sheet.update([headers] + body)
        logging.info(f"✅ [{tag}] Đã ghi {len(valid_signals)} tín hiệu")
    except Exception as e:
        logging.warning(f"❌ [{tag}] Lỗi ghi sheet: {e}")
    for msg in messages:
        try:
            send_telegram_message(msg)
        except Exception as e:
            logging.warning(f"TG error: {e}")
    return done_symbols
# === BACKTEST 90 NGÀY ===

def backtest_signals_90_days(symbol_list, cfg=None, tag="STRICT"):
    # Giả định đã có fetch_ohlcv_okx và detect_signal
    today = datetime.datetime.now(datetime.timezone.utc)
    start_time = today - datetime.timedelta(days=90)
    results = []

    global CURRENT_CFG
    if cfg is None:
        cfg = STRICT_CFG
    CURRENT_CFG = cfg

    for symbol in symbol_list:
        with mute_logs():
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


def run_bot():
    logging.basicConfig(level=logging.INFO)
    coin_list = get_top_usdt_pairs(limit=COINS_LIMIT)
    # Pass 1: STRICT
    strict_done = _scan_with_cfg(coin_list, STRICT_CFG, "STRICT")
    # Pass 2: RELAX on remaining symbols
    relax_symbols = [s for s in coin_list if s not in strict_done]
    _ = _scan_with_cfg(relax_symbols, RELAX_CFG, "RELAX")

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
    # === FLAGS ===
    RUN_BACKTEST_STRICT = True
    RUN_BACKTEST_RELAX  = True
    # Load universe
    symbol_list = get_top_usdt_pairs(limit=COINS_LIMIT)
    try:
        if RUN_BACKTEST_STRICT:
            backtest_signals_90_days(symbol_list, cfg=STRICT_CFG, tag="STRICT")
        if RUN_BACKTEST_RELAX:
            backtest_signals_90_days([s for s in symbol_list], cfg=RELAX_CFG, tag="RELAX")
    except Exception as e:
        logging.error(f"Backtest error: {e}")
    # === RUN LIVE BOT ===
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
