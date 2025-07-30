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

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)  # luôn bật DEBUG
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

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
ADX_THRESHOLD = 15
COINS_LIMIT = 200  # Số coin phân tích mỗi lượt



def rate_signal_strength(entry, sl, tp, short_trend, mid_trend):
    strength = 1
    if abs(tp - entry) / entry > 0.03:
        strength += 1
    if abs(entry - sl) / entry > 0.03:
        strength += 1
    if short_trend == mid_trend:
        strength += 1
    return '⭐' * min(strength, 5)

def append_to_sheet(row: dict):
    now = datetime.datetime.now(timezone('Asia/Ho_Chi_Minh')).strftime("%d/%m/%Y %H:%M")
    signal_text = f"{row['signal']} {rate_signal_strength(row['entry'], row['sl'], row['tp'], row['short_trend'], row['mid_trend'])}"

    row_data = [
        row['symbol'],
        signal_text,
        row['entry'],
        row['sl'],
        row['tp'],
        row['short_trend'],
        row['mid_trend'],
        now
    ]

    try:
        sheet_data = sheet.get_all_records()
        if any(r['Coin'] == row['symbol'] and row['Tín hiệu'].startswith(row['signal']) for r in sheet_data):
            logging.info(f"Đã có tín hiệu {row['symbol']} {row['signal']} → bỏ qua.")
            return

        logging.info(f"Ghi tín hiệu mới vào sheet: {row['symbol']} {row['signal']}")
        sheet.append_row(row_data)
    
    except Exception as e:
        logging.warning(f"Không thể ghi sheet: {e}")

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
        logging.debug(f"📥 Kết quả trả về từ OKX: status={response.status_code}, json={data}")

        if 'data' not in data or not data['data']:
            logging.warning(f"⚠️ Không có dữ liệu OHLCV: instId={symbol}, bar={timeframe}")
            return None

        df = pd.DataFrame(data["data"])
        df.columns = ["ts", "open", "high", "low", "close", "volume", "volCcy", "volCcyQuote", "confirm"]
        df["ts"] = pd.to_datetime(df["ts"], unit="ms")
        df = df.iloc[::-1].copy()

        # ✅ Chuyển các cột số sang float để tránh lỗi toán học
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        return df

    except Exception as e:
        logging.error(f"❌ Lỗi khi fetch ohlcv OKX cho {symbol} [{timeframe_input}]: {e}")
        return None

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
    
def detect_signal(df_15m, df_1h, symbol):
    if df_15m is None or df_1h is None:
        return None, None, None

    if len(df_15m) < 50 or df_15m[['ema20', 'ema50', 'rsi', 'macd', 'macd_signal']].isnull().any().any():
        return None, None, None
        
    if len(df_1h) < 50 or df_1h[['ema20', 'ema50', 'ema100', 'adx']].isnull().any().any():
        return None, None, None
    latest = df_15m.iloc[-1]
    trend_up = df_1h['ema20'].iloc[-1] > df_1h['ema50'].iloc[-1]
    trend_down = df_1h['ema20'].iloc[-1] < df_1h['ema50'].iloc[-1]
    logging.debug(f"{symbol}: RSI={latest['rsi']}, MACD={latest['macd']}, MACD_SIGNAL={latest['macd_signal']}, EMA20={latest['ema20']}, EMA50={latest['ema50']}, Volume={latest['volume']:.0f}")
    logging.debug(f"{symbol}: entry_long check: RSI<60? {latest['rsi'] < 60}, MACD>signal? {latest['macd'] > latest['macd_signal']}, EMA20>EMA50? {latest['ema20'] > latest['ema50']}")
    logging.debug(f"{symbol}: entry_short check: RSI>40? {latest['rsi'] > 40}, MACD<signal? {latest['macd'] < latest['macd_signal']}, EMA20<EMA50? {latest['ema20'] < latest['ema50']}")
    

    entry_long = (
        latest['rsi'] < 60 and
        latest['macd'] > latest['macd_signal'] and
        latest['ema20'] > latest['ema50']
    )

    entry_short = (
        latest['rsi'] > 40 and
        latest['macd'] < latest['macd_signal'] and
        latest['ema20'] < latest['ema50']
    )

    # Lọc xu hướng (1H)
    df1h = df_1h.copy()
    trend_up = (
        df1h['ema20'].iloc[-1] > df1h['ema50'].iloc[-1] > df1h['ema100'].iloc[-1]
        and df1h['adx'].iloc[-1] > ADX_THRESHOLD
    )
    if len(df1h) < 50 or df1h[['ema20', 'ema50', 'ema100', 'adx']].isnull().any().any():
        return None, None, None  # Bỏ qua nếu thiếu dữ liệu kỹ thuật
    
    trend_up = (
        df1h['ema20'].iloc[-1] > df1h['ema50'].iloc[-1] > df1h['ema100'].iloc[-1]
        and df1h['adx'].iloc[-1] > ADX_THRESHOLD
    )

    if entry_long and trend_up:
        return 'LONG', latest['close'], latest['low']
    elif entry_short and trend_down:
        return 'SHORT', latest['close'], latest['high']
    else:
        return None, None, None
    if entry_long:
        logging.info("✅ Tín hiệu LONG thoả điều kiện!")
        return 'LONG', latest['close'], latest['low']
    elif entry_short:
        logging.info("✅ Tín hiệu SHORT thoả điều kiện!")
        return 'SHORT', latest['close'], latest['high']
    else:
        logging.info("❌ Không thoả điều kiện LONG hoặc SHORT.")
        return None, None, None

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

def run_bot():
    logging.info("🚀 Bắt đầu phân tích tín hiệu...")

    # Load danh sách coin
    coin_list = fetch_coin_list()
    logging.info(f"✅ Đã load {len(coin_list)} coin từ sheet.")

    alert_coins = []   # Danh sách coin có tín hiệu mạnh để gửi Telegram
    count_signal = 0   # Đếm số coin có tín hiệu hợp lệ

    for row in coin_list:
        symbol = row['symbol']
        timeframe = row.get('timeframe', '15m')  # mặc định 15m nếu không có

        df_15m = fetch_ohlcv_okx(symbol, timeframe)
        df_1h = fetch_ohlcv_okx(symbol, '1h')

        if df_15m is None or df_1h is None:
            continue

        df_15m = dropna_tail(df_15m, 60)
        df_1h = dropna_tail(df_1h, 60)

        entry_ok, sl, tp = check_entry(df_15m)
        trend_ok, short_trend, mid_trend = check_trend(df_15m, df_1h)
        adx_ok = check_adx(df_1h)

        if entry_ok and trend_ok and adx_ok:
            row['entry'] = sl['entry']
            row['sl'] = sl['sl']
            row['tp'] = tp
            row['short_trend'] = short_trend
            row['mid_trend'] = mid_trend

            row_data = append_to_sheet(row)
            count_signal += 1

            # Chỉ gửi Telegram nếu >= 3⭐️
            strength = row.get("signal_strength", 0)
            if strength >= 3:
                alert_coins.append(row_data)

    # Tổng kết và gửi Telegram nếu có tín hiệu mạnh
    if alert_coins:
        message = "📢 TÍN HIỆU MẠNH (≥ 3⭐️)\n\n"
        for coin in alert_coins:
            message += f"• {coin['Coin']} ({coin['Tín hiệu']}) - Entry: {coin['Giá vào']}\n"
        send_telegram_message(message)
    else:
        logging.info("📭 Không có coin nào đạt tín hiệu mạnh (≥ 3⭐️).")

    # Tổng kết log cuối
    logging.info(f"✅ KẾT THÚC: Đã phân tích {len(coin_list)} coin. Có {count_signal} coin thỏa điều kiện.")

def get_top_usdt_pairs(limit=50):
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
