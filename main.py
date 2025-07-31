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
logger.setLevel(logging.INFO)  # luôn bật DEBUG/INFO
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

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
        logging.debug(f"📥 Kết quả trả về từ OKX: status={response.status_code}, json={data}")

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
    
def detect_signal(df_15m: pd.DataFrame, df_1h: pd.DataFrame, symbol: str):
    import numpy as np
    import logging

    # EMA
    df_15m["ema20"] = df_15m["close"].ewm(span=20).mean()
    df_15m["ema50"] = df_15m["close"].ewm(span=50).mean()

    # MACD
    df_15m["macd"] = df_15m["close"].ewm(span=12).mean() - df_15m["close"].ewm(span=26).mean()
    df_15m["macd_signal"] = df_15m["macd"].ewm(span=9).mean()

    # RSI
    delta = df_15m["close"].diff()
    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)
    avg_gain = pd.Series(gain).rolling(window=14).mean()
    avg_loss = pd.Series(loss).rolling(window=14).mean()
    rs = avg_gain / avg_loss
    df_15m["rsi"] = 100 - (100 / (1 + rs))

    # Bollinger Bands
    df_15m["bb_mid"] = df_15m["close"].rolling(window=20).mean()
    df_15m["bb_std"] = df_15m["close"].rolling(window=20).std()
    df_15m["bb_upper"] = df_15m["bb_mid"] + 2 * df_15m["bb_std"]
    df_15m["bb_lower"] = df_15m["bb_mid"] - 2 * df_15m["bb_std"]
    
    # ADX
    def calculate_adx(data, period=14):
        high, low, close = data["high"], data["low"], data["close"]
        plus_dm = high.diff().clip(lower=0)
        minus_dm = (-low.diff()).clip(lower=0)

        tr = pd.concat([
            high - low,
            (high - close.shift()).abs(),
            (low - close.shift()).abs()
        ], axis=1).max(axis=1)

        atr = tr.rolling(window=period).mean()
        plus_di = 100 * (plus_dm.ewm(alpha=1/period).mean() / atr)
        minus_di = 100 * (minus_dm.ewm(alpha=1/period).mean() / atr)
        dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di)
        return dx.ewm(alpha=1/period).mean()

    df_15m["adx"] = calculate_adx(df_15m)

    # Lấy chỉ số mới nhất
    latest = df_15m.iloc[-1]
    ema_up = latest["ema20"] > latest["ema50"]
    ema_down = latest["ema20"] < latest["ema50"]
    macd_cross_up = latest["macd"] > latest["macd_signal"]
    macd_cross_down = latest["macd"] < latest["macd_signal"]
    rsi = latest["rsi"]
    adx = latest["adx"]
    close_price = latest["close"]

    # Reversal check (nến đảo chiều gần nhất)
    # recent là danh sách giá đóng cửa 4 nến gần nhất
    recent = df_15m["close"].iloc[-4:].tolist()
    
    # Chỉ xử lý nếu recent đủ 4 nến
    if len(recent) == 4:
        is_bearish_reversal = all(recent[i] < recent[i-1] for i in range(1, 4))
        is_bullish_reversal = all(recent[i] > recent[i-1] for i in range(1, 4))
    else:
        is_bearish_reversal = False
        is_bullish_reversal = False
        
    # --- Volume ---
    try:
        vol_now = df_15m['volume'].iloc[-1]
        vol_avg = df_15m['volume'].rolling(20).mean().iloc[-1]
        volume_ok = vol_now > 0.7 * vol_avg
        logging.debug(f"{symbol}: Volume hiện tại = {vol_now:.2f}, TB 20 nến = {vol_avg:.2f}")
    except Exception as e:
        logging.warning(f"{symbol}: Không tính được volume: {e}")
        volume_ok = False

    if not volume_ok:
        logging.info(f"{symbol}: Volume yếu → bỏ qua tín hiệu.")
        return None, None, None
        
    # Logic vào lệnh
    signal = None
    if (
        rsi > 60 and ema_up and macd_cross_up and adx > 20
        and close_price < latest["bb_upper"]
        and not is_bearish_reversal
    ):
        signal = "LONG"
    elif (
        rsi < 40 and ema_down and macd_cross_down and adx > 20
        and close_price > latest["bb_lower"]
        and not is_bullish_reversal
    ):
        signal = "SHORT"

    if signal:
        return signal, None, None  # SL/TP sẽ xử lý sau

    return None, None, None

    # --- Entry / SL ---
    entry = latest['close']
    
    # Dữ liệu 10 nến gần nhất
    df_recent = df_15m.iloc[-10:]
    
    if signal == "LONG":
        sl = df_recent['low'].min()   # Swing Low = điểm thấp nhất
        tp = df_recent['high'].max()  # Swing High = điểm cao nhất
    elif signal == "SHORT":
        sl = df_recent['high'].max()  # Swing High = điểm cao nhất
        tp = df_recent['low'].min()   # Swing Low = điểm thấp nhất
    else:
        return None, None, None
    
    return signal, entry, sl, tp, volume_ok

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

    valid_signals = []
    messages = []
    count = 0

    for symbol in coin_list:
        logging.info(f"🔍 Phân tích {symbol}...")

        inst_id = symbol.upper().replace("/", "-") + "-SWAP"

        df_15m = fetch_ohlcv_okx(inst_id, "15m")
        df_1h = fetch_ohlcv_okx(inst_id, "1h")

        if df_15m is None or df_1h is None:
            continue

        df_15m = calculate_indicators(df_15m).dropna()
        df_1h = calculate_indicators(df_1h).dropna()
        # ✅ Tính volume hiện tại và trung bình 20 nến gần nhất
        try:
            vol_now = df_15m['volume'].iloc[-1]
            vol_avg = df_15m['volume'].rolling(20).mean().iloc[-1]
            volume_ok = vol_now > 0.6 * vol_avg
            logging.debug(f"{symbol}: Volume hiện tại = {vol_now:.0f}, TB 20 nến = {vol_avg:.0f}, volume_ok = {volume_ok}")
        except Exception as e:
            logging.warning(f"{symbol}: Không tính được volume_ok: {e}")
            volume_ok = False

        required_cols = ['ema20', 'ema50', 'rsi', 'macd', 'macd_signal']
        if not all(col in df_15m.columns for col in required_cols):
            logging.warning(f"⚠️ Thiếu cột trong df_15m: {df_15m.columns}")
            continue

        if df_15m[required_cols].isnull().any().any():
            logging.warning(f"⚠️ Có giá trị null trong df_15m: {df_15m[required_cols].isnull().sum().to_dict()}")
            continue

        signal, entry, sl, tp, volume_ok = detect_signal(df_15m, df_1h, symbol)

        if signal:
            short_trend, mid_trend = analyze_trend_multi(symbol)
            rating = calculate_signal_rating(signal, short_trend, mid_trend, volume_ok)  # ⭐️⭐️⭐️...

            now = datetime.datetime.now(pytz.timezone("Asia/Ho_Chi_Minh")).strftime("%d/%m/%Y %H:%M")
            # 🟢 LƯU VÀO GOOGLE SHEET nếu rating >= 1
            count += 1
            valid_signals.append([
                symbol,
                signal + " " + ("⭐️" * rating),
                entry,
                sl,
                tp,
                short_trend,
                mid_trend,
                now
            ])
            
            # 🟡 GỬI TELEGRAM nếu rating >= 3
            if rating >= 3:
                messages.append(
                    f"{symbol} ({signal}) {entry} → TP {tp} / SL {sl} ({'⭐️' * rating})"
                )
        
        time.sleep(1)
    
    # ✅ Gửi 1 tin nhắn tổng hợp
    if messages:
        message = "🆕 *TỔNG HỢP TÍN HIỆU MỚI*\n\n" + "\n".join(messages)
        send_telegram_message(message)

    # ✅ Ghi 1 lần duy nhất vào sheet
    if valid_signals:
        try:
            sheet = client.open_by_key(sheet_id).worksheet("DATA_FUTURE")
            for row in valid_signals:
                prepend_to_sheet(row)
            clean_old_rows()
        except Exception as e:
            logging.warning(f"Không thể ghi sheet: {e}")
        
    # ✅ Log tổng kết
    logging.info(f"✅ KẾT THÚC: Đã phân tích {len(coin_list)} coin. Có {count} coin thoả điều kiện.")
    
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
                if (today - row_date).days <= 2:
                    new_rows.append(row)
            except:
                new_rows.append(row)  # Nếu lỗi parse date thì giữ lại

        # Ghi lại: headers + rows mới
        sheet.update([headers] + new_rows)
        logging.info(f"🧹 Đã xoá những dòng quá 3 ngày (giữ lại {len(new_rows)} dòng)")

    except Exception as e:
        logging.warning(f"❌ Lỗi khi xoá dòng cũ: {e}")

def get_top_usdt_pairs(limit=200):
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
