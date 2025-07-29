import requests
import pandas as pd
import numpy as np
import time
import datetime
import json
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)  # luôn bật DEBUG
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

# ========== CẤU HÌNH ==========
TELEGRAM_TOKEN = '8467137353:AAFn2ualduQI8DIsIoy56ECWrf0eS82fwc0'
TELEGRAM_CHAT_ID = '8467137353'
SHEET_CSV_URL = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vQLyNCM0zVtZyDJZ6pPhhHv6EvQbgo1L0RgWhdwSOKc_TEr_qz_3b_zZkUO9HdIpElWmnqMddF_BIfZ/pub?gid=1144515173&single=true&output=csv'

# ========== THAM SỐ KỸ THUẬT ==========
TP_MULTIPLIER = 1.5
SL_MULTIPLIER = 1.0
ADX_THRESHOLD = 15
COINS_LIMIT = 200  # Số coin phân tích mỗi lượt

import requests
import pandas as pd
import logging

def fetch_ohlcv_okx(symbol: str, timeframe: str = "15m", limit: int = 100):
    try:
        timeframe = timeframe.lower()  # ✅ Đảm bảo đúng định dạng bar
        url = f"https://www.okx.com/api/v5/market/candles?instId={symbol}&bar={timeframe}&limit={limit}"
        logger.debug(f"📤 Gửi request nến OKX: {url}")
        
        res = requests.get(url)
        res_json = res.json()

        # Kiểm tra mã lỗi từ OKX
        if res_json.get("code") != "0":
            msg = res_json.get("msg", "Không rõ lỗi")
            logger.warning(f"⚠️ Không có dữ liệu nến cho {symbol} [{timeframe}]. Lỗi API: {msg}")
            return None

        raw = res_json.get("data", [])
        if not raw:
            logger.warning(f"⚠️ Không có nến cho {symbol} [{timeframe}]")
            return None

        # OKX trả về 9 cột: [ts, o, h, l, c, vol, vol_ccy, confirm, tag]
        try:
            df = pd.DataFrame(raw, columns=[
                "ts", "open", "high", "low", "close",
                "volume", "volume_ccy",
                "confirm", "tag"
            ])
        except Exception as e:
            logger.error(f"❌ Lỗi khi fetch OHLCV cho {symbol} [{timeframe}]: {e}")
            return None

        df = df[::-1]  # đảo ngược thời gian (cũ -> mới)
        df["ts"] = pd.to_datetime(df["ts"], unit="ms")
        df["close"] = df["close"].astype(float)
        df["volume"] = df["volume"].astype(float)

        return df

    except Exception as e:
        logger.error(f"❌ Lỗi fetch_ohlcv_okx({symbol}, {timeframe}): {e}")
        return None


def calculate_indicators(df):
    df['ema20'] = df['close'].ewm(span=20).mean()
    df['ema50'] = df['close'].ewm(span=50).mean()
    df['ema100'] = df['close'].ewm(span=100).mean()

    delta = df['close'].diff()
    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)
    avg_gain = pd.Series(gain).rolling(window=14).mean()
    avg_loss = pd.Series(loss).rolling(window=14).mean()
    rs = avg_gain / avg_loss
    df['rsi'] = 100 - (100 / (1 + rs))

    exp1 = df['close'].ewm(span=12).mean()
    exp2 = df['close'].ewm(span=26).mean()
    df['macd'] = exp1 - exp2
    df['macd_signal'] = df['macd'].ewm(span=9).mean()

    df['atr'] = (df['high'] - df['low']).rolling(window=14).mean()

    df['upper_bb'] = df['close'].rolling(window=20).mean() + 2 * df['close'].rolling(window=20).std()
    df['lower_bb'] = df['close'].rolling(window=20).mean() - 2 * df['close'].rolling(window=20).std()

    df['adx'] = calculate_adx(df)
    return df


def calculate_adx(df, period=14):
    high = df['high']
    low = df['low']
    close = df['close']

    plus_dm = high.diff()
    minus_dm = low.diff()

    plus_dm[plus_dm < 0] = 0
    minus_dm[minus_dm > 0] = 0
    minus_dm = minus_dm.abs()

    tr1 = high - low
    tr2 = abs(high - close.shift())
    tr3 = abs(low - close.shift())
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    atr = tr.rolling(window=period).mean()

    plus_di = 100 * (plus_dm.rolling(window=period).mean() / atr)
    minus_di = 100 * (minus_dm.rolling(window=period).mean() / atr)

    dx = (abs(plus_di - minus_di) / (plus_di + minus_di)) * 100
    adx = dx.rolling(window=period).mean()
    return adx
    
def detect_signal(df_15m, df_1h):
    if df_15m is None or df_1h is None:
        return None, None, None

    if len(df_15m) < 50 or df_15m[['ema20', 'ema50', 'rsi', 'macd', 'macd_signal']].isnull().any().any():
        return None, None, None
        
    if len(df_1h) < 50 or df_1h[['ema20', 'ema50', 'ema100', 'adx']].isnull().any().any():
        return None, None, None
    latest = df_15m.iloc[-1]
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
def send_telegram(msg: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": msg,
        "parse_mode": "Markdown"
    }
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        logging.error(f"Lỗi gửi Telegram: {e}")


def append_to_sheet(row: dict):
    now = datetime.datetime.now().strftime("%d/%m/%Y %H:%M")
    row_data = {
        'Coin': row['symbol'],
        'Tín hiệu': row['signal'],
        'Giá vào': row['entry'],
        'SL': row['sl'],
        'TP': row['tp'],
        'Xu hướng ngắn hạn': row['short_trend'],
        'Xu hướng trung hạn': row['mid_trend'],
        'Ngày': now
    }

    sheet_url = SHEET_CSV_URL.replace("/pub?", "/gviz/tq?tqx=out:csv&")
    try:
        sheet_df = pd.read_csv(sheet_url)
        if 'Coin' in sheet_df.columns:
            if any((sheet_df['Coin'] == row['symbol']) & (sheet_df['Tín hiệu'] == row['signal'])):
                logging.info(f"Đã có tín hiệu {row['symbol']} {row['signal']} → bỏ qua.")
                return
    except:
        logging.warning("Không kiểm tra được sheet cũ → ghi thêm.")

    sheet_append_url = SHEET_CSV_URL.replace('/edit?gid=', '/formResponse?gid=')
    logging.warning("Google Sheet đang ở dạng chỉ đọc. Cần dùng Google API để ghi nếu muốn ghi trực tiếp.")
    # Nếu có quyền ghi Google Sheet (OAuth/ServiceAccount) thì dùng gspread để append
def run_bot():
    logging.basicConfig(level=logging.INFO)
    coin_list = get_top_usdt_pairs(limit=COINS_LIMIT)
    count = 0

    for symbol in coin_list:
        logging.info(f"📈 Phân tích {symbol}...")
        df_15m = fetch_ohlcv(symbol, '15m')
        df_1h = fetch_ohlcv(symbol, '1h')
        if df_15m is None or df_1h is None:
            continue

        df_15m = calculate_indicators(df_15m)
        df_1h = calculate_indicators(df_1h)

        signal, entry, sl = detect_signal(df_15m, df_1h)
        if signal:
            tp = entry + (entry - sl) * TP_MULTIPLIER if signal == 'LONG' else entry - (sl - entry) * TP_MULTIPLIER
            short_trend, mid_trend = analyze_trend_multi(symbol)

            message = f"""📢 *TÍN HIỆU MỚI*  
*Coin:* {symbol}  
*Loại:* {signal}  
*Entry:* {round(entry, 4)}  
*SL:* {round(sl, 4)}  
*TP:* {round(tp, 4)}  
*Xu hướng:*  
- Ngắn hạn: {short_trend}  
- Trung hạn: {mid_trend}"""

            send_telegram(message)

            row = {
                'symbol': symbol,
                'signal': signal,
                'entry': entry,
                'sl': sl,
                'tp': tp,
                'short_trend': short_trend,
                'mid_trend': mid_trend
            }

            append_to_sheet(row)

        time.sleep(1)
    logging.info(f"✅ KẾT THÚC: Đã phân tích {len(coin_list)} coin. Có {count} coin thoả điều kiện.")

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
