import os
import json
import time
import math
import hmac
import base64
import hashlib
import logging
from datetime import datetime, timedelta, timezone 
import requests
import pandas as pd
import numpy as np

import gspread
from google.oauth2 import service_account

# ========== CONFIG ==========
OKX_BASE_URL = "https://www.okx.com"
CACHE_FILE = os.getenv("TRADE_CACHE_FILE", "trade_cache.json")

# Trading config
FUT_LEVERAGE = 6              # x6 isolated
NOTIONAL_PER_TRADE = 25.0     # 25 USDT position size (ký quỹ ~5$ với x5)
MAX_TRADES_PER_RUN = 10        # tối đa 10 lệnh / 1 lần cron

# Scanner config
MIN_ABS_CHANGE_PCT = 2.0      # chỉ lấy coin |24h change| >= 2%
MIN_VOL_USDT = 100000         # min 24h volume quote
TOP_N_BY_CHANGE = 300          # universe: top 300 theo độ biến động

# Google Sheet headers
SHEET_HEADERS = ["Coin", "Tín hiệu", "Entry", "SL", "TP", "Ngày"]

# ======== DYNAMIC TP CONFIG ========
TP_DYN_MIN_PROFIT_PCT   = 3.0   # chỉ bật TP động khi lãi >= 3%
TP_DYN_MAX_FLAT_BARS    = 3     # số nến 5m gần nhất để kiểm tra
TP_DYN_VOL_DROP_RATIO   = 0.5   # vol hiện tại < 50% avg 10 nến -> yếu
TP_DYN_EMA_LEN          = 5     # EMA-5
TP_DYN_FLAT_BARS = 2      # số nến 5m đi ngang trước khi thoát
TP_DYN_ENGULF = True      # bật thoát khi có engulfing
TP_DYN_VOL_DROP = True    # bật thoát khi vol giảm mạnh
TP_DYN_EMA_TOUCH = True   # bật thoát khi chạm EMA5
# ========== PUMP/DUMP PRO CONFIG ==========

PUMP_MIN_ABS_CHANGE_24H = 2.0       # |%change 24h| tối thiểu để được xem xét (lọc coin chết)
PUMP_MIN_VOL_USDT_24H   = 20000   # volume USDT 24h tối thiểu
PUMP_PRE_TOP_N          = 300       # lấy top 300 coin theo độ biến động 24h để refine

PUMP_MIN_CHANGE_15M     = 1.0       # %change 15m tối thiểu theo hướng LONG/SHORT
PUMP_MIN_CHANGE_5M      = 0.5       # %change 5m tối thiểu
PUMP_VOL_SPIKE_RATIO    = 0.1       # vol 15m hiện tại phải > 1x vol avg 10 nến trước

PUMP_MIN_CHANGE_1H      = 0.5       # %change 1h tối thiểu (tránh sóng quá yếu)
PUMP_MAX_CHANGE_1H      = 100.0      # %change 1h tối đa (tránh đu quá trễ)

# ================== HELPERS CHUNG ==================

def safe_float(x, default=0.0):
    """Ép kiểu float an toàn, nếu lỗi trả về default."""
    try:
        return float(x)
    except Exception:
        return default


def percent_change(new, old):
    """Tính % thay đổi giữa 2 giá trị."""
    if old == 0:
        return 0.0
    return (new - old) / old * 100.0
    
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%b %d %I:%M:%S %p",
    )


def now_str_vn():
    # Render dùng UTC -> +7h cho giờ VN
    return (datetime.utcnow() + timedelta(hours=7)).strftime("%d/%m/%Y %H:%M")
def is_quiet_hours_vn():
    """
    Trả về True nếu đang trong khung giờ 22h–06h (giờ VN),
    dùng để tắt Telegram ban đêm.
    """
    now_vn = datetime.utcnow() + timedelta(hours=7)
    return now_vn.hour >= 22 or now_vn.hour < 6
def is_backtest_time_vn():
    """
    Trả về True nếu giờ VN nằm trong khoảng 19:00 - 19:10.
    (bot chạy trong khung 10 phút đó thì sẽ chạy thêm backtest)
    """
    now_vn = datetime.utcnow() + timedelta(hours=7)
    return now_vn.hour == 19 and now_vn.minute <= 15

# ========== OKX REST CLIENT ==========

class OKXClient:
    def __init__(self, api_key, api_secret, passphrase, simulated_trading=False):
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        self.simulated_trading = simulated_trading

    def _timestamp(self):
        # ISO8601 ms format
        return (
            datetime.utcnow()
            .replace(tzinfo=timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z")
        )
    def set_leverage(self, instId, lever=5, posSide="long", mgnMode="isolated"):
        path = "/api/v5/account/set-leverage"
        body = {
            "instId": instId,
            "lever": str(lever),
            "mgnMode": mgnMode,
            "posSide": posSide
        }
    
        headers = self._headers("POST", path, body)
        r = requests.post(OKX_BASE_URL + path, headers=headers, data=json.dumps(body))
        print("[INFO] SET LEVERAGE RESP:", r.text)
        return r.json()


    def _sign(self, timestamp, method, path, body):
        if body is None:
            body = ""
        message = f"{timestamp}{method}{path}{body}"
        mac = hmac.new(
            self.api_secret.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256,
        )
        d = mac.digest()
        return base64.b64encode(d).decode()

    def _headers(self, method, path, body):
        ts = self._timestamp()
        sign = self._sign(ts, method, path, body)
        headers = {
            "OK-ACCESS-KEY": self.api_key,
            "OK-ACCESS-SIGN": sign,
            "OK-ACCESS-TIMESTAMP": ts,
            "OK-ACCESS-PASSPHRASE": self.passphrase,
            "Content-Type": "application/json",
        }
        if self.simulated_trading:
            headers["x-simulated-trading"] = "1"

        # DEBUG ký OKX
        logging.info("======== OKX SIGN DEBUG ========")
        logging.info("Method: %s", method)
        logging.info("Path: %s", path)
        logging.info("Timestamp: %s", ts)
        logging.info("Message for HMAC: %s", f"{ts}{method}{path}{body}")
        logging.info("Signature: %s", sign)
        logging.info("Headers: %s", headers)
        logging.info("================================")

        return headers

    def _request(self, method, path, params=None, body_dict=None):
        url = OKX_BASE_URL + path
        body_str = json.dumps(body_dict) if body_dict is not None else ""
        headers = self._headers(method, path, body_str if method == "POST" else "")
        try:
            if method == "GET":
                r = requests.get(url, headers=headers, params=params, timeout=15)
            else:
                r = requests.post(
                    url, headers=headers, params=params, data=body_str, timeout=15
                )

            if r.status_code != 200:
                logging.error("❌ OKX REQUEST FAILED")
                logging.error("URL: %s", r.url)
                logging.error("Status Code: %s", r.status_code)
                logging.error("Response: %s", r.text)
                r.raise_for_status()

            data = r.json()
            if data.get("code") != "0":
                logging.error(
                    "❌ OKX RESPONSE ERROR code=%s msg=%s",
                    data.get("code"),
                    data.get("msg"),
                )
            return data
        except Exception as e:
            logging.exception("Exception when calling OKX: %s", e)
            raise

    # ---------- PUBLIC ----------

    def get_spot_tickers(self):
        path = "/api/v5/market/tickers"
        params = {"instType": "SPOT"}
        data = self._request("GET", path, params=params)
        return data.get("data", [])

    def get_candles(self, inst_id, bar="15m", limit=100):
        path = "/api/v5/market/candles"
        params = {"instId": inst_id, "bar": bar, "limit": str(limit)}
        data = self._request("GET", path, params=params)
        return data.get("data", [])
    def get_swap_tickers(self):
        """
        Lấy toàn bộ tickers FUTURES (SWAP) trên OKX.
        Trả về list các dict: [{'instId': 'BTC-USDT-SWAP', ...}, ...]
        """
        path = "/api/v5/market/tickers"
        params = {"instType": "SWAP"}
        data = self._request("GET", path, params=params)
        return data.get("data", [])

    def get_swap_instruments(self):
        path = "/api/v5/public/instruments"
        params = {"instType": "SWAP"}
        data = self._request("GET", path, params=params)
        return data.get("data", [])

    # ---------- PRIVATE ----------
    #def get_open_positions(self):
        #"""
        #Lấy danh sách vị thế futures (SWAP) đang mở trên OKX.
        #"""
        #path = "/api/v5/account/positions?instType=SWAP"
        #data = self._request("GET", path, params=None)
        #return data.get("data", [])
    
    def get_open_positions(self):
        """
        Lấy danh sách vị thế futures (SWAP) đang mở trên OKX.
        """
        path = "/api/v5/account/positions?instType=SWAP"  # path gồm luôn query
        data = self._request("GET", path, params=None)    # KHÔNG dùng params
        return data.get("data", [])
        
    def get_usdt_balance(self):
        # NOTE: path bao gồm luôn query string để ký chính xác
        path = "/api/v5/account/balance?ccy=USDT"

        # không truyền params nữa, query đã nằm trong path
        data = self._request("GET", path, params=None)

        details = data.get("data", [])
        if not details:
            return 0.0

        detail = details[0]
        if "details" in detail and detail["details"]:
            avail = float(detail["details"][0].get("availBal", "0"))
        else:
            avail = float(detail.get("availBal", "0"))

        logging.info("[INFO] USDT khả dụng: %.8f", avail)
        return avail

    def set_isolated_leverage(self, inst_id, lever=FUT_LEVERAGE):
        path = "/api/v5/account/set-leverage"
        body = {
            "instId": inst_id,
            "lever": str(lever),
            "mgnMode": "isolated",
        }
        data = self._request("POST", path, body_dict=body)
        return data

    def place_futures_market_order(
        self, inst_id, side, pos_side, sz, td_mode="isolated", lever=FUT_LEVERAGE
    ):
        """
        side: buy/sell
        pos_side: long/short
        sz: contracts (string)
        """
        path = "/api/v5/trade/order"
        body = {
            "instId": inst_id,
            "tdMode": td_mode,
            "side": side,
            "posSide": pos_side,
            "ordType": "market",
            "sz": str(sz),
            "lever": str(lever),
        }
        logging.info("---- PLACE FUTURES MARKET ORDER ----")
        logging.info("Body: %s", body)
        data = self._request("POST", path, body_dict=body)
        logging.info("[OKX ORDER RESP] %s", data)
        return data

    def place_oco_tp_sl(
        self, inst_id, pos_side, side_close, sz, tp_px, sl_px, td_mode="isolated"
    ):
        """
        OCO TP/SL – 1 khớp thì lệnh kia tự hủy.
        """
        path = "/api/v5/trade/order-algo"
        body = {
            "instId": inst_id,
            "tdMode": td_mode,
            "side": side_close,
            "posSide": pos_side,
            "ordType": "oco",
            "sz": str(sz),
            "tpTriggerPx": f"{tp_px:.8f}",
            "tpOrdPx": "-1",  # market
            "slTriggerPx": f"{sl_px:.8f}",
            "slOrdPx": "-1",  # market
            "tpTriggerPxType": "last",
            "slTriggerPxType": "last",
        }
        logging.info("---- PLACE OCO TP/SL ----")
        logging.info("Body: %s", body)
        data = self._request("POST", path, body_dict=body)
        logging.info("[OKX OCO RESP] %s", data)
        return data

def load_trade_cache():
    """
    Đọc cache lệnh đã vào từ file JSON.
    Trả về list[dict].
    """
    if not os.path.exists(CACHE_FILE):
        return []
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        return []
    except Exception:
        return []


def save_trade_cache(trades):
    """
    Ghi lại list trades vào file JSON.
    """
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(trades, f, ensure_ascii=False)
    except Exception as e:
        logging.error("Lỗi save cache: %s", e)


def append_trade_to_cache(trade: dict):
    """
    Thêm 1 lệnh mới vào cache.
    trade: {'coin', 'signal', 'entry', 'tp', 'sl', 'time'}
    """
    trades = load_trade_cache()
    trades.append(trade)
    save_trade_cache(trades)
def eval_trades_with_prices(trades, price_map, only_today: bool):
    """
    Đếm TP/SL/OPEN cho list trades với price_map hiện tại.
    only_today=True -> chỉ tính lệnh của ngày hôm nay (theo time trong trade).
    """
    now_vn = datetime.utcnow() + timedelta(hours=7)
    today_str = now_vn.strftime("%d/%m/%Y")

    total = 0
    tp_count = 0
    sl_count = 0
    open_count = 0

    for t in trades:
        try:
            coin = t.get("coin")
            signal = str(t.get("signal") or "").upper()
            entry = float(t.get("entry") or 0)
            tp = float(t.get("tp") or 0)
            sl = float(t.get("sl") or 0)
            time_s = str(t.get("time") or "")

            if only_today and not time_s.startswith(today_str):
                continue

            price = price_map.get(coin)
            if price is None or price == 0:
                continue

            total += 1
            status = "OPEN"

            if signal == "LONG":
                if tp > 0 and price >= tp:
                    status = "TP"
                elif sl > 0 and price <= sl:
                    status = "SL"
            elif signal == "SHORT":
                if tp > 0 and price <= tp:
                    status = "TP"
                elif sl > 0 and price >= sl:
                    status = "SL"

            if status == "TP":
                tp_count += 1
            elif status == "SL":
                sl_count += 1
            else:
                open_count += 1
        except Exception:
            continue

    closed = tp_count + sl_count
    winrate = (tp_count / closed * 100) if closed > 0 else 0.0
    return total, tp_count, sl_count, open_count, winrate
def run_backtest_if_needed(okx: "OKXClient"):
    """
    Nếu đang trong khung giờ backtest (19:00 - 19:10 VN)
    thì chạy backtest với cache và gửi 1 tin Telegram.
    """
    if not is_backtest_time_vn():
        return

    trades = load_trade_cache()
    if not trades:
        logging.info("[BACKTEST] Cache trống, không có lệnh nào.")
        return

    # Lấy giá spot hiện tại cho toàn bộ USDT pairs
    try:
        tickers = okx.get_spot_tickers()
    except Exception as e:
        logging.error("[BACKTEST] Lỗi lấy giá OKX: %s", e)
        return

    price_map = {}
    for t in tickers:
        inst_id = t.get("instId")
        try:
            last = float(t.get("last", "0") or "0")
        except Exception:
            continue
        price_map[inst_id] = last

    # 1) Toàn bộ lịch sử cache
    total_all, tp_all, sl_all, open_all, win_all = eval_trades_with_prices(
        trades, price_map, only_today=False
    )

    # 2) Riêng ngày hôm nay
    total_today, tp_today, sl_today, open_today, win_today = eval_trades_with_prices(
        trades, price_map, only_today=True
    )

    msg = (
        f"[BT ALL] total={total_all} TP={tp_all} SL={sl_all} OPEN={open_all} win={win_all:.1f}%\n"
        f"[BT TODAY] total={total_today} TP={tp_today} SL={sl_today} OPEN={open_today} win={win_today:.1f}%"
    )

    logging.info(msg)
    send_telegram_message(msg)

# ========== GOOGLE SHEETS ==========

def get_gsheet_client():
    json_str = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    if not json_str:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON not set")
    info = json.loads(json_str)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = service_account.Credentials.from_service_account_info(
        info, scopes=scopes
    )
    return gspread.authorize(credentials)


def prepare_worksheet():
    spreadsheet_id = os.getenv("GOOGLE_SPREADSHEET_ID")
    worksheet_name = os.getenv("GOOGLE_WORKSHEET_NAME", "OKX_FUTURES")

    if not spreadsheet_id:
        raise RuntimeError("GOOGLE_SPREADSHEET_ID not set")

    gc = get_gsheet_client()
    sh = gc.open_by_key(spreadsheet_id)
    try:
        ws = sh.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=worksheet_name, rows="1000", cols="10")

    # Header
    existing = ws.row_values(1)
    if not existing:
        ws.insert_row(SHEET_HEADERS, 1)
    return ws


def get_recent_signals(ws, lookback_hours=24):
    records = ws.get_all_records()
    recent = set()
    cutoff = datetime.utcnow() + timedelta(hours=7) - timedelta(hours=lookback_hours)
    for row in records:
        try:
            date_str = row.get("Ngày") or row.get("Ngay") or ""
            dt = datetime.strptime(date_str, "%d/%m/%Y %H:%M")
            if dt >= cutoff:
                key = (row.get("Coin"), row.get("Tín hiệu"))
                recent.add(key)
        except Exception:
            continue
    return recent


def append_signals(ws, trades):
    rows = []
    for t in trades:
        rows.append(
            [
                t["coin"],
                t["signal"],
                f"{t['entry']:.8f}",
                f"{t['sl']:.8f}",
                f"{t['tp']:.8f}",
                t["time"],
            ]
        )
    if rows:
        ws.append_rows(rows, value_input_option="USER_ENTERED")
        logging.info(
            "[INFO] Đã append %d lệnh mới vào Google Sheet.", len(rows)
        )


# ========== TELEGRAM ==========

def send_telegram_message(text):
    # 1. Tắt thông báo trong khung giờ 22h–06h (giờ VN)
    if is_quiet_hours_vn():
        logging.info("[INFO] Quiet hours (22h–06h VN), skip Telegram.")
        return

    # 2. Gửi như bình thường ngoài khung giờ trên
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        logging.warning(
            "TELEGRAM_BOT_TOKEN hoặc TELEGRAM_CHAT_ID chưa cấu hình, bỏ qua gửi Telegram."
        )
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
    try:
        r = requests.post(url, json=payload, timeout=15)
        if r.status_code != 200:
            logging.error("Gửi Telegram lỗi: %s %s", r.status_code, r.text)
    except Exception as e:
        logging.exception("Exception khi gửi Telegram: %s", e)


# ========== SCANNER LOGIC ==========

def build_signals_pump_dump_pro(okx: "OKXClient"):
    """
    Bộ lọc PUMP/DUMP PRO:

    B1: Lấy toàn bộ SPOT tickers:
        - lọc theo |change24h| và vol24h (PUMP_MIN_ABS_CHANGE_24H, PUMP_MIN_VOL_USDT_24H)
        - sort theo abs_change24h, lấy top PUMP_PRE_TOP_N làm ứng viên

    B2: Với MỖI coin ứng viên:
        - Lấy 15m candles:
            + change_15m: (close_now - close_15m_trước) / close_15m_trước
            + change_1h:  (close_now - close_1h_trước) / close_1h_trước (~4 nến)
            + vol spike: vol_now > PUMP_VOL_SPIKE_RATIO * avg_vol_10_nến_trước
        - Lấy 5m candles:
            + change_5m
            + thân nến xung lực (body lớn, close gần high/low)

        - Điều kiện LONG:
            + change_15m >= PUMP_MIN_CHANGE_15M
            + change_5m  >= PUMP_MIN_CHANGE_5M
            + PUMP_MIN_CHANGE_1H <= change_1h <= PUMP_MAX_CHANGE_1H
            + vol spike
            + nến 5m cuối là nến xanh mạnh, close gần high

        - Điều kiện SHORT: ngược lại

    Trả về DataFrame giống format cũ:
        columns: instId, direction, change_pct, abs_change, last_price, vol_quote, score
    """

    # -------- B1: pre-filter bằng FUTURES tickers 24h (SWAP) --------
    try:
        fut_tickers = okx.get_swap_tickers()
    except Exception as e:
        logging.error("[PUMP_PRO] Lỗi get_swap_tickers: %s", e)
        return pd.DataFrame(
            columns=[
                "instId",
                "direction",
                "change_pct",
                "abs_change",
                "last_price",
                "vol_quote",
                "score",
            ]
        )

    pre_rows = []
    for t in fut_tickers:
        # t đôi khi là string ("BTC-USDT-SWAP"), đôi khi là dict {"instId": "..."}
        if isinstance(t, str):
            fut_id = t
        else:
            fut_id = t.get("instId", "")
        if not fut_id:
            continue

        # spot_id dùng làm "coin" chung cho bot & Google Sheet
        inst_id = fut_id.replace("-SWAP", "")   # "MOODENG-USDT"

        last = safe_float(t.get("last"))
        open24 = safe_float(t.get("open24h"))
        vol_quote = safe_float(t.get("volCcy24h"))  # volume theo USDT 24h

        if last <= 0 or open24 <= 0:
            continue

        change24 = percent_change(last, open24)
        abs_change24 = abs(change24)

        # chỉ lấy futures có biến động & volume đủ lớn
        if abs_change24 < PUMP_MIN_ABS_CHANGE_24H:
            continue
        if vol_quote < PUMP_MIN_VOL_USDT_24H:
            continue

        pre_rows.append(
            {
                "instId": inst_id,           # giữ dạng "MOODENG-USDT" như cũ
                "swapId": fut_id,         # dùng để gọi candles (ABC-USDT-SWAP)
                "last": last,
                "change24": change24,
                "abs_change24": abs_change24,
                "vol_quote": vol_quote,
            }
        )

    if not pre_rows:
        logging.info("[PUMP_PRO] Không có futures nào qua pre-filter 24h.")
        return pd.DataFrame(
            columns=[
                "instId",
                "direction",
                "change_pct",
                "abs_change",
                "last_price",
                "vol_quote",
                "score",
            ]
        )

    pre_df = pd.DataFrame(pre_rows)
    pre_df = pre_df.sort_values("abs_change24", ascending=False)
    pre_df = pre_df.head(PUMP_PRE_TOP_N)

    logging.info(
        "[PUMP_PRO] Pre-filter FUTURES còn %d coin ứng viên (top %d theo biến động 24h).",
        len(pre_df),
        PUMP_PRE_TOP_N,
    )

    # -------- B2: refine bằng 15m & 5m --------
    final_rows = []
    for row in pre_df.itertuples():
        inst_id = row.instId
        last_price = row.last
        vol_quote = row.vol_quote
        swap_id = getattr(row, "swapId", inst_id)
        # 15m candles
        try:
            c15 = okx.get_candles(swap_id, bar="15m", limit=20)
        except Exception as e:
            logging.warning("[PUMP_PRO] Lỗi get_candles 15m cho %s: %s", inst_id, e)
            continue
        if not c15 or len(c15) < 6:  # cần ít nhất 6 nến để tính 1h
            continue

        # sort theo thời gian tăng dần
        try:
            c15_sorted = sorted(c15, key=lambda x: int(x[0]))
        except Exception:
            c15_sorted = c15

        # nến hiện tại và nến trước đó
        try:
            o_now = safe_float(c15_sorted[-1][1])
            h_now = safe_float(c15_sorted[-1][2])
            l_now = safe_float(c15_sorted[-1][3])
            c_now = safe_float(c15_sorted[-1][4])
            vol_now = safe_float(c15_sorted[-1][5])  # thường là volCcy hoặc vol
        except Exception:
            continue
        try:
            c_15m_prev = safe_float(c15_sorted[-2][4])
        except Exception:
            c_15m_prev = c_now

        # close 1h trước (4 nến 15m)
        try:
            c_1h_prev = safe_float(c15_sorted[-5][4])
        except Exception:
            c_1h_prev = c_15m_prev

        change_15m = percent_change(c_now, c_15m_prev)
        change_1h = percent_change(c_now, c_1h_prev)

        # vol spike: so sánh vol_now với avg vol 10 nến trước đó
        vols_before = []
        for k in c15_sorted[-11:-1]:
            vols_before.append(safe_float(k[5]))
        if not vols_before:
            avg_vol_10 = 0
        else:
            avg_vol_10 = sum(vols_before) / len(vols_before)
        vol_spike_ratio = (vol_now / avg_vol_10) if avg_vol_10 > 0 else 0.0

        # 5m candles
        try:
            c5 = okx.get_candles(swap_id, bar="5m", limit=10)
        except Exception as e:
            logging.warning("[PUMP_PRO] Lỗi get_candles 5m cho %s: %s", inst_id, e)
            continue
        if not c5 or len(c5) < 3:
            continue
        try:
            c5_sorted = sorted(c5, key=lambda x: int(x[0]))
        except Exception:
            c5_sorted = c5
        try:
            o5_now = safe_float(c5_sorted[-1][1])
            h5_now = safe_float(c5_sorted[-1][2])
            l5_now = safe_float(c5_sorted[-1][3])
            c5_now = safe_float(c5_sorted[-1][4])
        except Exception:
            continue

        try:
            c5_prev = safe_float(c5_sorted[-2][4])
        except Exception:
            c5_prev = c5_now
        change_5m = percent_change(c5_now, c5_prev)

        # phân tích thân nến 5m
        range5 = max(h5_now - l5_now, 1e-8)
        body5 = abs(c5_now - o5_now)
        body_ratio = body5 / range5  # thân / range
        close_pos = (c5_now - l5_now) / range5  # vị trí close trong range: 0 = sát low, 1 = sát high

        # ----- điều kiện chung: 1h change không quá yếu / quá già -----
        # (nếu bạn đang tắt 1H thì có thể comment cả block này)
        if abs(change_1h) > PUMP_MAX_CHANGE_1H:
            # quá già, chạy xa rồi
            continue
        
        # ----- vol spike: vẫn cần nhưng nới nhẹ -----
        if vol_spike_ratio < PUMP_VOL_SPIKE_RATIO:
            # vol không đủ mạnh → bỏ
            continue
        
        direction = None
        
        # ----- LONG: lực tăng (nới) -----
        # Chỉ cần 1 trong 2 khung mạnh lên:
        # - 15m tăng đủ, 5m không quá xấu
        # HOẶC
        # - 5m tăng đủ, 15m không quá xấu
        if (
            (
                change_15m >= PUMP_MIN_CHANGE_15M and change_5m > -0.2
            )
            or
            (
                change_5m  >= PUMP_MIN_CHANGE_5M  and change_15m > -0.5
            )
        ):
            # Nến 5m xanh, thân khá lớn, close hơi lệch về phía high là được
            if c5_now > o5_now and body_ratio > 0.4 and close_pos > 0.55:
                direction = "LONG"
        
        # ----- SHORT: lực giảm (nới) -----
        # Tương tự: chỉ cần 1 trong 2 khung giảm mạnh
        if (
            (
                change_15m <= -PUMP_MIN_CHANGE_15M and change_5m < 0.2
            )
            or
            (
                change_5m  <= -PUMP_MIN_CHANGE_5M  and change_15m < 0.5
            )
        ):
            # Nến 5m đỏ, thân khá lớn, close hơi lệch về phía low là được
            if c5_now < o5_now and body_ratio > 0.4 and close_pos < 0.45:
                direction = "SHORT"
        
        if direction is None:
            # cả LONG/SHORT đều không thỏa → bỏ coin này
            continue


        # score = kết hợp cường độ 15m, 5m, 1h và vol spike
        score = (
            abs(change_15m)
            + abs(change_5m) * 1.5
            + abs(change_1h) * 0.5
            + max(0.0, min(vol_spike_ratio, 10.0))
        )

        final_rows.append(
            {
                "instId": inst_id,
                "direction": direction,
                "change_pct": change_15m,
                "abs_change": abs(change_15m),
                "last_price": last_price,
                "vol_quote": vol_quote,
                "score": score,
            }
        )

    if not final_rows:
        logging.info("[PUMP_PRO] Không coin nào pass filter PRO.")
        return pd.DataFrame(
            columns=[
                "instId",
                "direction",
                "change_pct",
                "abs_change",
                "last_price",
                "vol_quote",
                "score",
            ]
        )

    df = pd.DataFrame(final_rows)
    df = df.sort_values("score", ascending=False)
    logging.info("[PUMP_PRO] Sau refine còn %d coin pass filter.", len(df))
    return df
def run_dynamic_tp(okx: "OKXClient"):
    """
    TP động cho các lệnh futures đang mở.
    Chạy chung trong main, mỗi lần cron (15').
    Logic:
      - Chỉ xét lệnh đang LÃI >= TP_DYN_MIN_PROFIT_PCT.
      - Lấy nến 5m gần nhất, kiểm tra 4 dấu hiệu suy yếu:
          1) 3 nến 5m liên tiếp không còn tiến thêm theo hướng đang lãi.
          2) Nến đảo chiều mạnh (engulfing).
          3) Volume giảm mạnh so với trung bình 10 nến trước.
          4) Giá cắt EMA-5 ngược hướng.
      - Nếu 1 trong 4 điều kiện xảy ra -> đóng FULL vị thế.
    """
    logging.info("===== DYNAMIC TP START =====")

    positions = okx.get_open_positions()
    if not positions:
        logging.info("[TP_DYN] Không có vị thế futures nào đang mở.")
        return

    for p in positions:
        try:
            instId  = p.get("instId")
            posSide = p.get("posSide")        # 'long' / 'short'
            sz      = safe_float(p.get("availPos", "0"))
            avg_px  = safe_float(p.get("avgPx", "0"))  # giá vào bình quân
        except Exception:
            continue

        if not instId or sz <= 0 or avg_px <= 0:
            continue

        # Lấy nến 5m
        try:
            c5 = okx.get_candles(instId, bar="5m", limit=30)
        except Exception as e:
            logging.warning("[TP_DYN] Lỗi get_candles 5m cho %s: %s", instId, e)
            continue

        if not c5 or len(c5) < TP_DYN_FLAT_BARS + 5:
            continue

        # sort theo thời gian tăng dần
        try:
            c5_sorted = sorted(c5, key=lambda x: int(x[0]))
        except Exception:
            c5_sorted = c5

        closes = [safe_float(k[4]) for k in c5_sorted]
        opens  = [safe_float(k[1]) for k in c5_sorted]
        highs  = [safe_float(k[2]) for k in c5_sorted]
        lows   = [safe_float(k[3]) for k in c5_sorted]
        vols   = [safe_float(k[5]) for k in c5_sorted]

        if len(closes) < TP_DYN_FLAT_BARS + 1:
            continue

        c_now   = closes[-1]
        c_prev1 = closes[-2]
        c_prev2 = closes[-3]

        o_now   = opens[-1]
        o_prev1 = opens[-2]
        h_prev1 = highs[-2]
        l_prev1 = lows[-2]
        vol_now = vols[-1]

        # % lãi hiện tại
        if posSide == "long":
            profit_pct = (c_now - avg_px) / avg_px * 100.0
        else:
            profit_pct = (avg_px - c_now) / avg_px * 100.0

        if profit_pct < TP_DYN_MIN_PROFIT_PCT:
            # chưa đủ lãi để bật TP động
            continue

        # 1) 3 nến 5m không còn tiến thêm
        if posSide == "long":
            flat_move = not (c_now > c_prev1 > c_prev2)
        else:
            flat_move = not (c_now < c_prev1 < c_prev2)

        # 2) Nến đảo chiều mạnh (engulfing đơn giản)
        body_now  = abs(c_now - o_now)
        body_prev = abs(c_prev1 - o_prev1)
        engulfing = False
        if posSide == "long":
            # nến đỏ, thân lớn, đóng dưới low nến trước
            engulfing = (c_now < o_now) and (body_now > body_prev) and (c_now < l_prev1)
        else:
            # nến xanh, thân lớn, đóng trên high nến trước
            engulfing = (c_now > o_now) and (body_now > body_prev) and (c_now > h_prev1)

        # 3) Volume giảm mạnh
        vols_before = vols[-(TP_DYN_FLAT_BARS + 10):-1]  # 10 nến trước
        avg_vol10 = sum(vols_before) / max(len(vols_before), 1)
        vol_drop = avg_vol10 > 0 and (vol_now / avg_vol10) < TP_DYN_VOL_DROP_RATIO

        # 4) Giá cắt EMA-5 ngược chiều
        ema5 = calc_ema(closes[-(TP_DYN_EMA_LEN + 3):], TP_DYN_EMA_LEN)
        ema_break = False
        if ema5 is not None:
            if posSide == "long":
                ema_break = c_now < ema5
            else:
                ema_break = c_now > ema5

        should_close = flat_move or engulfing or vol_drop or ema_break

        logging.info(
            "[TP_DYN] %s %s profit=%.2f%% flat=%s engulf=%s vol_drop=%s ema_break=%s",
            instId, posSide, profit_pct, flat_move, engulfing, vol_drop, ema_break,
        )

        if should_close:
            logging.info("[TP_DYN] Đóng vị thế %s %s do tín hiệu suy yếu.", instId, posSide)
            try:
                okx.close_swap_position(instId, posSide)
            except Exception as e:
                logging.error("[TP_DYN] Lỗi close position %s %s: %s", instId, posSide, e)

    logging.info("===== DYNAMIC TP DONE =====")

def plan_trades_from_signals(df, okx: "OKXClient"):
    """
    Từ df_signals -> planned_trades.
    TP/SL tính theo ATR 15m của từng cặp.
    """
    planned = []
    now_s = now_str_vn()

    if df.empty:
        return planned

    top_df = df.head(MAX_TRADES_PER_RUN)

    logging.info("[INFO] Top signals:")
    logging.info(
        "%-4s %-12s %-8s %-8s %-10s %-10s",
        "i",
        "instId",
        "dir",
        "score",
        "change_pct",
        "last_price",
    )
    for i, row in enumerate(top_df.itertuples(), start=0):
        logging.info(
            "%-4d %-12s %-8s %4d %8.2f %10.6f",
            i,
            row.instId,
            row.direction,
            row.score,
            row.change_pct,
            row.last_price,
        )

    for row in top_df.itertuples():
        entry = row.last_price

        # 👉 TP/SL theo ATR
        tp, sl = calc_tp_sl_from_atr(okx, row.instId, row.direction, entry)

        planned.append(
            {
                "coin": row.instId,       # VD: MOODENG-USDT
                "signal": row.direction,  # LONG / SHORT
                "entry": entry,
                "tp": tp,
                "sl": sl,
                "time": now_s,
            }
        )

    logging.info("[INFO] Planned trades:")
    for t in planned:
        logging.info(
            "%s - %s - Entry=%.8f TP=%.8f SL=%.8f",
            t["coin"],
            t["signal"],
            t["entry"],
            t["tp"],
            t["sl"],
        )

    return planned


# ========== FUTURES SIZE CALC ==========

def build_swap_meta_map(instruments):
    """
    Return dict: instId -> {ctVal, lotSz, minSz}
    """
    meta = {}
    for ins in instruments:
        inst_id = ins.get("instId")
        if not inst_id:
            continue
        ct_val = float(ins.get("ctVal", "0") or "0")
        lot_sz = float(ins.get("lotSz", "0.001") or "0.001")
        min_sz = float(ins.get("minSz", lot_sz) or lot_sz)
        meta[inst_id] = {
            "ctVal": ct_val,
            "lotSz": lot_sz,
            "minSz": min_sz,
        }
    return meta
# ===== ATR & TP/SL HELPER =====

def calc_atr_15m(okx: "OKXClient", inst_id: str, period: int = 14, limit: int = 30):
    """
    Tính ATR (Average True Range) trên khung 15m cho 1 cặp.
    Dùng ~30 nến, lấy ATR 14 nến gần nhất.

    Trả về: atr (float) hoặc None nếu lỗi.
    """
    try:
        candles = okx.get_candles(inst_id, bar="15m", limit=limit)
    except Exception as e:
        logging.error("Lỗi get_candles cho %s: %s", inst_id, e)
        return None

    if not candles or len(candles) < period + 1:
        return None

    # OKX trả nến mới -> cũ, ta sort lại theo thời gian tăng dần
    try:
        candles_sorted = sorted(candles, key=lambda x: int(x[0]))
    except Exception:
        candles_sorted = candles

    trs = []
    # format nến OKX: [ts, o, h, l, c, ...]
    try:
        prev_close = float(candles_sorted[0][4])
    except Exception:
        return None

    for k in candles_sorted[1:]:
        try:
            high = float(k[2])
            low = float(k[3])
            close = float(k[4])
        except Exception:
            continue

        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close),
        )
        trs.append(tr)
        prev_close = close

    if len(trs) < period:
        return None

    atr = sum(trs[-period:]) / period
    return atr if atr > 0 else None


def calc_tp_sl_from_atr(okx: "OKXClient", inst_id: str, direction: str, entry: float):
    """
    Tính TP/SL theo ATR 15m:

    - risk = 1.2 * ATR (nhưng kẹp trong [0.6%; 8%] của giá)
    - TP = entry ± risk * RR (RR ~ 2.0)

    Trả về: (tp, sl)
    Nếu không tính được ATR -> fallback về 2% / 1%.
    """
    atr = calc_atr_15m(okx, inst_id)
    # fallback nếu ATR lỗi
    if not atr or atr <= 0:
        # fallback cũ: TP 2%, SL 1%
        if direction == "LONG":
            tp = entry * 1.02
            sl = entry * 0.99
        else:
            tp = entry * 0.98
            sl = entry * 1.01
        return tp, sl

    # risk thô theo ATR
    risk = 1.2 * atr
    risk_pct = risk / entry

    # kẹp risk_pct để tránh quá bé / quá to
    MIN_RISK_PCT = 0.006   # 0.6%
    MAX_RISK_PCT = 0.08    # 8%
    risk_pct = max(MIN_RISK_PCT, min(risk_pct, MAX_RISK_PCT))
    risk = risk_pct * entry

    RR = 2.0  # TP ~ 2R

    if direction.upper() == "LONG":
        sl = entry - risk
        tp = entry + risk * RR
    else:  # SHORT
        sl = entry + risk
        tp = entry - risk * RR

    return tp, sl


def calc_contract_size(price, notional_usdt, ct_val, lot_sz, min_sz):
    """
    price: last price
    notional_usdt: desired position notional
    ct_val: contract value (base coin)
    lot_sz: minimum increment in contracts
    """
    if price <= 0 or ct_val <= 0:
        return 0.0
    raw_contracts = notional_usdt / (price * ct_val)
    lots = math.floor(raw_contracts / lot_sz)
    contracts = lots * lot_sz
    if contracts < min_sz:
        return 0.0
    return contracts

def build_open_position_map(okx: OKXClient):
    """
    Trả về dict:
    {
      'BTC-USDT-SWAP': {'long': True/False, 'short': True/False},
      ...
    }
    dùng để biết symbol nào đã có LONG / SHORT đang mở.
    """
    positions = okx.get_open_positions()
    pos_map = {}
    for p in positions:
        try:
            inst_id = p.get("instId")
            pos_side = (p.get("posSide") or "").lower()    # 'long' / 'short'
            pos = float(p.get("pos", "0") or "0")
            if not inst_id or pos == 0:
                continue

            if inst_id not in pos_map:
                pos_map[inst_id] = {"long": False, "short": False}
            if pos_side in ("long", "short"):
                pos_map[inst_id][pos_side] = True
        except Exception:
            continue
    return pos_map
# ========== EXECUTE FUTURES TRADES ==========

def execute_futures_trades(okx: OKXClient, trades):
    if not trades:
        logging.info("[INFO] Không có lệnh futures nào để vào.")
        return

    # metadata SWAP (ctVal, lotSz, minSz...)
    swap_ins = okx.get_swap_instruments()
    swap_meta = build_swap_meta_map(swap_ins)

    # equity USDT
    avail_usdt = okx.get_usdt_balance()
    margin_per_trade = NOTIONAL_PER_TRADE / FUT_LEVERAGE
    max_trades_by_balance = int(avail_usdt // margin_per_trade)
    if max_trades_by_balance <= 0:
        logging.warning("[WARN] Không đủ USDT để vào bất kỳ lệnh nào.")
        return

    allowed_trades = trades[: max_trades_by_balance]

    # 🔥 LẤY VỊ THẾ ĐANG MỞ
    open_pos_map = build_open_position_map(okx)
    logging.info("[INFO] Open positions: %s", open_pos_map)

    # Gom các dòng để gửi 1 tin Telegram duy nhất
    telegram_lines = []

    for t in allowed_trades:
        coin = t["coin"]         # ví dụ 'BTC-USDT'
        signal = t["signal"]     # LONG / SHORT
        entry = t["entry"]
        tp = t["tp"]
        sl = t["sl"]

        # Spot -> Perp SWAP
        swap_inst = coin.replace("-USDT", "-USDT-SWAP")

        # ❗ Nếu đã có vị thế mở cùng hướng trên OKX -> bỏ qua, không mở thêm
        pos_info = open_pos_map.get(swap_inst, {"long": False, "short": False})
        if signal == "LONG" and pos_info.get("long"):
            logging.info(
                "[INFO] Đã có vị thế LONG đang mở với %s, bỏ qua tín hiệu mới.",
                swap_inst,
            )
            continue
        if signal == "SHORT" and pos_info.get("short"):
            logging.info(
                "[INFO] Đã có vị thế SHORT đang mở với %s, bỏ qua tín hiệu mới.",
                swap_inst,
            )
            continue

        meta = swap_meta.get(swap_inst)
        if not meta:
            logging.warning(
                "[WARN] Không tìm thấy futures cho %s -> %s, bỏ qua.",
                coin,
                swap_inst,
            )
            continue

        ct_val = meta["ctVal"]
        lot_sz = meta["lotSz"]
        min_sz = meta["minSz"]

        contracts = calc_contract_size(
            entry, NOTIONAL_PER_TRADE, ct_val, lot_sz, min_sz
        )
        if contracts <= 0:
            logging.warning(
                "[WARN] Không tính được contracts hợp lệ cho %s (price=%.8f ctVal=%g lotSz=%g minSz=%g)",
                swap_inst,
                entry,
                ct_val,
                lot_sz,
                min_sz,
            )
            continue

        pos_side = "long" if signal == "LONG" else "short"
        side_open = "buy" if signal == "LONG" else "sell"
        side_close = "sell" if signal == "LONG" else "buy"

        logging.info("🚀 *OKX FUTURES TRADE*")
        logging.info("Coin: %s", coin)
        logging.info("Future: %s", swap_inst)
        logging.info("Tín hiệu: *%s*", signal)
        logging.info("PosSide: %s", pos_side)
        logging.info("Qty contracts: %g", contracts)
        logging.info("Entry (sheet): %.8f", entry)
        logging.info("TP: %.8f", tp)
        logging.info("SL: %.8f", sl)

        # 1) Set leverage isolated x5
        try:
            okx.set_isolated_leverage(swap_inst, FUT_LEVERAGE)
        except Exception:
            logging.warning(
                "Không set được leverage cho %s, vẫn thử vào lệnh với leverage hiện tại.",
                swap_inst,
            )

        # 2) Mở vị thế
        # 2) Mở vị thế
        okx.set_leverage(swap_inst, lever=FUT_LEVERAGE, posSide=pos_side)   # <--- thêm posSide
        time.sleep(0.2)
        
        order_resp = okx.place_futures_market_order(
            inst_id=swap_inst,
            side=side_open,
            pos_side=pos_side,
            sz=contracts,
            td_mode="isolated",
            lever=FUT_LEVERAGE,
        )

        code = order_resp.get("code")
        if code != "0":
            msg = order_resp.get("msg", "")
            logging.error(
                "[OKX ORDER RESP] Lỗi mở lệnh: code=%s msg=%s", code, msg
            )
            # không gửi telegram lỗi, chỉ log
            continue

        # 3) Đặt TP/SL OCO
        oco_resp = okx.place_oco_tp_sl(
            inst_id=swap_inst,
            pos_side=pos_side,
            side_close=side_close,
            sz=contracts,
            tp_px=tp,
            sl_px=sl,
            td_mode="isolated",
        )
        oco_code = oco_resp.get("code")
        if oco_code != "0":
            logging.error(
                "[OKX OCO RESP] Lỗi đặt TP/SL OCO cho %s: code=%s msg=%s",
                swap_inst,
                oco_code,
                oco_resp.get("msg", ""),
            )

        # 4) Lệnh đã mở thành công -> lưu vào CACHE
        trade_cache_item = {
            "coin": coin,          # ví dụ: 'BTC-USDT'
            "signal": signal,      # 'LONG' / 'SHORT'
            "entry": entry,
            "tp": tp,
            "sl": sl,
            "time": now_str_vn(),  # thời điểm vào lệnh theo VN
        }
        append_trade_to_cache(trade_cache_item)

        # Đồng thời thêm dòng Telegram (bỏ -USDT)
        coin_name = coin.replace("-USDT", "")
        line = f"📊 LỆNH FUTURE | {coin_name}-{signal}-{entry:.6f}-{tp:.6f}-{sl:.6f}"
        telegram_lines.append(line)

    # Sau khi duyệt hết các lệnh:
    if telegram_lines:
        msg = "\n".join(telegram_lines)
        send_telegram_message(msg)
    else:
        logging.info("[INFO] Không có lệnh futures nào được mở thành công.")


def run_full_bot(okx):
    setup_logging()
    logging.info("===== OKX FUTURES BOT CRON START =====")

    # ENV
    api_key = os.getenv("OKX_API_KEY")
    api_secret = os.getenv("OKX_API_SECRET")
    passphrase = os.getenv("OKX_API_PASSPHRASE")
    simulated = os.getenv("OKX_SIMULATED_TRADING", "1") == "1"

    if not api_key or not api_secret or not passphrase:
        raise RuntimeError(
            "OKX_API_KEY / OKX_API_SECRET / OKX_API_PASSPHRASE chưa cấu hình."
        )

    okx = OKXClient(api_key, api_secret, passphrase, simulated_trading=simulated)


    # 1) Scan market với bộ lọc PUMP/DUMP PRO
    df_signals = build_signals_pump_dump_pro(okx)
    logging.info(
        "[INFO] PUMP/DUMP PRO trả về %d tín hiệu.", len(df_signals)
    )

    if df_signals.empty:
        logging.info("[INFO] Không có tín hiệu hợp lệ, dừng bot lần chạy này.")
        return

    # 2) Google Sheet
    try:
        ws = prepare_worksheet()
        #existing = get_recent_signals(ws)
    except Exception as e:
        logging.error("[ERROR] Google Sheet prepare lỗi: %s", e)
        return

    # 3) Plan trades
    planned_trades = plan_trades_from_signals(df_signals, okx)

    # 4) Append sheet
    append_signals(ws, planned_trades)

    # 5) Futures + Telegram
    execute_futures_trades(okx, planned_trades)
    
    # 6) Nếu đang trong khung 19:00 - 19:10 VN thì chạy backtest
    run_backtest_if_needed(okx)


def main():
    # Nếu muốn tính theo giờ VN:
    now_utc = datetime.now(timezone.utc)
    now_vn  = now_utc + timedelta(hours=7)   # VN = UTC+7
    minute  = now_vn.minute

    okx = OKXClient(
        api_key=os.getenv("OKX_API_KEY"),
        api_secret=os.getenv("OKX_API_SECRET"),
        passphrase=os.getenv("OKX_API_PASSPHRASE")
    )

    # Luôn ưu tiên TP dynamic trước
    run_dynamic_tp(okx)

    # Các mốc 5 - 20 - 35 - 50 phút thì chạy thêm FULL BOT
    # 5,20,35,50 đều có minute % 15 == 5
    if minute % 15 == 5:
        logging.info("[SCHED] %02d' -> CHẠY FULL BOT", minute)
        run_full_bot(okx)
    else:
        logging.info("[SCHED] %02d' -> CHỈ CHẠY TP DYNAMIC", minute)


if __name__ == "__main__":
    main()

