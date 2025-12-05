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
from google.oauth2.service_account import Credentials
import gspread
from google.oauth2 import service_account

# ========== CONFIG ==========
OKX_BASE_URL = "https://www.okx.com"
CACHE_FILE = os.getenv("TRADE_CACHE_FILE", "trade_cache.json")

# Trading config
FUT_LEVERAGE = 6              # x5 isolated
NOTIONAL_PER_TRADE = 30.0     # 30 USDT position size (ký quỹ ~5$ với x6)
MAX_TRADES_PER_RUN = 10        # tối đa 10 lệnh / 1 lần cron

# Circuit breaker theo phiên
SESSION_MAX_LOSS_PCT = 5.0  # Mỗi phiên lỗ tối đa -5% equity thì dừng trade
SESSION_STATE_FILE = os.getenv("SESSION_STATE_FILE", "session_state.json")

# Scanner config
MIN_ABS_CHANGE_PCT = 2.0      # chỉ lấy coin |24h change| >= 2%
MIN_VOL_USDT = 100000         # min 24h volume quote
TOP_N_BY_CHANGE = 300          # universe: top 300 theo độ biến động

# Google Sheet headers
SHEET_HEADERS = ["Coin", "Tín hiệu", "Entry", "SL", "TP", "Ngày"]

# ======== DYNAMIC TP CONFIG ========
TP_DYN_MIN_PROFIT_PCT   = 2.5   # chỉ bật TP động khi lãi >= 2.5%
TP_DYN_MAX_FLAT_BARS    = 3     # số nến 5m gần nhất để kiểm tra
TP_DYN_VOL_DROP_RATIO   = 0.5   # vol hiện tại < 50% avg 10 nến -> yếu
TP_DYN_EMA_LEN          = 5     # EMA-5
TP_DYN_FLAT_BARS = 2      # số nến 5m đi ngang trước khi thoát
TP_DYN_ENGULF = True      # bật thoát khi có engulfing
TP_DYN_VOL_DROP = True    # bật thoát khi vol giảm mạnh
TP_DYN_EMA_TOUCH = True   # bật thoát khi chạm EMA5
# ======== TRAILING TP CONFIG ========
TRAIL_START_PROFIT_PCT = 5.0   # bắt đầu kích hoạt trailing khi lãi >= 5% PnL
TRAIL_GIVEBACK_PCT     = 3.0   # nếu giá hồi ngược lại >= 2% từ đỉnh → chốt
TRAIL_LOOKBACK_BARS    = 30    # số nến 5m gần nhất để ước lượng đỉnh/đáy

# ========== PUMP/DUMP PRO CONFIG ==========
SL_DYN_SOFT_PCT_GOOD = 3.0   # thị trường ổn → cho chịu lỗ rộng hơn chút
SL_DYN_SOFT_PCT_BAD  = 2.0   # thị trường xấu → cắt sớm hơn
SL_DYN_TREND_PCT = 1.0       # 1%/15m đi ngược chiều thì coi là mạnh
SL_DYN_LOOKBACK = 3          # số cây 5m/15m để đo trend ngắn

# SL planned tối đa (khi đặt TP/SL ban đầu)
MAX_PLANNED_SL_PNL_PCT = 10.0   # cho phép lỗ tối đa 10% PnL nếu chạm SL
# SL khẩn cấp theo PnL%
MAX_EMERGENCY_SL_PNL_PCT = 5.0  # qua -5% là cắt khẩn cấp

PUMP_MIN_ABS_CHANGE_24H = 2.0       # |%change 24h| tối thiểu để được xem xét (lọc coin chết)
PUMP_MIN_VOL_USDT_24H   = 50000   # volume USDT 24h tối thiểu
PUMP_PRE_TOP_N          = 300       # lấy top 300 coin theo độ biến động 24h để refine

PUMP_MIN_CHANGE_15M     = 1.0       # %change 15m tối thiểu theo hướng LONG/SHORT
PUMP_MIN_CHANGE_5M      = 0.5       # %change 5m tối thiểu
PUMP_VOL_SPIKE_RATIO    = 0.1       # vol 15m hiện tại phải > 1x vol avg 10 nến trước

PUMP_MIN_CHANGE_1H      = 0.5       # %change 1h tối thiểu (tránh sóng quá yếu)
PUMP_MAX_CHANGE_1H      = 100.0      # %change 1h tối đa (tránh đu quá trễ)
DEADZONE_MIN_ATR_PCT = 0.2   # ví dụ: 0.4%/5m trở lên mới chơi

# ================== HELPERS CHUNG ==================
# =========================
#  BT ALL CACHE -> GOOGLE SHEETS
#  - Dùng env: GOOGLE_SERVICE_ACCOUNT_JSON, BT_SHEET_ID
#  - Lưu 1 dòng duy nhất BT_ALL (cộng dồn)
# =========================

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def _get_gspread_client():
    """Khởi tạo client gspread từ GOOGLE_SERVICE_ACCOUNT_JSON (env trên Render)."""
    raw_json = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
    info = json.loads(raw_json)
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    client = gspread.authorize(creds)
    return client


def _open_bt_cache_sheet():
    """
    Mở sheet BT_CACHE trong file có id = BT_SHEET_ID.
    Nếu chưa có thì tạo mới + set header.
    """
    sheet_id = os.environ["BT_SHEET_ID"]  # <--- env mới, bạn đã nói OK
    client = _get_gspread_client()
    doc = client.open_by_key(sheet_id)

    try:
        ws = doc.worksheet("BT_CACHE")
    except gspread.WorksheetNotFound:
        ws = doc.add_worksheet(title="BT_CACHE", rows=1000, cols=8)
        ws.update(
            "A1:H1",
            [[
                "date",
                "total",
                "tp",
                "sl",
                "open",
                "win_pct",
                "pnl_usdt",
                "note",
            ]],
        )
    return ws


def load_bt_all_from_sheet() -> dict:
    """
    Đọc dòng BT_ALL (note='BT_ALL') từ sheet BT_CACHE.
    Nếu chưa có -> trả về số 0 hết.
    """
    ws = _open_bt_cache_sheet()
    data = ws.get_all_records()  # list[dict]

    for row in data:
        if str(row.get("note", "")).strip().upper() == "BT_ALL":
            return {
                "total": int(row.get("total", 0)),
                "tp": int(row.get("tp", 0)),
                "sl": int(row.get("sl", 0)),
                "open": int(row.get("open", 0)),
                "win_pct": float(row.get("win_pct", 0.0)),
                "pnl_usdt": float(row.get("pnl_usdt", 0.0)),
            }

    # chưa có BT_ALL
    return {"total": 0, "tp": 0, "sl": 0, "open": 0, "win_pct": 0.0, "pnl_usdt": 0.0}


def save_bt_all_to_sheet(stats: dict) -> None:
    """
    Ghi / update dòng BT_ALL vào sheet BT_CACHE.
    stats yêu cầu: total, tp, sl, open, pnl_usdt
    """
    ws = _open_bt_cache_sheet()
    data = ws.get_all_records()

    bt_all_row_index = None  # index trong sheet (2 = dòng thứ 2, vì 1 là header)
    for idx, row in enumerate(data, start=2):
        if str(row.get("note", "")).strip().upper() == "BT_ALL":
            bt_all_row_index = idx
            break

    total = int(stats.get("total", 0))
    tp = int(stats.get("tp", 0))
    if total > 0:
        win_pct = tp * 100.0 / total
    else:
        win_pct = 0.0

    values = [
        datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        total,
        tp,
        int(stats.get("sl", 0)),
        int(stats.get("open", 0)),
        win_pct,
        float(stats.get("pnl_usdt", 0.0)),
        "BT_ALL",
    ]

    if bt_all_row_index is None:
        # chưa có -> append xuống cuối
        ws.append_row(values)
    else:
        # update dòng cũ
        ws.update(f"A{bt_all_row_index}:H{bt_all_row_index}", [values])


def accumulate_bt_all_with_today(bt_today: dict) -> dict:
    """
    Cộng dồn BT_TODAY vào BT_ALL & lưu lên sheet.

    bt_today dạng:
    {
        "total": ...,
        "tp": ...,
        "sl": ...,
        "open": ...,
        "pnl_usdt": ...
    }
    """
    current = load_bt_all_from_sheet()

    new_stats = {
        "total": current["total"] + int(bt_today.get("total", 0)),
        "tp": current["tp"] + int(bt_today.get("tp", 0)),
        "sl": current["sl"] + int(bt_today.get("sl", 0)),
        "open": current["open"] + int(bt_today.get("open", 0)),
        "pnl_usdt": current["pnl_usdt"] + float(bt_today.get("pnl_usdt", 0.0)),
    }

    save_bt_all_to_sheet(new_stats)
    return new_stats

def decide_risk_config(regime: str | None, session_flag: str | None):
    """
    Chọn cấu hình risk theo:
      - regime:  "GOOD" / "BAD" (market)
      - session_flag: "GOOD" / "BAD" (hiệu suất phiên trước)

    👉 Áp dụng CHỈ KHI ngoài deadzone.
    Deadzone vẫn dùng logic riêng (3x, 10 USDT...).
    """
    regime = (regime or "GOOD").upper()
    session_flag = (session_flag or "GOOD").upper()

    # 1) Market GOOD, session GOOD → FULL GAS
    if regime == "GOOD" and session_flag == "GOOD":
        return {
            "leverage": 6,
            "notional": 25.0,
            "tp_dyn_min_profit": 5.0,  # % giá
            "max_sl_pnl_pct": 5.0,
            "max_trades_per_run": 15,
        }

    # 2) Market GOOD, session BAD → Market ok nhưng bot đang bắn tệ
    if regime == "GOOD" and session_flag == "BAD":
        return {
            "leverage": 4,
            "notional": 15.0,
            "tp_dyn_min_profit": 5.0,
            "max_sl_pnl_pct": 5.0,
            "max_trades_per_run": 10,
        }

    # 3) Market BAD, session GOOD → Market xấu nhưng bot vừa bắn ngon
    if regime == "BAD" and session_flag == "GOOD":
        return {
            "leverage": 4,
            "notional": 20.0,
            "tp_dyn_min_profit": 3.0,
            "max_sl_pnl_pct": 4.0,
            "max_trades_per_run": 10,
        }

    # 4) Market BAD, session BAD → HARD DEFENSE MODE
    return {
        "leverage": 3,
        "notional": 10.0,
        "tp_dyn_min_profit": 3.0,
        "max_sl_pnl_pct": 3.0,
        "max_trades_per_run": 7,
    }


def apply_risk_config(okx: "OKXClient"):
    """
    Set lại các biến GLOBAL:
      FUT_LEVERAGE, NOTIONAL_PER_TRADE, TP_DYN_MIN_PROFIT_PCT,
      MAX_SL_PNL_PCT, MAX_TRADES_PER_RUN

    - Deadzone: luôn dùng cấu hình cố định.
    - Ngoài deadzone: dùng regime + session_flag.
    """
    global FUT_LEVERAGE, NOTIONAL_PER_TRADE
    global TP_DYN_MIN_PROFIT_PCT, MAX_SL_PNL_PCT, MAX_TRADES_PER_RUN

    # DEADZONE: giữ nguyên style scalping an toàn
    if is_deadzone_time_vn():
        FUT_LEVERAGE = 3
        NOTIONAL_PER_TRADE = 10.0
        TP_DYN_MIN_PROFIT_PCT = 3.0   # TP động bật khi lãi >= 3% giá
        MAX_SL_PNL_PCT = 3.0          # emergency SL -3% PnL
        MAX_TRADES_PER_RUN = 5
        logging.info("[RISK] DEADZONE config: lev=3, notional=10, tp_dyn=3%%, maxSL=3%%, max_trades=5")
        return

    # 👉 Ngoài DEADZONE: dùng 2 tầng regime + session_flag
    try:
        regime = detect_market_regime(okx)              # "GOOD"/"BAD" – bạn đã có hàm này
    except NameError:
        regime = "GOOD"  # fallback nếu chưa cài detect_market_regime

    try:
        session_flag = get_session_flag_for_next_session()  # "GOOD"/"BAD" – lấy từ backtest session
    except NameError:
        session_flag = "GOOD"  # fallback nếu chưa gắn hàm

    cfg = decide_risk_config(regime, session_flag)

    FUT_LEVERAGE = cfg["leverage"]
    NOTIONAL_PER_TRADE = cfg["notional"]
    TP_DYN_MIN_PROFIT_PCT = cfg["tp_dyn_min_profit"]
    MAX_SL_PNL_PCT = cfg["max_sl_pnl_pct"]
    MAX_TRADES_PER_RUN = cfg["max_trades_per_run"]

    logging.info(
        "[RISK] regime=%s session=%s -> lev=%dx, notional=%.1f, tp_dyn=%.1f%%, maxSL=%.1f%%, max_trades=%d",
        regime,
        session_flag,
        FUT_LEVERAGE,
        NOTIONAL_PER_TRADE,
        TP_DYN_MIN_PROFIT_PCT,
        MAX_SL_PNL_PCT,
        MAX_TRADES_PER_RUN,
    )

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
    
def parse_trade_time_to_utc_ms(time_str: str) -> int | None:
    """
    Convert 'dd/mm/YYYY HH:MM' (giờ VN) -> timestamp ms UTC
    dùng để so sánh với ts nến OKX (ms UTC).
    """
    try:
        dt_vn = datetime.strptime(time_str, "%d/%m/%Y %H:%M")
        # trade time đang lưu giờ VN -> trừ 7h về UTC
        dt_utc = dt_vn - timedelta(hours=7)
        return int(dt_utc.timestamp() * 1000)
    except Exception:
        return None

def is_quiet_hours_vn():
    """
    Trả về True nếu đang trong khung giờ 22h–06h (giờ VN),
    dùng để tắt Telegram ban đêm.
    """
    now_vn = datetime.utcnow() + timedelta(hours=7)
    return now_vn.hour >= 23 or now_vn.hour < 6
def is_backtest_time_vn():
    """
    Chạy backtest theo PHIÊN:
      - 09:05  -> tổng kết phiên 0–9
      - 15:05  -> tổng kết phiên 9–15
      - 20:05  -> tổng kết phiên 15–20
      - 22:50  -> tổng kết phiên 20–24
    """
    now_vn = datetime.utcnow() + timedelta(hours=7)
    h = now_vn.hour
    m = now_vn.minute

    # các lần cron full bot đang chạy ở phút 5,20,35,50
    if h in (9, 15, 20) and 5 <= m <= 9:
        return True
    if h == 22 and 50 <= m <= 59:
        return True

    return False

    
def is_deadzone_time_vn():
    """
    Phiên trưa 'deadzone' 10:30 - 16:00 giờ VN.
    Dùng cho chiến lược sideway / scalp an toàn.
    """
    now_vn = datetime.utcnow() + timedelta(hours=7)
    h = now_vn.hour
    m = now_vn.minute

    # 10:30–11:00
    if h == 10 and m >= 30:
        return True
    # 11:00–16:00
    if 11 <= h < 16:
        return True

    return False
def get_current_session_vn():
    """
    Chia 4 phiên:
      - 0–9   : '0-9'
      - 9–15  : '9-15'
      - 15–20 : '15-20'
      - 20–24 : '20-24'
    """
    now_vn = datetime.utcnow() + timedelta(hours=7)
    h = now_vn.hour
    if h < 9:
        return "0-9"
    elif h < 15:
        return "9-15"
    elif h < 20:
        return "15-20"
    else:
        return "20-24"

def get_session_from_hour_vn(hour: int) -> str:
    """
    Map giờ VN (0–23) sang phiên:
      - 0–9   : '0-9'
      - 9–15  : '9-15'
      - 15–20 : '15-20'
      - 20–24 : '20-24'
    """
    if hour < 9:
        return "0-9"
    elif hour < 15:
        return "9-15"
    elif hour < 20:
        return "15-20"
    else:
        return "20-24"


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
        
    def get_positions_history(self, limit: int = 1000):
        """
        Lấy lịch sử vị thế (positions-history) cho SWAP.
        Dùng để backtest REAL theo PnL từ OKX.
        """
        # NHỚ: query string để luôn trong path, params=None
        path = f"/api/v5/account/positions-history?instType=SWAP&limit={int(limit)}"
    
        data = self._request("GET", path, params=None)
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
    def get_total_equity_usdt(self) -> float:
        """
        Lấy tổng equity USDT (bao gồm cả vị thế đang mở + PnL chưa chốt).
        Dùng field 'eq' của OKX.
        """
        path = "/api/v5/account/balance?ccy=USDT"
        data = self._request("GET", path, params=None)
    
        details = data.get("data", [])
        if not details:
            return 0.0
    
        detail = details[0]
    
        # Cấu trúc OKX: data[0]["details"][0]["eq"] hoặc data[0]["eq"]
        if "details" in detail and detail["details"]:
            eq = float(detail["details"][0].get("eq", "0"))
        else:
            eq = float(detail.get("eq", "0"))
    
        logging.info("[INFO] Tổng equity USDT (eq): %.8f", eq)
        return eq

    def set_leverage(self, inst_id, lever=FUT_LEVERAGE, pos_side=None, mgn_mode="isolated"):
        """
        Set leverage cho 1 instId.
        - NET mode: không cần posSide.
        - LONG/SHORT (hedged) mode: yêu cầu posSide = 'long' hoặc 'short'.
        """
        path = "/api/v5/account/set-leverage"
        body = {
            "instId": inst_id,
            "lever": str(lever),
            "mgnMode": mgn_mode,
        }
        if pos_side is not None:
            body["posSide"] = pos_side  # 'long' hoặc 'short'
    
        data = self._request("POST", path, body_dict=body)
        logging.info("[INFO] SET LEVERAGE RESP: %s", data)
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
    def close_swap_position(self, inst_id, pos_side):
        """
        Đóng FULL vị thế bằng market order.
        """
        path = "/api/v5/trade/close-position"
        body = {
            "instId": inst_id,
            "mgnMode": "isolated",
            "posSide": pos_side
        }
        logging.info(f"[OKX] Close position: {inst_id} | {pos_side}")
        return self._request("POST", path, body_dict=body)


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

def load_session_state():
    """
    Đọc trạng thái circuit breaker theo phiên từ Google Sheet.

    Dạng dữ liệu:
    date, session, start_equity, blocked
    2025-12-04, 0-9, 18.5300, FALSE
    """
    try:
        ws = get_session_worksheet()
        if ws is None:
            return None

        records = ws.get_all_records()  # list[dict]
        if not records:
            return {}   # sheet rỗng -> chưa có state

        return records[0]  # state hợp lệ
    except Exception as e:
        logging.error("[SESSION] Lỗi load_session_state (API/GSHEET): %s", e)
        # Trả về state đặc biệt báo lỗi, để circuit breaker FAIL-SAFE
        return {"error": True}

        # Lấy dòng mới nhất
        last = records[-1]

        state = {
            "date": str(last.get("date") or ""),
            "session": str(last.get("session") or ""),
            "start_equity": float(last.get("start_equity") or 0),
            "blocked": bool(last.get("blocked")),
        }

        logging.info(
            "[SESSION] Load state từ sheet: date=%s, session=%s, blocked=%s, start_equity=%.4f",
            state["date"],
            state["session"],
            state["blocked"],
            state["start_equity"],
        )

        return state

    except Exception as e:
        logging.error("[SESSION] Lỗi load_session_state từ sheet: %s", e)
        return None
def save_session_state(state: dict):
    """
    Ghi thêm 1 dòng trạng thái mới vào Google Sheet.
    Mỗi lần cập nhật sẽ append 1 dòng, load luôn lấy dòng cuối cùng.
    """
    try:
        ws = get_session_worksheet()
        if ws is None:
            return

        row = [
            state.get("date", ""),
            state.get("session", ""),
            float(state.get("start_equity", 0) or 0),
            bool(state.get("blocked", False)),
        ]

        ws.append_row(row)
        logging.info(
            "[SESSION] Save state vào sheet: date=%s, session=%s, start_equity=%.4f, blocked=%s",
            row[0],
            row[1],
            row[2],
            row[3],
        )

    except Exception as e:
        logging.error("[SESSION] Lỗi save_session_state vào sheet: %s", e)

def check_session_circuit_breaker(okx: "OKXClient") -> bool:
    """
    Trả về:
        True  -> ĐƯỢC phép mở lệnh mới trong PHIÊN hiện tại
        False -> PHIÊN đã chạm -SESSION_MAX_LOSS_PCT%, KHÔNG mở lệnh mới.
    """

    now_vn = datetime.utcnow() + timedelta(hours=7)
    today = now_vn.strftime("%Y-%m-%d")
    session = get_current_session_vn()

    logging.info("========== [SESSION] KIỂM TRA CIRCUIT BREAKER ==========")
    logging.info("[SESSION] Thời gian VN: %s, phiên hiện tại: %s", now_vn, session)

    # 1) Load state hiện tại từ Google Sheet
    state = load_session_state() or {}
    st_date = state.get("date")
    st_session = state.get("session")
    blocked = bool(state.get("blocked", False))
    start_equity = float(state.get("start_equity") or 0.0)

    logging.info(
        "[SESSION] State hiện tại: date=%s, session=%s, blocked=%s, start_equity=%.4f",
        st_date, st_session, blocked, start_equity
    )

    # 2) Nếu đã sang NGÀY MỚI hoặc PHIÊN MỚI -> reset state, lấy equity ĐẦU PHIÊN
    if st_date != today or st_session != session or start_equity <= 0:
        equity = okx.get_total_equity_usdt()     # equity = tiền nhàn rỗi + margin lệnh đang chạy
        state = {
            "date": today,
            "session": session,
            "start_equity": equity,
            "blocked": False,
        }
        save_session_state(state)

        logging.warning(
            "[SESSION] RESET state cho ngày %s - phiên %s (start_equity=%.4f)",
            today, session, equity
        )
        # Vừa sang phiên mới -> chưa lỗ gì, cho phép trade
        return True

    # 3) Nếu đã bị khóa từ trước -> không cần tính lại, chặn luôn
    if blocked:
        logging.warning(
            "[SESSION] Phiên %s đang BỊ KHÓA (đã lỗ quá %.1f%%). Bỏ qua mở lệnh mới.",
            session, SESSION_MAX_LOSS_PCT
        )
        return False

    # 4) Tính PnL% của RIÊNG PHIÊN hiện tại so với start_equity đầu phiên
    equity_now = okx.get_total_equity_usdt()
    pnl_pct = (equity_now - start_equity) / start_equity * 100.0

    logging.info(
        "[SESSION] Phiên %s PnL=%.2f%% (start=%.4f, now=%.4f)",
        session, pnl_pct, start_equity, equity_now
    )

    # 5) Nếu lỗ vượt quá ngưỡng -> set blocked=True và CHẶN mở lệnh mới
    if pnl_pct <= -SESSION_MAX_LOSS_PCT:
        state["blocked"] = True
        save_session_state(state)

        logging.warning(
            "[SESSION] Phiên %s lỗ %.2f%% <= -%.2f%% → KHÓA PHIÊN, dừng mở lệnh mới.",
            session, pnl_pct, SESSION_MAX_LOSS_PCT
        )
        return False

    # 6) Ngược lại vẫn trong ngưỡng an toàn -> cho phép trade tiếp
    logging.info(
        "[SESSION] Phiên %s vẫn trong ngưỡng an toàn (PnL=%.2f%% > -%.2f%%) → TIẾP TỤC TRADE.",
        session, pnl_pct, SESSION_MAX_LOSS_PCT
    )
    return True


def load_real_trades_for_backtest(okx: "OKXClient") -> list[dict]:
    """
    Lấy danh sách closed positions REAL từ OKX để backtest thống kê.
    """
    try:
        trades = okx.get_positions_history(limit=1000)
        logging.info("[BACKTEST] Lấy được %d closed positions từ OKX.", len(trades))
        return trades
    except Exception as e:
        logging.error("[BACKTEST] Lỗi get_positions_history: %s", e)
        return []

    from datetime import datetime, timedelta

def summarize_real_backtest(trades: list[dict]) -> tuple[str, str, str]:
    """
    Trả về 3 đoạn text:
      - msg_all     : [✅BT ALL] ...
      - msg_today   : [✅BT TODAY] ...
      - msg_session : --- SESSION TODAY --- + 4 dòng [0-9], [9-15], [15-20], [20-24]
    Dựa trên PnL REAL từ OKX (field 'pnl').
    """
    # Không có trade nào
    if not trades:
        msg_all = "[✅BT ALL] total=0 TP=0 SL=0 OPEN=0 win=0.0% PNL=+0.00 USDT"
        msg_today = "[✅BT TODAY] total=0 TP=0 SL=0 OPEN=0 win=0.0% PNL=+0.00 USDT"
        msg_session = (
            "--- SESSION TODAY ---\n"
            "[0-9]   total=0 TP=0 SL=0 OPEN=0 win=0.0% PNL=+0.00 USDT\n"
            "[9-15]  total=0 TP=0 SL=0 OPEN=0 win=0.0% PNL=+0.00 USDT\n"
            "[15-20] total=0 TP=0 SL=0 OPEN=0 win=0.0% PNL=+0.00 USDT\n"
            "[20-24] total=0 TP=0 SL=0 OPEN=0 win=0.0% PNL=+0.00 USDT"
        )
        return msg_all, msg_today, msg_session

    # ---- helper chung ----
    def classify(filtered: list[dict]):
        total = len(filtered)
        tp = sl = even = 0
        pnl_sum = 0.0

        for t in filtered:
            try:
                pnl = float(t.get("pnl", "0"))
            except Exception:
                pnl = 0.0

            pnl_sum += pnl
            if pnl > 0:
                tp += 1
            elif pnl < 0:
                sl += 1
            else:
                even += 1

        win = (tp / total * 100.0) if total > 0 else 0.0
        return total, tp, sl, even, pnl_sum, win

    # Chuyển cTime/uTime -> datetime VN
    def get_vn_dt(t: dict):
        ctime_str = t.get("cTime") or t.get("uTime")
        if not ctime_str:
            return None
        try:
            ts = int(ctime_str) / 1000.0
            dt_utc = datetime.utcfromtimestamp(ts)
            return dt_utc + timedelta(hours=7)
        except Exception:
            return None

    now_vn = datetime.utcnow() + timedelta(hours=7)
    today_date = now_vn.date()

    # Lọc trades đóng trong ngày VN hôm nay
    trades_today: list[tuple[dict, datetime]] = []
    for t in trades:
        dt_vn = get_vn_dt(t)
        if dt_vn is None:
            continue
        if dt_vn.date() == today_date:
            trades_today.append((t, dt_vn))

    # ==================   ALL   ==================
    # ============== ALL ==================
    # trades: list các lệnh đã đóng (đang có sẵn)
    total, tp, sl, even, pnl_sum, win = classify(trades)

    # ============== TODAY ==================
    # trades_today: list[(t, dt_vn)], mình chỉ lấy phần t ra để classify
    only_today = [t for (t, _dt) in trades_today]
    t_total, t_tp, t_sl, t_even, t_pnl_sum, t_win = classify(only_today)

    # Build bt_today từ kết quả classify
    bt_today = {
        "total":   t_total,
        "tp":      t_tp,
        "sl":      t_sl,
        "open":    t_even,
        "pnl_usdt": t_pnl_sum,   # PnL hôm nay
    }

    # Cộng dồn BT_TODAY vào BT_ALL & lưu lên Google Sheet
    bt_all = accumulate_bt_all_with_today(bt_today)

    # Format message BT ALL (dùng số cộng dồn trong sheet)
    msg_all = (
        f"[✅BT ALL] total={bt_all['total']} | "
        f"TP={bt_all['tp']} SL={bt_all['sl']} OPEN={bt_all['open']} | "
        f"win={ (bt_all['tp']*100/bt_all['total']) if bt_all['total'] else 0:.1f}% | "
        f"PNL={bt_all['pnl_usdt']:+.2f} USDT"
    )

    # Format message BT TODAY (chỉ riêng hôm nay)
    msg_today = (
        f"[✅BT TODAY] total={bt_today['total']} | "
        f"TP={bt_today['tp']} SL={bt_today['sl']} OPEN={bt_today['open']} | "
        f"win={ (bt_today['tp']*100/bt_today['total']) if bt_today['total'] else 0:.1f}% | "
        f"PNL={bt_today['pnl_usdt']:+.2f} USDT"
    )

    # ==================== SESSION TODAY ====================
    # 4 phiên: [0-9], [9-15], [15-20], [20-24] (giống circuit breaker)
    sessions = [
        ("0-9",   0, 9),
        ("9-15",  9, 15),
        ("15-20", 15, 20),
        ("20-24", 20, 24),
    ]

    session_lines = ["--- SESSION TODAY ---"]
    for label, h_start, h_end in sessions:
        sess_trades = [
            t for (t, dt_vn) in trades_today
            if h_start <= dt_vn.hour < h_end
        ]
        s_total, s_tp, s_sl, s_even, s_pnl_sum, s_win = classify(sess_trades)
        line = (
            f"[{label}] total={s_total} TP={s_tp} SL={s_sl} OPEN={s_even} "
            f"win={s_win:.1f}% PNL={s_pnl_sum:+.2f} USDT"
        )
        session_lines.append(line)

    msg_session = "\n".join(session_lines)

    return msg_all, msg_today, msg_session

    # 1) Ưu tiên: Drive CSV
    try:
        trades_drive = load_history_from_drive()   # ✅ gọi đúng tên hàm
        if trades_drive:
            logging.info("[BACKTEST] Sử dụng history từ Drive (%d lệnh).", len(trades_drive))
            return trades_drive
    except Exception as e:
        logging.error("[BACKTEST] Lỗi load Drive CSV: %s", e)

    # 2) Fallback: cache JSON local
    try:
        trades_local = load_trade_cache()
        if trades_local:
            logging.info("[BACKTEST] Sử dụng history từ cache JSON (%d lệnh).", len(trades_local))
            return trades_local
    except:
        pass

    logging.info("[BACKTEST] Không có dữ liệu history.")
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
    
def get_session_from_time(time_s: str) -> str | None:
    if not time_s:
        return None

    try:
        h = int(time_s[11:13])
        m = int(time_s[14:16])
    except:
        return None

    t = h + m / 60.0  # chuyển sang dạng 11.5, 14.25…

    if 0 <= t < 9:
        return "0-9"
    elif 9 <= t < 15:
        return "9-15"
    elif 15 <= t < 20:
        return "15-20"
    else:
        return "20-24"

def calc_real_backtest_stats(okx: "OKXClient", trades: list):
    """
    Tính thống kê backtest từ REAL closed positions.

    - TP: số lệnh có realizedPnl > 0
    - SL: số lệnh có realizedPnl < 0
    - PNL_USDT: tổng realizedPnl (đơn vị USDT)
    - Chia theo:
        + ALL
        + TODAY
        + từng SESSION (0-9, 9-15, 15-20, 20-24)
    """
    # Thời gian VN hôm nay
    now_utc = datetime.utcnow()
    now_vn = now_utc + timedelta(hours=7)
    today_str = now_vn.strftime("%Y-%m-%d")

    def empty_stat():
        return {
            "total": 0,
            "tp": 0,
            "sl": 0,
            "pnl": 0.0,  # USDT
        }

    stats_all = empty_stat()
    stats_today = empty_stat()
    session_stats = {
        "0-9": empty_stat(),
        "9-15": empty_stat(),
        "15-20": empty_stat(),
        "20-24": empty_stat(),
    }

    for t in trades:
        try:
            pnl = float(t.get("realizedPnl", "0"))  # PnL thực tế
        except Exception:
            pnl = 0.0

        # Close time (ms)
        ctime_ms = int(t.get("cTime", t.get("uTime", "0")) or 0)
        if ctime_ms == 0:
            continue

        dt_utc = datetime.utcfromtimestamp(ctime_ms / 1000.0)
        dt_vn = dt_utc + timedelta(hours=7)
        date_vn = dt_vn.strftime("%Y-%m-%d")
        hour_vn = dt_vn.hour
        sess = get_session_from_hour_vn(hour_vn)

        # ALL
        stats_all["total"] += 1
        stats_all["pnl"] += pnl
        if pnl > 0:
            stats_all["tp"] += 1
        elif pnl < 0:
            stats_all["sl"] += 1

        # TODAY
        if date_vn == today_str:
            s = stats_today
            s["total"] += 1
            s["pnl"] += pnl
            if pnl > 0:
                s["tp"] += 1
            elif pnl < 0:
                s["sl"] += 1

            # SESSION TODAY
            ss = session_stats.get(sess)
            if ss is not None:
                ss["total"] += 1
                ss["pnl"] += pnl
                if pnl > 0:
                    ss["tp"] += 1
                elif pnl < 0:
                    ss["sl"] += 1

    # Đếm OPEN hiện tại (đang mở) để show cho đẹp
    open_positions = okx.get_open_positions()
    open_count = len(open_positions)

    # Tính win%
    def finalize(stat: dict, open_extra: int = 0):
        total_closed = stat["tp"] + stat["sl"]
        total_all = stat["tp"] + stat["sl"] + open_extra
        win_pct = (stat["tp"] / total_closed * 100.0) if total_closed > 0 else 0.0
        return total_all, win_pct

    all_total, all_win = finalize(stats_all, open_extra=open_count)
    today_total, today_win = finalize(stats_today, open_extra=open_count)

    sess_final = {}
    for k, v in session_stats.items():
        total, win = finalize(v, open_extra=0)  # OPEN tính chung, không chia phiên
        sess_final[k] = {
            "total": total,
            "tp": v["tp"],
            "sl": v["sl"],
            "pnl": v["pnl"],
            "win": win,
        }

    result = {
        "all": {
            "total": all_total,
            "tp": stats_all["tp"],
            "sl": stats_all["sl"],
            "open": open_count,
            "win": all_win,
            "pnl": stats_all["pnl"],
        },
        "today": {
            "total": today_total,
            "tp": stats_today["tp"],
            "sl": stats_today["sl"],
            "open": open_count,
            "win": today_win,
            "pnl": stats_today["pnl"],
        },
        "sessions": sess_final,
    }
    return result

def run_backtest_if_needed(okx: "OKXClient"):
    """
    Backtest REAL bằng lịch sử vị thế đã đóng trên OKX.
    Gửi 3 block:
      1) BT ALL
      2) BT TODAY
      3) SESSION TODAY (4 phiên)
    """
    logging.info("========== [BACKTEST] BẮT ĐẦU CHẠY BACKTEST REAL ==========")

    if not is_backtest_time_vn():
        return

    trades = load_real_trades_for_backtest(okx)
    msg_all, msg_today, msg_session = summarize_real_backtest(trades)

    send_telegram_message(msg_all + "\n" + msg_today + "\n\n" + msg_session)


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
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
import io
import csv
import tempfile

def get_drive_service():
    """
    Tạo service Google Drive từ GOOGLE_SERVICE_ACCOUNT_JSON
    """
    json_str = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    if not json_str:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON not set")
    info = json.loads(json_str)
    scopes = ["https://www.googleapis.com/auth/drive"]
    credentials = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    service = build("drive", "v3", credentials=credentials)
    return service

def load_history_from_drive():
    """
    Download file CSV từ Google Drive → trả về list[dict] trade.
    Mỗi dict có key: coin, signal, entry, tp, sl, time
    """
    file_id = os.getenv("GOOGLE_DRIVE_TRADE_FILE_ID")
    if not file_id:
        logging.warning("[DRIVE] GOOGLE_DRIVE_TRADE_FILE_ID chưa cấu hình.")
        return []

    try:
        service = get_drive_service()
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)

        done = False
        while not done:
            status, done = downloader.next_chunk()

        fh.seek(0)
        text = fh.read().decode("utf-8").splitlines()
        if not text:
            return []

        reader = csv.DictReader(text)
        trades = []
        for row in reader:
            # chuẩn hoá key
            trades.append({
                "coin": row.get("coin"),
                "signal": row.get("signal"),
                "entry": row.get("entry"),
                "tp": row.get("tp"),
                "sl": row.get("sl"),
                "time": row.get("time"),
            })
        logging.info("[DRIVE] Loaded %d trades from Drive CSV", len(trades))
        return trades
    except Exception as e:
        logging.error("[DRIVE] Lỗi load_history_from_drive: %s", e)
        return []
def append_trade_to_drive(trade: dict):
    """
    Append 1 lệnh vào file CSV trên Google Drive.
    - Nếu DRIVE_HISTORY_RESET_ONCE=1: bỏ qua dữ liệu cũ → chỉ dùng trade mới.
    - Luôn ghi lại header đầy đủ mỗi lần upload.
    """
    file_id = os.getenv("GOOGLE_DRIVE_TRADE_FILE_ID")
    if not file_id:
        logging.warning("[DRIVE] GOOGLE_DRIVE_TRADE_FILE_ID chưa cấu hình, bỏ qua append.")
        return

    reset_once = os.getenv("DRIVE_HISTORY_RESET_ONCE", "0") == "1"

    # 1) Load dữ liệu cũ (nếu không reset)
    if reset_once:
        logging.info("[DRIVE] RESET_ONCE=1 → xoá toàn bộ dữ liệu cũ, chỉ giữ trade mới.")
        data = []
    else:
        data = load_history_from_drive()

    # 2) Thêm trade mới
    data.append({
        "coin": str(trade.get("coin")),
        "signal": str(trade.get("signal")),
        "entry": str(trade.get("entry")),
        "tp": str(trade.get("tp")),
        "sl": str(trade.get("sl")),
        "time": str(trade.get("time")),
    })

    # 3) Ghi ra file CSV tạm (luôn có header)
    fieldnames = ["coin", "signal", "entry", "tp", "sl", "time"]

    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv", mode="w", newline="", encoding="utf-8") as tmp:
            writer = csv.DictWriter(tmp, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
            temp_path = tmp.name
    except Exception as e:
        logging.error("[DRIVE] Lỗi ghi file tạm CSV: %s", e)
        return

    # 4) Upload CSV lên Drive (overwrite file cũ)
    try:
        service = get_drive_service()
        media = MediaFileUpload(temp_path, mimetype="text/csv", resumable=False)

        service.files().update(
            fileId=file_id,
            media_body=media
        ).execute()

        logging.info("[DRIVE] Đã cập nhật history CSV trên Drive. Tổng lệnh: %d", len(data))
    except Exception as e:
        logging.error("[DRIVE] Lỗi upload CSV lên Drive: %s", e)

import gspread
from google.oauth2.service_account import Credentials

# Sheet dùng để lưu trạng thái circuit breaker
SESSION_SHEET_KEY = os.getenv("GSHEET_KEY")  # hoặc GSHEET_SESSION_KEY riêng nếu muốn
SESSION_STATE_SHEET_NAME = os.getenv("SESSION_STATE_SHEET_NAME", "SESSION_STATE")


def get_session_worksheet():
    """
    Mở (hoặc tạo) worksheet SESSION_STATE trong Google Sheet để lưu trạng thái phiên.
    """
    try:
        sa_info = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
        if not sa_info:
            logging.error("[SESSION] GOOGLE_SERVICE_ACCOUNT_JSON chưa cấu hình.")
            return None

        sa_info_json = json.loads(sa_info)

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_info(sa_info_json, scopes=scopes)
        gc = gspread.authorize(creds)

        if not SESSION_SHEET_KEY:
            logging.error("[SESSION] GSHEET_KEY chưa cấu hình.")
            return None

        sh = gc.open_by_key(SESSION_SHEET_KEY)

        try:
            ws = sh.worksheet(SESSION_STATE_SHEET_NAME)
        except gspread.WorksheetNotFound:
            # Chưa có sheet → tạo mới + header
            ws = sh.add_worksheet(title=SESSION_STATE_SHEET_NAME, rows=10, cols=10)
            ws.append_row(["date", "session", "start_equity", "blocked"])

        return ws

    except Exception as e:
        logging.error("[SESSION] Lỗi get_session_worksheet: %s", e)
        return None

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
    PUMP/DUMP PRO V2:
    - Giữ nguyên logic V1 (24h filter, 15m/5m momentum, vol_spike).
    - Bổ sung thêm:
        + Entry pullback (mid-body + EMA5 5m).
        + BTC 5m filter (tránh LONG khi BTC đỏ nến, SHORT khi BTC xanh).
        + Impulse 2–3 sóng (closes 5m cùng chiều).
        + Wick filter (tránh pump-xả wick dài).
        + Overextended filter (không đu quá xa high/low 15m).
        + EMA align: 5m / 15m / 1H cùng hướng.
    """

    # -------- B0: BTC 5m cho market filter --------
    btc_5m = None
    try:
        btc_c = okx.get_candles("BTC-USDT-SWAP", bar="5m", limit=2)
        if btc_c and len(btc_c) >= 2:
            btc_sorted = sorted(btc_c, key=lambda x: int(x[0]))
            btc_o = safe_float(btc_sorted[-1][1])
            btc_cl = safe_float(btc_sorted[-1][4])
            btc_5m = (btc_o, btc_cl)
    except Exception as e:
        logging.warning("[PUMP_PRO_V2] Lỗi get_candles BTC 5m: %s", e)
        btc_5m = None
    # -------- B0: BTC 5m cho market filter --------
    btc_change_5m = None
    try:
        btc_c = okx.get_candles("BTC-USDT-SWAP", bar="5m", limit=2)
        if btc_c and len(btc_c) >= 2:
            btc_sorted = sorted(btc_c, key=lambda x: int(x[0]))
            btc_o = safe_float(btc_sorted[-1][1])
            btc_cl = safe_float(btc_sorted[-1][4])
            if btc_o > 0:
                btc_change_5m = (btc_cl - btc_o) / btc_o * 100.0
    except Exception as e:
        logging.warning("[PUMP_PRO] Lỗi get_candles BTC 5m: %s", e)
        btc_change_5m = None
    # -------- B1: pre-filter bằng FUTURES tickers 24h (SWAP) --------
    try:
        fut_tickers = okx.get_swap_tickers()
    except Exception as e:
        logging.error("[PUMP_PRO_V2] Lỗi get_swap_tickers: %s", e)
        return pd.DataFrame(
            columns=[
                "instId",
                "direction",
                "change_pct",
                "abs_change",
                "last_price",
                "vol_quote",
                "score",
                "entry_pullback",
            ]
        )

    pre_rows = []
    for t in fut_tickers:
        if isinstance(t, str):
            fut_id = t
        else:
            fut_id = t.get("instId", "")
        if not fut_id:
            continue

        inst_id = fut_id.replace("-SWAP", "")
        last = safe_float(t.get("last"))
        open24 = safe_float(t.get("open24h"))
        vol_quote = safe_float(t.get("volCcy24h"))

        if last <= 0 or open24 <= 0:
            continue

        change24 = percent_change(last, open24)
        abs_change24 = abs(change24)

        if abs_change24 < PUMP_MIN_ABS_CHANGE_24H:
            continue
        if vol_quote < PUMP_MIN_VOL_USDT_24H:
            continue

        pre_rows.append(
            {
                "instId": inst_id,
                "swapId": fut_id,
                "last": last,
                "change24": change24,
                "abs_change24": abs_change24,
                "vol_quote": vol_quote,
            }
        )

    if not pre_rows:
        logging.info("[PUMP_PRO_V2] Không có futures nào qua pre-filter 24h.")
        return pd.DataFrame(
            columns=[
                "instId",
                "direction",
                "change_pct",
                "abs_change",
                "last_price",
                "vol_quote",
                "score",
                "entry_pullback",
            ]
        )

    pre_df = pd.DataFrame(pre_rows)
    pre_df = pre_df.sort_values("abs_change24", ascending=False)
    pre_df = pre_df.head(PUMP_PRE_TOP_N)

    logging.info(
        "[PUMP_PRO_V2] Pre-filter FUTURES còn %d coin ứng viên (top %d theo biến động 24h).",
        len(pre_df),
        PUMP_PRE_TOP_N,
    )

    # -------- B2: refine bằng 15m & 5m + filter nâng cao --------
    final_rows = []

    for row in pre_df.itertuples():
        inst_id = row.instId
        swap_id = getattr(row, "swapId", inst_id)
        last_price = row.last
        vol_quote = row.vol_quote

        # 15m candles
        try:
            c15 = okx.get_candles(swap_id, bar="15m", limit=40)
        except Exception as e:
            logging.warning("[PUMP_PRO_V2] Lỗi get_candles 15m cho %s: %s", inst_id, e)
            continue
        if not c15 or len(c15) < 10:
            continue

        try:
            c15_sorted = sorted(c15, key=lambda x: int(x[0]))
        except Exception:
            c15_sorted = c15

        try:
            o_now = safe_float(c15_sorted[-1][1])
            h_now = safe_float(c15_sorted[-1][2])
            l_now = safe_float(c15_sorted[-1][3])
            c_now = safe_float(c15_sorted[-1][4])
            vol_now_15 = safe_float(c15_sorted[-1][5])
        except Exception:
            continue

        try:
            c_15m_prev = safe_float(c15_sorted[-2][4])
        except Exception:
            c_15m_prev = c_now

        try:
            c_1h_prev = safe_float(c15_sorted[-5][4])
        except Exception:
            c_1h_prev = c_15m_prev

        change_15m = percent_change(c_now, c_15m_prev)
        change_1h = percent_change(c_now, c_1h_prev)

        vols_before_15 = [safe_float(k[5]) for k in c15_sorted[-11:-1]]
        avg_vol_10_15 = sum(vols_before_15) / len(vols_before_15) if vols_before_15 else 0.0
        vol_spike_ratio = (vol_now_15 / avg_vol_10_15) if avg_vol_10_15 > 0 else 0.0

        # 5m candles
        try:
            c5 = okx.get_candles(swap_id, bar="5m", limit=20)
        except Exception as e:
            logging.warning("[PUMP_PRO_V2] Lỗi get_candles 5m cho %s: %s", inst_id, e)
            continue
        if not c5 or len(c5) < 5:
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
            vol_now_5 = safe_float(c5_sorted[-1][5])
        except Exception:
            continue

        try:
            c5_prev1 = safe_float(c5_sorted[-2][4])
        except Exception:
            c5_prev1 = c5_now
        try:
            c5_prev2 = safe_float(c5_sorted[-3][4])
        except Exception:
            c5_prev2 = c5_prev1

        change_5m = percent_change(c5_now, c5_prev1)

        range5 = max(h5_now - l5_now, 1e-8)
        body5 = abs(c5_now - o5_now)
        body_ratio = body5 / range5
        close_pos = (c5_now - l5_now) / range5

        # ---- filter volume spike như V1 ----
        if abs(change_1h) > PUMP_MAX_CHANGE_1H:
            continue
        if vol_spike_ratio < PUMP_VOL_SPIKE_RATIO:
            continue

        # --------- Xác định direction như V1 (giữ nguyên) ---------
        direction = None

        if (
            (
                change_15m >= PUMP_MIN_CHANGE_15M and change_5m > -0.2
            )
            or
            (
                change_5m  >= PUMP_MIN_CHANGE_5M  and change_15m > -0.5
            )
        ):
            if c5_now > o5_now and body_ratio > 0.4 and close_pos > 0.55:
                direction = "LONG"

        if (
            (
                change_15m <= -PUMP_MIN_CHANGE_15M and change_5m < 0.2
            )
            or
            (
                change_5m  <= -PUMP_MIN_CHANGE_5M  and change_15m < 0.5
            )
        ):
            if c5_now < o5_now and body_ratio > 0.4 and close_pos < 0.45:
                direction = "SHORT"

        if direction is None:
            continue

        # ===== V2 FILTER 1: BTC 5m đồng pha =====
        if btc_5m is not None:
            btc_o, btc_cl = btc_5m
            if direction == "LONG" and btc_change_5m < -0.5:
                # BTC đỏ nến 5m -> tránh LONG alt
                continue
            if direction == "SHORT" and btc_change_5m> 0.5:
                # BTC xanh nến 5m -> tránh SHORT alt
                continue

        # ===== V2 FILTER 2: Impulse 2–3 sóng (closes 5m cùng chiều) =====
        if direction == "LONG":
            if not (c5_now > c5_prev1 > c5_prev2):
                continue
        else:  # SHORT
            if not (c5_now < c5_prev1 < c5_prev2):
                continue

        # ===== V2 FILTER 3: Wick filter (tránh pump-xả wick dài) =====
        upper_wick = h5_now - max(o5_now, c5_now)
        lower_wick = min(o5_now, c5_now) - l5_now

        if direction == "LONG":
            if upper_wick > body5 * 1.8:
                logging.info("[PUMP_PRO_V2] %s bỏ LONG vì râu trên quá dài.", inst_id)
                continue
        else:
            if lower_wick > body5 * 1.8:
                logging.info("[PUMP_PRO_V2] %s bỏ SHORT vì râu dưới quá dài.", inst_id)
                continue

        # ===== V2 FILTER 4: Overextended (không đu quá xa high/low 15m) =====
        highs_15 = [safe_float(k[2]) for k in c15_sorted]
        lows_15  = [safe_float(k[3]) for k in c15_sorted]
        if len(highs_15) >= 20 and len(lows_15) >= 20:
            recent_high = max(highs_15[-20:])
            recent_low  = min(lows_15[-20:])
            if direction == "LONG" and c_now > recent_high * 1.005:
                # quá xa đỉnh gần -> dễ đu đỉnh
                continue
            if direction == "SHORT" and c_now < recent_low * 0.995:
                # quá xa đáy gần -> dễ đu đáy
                continue

        # ===== V2 FILTER 5: EMA multi-TF align (5m, 15m, 1H) =====
        # 5m EMA9
        closes_5 = [safe_float(k[4]) for k in c5_sorted]
        ema9_5m = calc_ema(closes_5[-12:], 9) if len(closes_5) >= 10 else None

        # 15m EMA20
        closes_15 = [safe_float(k[4]) for k in c15_sorted]
        ema20_15m = calc_ema(closes_15[-25:], 20) if len(closes_15) >= 22 else None

        # 1H EMA50
        try:
            c1h = okx.get_candles(swap_id, bar="1H", limit=60)
        except Exception as e:
            logging.warning("[PUMP_PRO_V2] Lỗi get_candles 1H cho %s: %s", inst_id, e)
            c1h = []

        ema50_1h = None
        if c1h and len(c1h) >= 52:
            try:
                c1h_sorted = sorted(c1h, key=lambda x: int(x[0]))
            except Exception:
                c1h_sorted = c1h
            closes_1h = [safe_float(k[4]) for k in c1h_sorted]
            ema50_1h = calc_ema(closes_1h[-52:], 50)

        # nếu thiếu EMA nào thì bỏ qua EMA filter (không quá gắt)
        if ema9_5m and ema20_15m:
            if direction == "LONG":
                if not (c_now > ema9_5m and c_now > ema20_15m):
                    continue
            else:
                if not (c_now < ema9_5m and c_now < ema20_15m):
                    continue

        # ===== ENTRY PULLBACK: mid-body + EMA5 5m =====
        mid_body = (o5_now + c5_now) / 2.0
        ema5_5m = calc_ema(closes_5[-8:], 5) if len(closes_5) >= 6 else None

        if ema5_5m:
            if direction == "LONG":
                desired = max(mid_body, ema5_5m)
                entry_pullback = min(c5_now, desired)
            else:
                desired = min(mid_body, ema5_5m)
                entry_pullback = max(c5_now, desired)
        else:
            # fallback: dùng mid-body
            if direction == "LONG":
                entry_pullback = min(c5_now, mid_body)
            else:
                entry_pullback = max(c5_now, mid_body)

        if entry_pullback <= 0:
            entry_pullback = last_price

        # ===== score giống V1 (giữ nguyên) =====
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
                "entry_pullback": entry_pullback,
            }
        )

    if not final_rows:
        logging.info("[PUMP_PRO_V2] Không coin nào pass filter PRO V2.")
        return pd.DataFrame(
            columns=[
                "instId",
                "direction",
                "change_pct",
                "abs_change",
                "last_price",
                "vol_quote",
                "score",
                "entry_pullback",
            ]
        )

    df = pd.DataFrame(final_rows)
    df = df.sort_values("score", ascending=False)
    logging.info("[PUMP_PRO_V2] Sau refine còn %d coin pass filter.", len(df))
    return df
def build_signals_sideway_deadzone(okx: "OKXClient"):
    """
    Scanner phiên DEADZONE (10h30–15h30 VN):

    - Không bắt breakout pump/dump.
    - Ưu tiên coin volume lớn, biến động 24h vừa phải.
    - Tìm tín hiệu mean-reversion quanh EMA20 5m (giá lệch không quá xa EMA, có dấu hiệu quay lại).
    - Trả về DataFrame cùng format với build_signals_pump_dump_pro:
        columns: instId, direction, change_pct, abs_change, last_price, vol_quote, score
    """

    # Chỉ chạy đúng khung giờ deadzone, ngoài giờ thì trả DF rỗng
    if not is_deadzone_time_vn():
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

    try:
        fut_tickers = okx.get_swap_tickers()
    except Exception as e:
        logging.error("[SIDEWAY] Lỗi get_swap_tickers: %s", e)
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
        if isinstance(t, str):
            fut_id = t
        else:
            fut_id = t.get("instId", "")
        if not fut_id:
            continue

        inst_id = fut_id.replace("-SWAP", "")  # "ABC-USDT"

        last = safe_float(t.get("last"))
        open24 = safe_float(t.get("open24h"))
        vol_quote = safe_float(t.get("volCcy24h"))

        if last <= 0 or open24 <= 0:
            continue

        change24 = percent_change(last, open24)
        abs_change24 = abs(change24)

        # 🔹 Phiên trưa: tránh coin pump/dump quá mạnh & tránh coin chết
        if abs_change24 < 1.5:          # quá phẳng -> bỏ
            continue
        if abs_change24 > 30.0:         # biến động 24h >30% -> dễ pump/dump, để dành cho phiên tối
            continue
        if vol_quote < max(PUMP_MIN_VOL_USDT_24H, 2 * 10_000):  # volume đủ lớn
            continue

        pre_rows.append(
            {
                "instId": inst_id,
                "swapId": fut_id,
                "last": last,
                "change24": change24,
                "abs_change24": abs_change24,
                "vol_quote": vol_quote,
            }
        )

    if not pre_rows:
        logging.info("[SIDEWAY] Không coin nào qua pre-filter 24h.")
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
    # Ưu tiên coin volume lớn & biến động vừa phải
    pre_df = pre_df.sort_values(["vol_quote", "abs_change24"], ascending=[False, False])
    pre_df = pre_df.head(150)

    logging.info(
        "[SIDEWAY] Pre-filter còn %d coin ứng viên (top theo vol & biến động vừa phải).",
        len(pre_df),
    )

    final_rows = []

    # Lấy BTC 5m để tránh lúc BTC đang pump/dump mạnh
    btc_change_5m = None
    try:
        btc_c = okx.get_candles("BTC-USDT-SWAP", bar="5m", limit=2)
        if btc_c and len(btc_c) >= 2:
            btc_sorted = sorted(btc_c, key=lambda x: int(x[0]))
            btc_o = safe_float(btc_sorted[-2][4])
            btc_c_now = safe_float(btc_sorted[-1][4])
            if btc_o > 0:
                btc_change_5m = percent_change(btc_c_now, btc_o)
    except Exception as e:
        logging.warning("[SIDEWAY] Lỗi get_candles BTC 5m: %s", e)

    for row in pre_df.itertuples():
        inst_id = row.instId
        swap_id = getattr(row, "swapId", inst_id + "-SWAP")
        vol_quote = row.vol_quote

        # BTC đang biến động mạnh -> bỏ, không scalp phiên trưa
        if btc_change_5m is not None and abs(btc_change_5m) > 1.5:
            continue

        # Lấy 5m candles
        try:
            c5 = okx.get_candles(swap_id, bar="5m", limit=60)
        except Exception as e:
            logging.warning("[SIDEWAY] Lỗi get_candles 5m cho %s: %s", inst_id, e)
            continue

        if not c5 or len(c5) < 25:
            continue

        try:
            c5_sorted = sorted(c5, key=lambda x: int(x[0]))
        except Exception:
            c5_sorted = c5

        closes = [safe_float(k[4]) for k in c5_sorted]
        opens = [safe_float(k[1]) for k in c5_sorted]
        highs = [safe_float(k[2]) for k in c5_sorted]
        lows = [safe_float(k[3]) for k in c5_sorted]

        c_now = closes[-1]
        o_now = opens[-1]
        h_now = highs[-1]
        l_now = lows[-1]

        # ==== VOLATILITY FILTER: ATR% 5m ====
        ranges = [h - l for h, l in zip(highs[-20:], lows[-20:])]
        avg_range = sum(ranges) / max(1, len(ranges))
        atr_pct_5m = avg_range / c_now * 100.0 if c_now > 0 else 0.0

        # coin quá lì, mỗi nến dao động < DEADZONE_MIN_ATR_PCT% -> bỏ
        if atr_pct_5m < DEADZONE_MIN_ATR_PCT:
            continue
            
        # EMA20 5m để làm "trục" cho mean-reversion
        ema20_5m = calc_ema(closes[-25:], 20) if len(closes) >= 25 else None
        if ema20_5m is None or ema20_5m <= 0:
            continue

        # Độ lệch so với EMA20 (theo %)
        dist_pct = (c_now - ema20_5m) / ema20_5m * 100.0

        # Range & body nến hiện tại
        range_5m = max(h_now - l_now, 1e-8)
        body_5m = abs(c_now - o_now)
        body_ratio = body_5m / range_5m

        direction = None

        # ========= MEAN-REVERSION LOGIC =========
        # LONG: giá vừa "chọc xuống EMA20" rồi đóng trên EMA20, lệch không quá xa
        # require dist_pct nằm trong [-0.3%; +0.3%]
        DEADZONE_MAX_DIST = 0.5
        
        ...
        dist_ok = abs(dist_pct) <= DEADZONE_MAX_DIST
        small_range = range_5m / ema20_5m < 1  # bỏ nến quá dài (có thể là pump/dump mini)
        
        direction = None
        
        # LONG
        if (
            dist_ok
            and closes[-2] < ema20_5m <= c_now
            and body_ratio < 0.8
            and small_range
        ):
            direction = "LONG"
        
        # SHORT
        if (
            dist_ok
            and closes[-2] > ema20_5m >= c_now
            and body_ratio < 0.8
            and small_range
        ):
            if direction is None:
                direction = "SHORT"
        if direction is None:
            continue

        # score: ưu tiên coin volume lớn & lệch EMA vừa phải
        score = (
            vol_quote / 1e6  # scale theo triệu USDT
            - abs(dist_pct) * 2.0
        )

        final_rows.append(
            {
                "instId": inst_id,
                "direction": direction,
                "change_pct": dist_pct,            # dùng lệch EMA làm change_pct
                "abs_change": abs(dist_pct),
                "last_price": c_now,
                "vol_quote": vol_quote,
                "score": score,
            }
        )

    if not final_rows:
        logging.info("[SIDEWAY] Không coin nào pass filter sideway deadzone.")
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
    logging.info("[SIDEWAY] Sau refine còn %d coin pass filter.", len(df))
    return df


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
        # Nếu scanner đã tính sẵn entry_pullback thì dùng,
        # còn không thì fallback về last_price cho an toàn.
        entry = getattr(row, "entry_pullback", row.last_price)
        # 👉 TP/SL theo ATR, nhưng dựa trên entry "bớt FOMO"
        if is_deadzone_time_vn():
            tp, sl = calc_scalp_tp_sl(entry, row.direction)
        else:
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

def simulate_trade_result_with_candles(
    okx: "OKXClient",
    coin: str,
    signal: str,
    entry: float,
    tp: float,
    sl: float,
    time_str: str,
    bar: str = "5m",
    max_limit: int = 300,
):
    """
    Backtest 1 lệnh bằng nến lịch sử:
      - coin: 'MERL-USDT' (spot) hoặc 'MERL-USDT-SWAP' (perp)
      - signal: 'LONG' / 'SHORT'
      - entry, tp, sl: float
      - time_str: 'dd/mm/YYYY HH:MM' (giờ VN)

    Logic:
      - lấy ~300 nến 5m gần nhất
      - tìm nến có ts >= thời điểm vào lệnh
      - duyệt từng nến: kiểm tra high/low chạm TP/SL
      - nếu cả TP & SL cùng chạm trong 1 nến -> giả định XẤU NHẤT: SL trước
    """
    ts_entry = parse_trade_time_to_utc_ms(time_str)
    if ts_entry is None:
        return "UNKNOWN"

    # ƯU TIÊN BACKTEST BẰNG SWAP (Perpetual Futures)
    # coin: 'SAHARA-USDT'
    # swap_inst: 'SAHARA-USDT-SWAP'
    base = coin.replace("-USDT", "")
    swap_inst = f"{base}-USDT-SWAP"

    # Thử lấy nến từ SWAP trước
    try:
        candles = okx.get_candles(swap_inst, bar=bar, limit=max_limit)
        inst_id = swap_inst
    except Exception:
        candles = []

    # Nếu SWAP thất bại → fallback SPOT
    if not candles:
        try:
            candles = okx.get_candles(coin, bar=bar, limit=max_limit)
            inst_id = coin
        except Exception:
            return "NO_DATA"

    try:
        candles_sorted = sorted(candles, key=lambda x: int(x[0]))
    except Exception:
        candles_sorted = candles

    # tìm index bắt đầu từ lúc vào lệnh
    start_idx = None
    for i, k in enumerate(candles_sorted):
        try:
            ts_bar = int(k[0])
        except Exception:
            continue
        if ts_bar >= ts_entry:
            start_idx = i
            break

    if start_idx is None:
        # lệnh quá cũ, không nằm trong khoảng nến tải về
        return "OUT_OF_RANGE"

    sig = (signal or "").upper()

    for k in candles_sorted[start_idx:]:
        try:
            high = float(k[2])
            low  = float(k[3])
        except Exception:
            continue

        if sig == "LONG":
            hit_tp = high >= tp
            hit_sl = low  <= sl
        else:  # SHORT
            hit_tp = low  <= tp
            hit_sl = high >= sl

        # nếu trong 1 nến chạm cả TP & SL -> chọn kịch bản xấu: SL
        if hit_tp and hit_sl:
            return "SL"
        if hit_tp:
            return "TP"
        if hit_sl:
            return "SL"

    # duyệt hết mà không chạm TP/SL
    return "OPEN"

def calc_tp_sl_from_atr(okx: "OKXClient", inst_id: str, direction: str, entry: float):
    """
    TP/SL theo ATR 15m (phiên PUMP/DUMP):
      - risk_pct ~ ATR/price, kẹp [1%; 4%]
      - RR = 2 (TP ≈ 2R, SL ≈ 1R) 
    """
    atr = calc_atr_15m(okx, inst_id)
    if not atr or atr <= 0:
        # fallback nhẹ nhàng hơn: TP 1.5%, SL 1.0%
        if direction.upper() == "LONG":
            tp = entry * 1.015
            sl = entry * 0.99
        else:
            tp = entry * 0.985
            sl = entry * 1.01
        return tp, sl
    risk = 1.1 * atr
    risk_pct = risk / entry
    # kẹp risk_pct để tránh quá bé / quá to
    MIN_RISK_PCT = 0.006   # 0.6% giá (≈ -3% PnL với x5)
    MAX_RISK_PCT = 0.08    # 8% giá (trần kỹ thuật, nhưng sẽ bị PnL cap chặn lại bên dưới)

    risk_pct = max(MIN_RISK_PCT, min(risk_pct, MAX_RISK_PCT))

    # ✅ Giới hạn thêm: SL không được vượt MAX_SL_PNL_PCT (theo PnL%)
    # PnL% ≈ risk_pct * FUT_LEVERAGE * 100
    #  → risk_pct_max_theo_pnl = MAX_SL_PNL_PCT / FUT_LEVERAGE
    max_risk_pct_by_pnl = MAX_PLANNED_SL_PNL_PCT / FUT_LEVERAGE
    risk_pct = min(risk_pct, max_risk_pct_by_pnl)
    risk = risk_pct * entry

    regime = detect_market_regime(okx)
    if regime == "GOOD":
        RR = 2.0      # ăn dày khi thị trường đẹp
    else:
        RR = 1.0      # thị trường xấu → scalp RR 1:1 an toàn

    if direction.upper() == "LONG":
        sl = entry - risk
        tp = entry + risk * RR
    else:
        sl = entry + risk
        tp = entry - risk * RR

    return tp, sl

    
def calc_scalp_tp_sl(entry: float, direction: str):
    tp_pct = 0.02  # 2%
    sl_pct = 0.01  # 1%

    if direction.upper() == "LONG":
        tp = entry * (1 + tp_pct)
        sl = entry * (1 - sl_pct)
    else:
        tp = entry * (1 - tp_pct)
        sl = entry * (1 + sl_pct)
    return tp, sl


def calc_ema(prices, length):
    if not prices or len(prices) < length:
        return None
    ema = prices[0]
    alpha = 2 / (length + 1)
    for p in prices[1:]:
        ema = alpha * p + (1 - alpha) * ema
    return ema

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

    # ===== CHỌN LEVERAGE + SIZE THEO GIỜ & THỊ TRƯỜNG =====
    regime = detect_market_regime(okx)  # "GOOD" / "BAD"

    if is_deadzone_time_vn():
        # phiên trưa: luôn giảm size + leverage
        this_lever    = 3
        this_notional = 15.0          # chỉ 15 USDT / lệnh
    elif regime == "BAD":
        # thị trường xấu: giữ size 20$ nhưng hạ đòn bẩy
        this_lever    = 4
        this_notional = 20.0
    else:
        # thị trường tốt: full cấu hình
        this_lever    = FUT_LEVERAGE  # ví dụ 5x
        this_notional = NOTIONAL_PER_TRADE

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
            entry, this_notional, ct_val, lot_sz, min_sz
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

        # 1) Set leverage isolated x6
        #TWO WAY
        try:
            okx.set_leverage(swap_inst, this_lever, pos_side=pos_side)
        except Exception:
            logging.warning(
                "Không set được leverage cho %s, vẫn thử vào lệnh với leverage hiện tại.",
                swap_inst,
            )
        #NET MODE
        # 2) Mở vị thế
        #okx.set_leverage(swap_inst, lever=this_lever)
        time.sleep(0.2)
        
        order_resp = okx.place_futures_market_order(
            inst_id=swap_inst,
            side=side_open,
            pos_side=pos_side,
            sz=contracts,
            td_mode="isolated",
            lever=this_lever,
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
        # 3) Đặt OCO chỉ dùng SL, TP để rất xa cho an toàn
        #    → chốt lời sẽ do TP động + trailing TP xử lý.
        HARD_TP_CAP_PCT = 50.0  # TP trần cực xa ~ +50% giá, gần như không chạm

        if signal == "LONG":
            tp_hard = entry * (1 + HARD_TP_CAP_PCT / 100.0)
        else:  # SHORT
            tp_hard = entry * (1 - HARD_TP_CAP_PCT / 100.0)

        oco_resp = okx.place_oco_tp_sl(
            inst_id=swap_inst,
            pos_side=pos_side,
            side_close=side_close,
            sz=contracts,
            tp_px=tp_hard,  # TP rất xa, không còn vai trò TP 1R
            sl_px=sl,       # SL vẫn theo ATR + cap 10% như cũ
            td_mode="isolated",
        )

        oco_code = oco_resp.get("code")
        if oco_code != "0":
            msg = oco_resp.get("msg", "")
            logging.error(
                f"[OKX ORDER RESP] Không đặt được OCO TP/SL cho {swap_inst}: code={code} msg={msg}. ĐÓNG LỆNH NGAY để tránh mất kiểm soát."
            )
            try:
                okx.close_swap_position(swap_inst, pos_side)
            except Exception as e:
                logging.error(f"[OKX ORDER RESP] Lỗi đóng lệnh khẩn cho {swap_inst}: {e}")
            continue  # bỏ qua, không cho lệnh này tồn tại

        # 4) Lệnh đã mở thành công -> lưu vào CACHE
        trade_cache_item = {
            "coin": coin,          # ví dụ: 'BTC-USDT'
            "signal": signal,      # 'LONG' / 'SHORT'
            "entry": entry,
            "tp": tp,
            "sl": sl,
            "time": now_str_vn(),  # thời điểm vào lệnh theo VN
        }
        
        # Nếu muốn vẫn giữ cache JSON local thì có thể gọi cả 2:
        # append_trade_to_cache(trade_cache_item)
        
        # 🔥 Lưu lịch sử lên Google Drive (CSV)
        append_trade_to_drive(trade_cache_item)


        # Đóng thời thêm dòng Telegram (bỏ -USDT)
        coin_name = coin.replace("-USDT", "")
        line = f"{coin_name}-{signal}-{entry:.6f}"
        telegram_lines.append(line)


    # Sau khi duyệt hết các lệnh:
    if telegram_lines:
        msg = "📊 LỆNH FUTURE\n" + "\n".join(telegram_lines)
        send_telegram_message(msg)
    else:
        logging.info("[INFO] Không có lệnh futures nào được mở thành công.")

def run_dynamic_tp(okx: "OKXClient"):
    """
    TP động + SL động + TP trailing cho các lệnh futures đang mở.

    - Vẫn giữ:
        + Soft SL theo trend (SL_DYN_SOFT_PCT_GOOD/BAD, SL_DYN_LOOKBACK, SL_DYN_TREND_PCT)
        + SL khẩn cấp theo PnL% (MAX_EMERGENCY_SL_PNL_PCT)
        + TP động theo:
            * flat_move  (giá không tiến thêm)
            * engulfing  (nến đảo chiều nuốt)
            * vol_drop   (volume cạn)
            * ema_break  (giá phá EMA5)

    - Thêm:
        + TP trailing:
            * Nếu trong cửa sổ quan sát, lệnh đã từng đạt PnL% >= TP_TRAIL_START_PNL_PCT
              mà hiện tại PnL% <= TP_TRAIL_EXIT_PNL_PCT (gần hòa vốn) → đóng lệnh, coi như
              đã "kéo SL về entry" và không cho quay lại lỗ sâu nữa.
    """

    logging.info("[TP-DYN] === BẮT ĐẦU KIỂM TRA TP ===")

    positions = okx.get_open_positions()
    logging.info("[TP-DYN] Số vị thế đang mở: %d", len(positions))

    if not positions:
        logging.info("[TP-DYN] Không có vị thế futures nào đang mở.")
        return

    # --- BỐI CẢNH CHUNG ---
    in_deadzone = is_deadzone_time_vn()
    try:
        market_regime = detect_market_regime(okx)  # GOOD / BAD / ...
    except Exception as e:
        logging.error("[TP-DYN] Lỗi detect_market_regime: %s", e)
        market_regime = "UNKNOWN"

    # --- CONFIG CHO TP TRAILING ---
    # đã từng đạt PnL >= 5% mới bắt đầu trailing
    TP_TRAIL_START_PNL_PCT = 5.0
    # nếu đã từng >=5% mà giờ tụt về <= 0% thì chốt (không cho quay lại lỗ)
    TP_TRAIL_EXIT_PNL_PCT = 3.0
    # dùng toàn bộ 30 nến 5m hiện tại làm cửa sổ quan sát high/low PnL
    TP_TRAIL_LOOKBACK_BARS = 30

    for p in positions:
        try:
            instId  = p.get("instId")
            posSide = p.get("posSide")  # 'long' / 'short'
            pos     = safe_float(p.get("pos", "0"))
            avail   = safe_float(p.get("availPos", pos))
            sz      = avail if avail > 0 else pos
            avg_px  = safe_float(p.get("avgPx", "0"))

            logging.info("[TP-DYN] -> Kiểm tra %s | posSide=%s", instId, posSide)
        except Exception as e:
            logging.error("[TP-DYN] Lỗi đọc position: %s", e)
            continue

        if not instId or sz <= 0 or avg_px <= 0:
            continue

        # --- Lấy nến 5m ---
        try:
            c5 = okx.get_candles(instId, bar="5m", limit=TP_TRAIL_LOOKBACK_BARS)
        except Exception as e:
            logging.warning("[TP-DYN] Lỗi get_candles 5m %s: %s", instId, e)
            continue

        if not c5 or len(c5) < TP_DYN_FLAT_BARS + 10:
            # không đủ dữ liệu để đánh giá
            continue

        try:
            c5_sorted = sorted(c5, key=lambda x: int(x[0]))
        except Exception:
            c5_sorted = c5

        closes = [safe_float(k[4]) for k in c5_sorted]
        opens  = [safe_float(k[1]) for k in c5_sorted]
        highs  = [safe_float(k[2]) for k in c5_sorted]
        lows   = [safe_float(k[3]) for k in c5_sorted]
        vols   = [safe_float(k[5]) for k in c5_sorted]

        c_now   = closes[-1]
        c_prev1 = closes[-2]
        c_prev2 = closes[-3]

        o_now   = opens[-1]
        o_prev1 = opens[-2]
        h_prev1 = highs[-2]
        l_prev1 = lows[-2]
        vol_now = vols[-1]

        # ====== 1) TÍNH % GIÁ & % PnL (y như bản cũ) ======
        if posSide == "long":
            price_pct = (c_now - avg_px) / avg_px * 100.0
        else:  # short
            price_pct = (avg_px - c_now) / avg_px * 100.0

        pnl_pct = price_pct * FUT_LEVERAGE   # x5 → PnL% ≈ price% * 5

        # ====== 2) SL DYNAMIC (soft SL theo trend) ======
        # dùng cùng market_regime đã detect ở đầu hàm
        if market_regime == "BAD":
            soft_sl_pct = SL_DYN_SOFT_PCT_BAD
        else:
            soft_sl_pct = SL_DYN_SOFT_PCT_GOOD

        if pnl_pct <= -soft_sl_pct:
            # Lấy trend ngắn hạn (5m) để xem có ngược mạnh không
            try:
                swap_id = instId
                c = okx.get_candles(swap_id, bar="5m", limit=SL_DYN_LOOKBACK + 1)
                c_sorted = sorted(c, key=lambda x: int(x[0]))
                closes_tr = [float(k[4]) for k in c_sorted]
                if len(closes_tr) >= SL_DYN_LOOKBACK + 1:
                    base = closes_tr[-1 - SL_DYN_LOOKBACK]
                    trend_pct = (closes_tr[-1] - base) / base * 100.0
                else:
                    trend_pct = 0.0
            except Exception as e:
                logging.warning("[SL-DYN] Lỗi lấy candles cho %s: %s", instId, e)
                trend_pct = 0.0

            trend_against = False
            if posSide == "long" and trend_pct <= -SL_DYN_TREND_PCT:
                trend_against = True
            if posSide == "short" and trend_pct >= SL_DYN_TREND_PCT:
                trend_against = True

            if trend_against:
                logging.info(
                    "[SL-DYN] %s lỗ %.2f%% & trend ngược %.2f%% → CẮT LỖ SỚM (soft SL).",
                    instId,
                    pnl_pct,
                    trend_pct,
                )
                try:
                    okx.close_swap_position(instId, posSide)
                except Exception as e:
                    logging.error("[SL-DYN] Lỗi đóng lệnh %s: %s", instId, e)
                continue

        # ====== 3) SL KHẨN CẤP THEO PnL% (ví dụ -5% PnL) ======
        if pnl_pct <= -MAX_EMERGENCY_SL_PNL_PCT:
            logging.info(
                "[TP-DYN] %s lỗ %.2f%% <= -%.2f%% PnL → CẮT LỖ KHẨN CẤP.",
                instId,
                pnl_pct,
                MAX_EMERGENCY_SL_PNL_PCT,
            )
            try:
                okx.close_swap_position(instId, posSide)
            except Exception as e:
                logging.error("[TP-DYN] Lỗi đóng lệnh %s: %s", instId, e)
            continue

        # ====== 4) CHỌN NGƯỠNG KÍCH HOẠT TP ĐỘNG ======
        if in_deadzone:
            tp_dyn_threshold = 3.0  # deadzone: ăn ngắn
        else:
            if market_regime == "BAD":
                tp_dyn_threshold = 2.5   # thị trường xấu → ăn ngắn hơn
            else:
                tp_dyn_threshold = TP_DYN_MIN_PROFIT_PCT  # GOOD → config (mặc định 5%)

        if pnl_pct < tp_dyn_threshold:
            logging.info(
                "[TP-DYN] %s lãi %.2f%% < %.2f%% → bỏ qua TP động.",
                instId,
                pnl_pct,
                tp_dyn_threshold,
            )
            continue

        # ====== 5) TÍNH PnL CAO NHẤT TRONG CỬA SỔ (để trailing) ======
        max_pnl_window = 0.0
        for close_px in closes[-TP_TRAIL_LOOKBACK_BARS:]:
            if posSide == "long":
                price_pct_i = (close_px - avg_px) / avg_px * 100.0
            else:
                price_pct_i = (avg_px - close_px) / avg_px * 100.0
            pnl_pct_i = price_pct_i * FUT_LEVERAGE
            if pnl_pct_i > max_pnl_window:
                max_pnl_window = pnl_pct_i

        # ====== 6) 4 TÍN HIỆU TP ĐỘNG (giữ nguyên bản cũ) ======
        # 1) 3 nến không tiến thêm
        if posSide == "long":
            flat_move = not (c_now > c_prev1 > c_prev2)
        else:
            flat_move = not (c_now < c_prev1 < c_prev2)

        # 2) Engulfing đảo chiều
        body_now  = abs(c_now - o_now)
        body_prev = abs(c_prev1 - o_prev1)
        engulfing = False
        if posSide == "long":
            engulfing = (c_now < o_now) and (body_now > body_prev) and (c_now < l_prev1)
        else:
            engulfing = (c_now > o_now) and (body_now > body_prev) and (c_now > h_prev1)

        # 3) Volume drop
        vols_before = vols[-(TP_DYN_FLAT_BARS + 10):-1]
        avg_vol10 = sum(vols_before) / max(1, len(vols_before))
        vol_drop = (avg_vol10 > 0) and (
            (vol_now / avg_vol10) < TP_DYN_VOL_DROP_RATIO
        )

        # 4) EMA-5 break
        ema5 = calc_ema(closes[-(TP_DYN_EMA_LEN + 5):], TP_DYN_EMA_LEN)
        ema_break = False
        if ema5:
            if posSide == "long":
                ema_break = c_now < ema5
            else:
                ema_break = c_now > ema5

        logging.info(
            "[TP-DYN] %s pnl=%.2f%% (thr=%.2f%%, max_window=%.2f%%) | "
            "flat=%s | engulf=%s | vol_drop=%s | ema_break=%s",
            instId,
            pnl_pct,
            tp_dyn_threshold,
            max_pnl_window,
            flat_move,
            engulfing,
            vol_drop,
            ema_break,
        )

        # ====== 7) KẾT HỢP LOGIC TP ĐỘNG + TP TRAILING ======
        # TP động (logic cũ)
        should_close_dynamic = flat_move or engulfing or vol_drop or ema_break

        # TP trailing: đã từng lời >= 5% mà giờ tụt về quanh hòa vốn → không tham nữa
        should_close_trailing = (
            max_pnl_window >= TP_TRAIL_START_PNL_PCT
            and pnl_pct <= TP_TRAIL_EXIT_PNL_PCT
        )

        should_close = should_close_dynamic or should_close_trailing

        if should_close_trailing:
            logging.info(
                "[TP-TRAIL] %s đã từng lời >= %.2f%% (max=%.2f%%) nhưng hiện còn %.2f%% "
                "→ CHỐT THEO TRAILING (không cho quay lại lỗ).",
                instId,
                TP_TRAIL_START_PNL_PCT,
                max_pnl_window,
                pnl_pct,
            )

        if should_close:
            logging.info("[TP-DYN] → ĐÓNG vị thế %s (%s).", instId, posSide)
            try:
                okx.close_swap_position(instId, posSide)
            except Exception as e:
                logging.error("[TP-DYN] Lỗi đóng lệnh %s: %s", instId, e)
        else:
            logging.info("[TP-DYN] Giữ lệnh %s – chưa đến điểm thoát.", instId)

    logging.info("[TP-DYN] ===== DYNAMIC TP DONE =====")


def detect_market_regime(okx: "OKXClient"):
    """
    GOOD MARKET khi:
    - BTC 5m body đẹp (body_ratio > 0.55)
    - Wick không quá dài
    - Volume đều, không spike bất thường
    - Trend 5m/15m đồng pha
    BAD MARKET nếu ngược lại.
    """

    try:
        c5 = okx.get_candles("BTC-USDT-SWAP", bar="5m", limit=3)
        c15 = okx.get_candles("BTC-USDT-SWAP", bar="15m", limit=3)
    except:
        return "BAD"

    if not c5 or len(c5) < 2:
        return "BAD"

    # ==== 5m ====
    c5_s = sorted(c5, key=lambda x: int(x[0]))
    o5 = safe_float(c5_s[-1][1])
    h5 = safe_float(c5_s[-1][2])
    l5 = safe_float(c5_s[-1][3])
    c5_now = safe_float(c5_s[-1][4])

    body = abs(c5_now - o5)
    rng  = max(h5 - l5, 1e-8)
    body_ratio = body / rng

    # wick check
    upper_wick = h5 - max(o5, c5_now)
    lower_wick = min(o5, c5_now) - l5
    wick_ratio = (upper_wick + lower_wick) / rng

    # trend check 5m
    c_prev = safe_float(c5_s[-2][4])
    trend_5_up = c5_now > c_prev
    trend_5_dn = c5_now < c_prev

    # ==== 15m trend ====
    if c15 and len(c15) >= 2:
        c15_s = sorted(c15, key=lambda x: int(x[0]))
        c15_now = safe_float(c15_s[-1][4])
        c15_prev = safe_float(c15_s[-2][4])
        trend_15_up = c15_now > c15_prev
        trend_15_dn = c15_now < c15_prev
    else:
        trend_15_up = trend_15_dn = False

    # ======= RULES =======
    if (
        body_ratio > 0.55 and
        wick_ratio < 0.45 and
        (
            (trend_5_up and trend_15_up) or
            (trend_5_dn and trend_15_dn)
        )
    ):
        return "GOOD"

    return "BAD"

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
    # 0) chạy backtest
    run_backtest_if_needed(okx)

    # 1) Circuit breaker theo phiên: nếu lỗ quá -SESSION_MAX_LOSS_PCT% thì dừng mở lệnh mới
    logging.info("[BOT] Gọi check_session_circuit_breaker()...")
    if not check_session_circuit_breaker(okx):
        logging.info("[BOT] Circuit breaker kích hoạt → KHÔNG SCAN/MỞ LỆNH mới phiên này.")
        return
    logging.info("[BOT] Circuit breaker OK → tiếp tục chạy bot.")
    regime = detect_market_regime(okx)
    logging.info(f"[REGIME] Thị trường hiện tại: {regime}")
    if regime == "GOOD":
        current_notional = 30
    else:
        current_notional = 10
    # 2) CHỌN SCANNER THEO GIỜ
    if is_deadzone_time_vn():
        logging.info("[MODE] 10h30–15h30 VN -> dùng scanner SIDEWAY DEADZONE.")
        df_signals = build_signals_sideway_deadzone(okx)
    else:
        logging.info("[MODE] Ngoài deadzone -> dùng scanner PUMP/DUMP PRO.")
        df_signals = build_signals_pump_dump_pro(okx)

    logging.info("[INFO] Scanner trả về %d tín hiệu.", len(df_signals))

    if df_signals.empty:
        logging.info("[INFO] Không có tín hiệu hợp lệ, dừng bot lần chạy này.")
        return

    # 3) Google Sheet
    try:
        ws = prepare_worksheet()
        #existing = get_recent_signals(ws)
    except Exception as e:
        logging.error("[ERROR] Google Sheet prepare lỗi: %s", e)
        return

    # 4) Plan trades
    planned_trades = plan_trades_from_signals(df_signals, okx)

    # 5) Append sheet
    append_signals(ws, planned_trades)

    # 6) Futures + Telegram
    execute_futures_trades(okx, planned_trades)
    
def main():
    setup_logging()
    now_utc = datetime.now(timezone.utc)
    now_vn  = now_utc + timedelta(hours=7)   # VN = UTC+7
    minute  = now_vn.minute

    okx = OKXClient(
        api_key=os.getenv("OKX_API_KEY"),
        api_secret=os.getenv("OKX_API_SECRET"),
        passphrase=os.getenv("OKX_API_PASSPHRASE")
    )

    # 🔥 NEW: quyết định cấu hình risk mỗi lần cron chạy
    apply_risk_config(okx)
    # 1) TP động luôn chạy trước (dùng config mới)
    run_dynamic_tp(okx)
    #run_backtest_if_needed(okx)
    # 2) Các mốc 5 - 20 - 35 - 50 phút thì chạy thêm FULL BOT
    if minute % 15 == 5:
        logging.info("[SCHED] %02d' -> CHẠY FULL BOT", minute)
        run_full_bot(okx)
    else:
        logging.info("[SCHED] %02d' -> CHỈ CHẠY TP DYNAMIC", minute)

if __name__ == "__main__":
    main()
