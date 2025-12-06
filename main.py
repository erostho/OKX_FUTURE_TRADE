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

# Sheet dùng để lưu trạng thái circuit breaker
SESSION_SHEET_KEY = os.getenv("SESSION_SHEET_KEY")  # hoặc GSHEET_SESSION_KEY riêng nếu muốn
SESSION_STATE_SHEET_NAME = os.getenv("SESSION_STATE_SHEET_NAME", "SESSION_STATE")

# ========== CONFIG ==========
OKX_BASE_URL = "https://www.okx.com"
CACHE_FILE = os.getenv("TRADE_CACHE_FILE", "trade_cache.json")

# Trading config
FUT_LEVERAGE = 6              # x5 isolated
NOTIONAL_PER_TRADE = 30.0     # 30 USDT position size (ký quỹ ~5$ với x6)
MAX_TRADES_PER_RUN = 10       # tối đa 10 lệnh / 1 lần cron

# Circuit breaker theo phiên
SESSION_MAX_LOSS_PCT = 5.0  # Mỗi phiên lỗ tối đa -5% equity thì dừng trade
SESSION_STATE_FILE = os.getenv("SESSION_STATE_FILE", "session_state.json")

# Scanner config
MIN_ABS_CHANGE_PCT = 2.0      # chỉ lấy coin |24h change| >= 2%
MIN_VOL_USDT = 100000         # min 24h volume quote
TOP_N_BY_CHANGE = 300         # universe: top 300 theo độ biến động

# Google Sheet headers
SHEET_HEADERS = ["Coin", "Tín hiệu", "Entry", "SL", "TP", "Ngày"]
BT_CACHE_SHEET_NAME = "BT_TRADES_CACHE"   # tên sheet lưu cache lệnh đã đóng

# ======== DYNAMIC TP CONFIG ========
TP_DYN_MIN_PROFIT_PCT   = 2.5   # chỉ bật TP động khi lãi >= 2.5%
TP_DYN_MAX_FLAT_BARS    = 3     # số nến 5m gần nhất để kiểm tra
TP_DYN_VOL_DROP_RATIO   = 0.5   # vol hiện tại < 50% avg 10 nến -> yếu
TP_DYN_EMA_LEN          = 5     # EMA-5
TP_DYN_FLAT_BARS        = 2     # số nến 5m đi ngang trước khi thoát
TP_DYN_ENGULF           = True  # bật thoát khi có engulfing
TP_DYN_VOL_DROP         = True  # bật thoát khi vol giảm mạnh
TP_DYN_EMA_TOUCH        = True  # bật thoát khi chạm EMA5

# ======== TRAILING TP CONFIG ========
TRAIL_START_PROFIT_PCT = 5.0   # bắt đầu kích hoạt trailing khi lãi >= 5% PnL
TRAIL_GIVEBACK_PCT     = 3.0   # nếu giá hồi ngược lại >= 3% từ đỉnh → chốt
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
PUMP_MIN_VOL_USDT_24H   = 50000     # volume USDT 24h tối thiểu
PUMP_PRE_TOP_N          = 300       # lấy top 300 coin theo độ biến động 24h để refine

PUMP_MIN_CHANGE_15M     = 1.0       # %change 15m tối thiểu theo hướng LONG/SHORT
PUMP_MIN_CHANGE_5M      = 0.5       # %change 5m tối thiểu
PUMP_VOL_SPIKE_RATIO    = 0.1       # vol 15m hiện tại phải > 1x vol avg 10 nến trước

PUMP_MIN_CHANGE_1H      = 0.5       # %change 1h tối thiểu (tránh sóng quá yếu)
PUMP_MAX_CHANGE_1H      = 100.0     # %change 1h tối đa (tránh đu quá trễ)
DEADZONE_MIN_ATR_PCT    = 0.2       # ví dụ: 0.2%/5m trở lên mới chơi

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
    sheet_id = os.environ["BT_SHEET_ID"]
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
        ws.append_row(values)
    else:
        ws.update(f"A{bt_all_row_index}:H{bt_all_row_index}", [values])


def accumulate_bt_all_with_today(bt_today: dict) -> dict:
    """
    Cộng dồn BT_TODAY vào BT_ALL & lưu lên sheet.
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
    """
    regime = (regime or "GOOD").upper()
    session_flag = (session_flag or "GOOD").upper()

    # 1) Market GOOD, session GOOD → FULL GAS
    if regime == "GOOD" and session_flag == "GOOD":
        return {
            "leverage": 6,
            "notional": 25.0,
            "tp_dyn_min_profit": 5.0,
            "max_sl_pnl_pct": 5.0,
            "max_trades_per_run": 15,
        }

    # 2) Market GOOD, session BAD
    if regime == "GOOD" and session_flag == "BAD":
        return {
            "leverage": 4,
            "notional": 15.0,
            "tp_dyn_min_profit": 5.0,
            "max_sl_pnl_pct": 5.0,
            "max_trades_per_run": 10,
        }

    # 3) Market BAD, session GOOD
    if regime == "BAD" and session_flag == "GOOD":
        return {
            "leverage": 4,
            "notional": 20.0,
            "tp_dyn_min_profit": 3.0,
            "max_sl_pnl_pct": 4.0,
            "max_trades_per_run": 10,
        }

    # 4) Market BAD, session BAD → HARD DEFENSE
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
    """
    global FUT_LEVERAGE, NOTIONAL_PER_TRADE
    global TP_DYN_MIN_PROFIT_PCT, MAX_SL_PNL_PCT, MAX_TRADES_PER_RUN

    # DEADZONE: giữ nguyên style scalping an toàn
    if is_deadzone_time_vn():
        FUT_LEVERAGE = 3
        NOTIONAL_PER_TRADE = 10.0
        TP_DYN_MIN_PROFIT_PCT = 3.0
        MAX_SL_PNL_PCT = 3.0
        MAX_TRADES_PER_RUN = 5
        logging.info("[RISK] DEADZONE config: lev=3, notional=10, tp_dyn=3%%, maxSL=3%%, max_trades=5")
        return

    # Ngoài DEADZONE: dùng 2 tầng regime + session_flag
    try:
        regime = detect_market_regime(okx)
    except NameError:
        regime = "GOOD"

    try:
        session_flag = get_session_flag_for_next_session()  # nếu có
    except NameError:
        session_flag = "GOOD"

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
    try:
        return float(x)
    except Exception:
        return default


def percent_change(new, old):
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
    return (datetime.utcnow() + timedelta(hours=7)).strftime("%d/%m/%Y %H:%M")


def parse_trade_time_to_utc_ms(time_str: str) -> int | None:
    try:
        dt_vn = datetime.strptime(time_str, "%d/%m/%Y %H:%M")
        dt_utc = dt_vn - timedelta(hours=7)
        return int(dt_utc.timestamp() * 1000)
    except Exception:
        return None


def is_quiet_hours_vn():
    now_vn = datetime.utcnow() + timedelta(hours=7)
    return now_vn.hour >= 23 or now_vn.hour < 6


def is_backtest_time_vn():
    """
    Chạy backtest theo PHIÊN:
      - 09:05  -> tổng kết phiên 0–9
      - 15:05  -> tổng kết phiên 9–15
      - 17:50  -> tổng kết phiên 15–20 (bạn đang để 17)
      - 22:50  -> tổng kết phiên 20–24
    """
    now_vn = datetime.utcnow() + timedelta(hours=7)
    h = now_vn.hour
    m = now_vn.minute

    if h in (9, 15, 18) and 5 <= m <= 59:
        return True
    if h == 22 and 50 <= m <= 59:
        return True
    return False


def is_deadzone_time_vn():
    """
    Phiên trưa 'deadzone' 10:30 - 16:00 giờ VN.
    """
    now_vn = datetime.utcnow() + timedelta(hours=7)
    h = now_vn.hour
    m = now_vn.minute

    if h == 10 and m >= 30:
        return True
    if 11 <= h < 16:
        return True
    return False


def get_current_session_vn():
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
        path = "/api/v5/market/tickers"
        params = {"instType": "SWAP"}
        data = self._request("GET", path, params=params)
        return data.get("data", [])

    def get_swap_instruments(self):
        path = "/api/v5/public/instruments"
        params = {"instType": "SWAP"}
        data = self._request("GET", path, params=params)
        return data.get("data", [])

    def get_open_positions(self):
        path = "/api/v5/account/positions?instType=SWAP"
        data = self._request("GET", path, params=None)
        return data.get("data", [])

    def get_positions_history(self, inst_type="SWAP", after=None, limit=1000):
        qs = f"instType={inst_type}&limit={limit}"
        if after:
            qs += f"&after={after}"
        path = f"/api/v5/account/positions-history?{qs}"
        data = self._request("GET", path, params=None)
        return data.get("data", [])

    def get_usdt_balance(self):
        path = "/api/v5/account/balance?ccy=USDT"
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
        path = "/api/v5/account/balance?ccy=USDT"
        data = self._request("GET", path, params=None)

        details = data.get("data", [])
        if not details:
            return 0.0

        detail = details[0]
        if "details" in detail and detail["details"]:
            eq = float(detail["details"][0].get("eq", "0"))
        else:
            eq = float(detail.get("eq", "0"))

        logging.info("[INFO] Tổng equity USDT (eq): %.8f", eq)
        return eq

    def set_leverage(self, inst_id, lever=FUT_LEVERAGE, pos_side=None, mgn_mode="isolated"):
        path = "/api/v5/account/set-leverage"
        body = {
            "instId": inst_id,
            "lever": str(lever),
            "mgnMode": mgn_mode,
        }
        if pos_side is not None:
            body["posSide"] = pos_side

        data = self._request("POST", path, body_dict=body)
        logging.info("[INFO] SET LEVERAGE RESP: %s", data)
        return data

    def place_futures_market_order(
        self, inst_id, side, pos_side, sz, td_mode="isolated", lever=FUT_LEVERAGE
    ):
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
        path = "/api/v5/trade/order-algo"
        body = {
            "instId": inst_id,
            "tdMode": td_mode,
            "side": side_close,
            "posSide": pos_side,
            "ordType": "oco",
            "sz": str(sz),
            "tpTriggerPx": f"{tp_px:.8f}",
            "tpOrdPx": "-1",
            "slTriggerPx": f"{sl_px:.8f}",
            "slOrdPx": "-1",
            "tpTriggerPxType": "last",
            "slTriggerPxType": "last",
        }
        logging.info("---- PLACE OCO TP/SL ----")
        logging.info("Body: %s", body)
        data = self._request("POST", path, body_dict=body)
        logging.info("[OKX OCO RESP] %s", data)
        return data

    def close_swap_position(self, inst_id, pos_side):
        path = "/api/v5/trade/close-position"
        body = {
            "instId": inst_id,
            "mgnMode": "isolated",
            "posSide": pos_side,
        }
        logging.info(f"[OKX] Close position: {inst_id} | {pos_side}")
        return self._request("POST", path, body_dict=body)


# ========= CÁC HÀM CACHE TRADES CHO BACKTEST REAL =========

def load_trade_cache():
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


# ===== SESSION SHEET (circuit breaker) =====
def get_session_worksheet():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    sa_info_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not sa_info_json:
        logging.error("[SESSION] GOOGLE_SERVICE_ACCOUNT_JSON chưa cấu hình.")
        return None

    try:
        sa_info = json.loads(sa_info_json)
    except Exception as e:
        logging.error("[SESSION] Lỗi parse GOOGLE_SERVICE_ACCOUNT_JSON: %s", e)
        return None

    try:
        creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
        gc = gspread.authorize(creds)
    except Exception as e:
        logging.error("[SESSION] Lỗi khởi tạo gspread: %s", e)
        return None

    if not SESSION_SHEET_KEY:
        logging.error("[SESSION] SESSION_SHEET_KEY chưa cấu hình.")
        return None

    try:
        sh = gc.open_by_key(SESSION_SHEET_KEY)
        try:
            ws = sh.worksheet(SESSION_STATE_SHEET_NAME)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(
                title=SESSION_STATE_SHEET_NAME,
                rows=10,
                cols=10
            )
            ws.append_row(["date", "session", "start_equity", "blocked"])
        return ws
    except Exception as e:
        logging.error("[SESSION] Lỗi get_session_worksheet: %s", e)
        return None


# ===== SHEET CACHE REAL TRADES (BT_TRADES_CACHE) =====

def get_bt_cache_worksheet():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    sa_info_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not sa_info_json:
        logging.error("[BT-CACHE] GOOGLE_SERVICE_ACCOUNT_JSON chưa cấu hình.")
        return None

    creds = Credentials.from_service_account_info(json.loads(sa_info_json), scopes=scopes)
    gc = gspread.authorize(creds)

    sheet_id = os.getenv("BT_SHEET_ID")
    if not sheet_id:
        logging.error("[BT-CACHE] BT_SHEET_ID chưa cấu hình.")
        return None

    sh = gc.open_by_key(sheet_id)

    try:
        ws = sh.worksheet(BT_CACHE_SHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=BT_CACHE_SHEET_NAME, rows=10, cols=10)
        ws.append_row(["posId", "instId", "side", "sz",
                       "openPx", "closePx", "pnl", "cTime"])
        logging.info("[BT-CACHE] Tạo sheet %s mới.", BT_CACHE_SHEET_NAME)

    return ws


def load_bt_cache():
    """
    Đọc toàn bộ cache trades từ sheet BT_TRADES_CACHE.
    Trả về list[dict].
    """
    ws = get_bt_cache_worksheet()
    if not ws:
        return []

    rows = ws.get_all_records()
    trades = []
    for r in rows:
        if not r.get("posId"):
            continue
        try:
            trades.append({
                "posId": str(r.get("posId", "")),
                "instId": r.get("instId", ""),
                "side": r.get("side", ""),
                "sz": float(r.get("sz", 0) or 0),
                "openAvgPx": float(r.get("openPx", 0) or 0),
                "closeAvgPx": float(r.get("closePx", 0) or 0),
                "pnl": float(r.get("pnl", 0) or 0),
                "cTime": int(r.get("cTime", 0) or 0),
            })
        except Exception as e:
            logging.error("[BT-CACHE] Lỗi parse row %s: %s", r, e)
    logging.info("[BT-CACHE] Load cache: %d trades.", len(trades))
    return trades


def append_bt_cache(new_trades):
    """
    Append thêm các lệnh mới vào sheet cache, CHỐNG TRÙNG bằng posId.
    """
    if not new_trades:
        return

    ws = get_bt_cache_worksheet()
    if not ws:
        return

    # lấy toàn bộ posId đã có (cột A, bỏ header)
    try:
        existing_pos_ids = ws.col_values(1)[1:]
    except Exception:
        existing_pos_ids = []
    existing_set = set(str(x) for x in existing_pos_ids if x)

    rows = []
    added = 0
    for t in new_trades:
        pos_id = str(t.get("posId", "") or "")
        if not pos_id:
            continue
        if pos_id in existing_set:
            continue  # đã có trong cache
        existing_set.add(pos_id)

        rows.append([
            pos_id,
            t.get("instId", ""),
            t.get("side", ""),
            t.get("sz", ""),
            t.get("openAvgPx", ""),
            t.get("closeAvgPx", ""),
            t.get("pnl", ""),
            t.get("cTime", ""),
        ])
        added += 1

    if rows:
        ws.append_rows(rows, value_input_option="RAW")
    logging.info("[BT-CACHE] Append %d trades mới vào cache.", added)


# ======= SESSION STATE (circuit breaker) TIẾP =======

def load_session_state(today: str, session: str):
    ws = get_session_worksheet()
    if ws is None:
        return None

    try:
        records = ws.get_all_records()
    except Exception as e:
        logging.error("[SESSION] Lỗi load_session_state: %s", e)
        return None

    filtered = [r for r in records
                if str(r.get("date")) == today and str(r.get("session")) == session]

    if not filtered:
        return None
    return filtered[-1]


def save_session_state(state: dict):
    ws = get_session_worksheet()
    if ws is None:
        return

    try:
        ws.append_row(
            [
                state.get("date"),
                state.get("session"),
                float(state.get("start_equity", 0)),
                bool(state.get("blocked", False)),
            ]
        )
    except Exception as e:
        logging.error("[SESSION] Lỗi save_session_state: %s", e)


def check_session_circuit_breaker(okx) -> bool:
    now_vn = datetime.utcnow() + timedelta(hours=7)
    today = now_vn.date().isoformat()
    session = get_current_session_vn()
    equity = okx.get_total_equity_usdt()
    max_loss_pct = float(os.getenv("SESSION_MAX_LOSS_PCT", "5"))

    logging.info(
        "[SESSION] Thời gian VN: %s, phiên hiện tại: %s | equity=%.4f",
        now_vn, session, equity
    )

    state = load_session_state(today, session)

    if state is None:
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
        return True

    start_equity = float(state.get("start_equity", 0) or 0)
    blocked = str(state.get("blocked", "")).upper() == "TRUE"

    logging.info(
        "[SESSION] State hiện tại: date=%s, session=%s, blocked=%s, start_equity=%.4f",
        state.get("date"), state.get("session"), blocked, start_equity
    )

    if blocked:
        logging.warning(
            "[SESSION] Phiên %s đang BỊ KHÓA (đã lỗ quá %.1f%%). Không mở lệnh mới!",
            session, max_loss_pct
        )
        return False

    if start_equity <= 0:
        start_equity = equity
        state = {
            "date": today,
            "session": session,
            "start_equity": start_equity,
            "blocked": False,
        }
        save_session_state(state)
        logging.warning(
            "[SESSION] start_equity <= 0 -> đặt lại bằng equity=%.4f cho phiên %s",
            equity, session
        )
        return True

    pnl_pct = (equity - start_equity) / start_equity * 100.0

    logging.info(
        "[SESSION] PnL phiên %s: %.2f%% (equity=%.4f, start_equity=%.4f, max_loss=%.1f%%)",
        session, pnl_pct, equity, start_equity, max_loss_pct
    )

    if pnl_pct <= -max_loss_pct:
        state = {
            "date": today,
            "session": session,
            "start_equity": start_equity,
            "blocked": True,
        }
        save_session_state(state)
        logging.warning(
            "[SESSION] Phiên %s BỊ KHÓA do lỗ %.2f%% (ngưỡng=%.1f%%). Không mở lệnh mới!",
            session, pnl_pct, max_loss_pct
        )
        return False

    logging.info("[SESSION] Circuit breaker OK -> tiếp tục cho phép mở lệnh.")
    return True


# ===== BACKTEST REAL: LẤY HISTORY TỪ OKX + CACHE =====

def load_real_trades_for_backtest(okx):
    """
    Dùng real history:
    - Lần 1: load tất cả từ OKX, lưu cache.
    - Các lần sau: chỉ lấy thêm các lệnh mới đóng sau cTime cuối cùng trong cache.
    """
    cached = load_bt_cache()
    last_c_time = max((t["cTime"] for t in cached), default=0)
    logging.info("[BACKTEST] Cache hiện tại: %d trades, last_c_time=%s",
                 len(cached), last_c_time or "None")

    new_raw = okx.get_positions_history(inst_type="SWAP", after=last_c_time, limit=100)
    new_trades = []
    for d in new_raw:
        try:
            new_trades.append({
                "posId": d.get("posId"),
                "instId": d.get("instId"),
                "side": d.get("side"),
                "sz": float(d.get("sz", 0) or 0),
                "openAvgPx": float(d.get("openAvgPx", 0) or 0),
                "closeAvgPx": float(d.get("closeAvgPx", 0) or 0),
                "pnl": float(d.get("pnl", 0) or 0),
                "cTime": int(d.get("cTime", 0) or 0),
            })
        except Exception as e:
            logging.error("[BACKTEST] Lỗi parse history item %s: %s", d, e)

    logging.info("[BACKTEST] Lịch sử mới lấy từ OKX: %d trades.", len(new_trades))

    # lưu thêm (đã có chống trùng trong append_bt_cache)
    append_bt_cache(new_trades)

    # load lại toàn bộ cache để chắc chắn là sạch trùng
    all_trades = load_bt_cache()
    logging.info("[BACKTEST] Tổng số trades dùng để BT ALL: %d", len(all_trades))
    return all_trades


def summarize_real_backtest(trades: list[dict]) -> tuple[str, str, str]:
    """
    Trả về 3 đoạn text:
      - msg_all
      - msg_today
      - msg_session
    Dựa trên PnL REAL từ OKX (field 'pnl') + BT_ALL cộng dồn trong sheet BT_CACHE.
    """
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

    def get_vn_dt(t: dict):
        ctime_str = t.get("cTime")
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

    trades_today: list[tuple[dict, datetime]] = []
    for t in trades:
        dt_vn = get_vn_dt(t)
        if dt_vn is None:
            continue
        if dt_vn.date() == today_date:
            trades_today.append((t, dt_vn))

    # ALL
    total, tp, sl, even, pnl_sum, win = classify(trades)

    # TODAY
    only_today = [t for (t, _dt) in trades_today]
    t_total, t_tp, t_sl, t_even, t_pnl_sum, t_win = classify(only_today)

    bt_today = {
        "total": t_total,
        "tp": t_tp,
        "sl": t_sl,
        "open": t_even,
        "pnl_usdt": t_pnl_sum,
    }

    # cộng dồn vào BT_ALL
    bt_all = accumulate_bt_all_with_today(bt_today)

    msg_all = (
        f"[✅BT ALL] total={bt_all['total']} | "
        f"TP={bt_all['tp']} SL={bt_all['sl']} OPEN={bt_all['open']} | "
        f"win={(bt_all['tp'] * 100 / bt_all['total']) if bt_all['total'] else 0:.1f}% | "
        f"PNL={bt_all['pnl_usdt']:+.2f} USDT"
    )

    msg_today = (
        f"[✅BT TODAY] total={bt_today['total']} | "
        f"TP={bt_today['tp']} SL={bt_today['sl']} OPEN={bt_today['open']} | "
        f"win={(bt_today['tp'] * 100 / bt_today['total']) if bt_today['total'] else 0:.1f}% | "
        f"PNL={bt_today['pnl_usdt']:+.2f} USDT"
    )

    sessions = [
        ("0-9", 0, 9),
        ("9-15", 9, 15),
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


# (phần cũ load_history_from_drive / trade_cache vẫn giữ nguyên cho bot khác nếu cần)
def save_trade_cache(trades):
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(trades, f, ensure_ascii=False)
    except Exception as e:
        logging.error("Lỗi save cache: %s", e)


def append_trade_to_cache(trade: dict):
    trades = load_trade_cache()
    trades.append(trade)
    save_trade_cache(trades)


def eval_trades_with_prices(trades, price_map, only_today: bool):
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
    except Exception:
        return None
    t = h + m / 60.0
    if 0 <= t < 9:
        return "0-9"
    elif 9 <= t < 15:
        return "9-15"
    elif 15 <= t < 20:
        return "15-20"
    else:
        return "20-24"


# ===== HÀM BACKTEST REAL TRIGGER THEO LỊCH =====

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
        logging.info("[BACKTEST] Không nằm trong khung giờ backtest, bỏ qua.")
        return

    trades = load_real_trades_for_backtest(okx)
    msg_all, msg_today, msg_session = summarize_real_backtest(trades)

    send_telegram_message(msg_all + "\n" + msg_today + "\n\n" + msg_session)


# ================= GOOGLE SHEETS KHÁC, DRIVE, TELEGRAM, SCANNER, TP DYNAMIC, v.v.
# (Toàn bộ phần dưới giữ nguyên như file bạn gửi – mình chỉ bỏ đoạn "..." lỗi cú pháp)

# ... 🔽 TỪ ĐÂY TRỞ XUỐNG LÀ NGUYÊN XI NHƯ FILE BẠN (scanner, TP động, execute, v.v.)
# Do giới hạn trả lời, mình không lặp lại từng dòng ở đây nữa.
# Bạn chỉ cần ghép đoạn BACKTEST mới ở trên vào đúng file hiện tại của bạn:
# - Thay thế các hàm load_bt_cache / append_bt_cache / load_real_trades_for_backtest /
#   summarize_real_backtest / run_backtest_if_needed bằng bản mình vừa gửi.
# - Các hàm khác dưới phần scanner + execute + main giữ y chang.
