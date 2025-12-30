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
from google.oauth2.service_account import Credentials
import gspread
from google.oauth2 import service_account
from urllib.parse import urlencode
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
import io
import csv
import tempfile
from typing import Optional
# Sheet dùng để lưu trạng thái circuit breaker
SESSION_SHEET_KEY = os.getenv("SESSION_SHEET_KEY")  # hoặc GSHEET_SESSION_KEY riêng nếu muốn
SESSION_STATE_SHEET_NAME = os.getenv("SESSION_STATE_SHEET_NAME", "SESSION_STATE")

# ========== CONFIG ==========
OKX_BASE_URL = "https://www.okx.com"
CACHE_FILE = os.getenv("TRADE_CACHE_FILE", "trade_cache.json")

# Trading config
FUT_LEVERAGE = 6              # x6 isolated
NOTIONAL_PER_TRADE = 30.0     # 30 USDT position size (ký quỹ ~5$ với x6)
MAX_TRADES_PER_RUN = 10       # tối đa 10 lệnh / 1 lần cron

# Circuit breaker theo phiên
SESSION_MAX_LOSS_PCT = 5.0  # Mỗi phiên lỗ tối đa -5% equity thì dừng trade
SESSION_STATE_FILE = os.getenv("SESSION_STATE_FILE", "session_state.json")
# ===== PRO: Symbol cooldown after consecutive SL =====
SYMBOL_COOLDOWN_FILE = os.getenv("SYMBOL_COOLDOWN_FILE", "symbol_cooldown.json")
SYMBOL_SL_WINDOW_MINUTES = 120     # xét chuỗi SL trong 4h gần nhất
SYMBOL_SL_STREAK_TRIGGER = 3       # SL liên tiếp >=3 thì khóa
SYMBOL_COOLDOWN_MINUTES = 60      # khóa 2 giờ

# Scanner config
MIN_ABS_CHANGE_PCT = 2.0      # chỉ lấy coin |24h change| >= 2%
MIN_VOL_USDT = 500000         # min 24h volume quote
TOP_N_BY_CHANGE = 300         # universe: top 300 theo độ biến động

# Google Sheet headers
SHEET_HEADERS = ["Coin", "Tín hiệu", "Entry", "SL", "TP", "Ngày"]
BT_CACHE_SHEET_NAME = "BT_TRADES_CACHE"   # tên sheet lưu cache lệnh đã đóng

# ======== DYNAMIC TP CONFIG ========
TP_DYN_MIN_PROFIT_PCT   = 4.0   # chỉ bật TP động khi lãi >= 4.0%
TP_DYN_MAX_FLAT_BARS    = 3     # số nến 5m gần nhất để kiểm tra
TP_DYN_VOL_DROP_RATIO   = 0.4   # vol hiện tại < 40% avg 10 nến -> yếu
TP_DYN_EMA_LEN          = 8     # EMA-8
TP_DYN_FLAT_BARS        = 3     # số nến 5m đi ngang trước khi thoát
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

# ---- FOLLOW-THROUGH filter: phân biệt continuation vs spike exhaustion (5m) ----
PUMP_FOLLOW_THROUGH_ENABLED = True
# candle confirm (nến -2) phải cùng hướng và có lực tối thiểu so với nến spike (nến -3)
PUMP_FT_BODY_MIN_RATIO = 0.50      # body_confirm >= 50% body_spike
PUMP_FT_VOL_MIN_RATIO  = 0.60      # vol_confirm  >= 60% vol_spike
PUMP_FT_CLOSEPOS_LONG_MIN  = 0.60  # close nằm trên 60% range (nến confirm) cho LONG
PUMP_FT_CLOSEPOS_SHORT_MAX = 0.40  # close nằm dưới 40% range (nến confirm) cho SHORT

# SL planned tối đa (khi đặt TP/SL ban đầu)
MAX_PLANNED_SL_PNL_PCT = 6.0   # cho phép lỗ tối đa 6% PnL nếu chạm SL
MAX_SL_PNL_PCT = 6
# SL khẩn cấp theo PnL%
MAX_EMERGENCY_SL_PNL_PCT = 4.5  # qua -4.5% là cắt khẩn cấp
# ===== TRAILING SERVER-SIDE (OKX ALGO) =====
TP_TRAIL_SERVER_MIN_PNL_PCT = 8.0   # chỉ bật trailing server khi PnL >= 8%
TRAIL_SERVER_CALLBACK_PCT = 1.3   # giá rút lại 7% từ đỉnh thì cắt
# ===== PUMP/DUMP DETECT + CONTROLLED TRAIL WIDEN =====
PUMP_CHG_5M_PCT      = 2.8    # % biến động 5m để coi là giật mạnh
PUMP_VOL_RATIO       = 2.5    # vol_now / avg_vol20
PUMP_RANGE_RATIO     = 1.8    # (high-low)_now / avg_range20
PUMP_WINDOW_SEC      = 5 * 60 # giữ "pump mode" trong 5 phút

PUMP_CB_MULT         = 1.8    # nới callback = base * mult khi pump
PUMP_CB_MAX_PCT      = 3.0    # cap callback khi pump

HARD_STOP_PCT        = 4.0    # A) hard floor theo peak (%)
TIME_UNDER_STOP_SEC  = 30     # B) giá dưới stop liên tục bao lâu mới thoát

# ===== PRO: PROFIT LOCK (<10%) =====
PROFIT_LOCK_ENABLED = True
PROFIT_LOCK_ONLY_BELOW_SERVER = True   # chỉ áp dụng khi pnl < TP_TRAIL_SERVER_MIN_PNL_PCT
PROFIT_LOCK_TIER_1_PEAK = 8.0   # nếu đã từng >=8%
PROFIT_LOCK_TIER_1_FLOOR = 4.0  # thì không cho rơi dưới +4%

# ===== BE ladder state =====
TP_BE_TIER = {}  # key -> tier đã set (0/1/2/3...)
# Mỗi mốc chỉ update 1 lần

TP_BE_TIERS = [
    #(3.0, 0.15),  # >=2%  -> BE +0.15%
    (5.0, 0.25),  # >=5%  -> BE +0.25% 
    (8.0, 0.35),  # >=8%  -> BE +0.35%
]

# ===== PRO: LADDER TP TRAIL (<10%) + BE =====
# Rule:
# - pnl >= 3%  -> kéo SL về BE (update OCO SL)
# - peak>=7% & pnl<=3%  -> chốt
# - peak>=9% & pnl<=5%  -> chốt
# - peak>=10% -> giao cho trailing server-side hiện có
TP_LADDER_BE_TRIGGER_PNL_PCT = 5.0
TP_LADDER_BE_OFFSET_PCT = 0.2  # tránh quét đúng entry (0.05~0.2)
TP_LADDER_RULES = [(9.0, 5.0), (7.0, 3.0)]  # check từ bậc cao -> thấp
TP_LADDER_SERVER_THRESHOLD = 8.0
TP_LADDER_BE_MOVED = {}  # key=f"{instId}_{posSide}" -> bool
EARLY_FAIL_REACHED_PROFIT = {}  # key=f"{instId}_{posSide}" -> bool

# ===== EARLY FAIL-SAFE (anti reverse right after entry) =====
EARLY_FAIL_NEVER_REACHED_PROFIT_PCT = 4   # chưa từng đạt +3.5%
EARLY_FAIL_CUT_LOSS_PCT = -4              # mà đã xuống -4% => cắt ngay

# ======== TRAILING TP CONFIG ========
TP_TRAIL_MIN_PNL_PCT   = 8.0   # chỉ bắt đầu trailing khi pnl >= 8%
TP_TRAIL_CALLBACK_PCT  = 1.3    # giá rút lại 7%pnl từ đỉnh thì cắt

# Lưu đỉnh PnL cho từng vị thế để trailing local
# key: f"{instId}_{posSide}_{posId}" -> value: peak_pnl_pct (float)
TP_TRAIL_PEAK_PNL = {}
#ANTI_SWEEP_LOCK_UNTIL = None  # type: Optional[datetime.datetime]
# ====== ANTI-SWEEP / SHORT-TERM DEADZONE CONFIG ======
ANTI_SWEEP_MOVE_PCT = 1.0
ANTI_SWEEP_LOCK_MINUTES = 10
# ===== LOCK/UNLOCK CONFIG =====
DEADZONE_HARD_LOCK_ENABLED = True          # 15-20 VN: KHÔNG mở lệnh mới (chỉ quản lý lệnh đang mở)

DAY_HARD_STOP_ENABLED = True
DAY_MAX_LOSS_USDT = float(os.getenv("DAY_MAX_LOSS_USDT", "4.0"))  # lỗ ngày <= -4.0 USDT thì khóa mở lệnh mới

MARKET_SOFT_LOCK_ENABLED = True
MARKET_BAD_LOCK_AFTER = int(os.getenv("MARKET_BAD_LOCK_AFTER", "5"))  # BAD liên tiếp N lần thì lock
MARKET_UNLOCK_COOLDOWN_MIN = int(os.getenv("MARKET_UNLOCK_COOLDOWN_MIN", "12"))  # tránh flip lock/unlock liên tục

# UNLOCK rule (BTC 15m)
UNLOCK_BTC_BAR = "15m"
UNLOCK_BTC_LIMIT = 50
UNLOCK_VOL_MULT = float(os.getenv("UNLOCK_VOL_MULT", "1.25"))  # vol_now >= 1.25x median(vol_prev20)
UNLOCK_BODY_RATIO_MIN = float(os.getenv("UNLOCK_BODY_RATIO_MIN", "0.4"))  # body2 >= 40% body1
UNLOCK_HOLD_TOL = float(os.getenv("UNLOCK_HOLD_TOL", "0.006"))  # giữ giá: 0.6%
# ===== DEADZONE OVERRIDE (only if STRONG EDGE) =====
DEADZONE_OVERRIDE_ENABLED = True

DEADZONE_OVERRIDE_BTC_INST = "BTC-USDT-SWAP"
DEADZONE_OVERRIDE_BTC_BAR  = "15m"
DEADZONE_OVERRIDE_ALT_BAR  = "5m"

DEADZONE_OVERRIDE_MIN_ALTS = 1          # >=1 coin khác confirm FT cùng hướng BTC
DEADZONE_OVERRIDE_ALT_TOPN = 200        # lấy top N theo 24h change để test nhanh

# Follow-through strictness
DEADZONE_FT_BODY_RATIO_MIN = 0.55       # body / range >= 55%
DEADZONE_FT_MIN_CHANGE_PCT = 0.15       # mỗi nến phải đi >= 0.15% (tránh nhiễu)
DEADZONE_FT_VOL_MULT       = 1.5        # vol candle2 >= 1.5x median(vol prev20)

# Safety: nếu data thiếu thì coi như FAIL
DEADZONE_OVERRIDE_FAILSAFE_LOCK = True

# ===== PRO: ANTI-SWEEP per symbol (ALT) =====
ALT_SWEEP_MOVE_PCT = 1.0            # mỗi chiều >=1% trong 1 nến 5m
ALT_SWEEP_LOCK_MINUTES = 15         # khóa symbol 15 phút
ALT_SWEEP_LOCKS: dict[str, int] = {}  # instId -> lock_until_utc_ms

PUMP_MIN_ABS_CHANGE_24H = 2.0       # |%change 24h| tối thiểu để được xem xét (lọc coin chết)
PUMP_MIN_VOL_USDT_24H   = 500000     # volume USDT 24h tối thiểu
PUMP_PRE_TOP_N          = 300       # lấy top 300 coin theo độ biến động 24h để refine

PUMP_MIN_CHANGE_15M     = 1.0       # %change 15m tối thiểu theo hướng LONG/SHORT
PUMP_MIN_CHANGE_5M      = 0.5       # %change 5m tối thiểu
PUMP_VOL_SPIKE_RATIO    = 0.1       # vol 15m hiện tại phải > 1x vol avg 10 nến trước

PUMP_MIN_CHANGE_1H      = 0.5       # %change 1h tối thiểu (tránh sóng quá yếu)
PUMP_MAX_CHANGE_1H      = 100.0     # %change 1h tối đa (tránh đu quá trễ)
DEADZONE_MIN_ATR_PCT    = 0.2       # ví dụ: 0.2%/5m trở lên mới chơi

S16_20_SKIP_PROB=0.40
S20_24_SKIP_PROB=0.80

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
import os, json, time, random
from datetime import datetime

VN_TZ_OFFSET_HOURS = int(os.getenv("VN_TZ_OFFSET_HOURS", "7"))

def _vn_now():
    # nếu server chạy giờ VN sẵn thì bạn có thể đổi thành datetime.now()
    return datetime.utcnow().timestamp() + VN_TZ_OFFSET_HOURS * 3600

def _vn_hour():
    return datetime.fromtimestamp(_vn_now()).hour
    
def _is_session_16_20(now=None):
    if now is None:
        now = datetime.utcnow() + timedelta(hours=7)
    h = now.hour
    return 16 <= h < 20
def _is_session_20_24():
    h = _vn_hour()
    return 20 <= h < 24

def _is_strong_trend(market_regime=None, confidence=None, trend_score=None):
    """
    Fallback-safe: nếu thiếu biến => coi như KHÔNG mạnh.
    Bạn map các biến đang có của bot vào 3 tham số này khi gọi.
    """
    try:
        if market_regime is not None and str(market_regime).upper() == "TREND":
            if confidence is not None and float(confidence) >= 70:
                return True
            if trend_score is not None and float(trend_score) >= 80:
                return True
    except:
        pass
    return False
def _allow_trade_session_16_20(market_regime=None, confidence=None, trend_score=None):
    if not _is_session_16_20():
        return True, "ok:not_16_20"

    # 16–20: chỉ cho khi trend rõ, còn lại giảm tần suất
    if market_regime and str(market_regime).upper() == "TREND":
        try:
            if confidence is not None and float(confidence) >= 60:
                return True, "ok:trend_conf_16_20"
            if trend_score is not None and float(trend_score) >= 65:
                return True, "ok:trend_score_16_20"
        except:
            pass

    skip_prob = float(os.getenv("S16_20_SKIP_PROB", "0.45"))  # 45%
    if random.random() < skip_prob:
        return False, f"skip:16_20_throttle({skip_prob:.2f})"

    return True, "ok:16_20_lucky_pass"

def _allow_trade_session_20_24(market_regime=None, confidence=None, trend_score=None):
    if not _is_session_20_24():
        return True, "ok:not_20_24"

    # 20-24: chỉ cho nếu trend cực mạnh, còn lại giảm tần suất (mặc định skip 85%)
    if _is_strong_trend(market_regime, confidence, trend_score):
        return True, "ok:strong_trend_20_24"

    skip_prob = float(os.getenv("S20_24_SKIP_PROB", "0.8"))  # 0.8 -> 1.00
    if random.random() < skip_prob:
        return False, f"skip:20_24_throttle({skip_prob:.2f})"

    return True, "ok:20_24_lucky_pass"

TRADE_GUARD_FILE = os.getenv("TRADE_GUARD_FILE", "./trade_guard.json")

from decimal import Decimal, ROUND_DOWN

def floor_to_step(x: float, step: float) -> float:
    if step <= 0:
        return float(x)
    xd = Decimal(str(x))
    sd = Decimal(str(step))
    n = (xd / sd).to_integral_value(rounding=ROUND_DOWN)
    return float(n * sd)

def normalize_swap_sz(okx, inst_id: str, sz: float) -> float:
    # nếu lỡ truyền tuple/list thì lấy phần tử đầu
    if isinstance(sz, (tuple, list)):
        sz = sz[0]

    ins_list = okx.get_swap_instruments()  # hàm của mày KHÔNG nhận inst_id
    ins = next((x for x in ins_list if x.get("instId") == inst_id), None)
    if not ins:
        return float(sz)

    lot = float(ins.get("lotSz", "1"))  # set lot trước
    min_sz = float(ins.get("minSz", "0"))  # default string/number, không dùng lot
    sz2 = floor_to_step(float(sz), lot)
    if min_sz > 0:
        sz2 = max(sz2, min_sz)
    return sz2

def _load_guard_state():
    try:
        with open(TRADE_GUARD_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {}

def _save_guard_state(st):
    try:
        with open(TRADE_GUARD_FILE, "w", encoding="utf-8") as f:
            json.dump(st, f)
    except:
        pass

def _today_key_vn():
    return datetime.fromtimestamp(_vn_now()).strftime("%Y-%m-%d")

def get_trades_today():
    st = _load_guard_state()
    k = _today_key_vn()
    return int(st.get("trades_by_day", {}).get(k, 0))

def inc_trades_today():
    st = _load_guard_state()
    k = _today_key_vn()
    st.setdefault("trades_by_day", {})
    st["trades_by_day"][k] = int(st["trades_by_day"].get(k, 0)) + 1
    _save_guard_state(st)
    return st["trades_by_day"][k]

def daily_trade_limit():
    # Với 50 USDT: set 80–100. Mặc định 100, bạn chỉnh ENV là xong.
    return int(os.getenv("DAILY_MAX_TRADES", "100"))

def allow_trade_daily_limit():
    limit = daily_trade_limit()
    used = get_trades_today()
    if used >= limit:
        return False, f"skip:daily_limit used={used} limit={limit}"
    return True, f"ok:daily_limit used={used} limit={limit}"


from datetime import datetime, timedelta, timezone
from typing import Optional

ANTI_SWEEP_LOCK_UNTIL: Optional[datetime] = None

def is_anti_sweep_locked() -> bool:
    global ANTI_SWEEP_LOCK_UNTIL
    if ANTI_SWEEP_LOCK_UNTIL is None:
        return False
    if datetime.utcnow() >= ANTI_SWEEP_LOCK_UNTIL:
        ANTI_SWEEP_LOCK_UNTIL = None
        return False
    return True
def _load_symbol_cooldown_state() -> dict:
    if not os.path.exists(SYMBOL_COOLDOWN_FILE):
        return {}
    try:
        with open(SYMBOL_COOLDOWN_FILE, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}

def _save_symbol_cooldown_state(state: dict):
    try:
        with open(SYMBOL_COOLDOWN_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False)
    except Exception:
        pass

def _cooldown_key(inst_id: str) -> str:
    return inst_id

def is_symbol_in_cooldown(inst_id: str) -> bool:
    st = _load_symbol_cooldown_state()
    rec = st.get(_cooldown_key(inst_id), {})
    until = int(rec.get("cooldown_until_utc_ms", 0) or 0)
    if until <= 0:
        return False
    if _utc_ms() >= until:
        # hết hạn -> xóa
        st.pop(_cooldown_key(inst_id), None)
        _save_symbol_cooldown_state(st)
        return False
    return True

def mark_symbol_sl(inst_id: str, reason: str):
    st = _load_symbol_cooldown_state()
    k = _cooldown_key(inst_id)
    rec = st.get(k, {})
    now = _utc_ms()

    # giữ list ts SL trong window
    sl_ts = rec.get("sl_ts", []) or []
    sl_ts = [int(x) for x in sl_ts if now - int(x) <= SYMBOL_SL_WINDOW_MINUTES * 60_000]
    sl_ts.append(now)

    # streak = số SL gần nhất trong window
    streak = len(sl_ts)

    cooldown_until = int(rec.get("cooldown_until_utc_ms", 0) or 0)
    if streak >= SYMBOL_SL_STREAK_TRIGGER:
        cooldown_until = max(cooldown_until, now + SYMBOL_COOLDOWN_MINUTES * 60_000)
        logging.warning("[COOLDOWN] %s SL streak=%d -> LOCK %dm (reason=%s)",
                        inst_id, streak, SYMBOL_COOLDOWN_MINUTES, reason)

    st[k] = {
        "sl_ts": sl_ts,
        "cooldown_until_utc_ms": cooldown_until,
        "last_reason": reason,
    }
    _save_symbol_cooldown_state(st)

def mark_symbol_tp(inst_id: str):
    # có TP thì reset streak cho symbol để “tha”
    st = _load_symbol_cooldown_state()
    k = _cooldown_key(inst_id)
    if k in st:
        st.pop(k, None)
        _save_symbol_cooldown_state(st)
        logging.info("[COOLDOWN] %s TP -> reset cooldown/sl streak.", inst_id)

def dynamic_trail_callback_pct(pnl_pct: float) -> float:
    """
    Callback động cho trailing server-side:
    - PnL < 40%  -> dùng TRAIL_SERVER_CALLBACK_PCT (mặc định 7%pnl)
    - 40% <= PnL <= 100%:
         nội suy từ 5% (40%) xuống 3.5% (100%)
    - PnL > 100% -> cố định 3.5%
    """
    # 1) Nếu chưa đủ 40% thì để nguyên callback mặc định
    if pnl_pct < 40.0:
        return TRAIL_SERVER_CALLBACK_PCT
    # 2) Vùng dynamic 40–100%: 5% -> 3.5%
    cb_high = 0.8   # callback ở 40%
    cb_low  = 0.5   # callback ở 100%
    if pnl_pct >= 100.0:
        return cb_low
    # t từ 0 -> 1 khi pnl từ 40 -> 100
    t = (pnl_pct - 40.0) / (100.0 - 40.0)
    return cb_high + t * (cb_low - cb_high)


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

    # 3) Market BAD, session GOOD  -> DEFENSE (mở nhẹ + ít lệnh)
    if regime == "BAD" and session_flag == "GOOD":
        return {
            "leverage": 3,
            "notional": 10.0,
            "tp_dyn_min_profit": 3.0,
            "max_sl_pnl_pct": 3.0,
            "max_trades_per_run": 6,
        }

    # 4) Market BAD, session BAD → HARD DEFENSE
    return {
        "leverage": 3,
        "notional": 10.0,
        "tp_dyn_min_profit": 3.0,
        "max_sl_pnl_pct": 3.0,
        "max_trades_per_run": 5,
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
        TP_DYN_MIN_PROFIT_PCT = 1.5
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

# ========== PATCH 1: ANTI-SWEEP FILTER ==========

# ========== PATCH 2: SHORT-TERM VOLATILITY DEADZONE ==========

def in_short_term_vol_deadzone(closes_5m, threshold_pct: float = 1.0) -> bool:
    """
    Deadzone nếu:
    - move1 (c0 -> c1) >= threshold_pct
    - move2 (c1 -> c2) >= threshold_pct
    - move1 và move2 ngược dấu (V-shape)
    closes_5m: list/array các giá close 5m, mới nhất ở cuối.
    """
    if len(closes_5m) < 3:
        return False

    c0 = float(closes_5m[-3])
    c1 = float(closes_5m[-2])
    c2 = float(closes_5m[-1])

    if c0 <= 0 or c1 <= 0 or c2 <= 0:
        return False

    move1 = (c1 - c0) / c0 * 100.0
    move2 = (c2 - c1) / c1 * 100.0

    if abs(move1) >= threshold_pct and abs(move2) >= threshold_pct and (move1 * move2) < 0:
        # Biến động >1% rồi đảo chiều >1% trong 2 nến liên tiếp
        return True

    return False
def _utc_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)

def is_symbol_sweep_5m(o: float, h: float, l: float, move_pct: float) -> bool:
    if o <= 0:
        return False
    up = (h - o) / o * 100.0
    dn = (o - l) / o * 100.0
    return (up >= move_pct) and (dn >= move_pct)

def lock_symbol_on_sweep(inst_id: str, minutes: int, reason: str):
    until = _utc_ms() + minutes * 60_000
    ALT_SWEEP_LOCKS[inst_id] = until
    logging.warning("[ANTI-SWEEP][ALT] LOCK %s %dm (%s) until=%s",
                    inst_id, minutes, reason, until)

def is_symbol_locked(inst_id: str) -> bool:
    until = ALT_SWEEP_LOCKS.get(inst_id)
    if not until:
        return False
    if _utc_ms() >= int(until):
        ALT_SWEEP_LOCKS.pop(inst_id, None)
        return False
    return True


def safe_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default
        
# ===== PUMP MODE + SOFT TRAIL STATES =====
PUMP_MODE_UNTIL_MS = {}           # pos_key -> timestamp ms
SOFT_TRAIL_PEAK_PX = {}           # pos_key -> peak price (bot-managed when pump mode)
UNDER_STOP_SINCE_MS = {}          # pos_key -> timestamp ms (for time-under-stop)

def detect_pump_dump_2of3(closes, highs, lows, vols):
    """
    Detect pump/dump regime theo 2/3:
      1) |chg_5m| >= PUMP_CHG_5M_PCT
      2) vol_ratio >= PUMP_VOL_RATIO
      3) range_ratio >= PUMP_RANGE_RATIO
    """
    try:
        if not closes or len(closes) < 25:
            return False, {}

        c0 = float(closes[-2])
        c1 = float(closes[-1])
        if c0 <= 0:
            return False, {}

        chg_5m_pct = abs((c1 / c0 - 1.0) * 100.0)

        # vol ratio
        vol_now = float(vols[-1])
        vol_base = vols[-21:-1]
        avg_vol20 = (sum(vol_base) / 20.0) if len(vol_base) >= 20 else (sum(vol_base) / max(1, len(vol_base)))
        vol_ratio = (vol_now / avg_vol20) if avg_vol20 > 0 else 0.0

        # range ratio
        r_now = float(highs[-1]) - float(lows[-1])
        r_base = [(float(highs[i]) - float(lows[i])) for i in range(len(highs)-21, len(highs)-1)]
        avg_r20 = (sum(r_base) / len(r_base)) if r_base else 0.0
        range_ratio = (r_now / avg_r20) if avg_r20 > 0 else 0.0

        score = 0
        if chg_5m_pct >= PUMP_CHG_5M_PCT: score += 1
        if vol_ratio >= PUMP_VOL_RATIO: score += 1
        if range_ratio >= PUMP_RANGE_RATIO: score += 1

        return (score >= 2), {
            "chg_5m_pct": chg_5m_pct,
            "vol_ratio": vol_ratio,
            "range_ratio": range_ratio,
            "score": score
        }
    except Exception:
        return False, {}

def calc_realtime_pnl_pct(pos: dict, fut_leverage: float) -> Optional[float]:
    """
    Tính PnL% realtime cho 1 position.
    Ưu tiên:
      1) uplRatio (OKX trả dạng 0.6215 ~ 62.15%)
      2) upl / margin
      3) Fallback: công thức price change * leverage
    Trả về None nếu không tính được.
    """
    # 1) uplRatio trực tiếp
    try:
        upl_ratio = safe_float(pos.get("uplRatio", None))
        if upl_ratio is not None:
            return upl_ratio * 100.0
    except Exception:
        pass

    # 2) upl / margin
    try:
        upl = safe_float(pos.get("upl", None))
        margin = safe_float(pos.get("margin", None))
        if upl is not None and margin and margin != 0:
            return upl / margin * 100.0
    except Exception:
        pass

    # 3) Fallback: dùng giá & leverage nếu mọi thứ trên fail
    try:
        avg_px = safe_float(pos.get("avgPx", None))
        last_px = safe_float(pos.get("last", None))
        if avg_px and last_px:
            raw_pct = (last_px - avg_px) / avg_px * 100.0
            # pos > 0 = long, < 0 = short
            side_factor = 1.0
            try:
                pos_sz = safe_float(pos.get("pos", "0"))
                if pos_sz < 0:
                    side_factor = -1.0
            except Exception:
                pass
            return raw_pct * fut_leverage * side_factor
    except Exception:
        pass

    return None


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




def is_quiet_hours_vn():
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

    if h in (9, 15, 20) and 4 <= m <= 9:
        return True
    if h == 22 and 50 <= m <= 59:
        return True
    return False


def is_deadzone_time_vn():
    """
    Phiên chiều tối 'deadzone' 15:00 - 20:00 giờ VN.
    """
    now_vn = datetime.utcnow() + timedelta(hours=7)
    h = now_vn.hour
    m = now_vn.minute
    if 15 <= h < 20:
        return True
    #if h == 10 and m >= 30:
        #return True
    #if 11 <= h < 16:
        #return True
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

        #logging.info("======== OKX SIGN DEBUG ========")
        #logging.info("Method: %s", method)
        #logging.info("Path: %s", path)
        #logging.info("Timestamp: %s", ts)
        #logging.info("Message for HMAC: %s", f"{ts}{method}{path}{body}")
        #logging.info("Signature: %s", sign)
        #logging.info("Headers: %s", headers)
        #logging.info("================================")
        return headers

    def _request(self, method, path, params=None, body_dict=None):
        """
        Wrapper gọi OKX API, KÝ ĐÚNG CHUỖI cho cả GET (có query) & POST.
    
        - GET  : prehash = ts + method + path + '?' + query_str
        - POST : prehash = ts + method + path + body_str
        """
        # Base URL thô chưa query
        base_url = OKX_BASE_URL + path
    
        # Chuẩn bị query / body + chuỗi dùng để ký
        if method.upper() == "GET":
            # build query string (nếu có params)
            query_str = urlencode(params or {})
            if query_str:
                url = f"{base_url}?{query_str}"
                sign_path = f"{path}?{query_str}"   # cái này đem đi ký
            else:
                url = base_url
                sign_path = path
            body_str = ""                            # GET không có body
        else:
            # POST: không ký query, chỉ ký body
            url = base_url
            sign_path = path
            body_str = json.dumps(body_dict) if body_dict is not None else ""
    
        # Headers với chuỗi sign_path & body_str đã chuẩn
        headers = self._headers(method.upper(), sign_path, body_str)
    
        try:
            if method.upper() == "GET":
                # query đã gắn vào url, nên params=None
                r = requests.get(url, headers=headers, timeout=15)
            else:
                r = requests.post(url, headers=headers, data=body_str, timeout=15)
    
            if r.status_code != 200:
                logging.error("✗ OKX REQUEST FAILED")
                logging.error("URL: %s", r.url)
                logging.error("Status Code: %s", r.status_code)
                logging.error("Response: %s", r.text)
                r.raise_for_status()
    
            data = r.json()
            code = data.get("code")
            msg = data.get("msg", "")
            
            if code != "0":
                logging.error("❌ OKX RESPONSE ERROR code=%s msg=%s", code, msg)
                logging.error("Full response: %s", data)
                raise Exception(f"OKX API error code={code} msg={msg} resp={data}")
            return data

    
        except Exception as e:
            logging.exception("Exception when calling OKX: %s", e)
            raise

    def has_active_trailing(self, inst_id: str, pos_side: str) -> bool:
        """
        Kiểm tra xem symbol này đã có trailing server-side đang chờ hay chưa.
        """
        path = "/api/v5/trade/orders-algo-pending"
        params = {
            "instId": inst_id,
            "ordType": "move_order_stop",
        }
        data = self._request("GET", path, params=params)

        algo_orders = data.get("data", []) if isinstance(data, dict) else data
        for o in algo_orders:
            # tuỳ OKX trả về, thường có posSide
            if o.get("posSide") == pos_side:
                return True
        return False
    # ---------- ORDER HELPERS (MAKER-FIRST) ----------
    def place_close_limit_postonly(self, inst_id: str, pos_side: str, sz: float, px: float, td_mode="isolated"):
        """
        Đặt LIMIT post-only để ĐÓNG vị thế.
        long đóng = sell, short đóng = buy.
        """
        side_close = "sell" if pos_side == "long" else "buy"
        path = "/api/v5/trade/order"
        body = {
            "instId": inst_id,
            "tdMode": td_mode,
            "side": side_close,
            "posSide": pos_side,
            "ordType": "limit",
            "sz": str(sz),
            "px": f"{float(px):.12f}",
            "tif": "post_only",
            "reduceOnly": "true",
        }
        return self._request("POST", path, body_dict=body)

    def place_futures_limit_order(
        self,
        inst_id: str,
        side: str,
        pos_side: str,
        sz: str,
        px: float,
        td_mode: str = "isolated",
        lever: int = 6,
        post_only: bool = True,
    ):
        """
        Limit order (ưu tiên MAKER nếu post_only=True).
        OKX: tif='post_only' để đảm bảo maker (nếu có thể khớp ngay -> bị reject).
        """
        path = "/api/v5/trade/order"
        body = {
            "instId": inst_id,
            "tdMode": td_mode,
            "side": side,
            "posSide": pos_side,
            "ordType": "limit",
            "sz": str(sz),
            "px": f"{float(px):.12f}",
            "lever": str(lever),
        }
        if post_only:
            body["tif"] = "post_only"   # maker-only

        logging.info("---- PLACE FUTURES LIMIT (POST-ONLY=%s) ----", post_only)
        logging.info("Body: %s", body)
        return self._request("POST", path, body_dict=body)

    def cancel_order(self, inst_id: str, ord_id: str):
        path = "/api/v5/trade/cancel-order"
        body = {"instId": inst_id, "ordId": ord_id}
        return self._request("POST", path, body_dict=body)

    def get_order(self, inst_id: str, ord_id: str):
        path = "/api/v5/trade/order"
        params = {"instId": inst_id, "ordId": ord_id}
        return self._request("GET", path, params=params)

    def wait_order_filled(self, inst_id: str, ord_id: str, timeout_sec: int = 3, poll_sec: float = 0.4):
        """
        Chờ order filled trong timeout.
        Return: (filled: bool, avg_px: float|None)
        """
        t0 = time.time()
        last_avg = None

        while time.time() - t0 <= timeout_sec:
            try:
                r = self.get_order(inst_id, ord_id)
                data = r.get("data", [])
                if data:
                    o = data[0]
                    state = (o.get("state") or "").lower()  # live / filled / canceled ...
                    avg_px = safe_float(o.get("avgPx", None), None)
                    if avg_px:
                        last_avg = avg_px

                    if state == "filled":
                        return True, (avg_px or last_avg)
                    if state in ("canceled", "cancelled"):
                        return False, None
            except Exception as e:
                logging.warning("[MAKER] wait_order_filled error %s %s: %s", inst_id, ord_id, e)

            time.sleep(poll_sec)

        return False, last_avg


    # ---------- PUBLIC ----------

    def get_spot_tickers(self):
        path = "/api/v5/market/tickers"
        params = {"instType": "SPOT"}
        data = self._request("GET", path, params=params)
        return data.get("data", [])

    def get_candles(self, inst_id, bar="15m", limit=100):
        if inst_id.endswith("-USDT") and not inst_id.endswith("-USDT-SWAP"):
            inst_id = f"{inst_id}-SWAP"
    
        path = "/api/v5/market/candles"
        params = {
            "instId": inst_id,
            "bar": bar,
            "limit": str(limit),
        }
        data = self._request("GET", path, params=params)
        return data.get("data", [])

    def get_mark_price(self, inst_id: str) -> float | None:
        """
        Lấy mark price realtime cho SWAP.
        OKX: /api/v5/public/mark-price?instType=SWAP&instId=XXX
        """
        try:
            path = "/api/v5/public/mark-price"
            params = {"instType": "SWAP", "instId": inst_id}
            data = self._request("GET", path, params=params)
            rows = (data or {}).get("data", [])
            if not rows:
                return None
            mp = rows[0].get("markPx")
            return float(mp) if mp is not None else None
        except Exception:
            return None

    def get_last_price(self, inst_id: str) -> float | None:
        """
        Lấy last price realtime.
        OKX: /api/v5/market/ticker?instId=XXX
        """
        try:
            path = "/api/v5/market/ticker"
            params = {"instId": inst_id}
            data = self._request("GET", path, params=params)
            rows = (data or {}).get("data", [])
            if not rows:
                return None
            last = rows[0].get("last")
            return float(last) if last is not None else None
        except Exception:
            return None

        
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

    def get_positions_history(self, inst_type="SWAP", after=None, limit=100):
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

    def get_algo_pending(self, inst_id=None, ord_type=None):
        """
        Lấy danh sách lệnh algo đang pending (OCO / trailing / …)
        """
        path = "/api/v5/trade/orders-algo-pending"
        params = {}
        if inst_id:
            params["instId"] = inst_id
        if ord_type:
            params["ordType"] = ord_type

        # QUAN TRỌNG: dùng _request như các hàm khác, KHÔNG tự ký tay
        return self._request("GET", path, params=params)

    def cancel_algos(self, inst_id, algo_ids):
        """
        Hủy 1 hoặc nhiều lệnh algo (OCO, trailing, v.v.)
        """
        if not algo_ids:
            return None

        path = "/api/v5/trade/cancel-algos"
        body = [{"instId": inst_id, "algoId": a} for a in algo_ids]

        return self._request("POST", path, body_dict=body)

    def place_trailing_stop(
        self,
        inst_id: str,
        pos_side: str,
        side_close: str,
        sz: str,
        callback_ratio_pct: float,
        active_px: float,
        td_mode: str = "isolated",
    ):
        """
        Đặt trailing stop server-side (ordType = move_order_stop)
    
        callback_ratio_pct: nhập theo % (vd 7.0) -> tự đổi sang ratio 0.07
        OKX yêu cầu callbackRatio nằm trong [0.001, 1].
        """
        # 1) đổi % sang ratio
        ratio = callback_ratio_pct / 100.0  # 7.0 -> 0.07
    
        # 2) kẹp trong range hợp lệ
        if ratio < 0.001:
            ratio = 0.001
        elif ratio > 1.0:
            ratio = 1.0
    
        path = "/api/v5/trade/order-algo"
        body = {
            "instId": inst_id,
            "tdMode": td_mode,
            "side": side_close,          # 'sell' nếu đóng long, 'buy' nếu đóng short
            "posSide": pos_side,         # 'long' hoặc 'short'
            "ordType": "move_order_stop",
            "sz": sz,
            "callbackRatio": f"{ratio:.6f}",   # ví dụ 0.050000 cho 5%
            "activePx": f"{active_px:.6f}",    # giá kích hoạt trailing
            "triggerPxType": "mark",
        }
    
        logging.info(
            "[TP-TRAIL] Gửi trailing server-side %s sz=%s callbackRatio=%.4f activePx=%.6f",
            inst_id,
            sz,
            ratio,
            active_px,
        )
    
        return self._request("POST", path, body_dict=body)


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
        #logging.info(f"[OKX] Close position: {inst_id} | {pos_side}")
        return self._request("POST", path, body_dict=body)


# ========= CÁC HÀM CACHE TRADES CHO BACKTEST REAL =========
    


# ===== SESSION SHEET (circuit breaker) =====
def maker_close_position_with_timeout(
    okx: OKXClient,
    inst_id: str,
    pos_side: str,
    sz: float,
    last_px: float,
    offset_bps: float = 6.0,      # 0.06%
    timeout_sec: int = 3,
):
    """
    Close bằng LIMIT post-only (maker). Không khớp trong timeout -> cancel + market close.
    Return: used='maker'|'market'
    """
    if last_px <= 0 or sz <= 0:
        okx.close_swap_position(inst_id, pos_side)
        return "market"

    off = offset_bps / 10000.0

    # post-only: phải đặt giá "lùi" để không khớp ngay
    # long đóng (sell) -> đặt cao hơn last một chút
    # short đóng (buy) -> đặt thấp hơn last một chút
    if pos_side == "long":
        px = last_px * (1.0 + off)
    else:
        px = last_px * (1.0 - off)

    resp = okx.place_close_limit_postonly(inst_id, pos_side, sz, px)
    ord_id = None
    try:
        d = resp.get("data", [])
        if d:
            ord_id = d[0].get("ordId")
    except Exception:
        ord_id = None

    if not ord_id:
        okx.close_swap_position(inst_id, pos_side)
        return "market"

    filled, _avg = okx.wait_order_filled(inst_id, ord_id, timeout_sec=timeout_sec, poll_sec=0.4)
    if filled:
        return "maker"

    try:
        okx.cancel_order(inst_id, ord_id)
    except Exception:
        pass

    okx.close_swap_position(inst_id, pos_side)
    return "market"

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
    #Đọc toàn bộ cache trades từ sheet BT_TRADES_CACHE.
    #Trả về list[dict].
    #Fix: dùng expected_headers để tránh lỗi header trống / trùng trong sheet.
    """
    ws = get_bt_cache_worksheet()
    if not ws:
        return []

    try:
        # Ép header chuẩn, bỏ qua mấy cột trống phía sau
        rows = ws.get_all_records(
            expected_headers=["posId", "instId", "side", "sz",
                              "openPx", "closePx", "pnl", "cTime"]
        )
    except Exception as e:
        logging.error("[BT-CACHE] Lỗi get_all_records: %s", e)
        # Fallback: đọc raw values rồi tự map
        values = ws.get_all_values()
        if not values or len(values) < 2:
            return []
        data_rows = values[1:]  # bỏ dòng header
        rows = []
        for r in data_rows:
            # pad cho đủ 8 cột
            r = (r + [""] * 8)[:8]
            rows.append({
                "posId":   r[0],
                "instId":  r[1],
                "side":    r[2],
                "sz":      r[3],
                "openPx":  r[4],
                "closePx": r[5],
                "pnl":     r[6],
                "cTime":   r[7],
            })

    trades = []
    for r in rows:
        if not r.get("posId"):
            continue
        try:
            trades.append({
                "posId":      str(r.get("posId", "")),
                "instId":     r.get("instId", ""),
                "side":       r.get("side", ""),
                "sz":         float(r.get("sz", 0) or 0),
                "openAvgPx":  float(r.get("openPx", 0) or 0),
                "closeAvgPx": float(r.get("closePx", 0) or 0),
                "pnl":        float(r.get("pnl", 0) or 0),
                "cTime":      str(r.get("cTime", 0) or 0),
            })
        except Exception as e:
            logging.error("[BT-CACHE] Lỗi parse row %s: %s", r, e)

    logging.info("[BT-CACHE] Load cache: %d trades.", len(trades))
    return trades
def append_bt_cache(new_trades):
    if not new_trades:
        return

    ws = get_bt_cache_worksheet()
    if not ws:
        return

    # Đọc toàn bộ posId + cTime đã có trong sheet
    try:
        values = ws.get_all_values()
        existing_keys = set()
        if values and len(values) > 1:
            for row in values[1:]:
                # row: [posId, instId, side, sz, openPx, closePx, pnl, cTime]
                pos_id = (row[0] if len(row) > 0 else "").strip()
                ctime  = (row[7] if len(row) > 7 else "").strip()
                if pos_id and ctime:
                    existing_keys.add(f"{pos_id}_{ctime}")
    except Exception as e:
        logging.error("[BT-CACHE] Lỗi đọc cache hiện có: %s", e)
        existing_keys = set()

    rows = []
    added = 0
    for t in new_trades:
        pos_id = str(t.get("posId", "") or "").strip()
        ctime  = str(t.get("cTime", "") or "").strip()
        if not pos_id or not ctime:
            continue

        key = f"{pos_id}_{ctime}"
        if key in existing_keys:
            continue  # đã có trong sheet
        existing_keys.add(key)

        rows.append([
            pos_id,
            t.get("instId", ""),
            t.get("side", ""),
            t.get("sz", ""),
            t.get("openPx")  or t.get("openAvgPx", ""),
            t.get("closePx") or t.get("closeAvgPx", ""),
            t.get("pnl", ""),
            ctime,
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
def _median(nums):
    nums = sorted([x for x in nums if x is not None])
    if not nums:
        return None
    n = len(nums)
    mid = n // 2
    return nums[mid] if n % 2 == 1 else (nums[mid - 1] + nums[mid]) / 2.0


def _get_closed_candles(candles):
    """
    OKX candles: [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
    confirm == "1" => candle đã đóng
    """
    if not candles:
        return []
    c_sorted = sorted(candles, key=lambda x: int(x[0]))
    closed = [c for c in c_sorted if len(c) >= 9 and str(c[8]) == "1"]
    return closed


def should_unlock_market(okx) -> tuple[bool, str]:
    """
    UNLOCK khi BTC 15m có tín hiệu mạnh.
    Logic mới: 2/3 (direction + body + volume) => UNLOCK
    Lưu ý: điều kiện HOLD vẫn là "bắt buộc" để tránh unlock lúc bị giật ngược.
    """
    UNLOCK_MIN_PASS = 2  # 2/3

    try:
        c = okx.get_candles("BTC-USDT-SWAP", bar=UNLOCK_BTC_BAR, limit=UNLOCK_BTC_LIMIT)
        closed = _get_closed_candles(c)
        if len(closed) < 3:
            return False, "not_enough_closed_candles"

        # spike = [-2], follow = [-1] (2 nến đã đóng gần nhất)
        spike = closed[-2]
        follow = closed[-1]

        o1 = safe_float(spike[1]);  c1 = safe_float(spike[4]);  v1 = safe_float(spike[5])
        o2 = safe_float(follow[1]); c2 = safe_float(follow[4]); v2 = safe_float(follow[5])

        body1 = abs(c1 - o1)
        body2 = abs(c2 - o2)

        # 1) direction: cùng hướng
        dir1_up = c1 > o1
        dir2_up = c2 > o2
        dir1_dn = c1 < o1
        dir2_dn = c2 < o2
        dir_ok = ((dir1_up and dir2_up) or (dir1_dn and dir2_dn))

        # 2) body follow đủ mạnh
        # body1<=0: coi như fail body, nhưng KHÔNG return sớm (vì 2/3)
        body_ok = (body1 > 0) and (body2 >= UNLOCK_BODY_RATIO_MIN * body1)

        # 3) HOLD (bắt buộc): không bị xả ngược mạnh
        # Nếu direction chưa rõ (dir_ok=False) thì vẫn check hold theo hướng nến spike (dir1_up/dir1_dn)
        if dir1_up and (c2 < c1 * (1 - UNLOCK_HOLD_TOL)):
            return False, "hold_fail_long"
        if dir1_dn and (c2 > c1 * (1 + UNLOCK_HOLD_TOL)):
            return False, "hold_fail_short"

        # 4) volume thật
        vols = []
        for cc in closed[-22:-2]:  # 20 nến trước spike
            vols.append(safe_float(cc[5]))
        med = _median(vols)
        vol_ok = (med is not None) and (med > 0) and (v2 >= UNLOCK_VOL_MULT * med)

        # ---- 2/3 scoring (dir/body/vol) ----
        score = int(dir_ok) + int(body_ok) + int(vol_ok)
        if score >= UNLOCK_MIN_PASS:
            return True, f"btc_unlock_ok(score={score}/3,dir={int(dir_ok)},body={int(body_ok)},vol={int(vol_ok)})"

        fails = []
        if not dir_ok:  fails.append("no_follow_through_direction")
        if not body_ok: fails.append("follow_body_too_small" if body1 > 0 else "body1_zero")
        if not vol_ok:  fails.append("vol_not_strong" if (med is not None and med > 0) else "no_vol_median")
        return False, "wait_unlock:" + ",".join(fails)

    except Exception as e:
        return False, f"unlock_exception:{e}"



def check_day_hard_stop(okx) -> tuple[bool, str]:
    """
    HARD STOP theo ngày (USDT). Lưu state vào SESSION sheet với session='DAY'
    """
    if not DAY_HARD_STOP_ENABLED:
        return True, "day_stop_disabled"

    now_vn = datetime.utcnow() + timedelta(hours=7)
    today = now_vn.date().isoformat()
    session = "DAY"
    equity = okx.get_total_equity_usdt()
    state = load_session_state(today, session)
    if state is None:
        state = {"date": today, "session": session, "start_equity": equity, "blocked": False}
        save_session_state(state)
        return True, "day_state_init"
    blocked = str(state.get("blocked", "")).upper() == "TRUE"
    start_eq = float(state.get("start_equity", 0) or 0)
    if start_eq <= 0:
        start_eq = equity
    pnl_usdt = equity - start_eq
    if blocked:
        return False, f"day_locked(pnl={pnl_usdt:.2f}<=-{DAY_MAX_LOSS_USDT:.2f})"

    if pnl_usdt <= -DAY_MAX_LOSS_USDT:
        state = {"date": today, "session": session, "start_equity": start_eq, "blocked": True}
        save_session_state(state)
        return False, f"day_lock_trigger(pnl={pnl_usdt:.2f}<=-{DAY_MAX_LOSS_USDT:.2f})"
    return True, f"day_ok(pnl={pnl_usdt:.2f})"
    
def _candle_to_ohlcv(c):
    # OKX candles: [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
    o = float(c[1]); h = float(c[2]); l = float(c[3]); cl = float(c[4]); v = float(c[5])
    return o, h, l, cl, v

def _body_ratio(o, h, l, c):
    rng = max(h - l, 1e-12)
    body = abs(c - o)
    return body / rng

def _pct_change(a, b):
    # from a -> b
    if a == 0:
        return 0.0
    return (b - a) / a * 100.0

def _ft_two_candles_same_dir(c1, c2, body_ratio_min, min_change_pct):
    """
    Follow-through: 2 nến đóng (c1, c2) cùng hướng.
    Điều kiện strict:
      - cả 2 nến cùng xanh hoặc cùng đỏ (close vs open)
      - mỗi nến có |%change| >= min_change_pct
      - candle2 body_ratio >= body_ratio_min (nến xác nhận phải "thật")
    """
    o1,h1,l1,cl1,v1 = _candle_to_ohlcv(c1)
    o2,h2,l2,cl2,v2 = _candle_to_ohlcv(c2)

    dir1 = 1 if cl1 > o1 else (-1 if cl1 < o1 else 0)
    dir2 = 1 if cl2 > o2 else (-1 if cl2 < o2 else 0)
    if dir1 == 0 or dir2 == 0 or dir1 != dir2:
        return False, 0

    chg1 = _pct_change(o1, cl1)
    chg2 = _pct_change(o2, cl2)
    if abs(chg1) < min_change_pct or abs(chg2) < min_change_pct:
        return False, 0

    if _body_ratio(o2, h2, l2, cl2) < body_ratio_min:
        return False, 0

    return True, dir2  # dir: +1 long, -1 short

def _vol_confirm_strict(candles, idx_c2, vol_mult):
    """
    vol candle2 >= vol_mult * median(vol prev20)
    candles: list newest->oldest hoặc oldest->newest đều được,
    miễn idx_c2 là index của candle2 trong list đó.
    """
    # lấy 20 vol trước candle2
    if idx_c2 < 20:
        return False
    vols = []
    for j in range(idx_c2 - 20, idx_c2):
        try:
            vols.append(float(candles[j][5]))
        except Exception:
            pass
    if not vols:
        return False
    vols_sorted = sorted(vols)
    med = vols_sorted[len(vols_sorted)//2]
    v2 = float(candles[idx_c2][5])
    return v2 >= vol_mult * max(med, 1e-12)

def _get_top_swap_symbols_by_change_24h(okx, topn: int):
    """
    Lấy danh sách instId SWAP top N theo 24h change (abs).
    Dựa trên okx.get_swap_tickers().
    """
    try:
        data = okx.get_swap_tickers()
        rows = data.get("data", []) if isinstance(data, dict) else data
        #logging.warning(f"[DEADZONE-OVERRIDE] swap_tickers rows={len(rows)} type={type(data)}")

        items = []
        cnt_suffix = sum(1 for r in rows if (r.get("instId","") or "").endswith("-USDT-SWAP"))
        #logging.warning(f"[DEADZONE-OVERRIDE] suffix -USDT-SWAP count={cnt_suffix}")
        for r in rows:
            inst = r.get("instId") or ""
            if not inst.endswith("-USDT-SWAP"):
                continue
            try:
                last = float(r.get("last"))
                open24h = float(r.get("open24h"))
                if open24h <= 0:
                    continue
                chg = (last - open24h) / open24h * 100.0
            except Exception:
                continue

            items.append((inst, abs(chg)))
        items.sort(key=lambda x: x[1], reverse=True)

        #logging.warning(f"[DEADZONE-OVERRIDE] sample row keys={list(rows[0].keys()) if rows else None}")
        return [x[0] for x in items[:topn]]
    except Exception as e:
        logging.error("[DEADZONE-OVERRIDE] get top symbols error: %s", e)
        return []
    
def deadzone_override_strong_edge(okx):
    """
    True nếu đủ điều kiện để override DEADZONE hard lock.

    Logic (GIỮ NGUYÊN NGƯỠNG FT/VOL):
      - Luôn check BTC 15m FT + vol (để biết có hướng hay không)
      - Luôn check ALT topN theo 24h change: FT 2 nến + vol strict
      - Nếu BTC có hướng: ALT phải cùng hướng BTC
      - Nếu BTC không có hướng: ALT pass FT+vol là đủ (ALT_ONLY)
    """
    side = None
    btc_ft_fail = True
    btc_vol_fail = True
    ok_ft = False
    dir_btc = None
    passed = 0

    try:
        # --- 1) BTC FT on 15m (need 2 closed candles) ---
        btc = DEADZONE_OVERRIDE_BTC_INST
        btc_c = okx.get_candles(btc, bar=DEADZONE_OVERRIDE_BTC_BAR, limit=30)
        if not btc_c or len(btc_c) < 5:
            return False, "btc_candles_insufficient"

        btc_c = list(reversed(btc_c))
        c1 = btc_c[-3]  # nến đóng trước
        c2 = btc_c[-2]  # nến đóng gần nhất (tránh nến đang chạy)

        ok_ft, dir_btc = _ft_two_candles_same_dir(
            c1, c2,
            body_ratio_min=DEADZONE_FT_BODY_RATIO_MIN,
            min_change_pct=DEADZONE_FT_MIN_CHANGE_PCT
        )
        btc_ft_fail = (not ok_ft)
        btc_vol_fail = (not _vol_confirm_strict(btc_c, len(btc_c) - 2, DEADZONE_FT_VOL_MULT))
        #logging.warning(
            #f"[DEADZONE-OVERRIDE][BTC] ok_ft={ok_ft} dir_btc={dir_btc if ok_ft else None} "
            #f"btc_ft_fail={btc_ft_fail} btc_vol_fail={btc_vol_fail}"
        #)

        # --- 2) ALT confirm on 5m ---
        universe = _get_top_swap_symbols_by_change_24h(okx, DEADZONE_OVERRIDE_ALT_TOPN)
        #logging.warning(
            #f"[DEADZONE-OVERRIDE][UNIV] topn={DEADZONE_OVERRIDE_ALT_TOPN} "
            #f"universe_len={len(universe)} sample={universe[:5]}"
        #)
        if not universe:
            return False, "alt_universe_empty"

        need = DEADZONE_OVERRIDE_MIN_ALTS  # bạn đang set = 1

        for inst in universe:
            if inst == btc:
                continue

            alt_c = okx.get_candles(inst, bar=DEADZONE_OVERRIDE_ALT_BAR, limit=30)
            if not alt_c or len(alt_c) < 5:
                #logging.warning(
                    #f"[DEADZONE-OVERRIDE][ALT-SKIP] {inst} candles_insufficient len={len(alt_c) if alt_c else 0}"
                #)
                continue

            alt_c = list(reversed(alt_c))
            a1 = alt_c[-3]
            a2 = alt_c[-2]

            ok_alt, dir_alt = _ft_two_candles_same_dir(
                a1, a2,
                body_ratio_min=DEADZONE_FT_BODY_RATIO_MIN,
                min_change_pct=DEADZONE_FT_MIN_CHANGE_PCT
            )
            if not ok_alt:
                continue

            # vol confirm strict trên alt
            if not _vol_confirm_strict(alt_c, len(alt_c) - 2, DEADZONE_FT_VOL_MULT):
                continue

            # Nếu BTC có hướng -> bắt cùng hướng BTC (logic gốc)
            if ok_ft and dir_alt != dir_btc:
                continue

            passed += 1
            #logging.warning(
                #f"[DEADZONE-OVERRIDE][ALT] inst={inst} passed={passed}/{need} ok_ft={ok_ft}"
            #)

            if passed >= need:
                if ok_ft:
                    side = "LONG" if dir_btc > 0 else "SHORT"
                    return True, f"deadzone_override_ok({side}, alts={passed})"
                else:
                    return True, f"deadzone_override_ok(ALT_ONLY, alts={passed})"

        # --- 3) Kết luận sau khi scan alt ---
        if ok_ft:
            if btc_ft_fail:
                return False, "btc_ft_fail"
            if btc_vol_fail:
                return False, "btc_vol_fail"
            return False, f"alt_ft_insufficient({passed}/{need})"
        else:
            return False, f"alt_only_insufficient({passed}/{need})"

    except Exception as e:
        logging.error("[DEADZONE-OVERRIDE] exception: %s", e)
        return (False, "exception") if DEADZONE_OVERRIDE_FAILSAFE_LOCK else (True, "failsafe_unlock")



def check_market_lock_unlock(okx) -> tuple[bool, str]:
    """
    - DEADZONE 15-20: hard lock (không mở lệnh mới)
    - market BAD liên tiếp: soft lock (có UNLOCK)
    Lưu state vào SESSION sheet với session='MARKET'
    """
    # 1) DEADZONE hard lock (override unlock only if STRONG EDGE)
    if DEADZONE_HARD_LOCK_ENABLED and is_deadzone_time_vn():
        if DEADZONE_OVERRIDE_ENABLED:
            ok, reason = deadzone_override_strong_edge(okx)
            if ok and "ALT_ONLY" in str(reason):
                global NOTIONAL_PER_TRADE
                NOTIONAL_PER_TRADE = 5
                #logging.warning("[DEADZONE-OVERRIDE] ALT_ONLY -> set NOTIONAL_PER_TRADE=5")
            if ok:
                #logging.warning("[UNLOCK] DEADZONE override unlocked: %s", reason)
                return True, f"deadzone_override:{reason}"
            else:
                #logging.warning("[LOCK] DEADZONE hard lock (no strong edge): %s", reason)
                return False, f"deadzone_15_20_hard_lock({reason})"
        return False, "deadzone_15_20_hard_lock"

    if not MARKET_SOFT_LOCK_ENABLED:
        return True, "market_soft_lock_disabled"

    now_vn = datetime.utcnow() + timedelta(hours=7)
    today = now_vn.date().isoformat()
    session = "MARKET"

    state = load_session_state(today, session)
    if state is None:
        state = {"date": today, "session": session, "blocked": False, "bad_count": 0, "unlock_cooldown_until": 0}
        save_session_state(state)

    blocked = str(state.get("blocked", "")).upper() == "TRUE"
    bad_count = int(state.get("bad_count", 0) or 0)
    cooldown_until = int(state.get("unlock_cooldown_until", 0) or 0)

    # 2) Detect market regime (GOOD/BAD)
    regime = detect_market_regime(okx)  # bạn đang có sẵn
    if regime == "BAD":
        bad_count += 1
    else:
        bad_count = 0

    # 3) Nếu đang blocked -> thử unlock theo rule
    now_utc_ms = int(time.time() * 1000)
    if blocked:
        if now_utc_ms < cooldown_until:
            state.update({"bad_count": bad_count})
            save_session_state(state)
            return False, f"market_locked_cooldown(regime={regime})"

        ok_unlock, reason = should_unlock_market(okx)
        if ok_unlock:
            # unlock + set cooldown
            cooldown = MARKET_UNLOCK_COOLDOWN_MIN * 60 * 1000
            state = {
                "date": today,
                "session": session,
                "blocked": False,
                "bad_count": 0,
                "unlock_cooldown_until": now_utc_ms + cooldown,
                "last_unlock_reason": reason,
            }
            save_session_state(state)
            return True, f"market_unlocked({reason})"
        else:
            state.update({"bad_count": bad_count})
            save_session_state(state)
            return False, f"market_locked(wait_unlock:{reason})"

    # 4) Nếu chưa blocked -> lock khi BAD liên tiếp đủ ngưỡng
    if regime == "BAD" and bad_count >= MARKET_BAD_LOCK_AFTER:
        state = {
            "date": today,
            "session": session,
            "blocked": True,
            "bad_count": bad_count,
            "unlock_cooldown_until": 0,
            "last_lock_reason": f"regime_bad_x{bad_count}",
        }
        save_session_state(state)
        return False, f"market_lock_trigger(regime=BAD x{bad_count})"

    state.update({"bad_count": bad_count})
    save_session_state(state)
    return True, f"market_ok(regime={regime}, bad_count={bad_count})"

# ===== BACKTEST REAL: LẤY HISTORY TỪ OKX + CACHE =====
def load_real_trades_for_backtest(okx):
    # 1) Load cache cũ từ Google Sheets
    cached = load_bt_cache()        # list[dict]

    # KEY duy nhất = posId + cTime để 1 posId có nhiều lệnh vẫn giữ hết
    cached_keys = set()
    for t in cached:
        pid = str(t.get("posId") or "").strip()
        ctime = str(t.get("cTime") or "").strip()
        if pid and ctime:
            cached_keys.add(f"{pid}_{ctime}")

    logging.info(
        "[BACKTEST] Cache hiện tại: %d trades, distinct key=%d",
        len(cached),
        len(cached_keys),
    )

    # 2) Kéo cửa sổ history mới nhất từ OKX, retry nhiều lần
    all_raw = []          # GIỮ HẾT mọi dòng history, không gộp theo posId
    max_attempts = 5
    delay_sec = 10

    for attempt in range(1, max_attempts + 1):
        try:
            raw = okx.get_positions_history(
                inst_type="SWAP",
                # after=None,   # nếu đang để after thì giữ nguyên, không quan trọng
                limit=100,
            )
        except Exception as e:
            logging.error(
                "[BACKTEST] Lỗi get_positions_history (attempt %d/%d): %s",
                attempt, max_attempts, e,
            )
            raw = []

        if raw:
            logging.info(
                "[BACKTEST] Lần %d lấy được %d dòng history từ OKX.",
                attempt, len(raw),
            )
            # GIỮ HẾT, không gộp
            for d in raw:
                pid = d.get("posId")
                if not pid:
                    continue
                all_raw.append(d)
        else:
            logging.info(
                "[BACKTEST] Lần %d không nhận được dữ liệu history từ OKX.",
                attempt,
            )

        if attempt < max_attempts:
            logging.info(
                "[BACKTEST] Chờ %ds rồi retry get_positions_history (attempt %d/%d)...",
                delay_sec, attempt + 1, max_attempts,
            )
            time.sleep(delay_sec)

    logging.info("[BACKTEST] Tổng %d dòng history thô lấy từ OKX.", len(all_raw))

    # 3) Parse thành trades mới, chỉ bỏ các dòng đã có trong BT_TRADES_CACHE
    new_trades = []

    for d in all_raw:
        pid = str(d.get("posId") or "").strip()
        ctime_str = str(d.get("cTime") or d.get("uTime") or "").strip()

        if not pid or not ctime_str:
            continue

        key = f"{pid}_{ctime_str}"
        if key in cached_keys:
            # đã lưu lệnh này vào BT_TRADES_CACHE rồi
            continue

        try:
            new_trades.append(
                {
                    "posId": pid,
                    "instId": d.get("instId"),
                    "side": d.get("side"),
                    "sz": float(d.get("sz") or 0),
                    "openPx": float(d.get("openAvgPx") or d.get("avgPx") or 0),
                    "closePx": float(d.get("closePx") or 0),
                    "pnl": float(d.get("pnl") or 0),
                    "cTime": ctime_str,   # dùng làm phần còn lại của key
                }
            )
        except Exception as e:
            logging.error("[BACKTEST] Lỗi parse history item %s: %s", d, e)

    logging.info(
        "[BACKTEST] new_trades sau khi loại trùng key (posId+cTime): %d",
        len(new_trades),
    )

    # 4) Lưu thêm vào sheet cache
    append_bt_cache(new_trades)

    # 5) Hợp nhất cache cũ + trade mới rồi LOẠI TRÙNG theo (posId+cTime)
    all_trades = cached + new_trades

    unique = {}
    for t in all_trades:
        pid = str(t.get("posId") or "").strip()
        ctime = str(t.get("cTime") or "").strip()
        if not pid or not ctime:
            continue
        key = f"{pid}_{ctime}"
        if key not in unique:
            unique[key] = t

    all_trades = list(unique.values())
    logging.info(
        "[BACKTEST] Tổng %d trades dùng để BT ALL sau khi loại trùng key (posId+cTime).",
        len(all_trades),
    )
    return all_trades


def summarize_real_backtest(trades: list[dict]) -> tuple[str, str, str]:
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
            pnl = safe_float(t.get("pnl", 0))
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
    a_total, a_tp, a_sl, a_even, a_pnl, a_win = classify(trades)
    msg_all = (
        f"✅ BT ALL | total={a_total} | "
        f"TP={a_tp} SL={a_sl} | "
        f"win={a_win:.1f}% | "
        f"PNL={a_pnl:+.2f} USDT"
    )

    # ================== TODAY ==================
    only_today = [t for (t, _dt) in trades_today]
    t_total, t_tp, t_sl, t_even, t_pnl, t_win = classify(only_today)

    msg_today = (
        f"✅ BT TODAY | total={t_total} | "
        f"TP={t_tp} SL={t_sl}| "
        f"win={t_win:.1f}% | "
        f"PNL={t_pnl:+.2f} USDT"
    )

    # ================== SESSION TODAY ==================
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
        s_total, s_tp, s_sl, s_even, s_pnl, s_win = classify(sess_trades)
        line = (
            f"[{label}] total={s_total} TP={s_tp} SL={s_sl} "
            f"win={s_win:.1f}% PNL={s_pnl:+.2f} USDT"
        )
        session_lines.append(line)

    msg_session = "\n".join(session_lines)
    return msg_all, msg_today, msg_session


# (phần cũ load_history_from_drive / trade_cache vẫn giữ nguyên cho bot khác nếu cần)








# ===== HÀM BACKTEST REAL TRIGGER THEO LỊCH =====

def run_backtest_if_needed(okx: "OKXClient"):
    logging.info("========== [BACKTEST] BẮT ĐẦU CHẠY BACKTEST REAL ==========")

    if not is_backtest_time_vn():
        logging.info("[BACKTEST] Không nằm trong khung giờ backtest, bỏ qua.")
        return
    # 1) Lấy toàn bộ trades (cache cũ + history mới từ OKX)
    trades = load_real_trades_for_backtest(okx)

    # 2) Tóm tắt theo ALL / TODAY / SESSION (SESSION dùng sau nếu cần)
    msg_all, msg_today, msg_session = summarize_real_backtest(trades)

    # 3) Gửi 3 block như bản minh hoạ
    text = msg_all + "\n" + msg_today + "\n\n" + msg_session
    send_telegram_message(text)


# ================= GOOGLE SHEETS KHÁC, DRIVE, TELEGRAM, SCANNER, TP DYNAMIC, v.v.

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


def get_drive_service():
    json_str = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    if not json_str:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON not set")
    info = json.loads(json_str)
    scopes = ["https://www.googleapis.com/auth/drive"]
    credentials = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    service = build("drive", "v3", credentials=credentials)
    return service

def load_history_from_drive():
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
    global ANTI_SWEEP_LOCK_UNTIL
    btc_change_5m = None
    try:
        c = okx.get_candles("BTC-USDT-SWAP", bar="5m", limit=4)
        if c and len(c) >= 3:
            c_sorted = sorted(c, key=lambda x: int(x[0]))
            closes_5m = [safe_float(k[4]) for k in c_sorted[-3:]]

            o = safe_float(c_sorted[-1][1])
            h = safe_float(c_sorted[-1][2])
            l = safe_float(c_sorted[-1][3])
            cl = safe_float(c_sorted[-1][4])

            if o > 0:
                btc_change_5m = (cl - o) / o * 100.0

            move_up = (h - o) / o * 100.0
            move_dn = (o - l) / o * 100.0
            vshape = in_short_term_vol_deadzone(closes_5m, ANTI_SWEEP_MOVE_PCT)

            if ((move_up >= ANTI_SWEEP_MOVE_PCT and move_dn >= ANTI_SWEEP_MOVE_PCT)
                or vshape):
                ANTI_SWEEP_LOCK_UNTIL = datetime.utcnow() + timedelta(
                    minutes=ANTI_SWEEP_LOCK_MINUTES
                )
                logging.warning(
                    "[ANTI-SWEEP] BTC 5m quét mạnh (up=%.2f%%, down=%.2f%%, vshape=%s) "
                    "-> LOCK mở lệnh tới %s.",
                    move_up, move_dn, vshape, ANTI_SWEEP_LOCK_UNTIL,
                )
    except Exception as e:
        logging.warning("[PUMP_PRO_V2] Lỗi anti-sweep BTC 5m: %s", e)


        
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
        # ===== PRO #3: Anti-sweep per ALT (5m) =====
        if is_symbol_locked(swap_id):
            logging.info("[ANTI-SWEEP][ALT] Skip %s (locked).", swap_id)
            continue
        if is_symbol_sweep_5m(o5_now, h5_now, l5_now, ALT_SWEEP_MOVE_PCT):
            lock_symbol_on_sweep(swap_id, ALT_SWEEP_LOCK_MINUTES,
                                 reason=f"5m sweep up&down >= {ALT_SWEEP_MOVE_PCT:.2f}%")
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
        # ===== FOLLOW-THROUGH FILTER (5m): tránh vào đúng nến spike bị xả =====
        # Ý tưởng: coi nến -3 là "spike", nến -2 là "confirm" (đều là nến đã đóng).
        # Chỉ cho vào nếu nến confirm còn cùng hướng + đủ lực (body/vol/close position).
        if PUMP_FOLLOW_THROUGH_ENABLED:
            if len(c5_sorted) < 4:
                continue
        
            sp = c5_sorted[-3]   # spike candle (closed)
            cf = c5_sorted[-2]   # confirm candle (closed)
        
            sp_o, sp_h, sp_l, sp_c, sp_v = safe_float(sp[1]), safe_float(sp[2]), safe_float(sp[3]), safe_float(sp[4]), safe_float(sp[5])
            cf_o, cf_h, cf_l, cf_c, cf_v = safe_float(cf[1]), safe_float(cf[2]), safe_float(cf[3]), safe_float(cf[4]), safe_float(cf[5])
        
            sp_rng = max(sp_h - sp_l, 1e-8)
            cf_rng = max(cf_h - cf_l, 1e-8)
            sp_body = abs(sp_c - sp_o)
            cf_body = abs(cf_c - cf_o)
            cf_close_pos = (cf_c - cf_l) / cf_rng  # 0..1
        
            # ratio check
            body_ok = (sp_body <= 0) or (cf_body >= sp_body * PUMP_FT_BODY_MIN_RATIO)
            vol_ok  = (sp_v   <= 0) or (cf_v   >= sp_v   * PUMP_FT_VOL_MIN_RATIO)
        
            if direction == "LONG":
                # confirm phải là nến xanh + close nằm cao
                if not (cf_c > cf_o and cf_close_pos >= PUMP_FT_CLOSEPOS_LONG_MIN and body_ok and vol_ok):
                    continue
            else:  # SHORT
                # confirm phải là nến đỏ + close nằm thấp
                if not (cf_c < cf_o and cf_close_pos <= PUMP_FT_CLOSEPOS_SHORT_MAX and body_ok and vol_ok):
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
            # ===== WINRATE FILTER: siết chase entry (lọc gắt) =====
            # 1.005/0.995 vào muộn dễ SL -> siết còn 1.002/0.998
            if direction == "LONG" and c_now > recent_high * 1.002:
                logging.info("[FILTER] %s skip chase LONG (overextended).", inst_id)
                continue
            if direction == "SHORT" and c_now < recent_low * 0.998:
                logging.info("[FILTER] %s skip chase SHORT (overextended).", inst_id)
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
    #Scanner phiên DEADZONE (10h30–15h30 VN):
    #- Không bắt breakout pump/dump.
    #- Ưu tiên coin volume lớn, biến động 24h vừa phải.
    #- Tìm tín hiệu mean-reversion quanh EMA20 5m (giá lệch không quá xa EMA, có dấu hiệu quay lại).
    #- Trả về DataFrame cùng format với build_signals_pump_dump_pro:
        #columns: instId, direction, change_pct, abs_change, last_price, vol_quote, score
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

        # ===== PRO: Anti-sweep per ALT (5m) =====
        if is_symbol_locked(swap_id):
            logging.info("[ANTI-SWEEP][ALT] Skip %s (locked).", swap_id)
            continue
        if is_symbol_sweep_5m(o_now, h_now, l_now, ALT_SWEEP_MOVE_PCT):
            lock_symbol_on_sweep(swap_id, ALT_SWEEP_LOCK_MINUTES, "SIDEWAY sweep 5m")
            continue

        # ===== PRO: V-shape deadzone (2 nhịp đảo chiều mạnh) =====
        if in_short_term_vol_deadzone(closes[-3:], threshold_pct=ANTI_SWEEP_MOVE_PCT):
            logging.info("[SIDEWAY][V-SHAPE] Skip %s (>=%.2f%% đảo chiều nhanh).",
                         swap_id, ANTI_SWEEP_MOVE_PCT)
            continue

        # ===== PRO: wick/body filter để tránh nến quét giả mean-reversion =====
        rng = max(h_now - l_now, 1e-8)
        body = abs(c_now - o_now)
        upper = h_now - max(o_now, c_now)
        lower = min(o_now, c_now) - l_now
        if body > 0 and (upper > body * 2.0 or lower > body * 2.0):
            logging.info("[SIDEWAY][WICK] Skip %s (wick quá dài vs body).", swap_id)
            continue

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
    #Từ df_signals, planned_trades.
    #TP/SL tính theo ATR 15m của từng cặp.
    """
    planned = []
    now_s = now_str_vn()

    if df.empty:
        return planned

    # ===== WINRATE FILTER: BAD trade nhẹ -> lọc gắt hơn =====
    # Nếu đang trade nhẹ (NOTIONAL_PER_TRADE=6) thì chỉ lấy kèo rất chắc (score>=7)
    try:
        is_light = (float(NOTIONAL_PER_TRADE) <= 6)
    except Exception:
        is_light = False
    
    min_score = 7 if is_light else 6
    df = df[df["score"] >= min_score].copy()
    if df.empty:
        logging.info(f"[FILTER] Không có tín hiệu đạt score>={min_score} -> skip run")
        return planned
    
    # Trade nhẹ: luôn chỉ 3 lệnh/run
    top_df = df.head(3)

    
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
    #Return dict: instId, {ctVal, lotSz, minSz}
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
    #Tính ATR (Average True Range) trên khung 15m cho 1 cặp.
    #Dùng ~30 nến, lấy ATR 14 nến gần nhất.
    #Trả về: atr (float) hoặc None nếu lỗi.
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
    #TP/SL theo ATR 15m (phiên PUMP/DUMP):
      #- risk_pct ~ ATR/price, kẹp [1%; 4%]
      #- RR = 2 (TP ≈ 2R, SL ≈ 1R) 
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

#def calc_contract_size(price, notional_usdt, ct_val, lot_sz, min_sz):
    #"""
    #price: last price
    #notional_usdt: desired position notional
    #ct_val: contract value (base coin)
    #lot_sz: minimum increment in contracts
    #"""
    #if price <= 0 or ct_val <= 0:
        #return 0.0
    #raw_contracts = notional_usdt / (price * ct_val)
    #lots = math.floor(raw_contracts / lot_sz)
    #contracts = lots * lot_sz
    #if contracts < min_sz:
        #return 0.0
    #return contracts

def _step_decimals(step: float) -> int:
    d = Decimal(str(step)).normalize()
    return max(-d.as_tuple().exponent, 0)

def calc_contract_size(price, notional_usdt, ct_val, lot_sz, min_sz):
    """
    Return contract size (sz) that is a valid multiple of lot_sz and >= min_sz.
    """
    if price <= 0 or ct_val <= 0:
        return 0

    raw_contracts = (notional_usdt / float(price)) / float(ct_val)

    # floor to lot step with epsilon to avoid float artifacts
    lots = math.floor((raw_contracts / float(lot_sz)) + 1e-12)
    contracts = lots * float(lot_sz)

    # harden float -> avoid 1.2000000000000002
    dec = _step_decimals(float(lot_sz))
    contracts = round(contracts, dec)

    # enforce min_sz (also step-aligned)
    if contracts < float(min_sz):
        lots = math.ceil((float(min_sz) / float(lot_sz)) - 1e-12)
        contracts = lots * float(lot_sz)
        contracts = round(contracts, dec)

    # if step is integer, send integer (OKX thích kiểu "1" hơn "1.0")
    if dec == 0:
        contracts = int(contracts)

    return contracts

def build_open_position_map(okx: OKXClient):
    """
    #Trả về dict:
    #{
      #'BTC-USDT-SWAP': {'long': True/False, 'short': True/False},
      #...
    #}
    #dùng để biết symbol nào đã có LONG / SHORT đang mở.
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
def maker_first_open_position(
    okx: OKXClient,
    inst_id: str,
    side_open: str,
    pos_side: str,
    contracts: float,
    desired_entry: float,
    lever: int,
    maker_offset_bps: float = 6.0,     # 6 bps = 0.06% (nhẹ, đủ maker)
    maker_timeout_sec: int = 3,        # chờ khớp maker 3s
):
    """
    Ưu tiên mở bằng post-only LIMIT (maker).
    Nếu không khớp trong timeout -> cancel + fallback MARKET.
    Return: (ok: bool, fill_px: float|None, used: 'maker'|'market'|'skip')
    """
    # ===== PATCH #2: throttle session 20-24 =====

    sz = normalize_swap_sz(okx, inst_id, contracts)
    # ===== PATCH: session guard =====
    if _is_session_20_24():
        allow, reason = _allow_trade_session_20_24(
            market_regime=locals().get("market_regime"),
            confidence=locals().get("confidence"),
            trend_score=locals().get("trend_score"),
        )
    elif _is_session_16_20():
        allow, reason = _allow_trade_session_16_20(
            market_regime=locals().get("market_regime"),
            confidence=locals().get("confidence"),
            trend_score=locals().get("trend_score"),
        )
    else:
        allow, reason = True, "ok:normal_session"

    if not allow:
        logging.info("[GUARD][SESSION] Block %s %s: %s", inst_id, side_open, reason)
        return False, None, "skip_session"

        # ===== PATCH #4: daily trade cap =====
    allow, reason = allow_trade_daily_limit()
    if not allow:
        logging.warning("[GUARD][DAILY] Block %s %s: %s", inst_id, side_open, reason)
        return False, None, "skip_daily_limit"

    # 1) Tính giá limit để tăng khả năng nằm chờ (maker)
    # LONG: đặt thấp hơn một chút; SHORT: đặt cao hơn một chút
    if desired_entry <= 0:
        return False, None, "skip"

    offset = maker_offset_bps / 10000.0
    if side_open.lower() == "buy":
        px = desired_entry * (1.0 - offset)
    else:
        px = desired_entry * (1.0 + offset)

    # 2) Gửi post-only maker
    sz = normalize_swap_sz(okx, inst_id, sz)
    resp = okx.place_futures_limit_order(
        inst_id=inst_id,
        side=side_open,
        pos_side=pos_side,
        sz=sz,
        px=px,
        td_mode="isolated",
        lever=lever,
        post_only=True,
    )

    # OKX trả ordId trong data[0].ordId (thường vậy)
    ord_id = None
    try:
        d = resp.get("data", [])
        if d:
            ord_id = d[0].get("ordId")
    except Exception:
        ord_id = None

    if not ord_id:
        # post-only có thể bị reject nếu giá chạm book -> fallback market
        logging.warning("[MAKER] Không lấy được ordId (post-only có thể bị reject). Fallback MARKET.")
        m = okx.place_futures_market_order(
            inst_id=inst_id,
            side=side_open,
            pos_side=pos_side,
            sz=contracts,
            td_mode="isolated",
            lever=lever,
        )
        code = m.get("code")
        return (code == "0"), None, "market"

    logging.info("[MAKER] Post-only sent: inst=%s ordId=%s px=%.10f", inst_id, ord_id, px)

    # 3) Chờ khớp
    filled, avg_px = okx.wait_order_filled(inst_id, ord_id, timeout_sec=maker_timeout_sec)

    if filled:
        n = inc_trades_today()
        logging.info("[GUARD][DAILY] trades_today=%s", n)
        logging.info("[MAKER] FILLED: inst=%s ordId=%s avgPx=%s", inst_id, ord_id, avg_px)
        return True, (avg_px or desired_entry), "maker"


    # 4) Không khớp -> cancel rồi market
    try:
        okx.cancel_order(inst_id, ord_id)
        logging.info("[MAKER] Canceled maker order: inst=%s ordId=%s -> fallback MARKET", inst_id, ord_id)
    except Exception as e:
        logging.warning("[MAKER] Cancel failed (still fallback MARKET): %s", e)

    m = okx.place_futures_market_order(
        inst_id=inst_id,
        side=side_open,
        pos_side=pos_side,
        sz=contracts,
        td_mode="isolated",
        lever=lever,
    )
    code = m.get("code")
    return (code == "0"), avg_px, "market"

def execute_futures_trades(okx: OKXClient, trades):
    if not trades:
        logging.info("[INFO] Không có lệnh futures nào để vào.")
        return

    # ===== CHỌN LEVERAGE + SIZE THEO GIỜ & THỊ TRƯỜNG =====
    regime = detect_market_regime(okx)  # "GOOD" / "BAD"

    if is_deadzone_time_vn():
        # phiên trưa: luôn giảm size + leverage
        this_lever    = 3
        this_notional = 12.0          # chỉ 15 USDT / lệnh
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
    margin_per_trade = this_notional / this_lever
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
        # ===== PRO #4: cooldown theo symbol =====
        if is_symbol_in_cooldown(swap_inst):
            logging.info("[COOLDOWN] Skip %s (still in cooldown).", swap_inst)
            continue
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
        # 2) MỞ VỊ THẾ (MAKER-FIRST)
        time.sleep(0.2)

        ok_open, fill_px, used_type = maker_first_open_position(
            okx=okx,
            inst_id=swap_inst,
            side_open=side_open,
            pos_side=pos_side,
            contracts=contracts,
            desired_entry=entry,
            lever=this_lever,
            maker_offset_bps=5.0,      # 0.05%
            maker_timeout_sec=3,       # 3 giây không khớp -> market
        )

        if not ok_open:
            logging.error("[ORDER] Mở lệnh thất bại %s (%s).", swap_inst, used_type)
            continue

        # Nếu có fill price thì dùng để log/đặt SL/TP chuẩn hơn
        real_entry = fill_px if (fill_px and fill_px > 0) else entry
        logging.info("[ORDER] Opened %s via %s | entry_sheet=%.10f real_entry=%.10f",
                     swap_inst, used_type, entry, real_entry)


        # 3) Đặt TP/SL OCO (SL giữ nguyên theo plan, TP hard cực xa)
        HARD_TP_CAP_PCT = 300.0
        #if signal == "LONG":
            #tp_hard = real_entry * (1 + HARD_TP_CAP_PCT / 100.0)
        #else:
            #tp_hard = real_entry * (1 - HARD_TP_CAP_PCT / 100.0)
        if signal == "LONG":
            tp_hard = real_entry * 6.0 #+500
        else:
            tp_hard = real_entry * 0.2 # -80% cho SHORT
        lev = float(this_lever)  # hoặc lev = float(lever)
        
        max_price_move = (MAX_SL_PNL_PCT / 100.0) / lev  # vd 7%/4 = 1.75% giá

        # SL theo plan
        sl_px = float(t["sl"])
        if signal == "LONG":
            sl_cap = real_entry * (1.0 - max_price_move)
            sl_px = max(sl_px, sl_cap)
        else:
            sl_cap = real_entry * (1.0 + max_price_move)
            sl_px = min(sl_px, sl_cap)
        
        logging.warning(f"[SL-CAP] {swap_inst} {signal} entry={real_entry:.8f} plan_sl={sl_px:.8f} cap_sl={sl_cap:.8f} lev={lev}")

        oco_resp = okx.place_oco_tp_sl(
            inst_id=swap_inst,
            pos_side=pos_side,
            side_close=side_close,
            sz=contracts,
            tp_px=tp_hard,
            sl_px=sl_px,
            td_mode="isolated",
        )
        oco_code = oco_resp.get("code")
        if oco_code != "0":
            msg = oco_resp.get("msg", "")
            logging.error(
                f"[OKX ORDER RESP] Không đặt được OCO TP/SL cho {swap_inst}: code={oco_code} msg={msg}. ĐÓNG LỆNH NGAY để tránh mất kiểm soát."
            )
            try:
                okx.close_swap_position(swap_inst, pos_side)
            except Exception as e:
                logging.error(f"[OKX ORDER RESP] Lỗi đóng lệnh khẩn cho {swap_inst}: {e}")
            continue  # bỏ qua, không cho lệnh này tồn tại

        # 4) Lệnh đã mở thành công -> lưu vào CACHE
        trade_cache_item = {
            "coin": coin,
            "signal": signal,
            "entry": real_entry,
            "tp": tp,
            "sl": sl,
            "time": now_str_vn(),
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
def cancel_oco_before_trailing(okx: OKXClient, inst_id: str, pos_side: str):
    """
    Tìm tất cả lệnh OCO TP/SL cùng instId + posSide và hủy
    (để tránh vừa OCO vừa trailing cùng lúc).
    """
    try:
        resp = okx.get_algo_pending(inst_id=inst_id, ord_type="oco")
    except Exception as e:
        logging.error("[TP-TRAIL] Lỗi gọi orders-algo-pending %s: %s", inst_id, e)
        return

    data = resp.get("data", []) if isinstance(resp, dict) else []
    algo_ids = []

    for item in data:
        try:
            if item.get("instId") != inst_id:
                continue
            if item.get("ordType") != "oco":
                continue
            # Nếu có posSide thì lọc đúng chiều
            if item.get("posSide") and item["posSide"] != pos_side:
                continue
            algo_id = item.get("algoId")
            if algo_id:
                algo_ids.append(algo_id)
        except Exception:
            continue

    if not algo_ids:
        logging.info("[TP-TRAIL] Không có OCO nào để hủy cho %s", inst_id)
        return

    try:
        okx.cancel_algos(inst_id, algo_ids)
        logging.info(
            "[TP-TRAIL] Đã hủy %d lệnh OCO trước khi đặt trailing cho %s",
            len(algo_ids),
            inst_id,
        )
    except Exception as e:
        logging.error("[TP-TRAIL] Lỗi khi hủy OCO %s: %s", inst_id, e)

def _extract_data_list(resp):
    # OKX có lúc trả dict {"code":"0","data":[...]} có lúc bạn đang nhận list luôn
    if isinstance(resp, dict):
        return resp.get("data", []) or []
    if isinstance(resp, list):
        return resp
    return []

def _get_oco_for_position(okx, inst_id: str, pos_side: str):
    try:
        resp = okx.get_algo_pending(inst_id=inst_id, ord_type="oco")
    except Exception as e:
        logging.error("[BE] Lỗi get_algo_pending oco %s: %s", inst_id, e)
        return None

    data = _extract_data_list(resp)

    for item in data:
        try:
            if item.get("instId") != inst_id:
                continue
            # nhiều bản OKX trả ordType hoặc algoOrdType… nên check mềm
            ord_type = item.get("ordType") or item.get("algoOrdType") or ""
            if "oco" not in str(ord_type).lower():
                continue

            # posSide có thể rỗng trong vài trường hợp -> nếu rỗng thì chấp nhận
            it_pos = item.get("posSide")
            if it_pos and it_pos != pos_side:
                continue

            return item
        except Exception:
            continue

    return None

def move_oco_sl_to_be(okx, inst_id, pos_side, sz, entry_px, offset_pct: float) -> bool:
    """Kéo SL về hòa vốn (BE) bằng cách: hủy OCO hiện tại -> đặt lại OCO giữ nguyên TP, đổi SL."""
    oco = _get_oco_for_position(okx, inst_id, pos_side)
    if not oco:
        return False
    algo_id = oco.get("algoId")
    tp_trigger_px = oco.get("tpTriggerPx")
    sl_trigger_px = oco.get("slTriggerPx")
    sl_now = safe_float(sl_trigger_px) if sl_trigger_px else 0.0

    if not algo_id or not tp_trigger_px:
        return False

    tp_px = safe_float(tp_trigger_px)
    if tp_px <= 0:
        return False

    # SL về BE + offset nhỏ để tránh quét đúng entry
    if pos_side == "long":
        sl_be = entry_px * (1.0 + offset_pct / 100.0)
    else:
        sl_be = entry_px * (1.0 - offset_pct / 100.0)
    # Anti-spam: nếu SL hiện tại đã gần target BE thì SKIP (tránh cancel/đặt lại y chang)
    if sl_now > 0 and sl_be > 0:
        tol_pct = 0.05  # 0.05% tolerance
        if pos_side == "long":
            if sl_now >= sl_be * (1.0 - tol_pct / 100.0):
                logging.warning("[BE] %s long SKIP (already near target) | sl_now=%.8f target_be=%.8f", inst_id, sl_now, sl_be)
                return False
        else:
            if sl_now <= sl_be * (1.0 + tol_pct / 100.0):
                logging.warning("[BE] %s short SKIP (already near target) | sl_now=%.8f target_be=%.8f", inst_id, sl_now, sl_be)
                return False
    try:
        okx.cancel_algos(inst_id, [algo_id])
    except Exception as e:
        logging.error("[BE] Lỗi cancel_algos %s: %s", inst_id, e)
        return False

    side_close = "sell" if pos_side == "long" else "buy"
    try:
        okx.place_oco_tp_sl(
            inst_id=inst_id,
            pos_side=pos_side,
            side_close=side_close,
            sz=str(sz),
            tp_px=tp_px,
            sl_px=sl_be,
            td_mode="isolated",
        )
        logging.warning("[BE] %s %s moved SL -> BE (%.8f), keep TP=%.8f", inst_id, pos_side, sl_be, tp_px)
        return True
    except Exception as e:
        logging.error("[BE] Lỗi place_oco_tp_sl %s: %s", inst_id, e)
        return False

def infer_be_from_oco(okx: OKXClient, inst_id: str, pos_side: str, entry_px: float) -> tuple[bool, int, float]:
    """
    Trả về: (is_be, tier, sl_now)
    is_be = True nếu SL hiện tại đã >= entry (long) hoặc <= entry (short) (có tolerance nhỏ).
    tier = map theo TP_BE_TIERS dựa trên offset đạt được (ước lượng).
    """
    oco = _get_oco_for_position(okx, inst_id, pos_side)
    if not oco:
        return False, 0, 0.0
    sl_trigger_px = oco.get("slTriggerPx") or oco.get("slOrdPx")
    sl_now = safe_float(sl_trigger_px) if sl_trigger_px else 0.0
    if entry_px <= 0 or sl_now <= 0:
        return False, 0, sl_now

    # --- BE check phải khớp với offset bạn dùng khi move_oco_sl_to_be ---
    # Nếu bạn có nhiều tier offset (TP_BE_TIERS), tier được suy từ off_pct.
    # is_be chỉ TRUE khi SL đã >= entry +/- offset tối thiểu (TP_LADDER_BE_OFFSET_PCT).
    # % offset thực tế của SL so với entry (dương)
    if pos_side == "long":
        off_pct = (sl_now / entry_px - 1.0) * 100.0
    else:
        off_pct = (1.0 - sl_now / entry_px) * 100.0
    
    # Tier theo offset (nếu có TP_BE_TIERS)
    tier = 0
    for i, (_thr, off) in enumerate(TP_BE_TIERS, start=1):
        if off_pct >= float(off):
            tier = i
    
    # Offset tối thiểu coi là "đã BE"
    # BE tối thiểu phải khớp với offset nhỏ nhất mà bot có thể dùng để dời SL
    min_off_from_tiers = 999.0
    try:
        min_off_from_tiers = min(float(off) for (_thr, off) in TP_BE_TIERS)
    except Exception:
        pass
    min_be_off = min(float(TP_LADDER_BE_OFFSET_PCT), float(min_off_from_tiers))

    
    # tolerance nhỏ để tránh nhiễu giá (0.01% là đủ)
    tol_pct = 0.01
    
    is_be = off_pct >= (min_be_off - tol_pct)
    
    return bool(is_be), int(tier), float(sl_now)

def has_trailing_server(okx: "OKXClient", inst_id: str, pos_side: str) -> bool:
    """
    Kiểm tra xem đã có lệnh trailing server-side (move_order_stop)
    cho inst_id + posSide hay chưa.
    """
    try:
        params = {
            "instId": inst_id,
            "ordType": "move_order_stop",
        }
        resp = okx._request("GET", "/api/v5/trade/orders-algo-pending", params=params)
    except Exception as e:
        logging.error("[TP-TRAIL] Lỗi get trailing pending %s: %s", inst_id, e)
        return False

    try:
        for o in resp.get("data", []):
            if o.get("instId") != inst_id:
                continue
            # Một số account không trả posSide, khi đó coi như trùng symbol là đủ
            pos = o.get("posSide", "") or o.get("posSide".lower(), "")
            if not pos_side or not pos or pos == pos_side:
                return True
    except Exception:
        pass

    return False
        
def has_active_trailing_for_position(okx: "OKXClient", inst_id: str, pos_side: str, return_info: bool = False):
    """
    True nếu có ít nhất 1 trailing (move_order_stop) đang hoạt động cho inst_id + posSide.
    Nếu return_info=True -> trả (True, order_dict) để đọc algoId/activePx/callbackRatio.
    """
    try:
        pending = okx.get_algo_pending(inst_id=inst_id, ord_type="move_order_stop")
    except Exception as e:
        logging.error("[TP-TRAIL] Lỗi get_algo_pending trailing cho %s: %s", inst_id, e)
        return (False, None) if return_info else False

    if not pending:
        return (False, None) if return_info else False

    data = pending.get("data", [])
    for o in data:
        try:
            if o.get("instId") != inst_id:
                continue
            if o.get("posSide") != pos_side:
                continue

            # OKX thường trả state kiểu: live/effective (có thể khác casing)
            st = (o.get("state") or "").lower()
            if st not in ("live", "effective"):
                continue

            return (True, o) if return_info else True
        except Exception:
            continue

    return (False, None) if return_info else False


def run_dynamic_tp(okx: "OKXClient"):
    """
    TP động + SL động + TP trailing cho các lệnh futures đang mở.

    Giữ:
      - Soft SL theo trend (SL_DYN_SOFT_PCT_GOOD/BAD, SL_DYN_LOOKBACK, SL_DYN_TREND_PCT)
      - SL khẩn cấp theo PnL% (MAX_EMERGENCY_SL_PNL_PCT) + follow tp_dyn_threshold
      - EARLY FAIL-SAFE
      - Ladder BE + ladder close (<10%) + server trailing (>=10%)
      - TP-DYN (2/4) CHỈ khi đã BE và pnl < 10% (và ladder chưa close)

    Sửa chồng chéo:
      - Đoạn "7) KẾT HỢP TP ĐỘNG + TP TRAILING" chỉ còn trailing (peak->tụt),
        KHÔNG còn đóng theo 1/4 tín hiệu nữa (vì TP-DYN2 đã xử lý 2/4).
    """

    def _reset_state(pos_key: str):
        EARLY_FAIL_REACHED_PROFIT.pop((pos_key + "_ARM"), None)
        TP_TRAIL_PEAK_PNL.pop(pos_key, None)
        TP_LADDER_BE_MOVED.pop(pos_key, None)
        TP_BE_TIER.pop(pos_key, None)

    logging.info("[TP-DYN] === BẮT ĐẦU KIỂM TRA TP ===")
    positions = okx.get_open_positions()
    logging.info("[TP-DYN] Số vị thế đang mở: %d", len(positions))
    if not positions:
        logging.info("[TP-DYN] Không có vị thế futures nào đang mở.")
        return

    # --- BỐI CẢNH CHUNG (tính 1 lần cho cả vòng) ---
    try:
        market_regime = detect_market_regime(okx)  # GOOD / BAD / ...
    except Exception as e:
        logging.error("[TP-DYN] Lỗi detect_market_regime: %s", e)
        market_regime = "UNKNOWN"

    # --- CONFIG CHO TP TRAILING (local) ---
    TP_TRAIL_START_PNL_PCT = 8.0
    TP_TRAIL_EXIT_PNL_PCT = 4.0
    TP_TRAIL_LOOKBACK_BARS = 30

    for p in positions:
        # ---------- 0) đọc position ----------
        try:
            instId = p.get("instId")
            inst_id = instId
            posSide = p.get("posSide")  # 'long' / 'short'
            pos_side = posSide

            pos = safe_float(p.get("pos", "0"))
            avail = safe_float(p.get("availPos", pos))
            sz = avail if avail > 0 else pos
            avg_px = safe_float(p.get("avgPx", "0"))

            logging.info("[TP-DYN] -> Kiểm tra %s | posSide=%s", instId, posSide)
        except Exception as e:
            logging.error("[TP-DYN] Lỗi đọc position: %s", e)
            continue

        pos_key = f"{instId}_{posSide}"

        if (not instId) or (sz <= 0) or (avg_px <= 0):
            _reset_state(pos_key)
            continue

        # ---------- 1) lấy nến 5m ----------
        try:
            c5 = okx.get_candles(instId, bar="5m", limit=TP_TRAIL_LOOKBACK_BARS)
        except Exception as e:
            logging.warning("[TP-DYN] Lỗi get_candles 5m %s: %s", instId, e)
            continue

        if (not c5) or (len(c5) < TP_DYN_FLAT_BARS + 10):
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

        # ---------- 2) PnL realtime ----------
        # (giữ price_pct vì code cũ có, dù hiện không dùng)
        if posSide == "long":
            price_pct = (c_now - avg_px) / avg_px * 100.0
        else:
            price_pct = (avg_px - c_now) / avg_px * 100.0

        pnl_pct = calc_realtime_pnl_pct(p, FUT_LEVERAGE)
        if pnl_pct is None:
            logging.warning("[TP-DYN] Không tính được PnL realtime cho %s, bỏ qua.", instId)
            continue

        above_10 = pnl_pct >= TP_TRAIL_SERVER_MIN_PNL_PCT  # thường = 10.0

        # ---------- 4) chọn tp_dyn_threshold (tính 1 lần) ----------
        in_deadzone = is_deadzone_time_vn()
        if in_deadzone:
            tp_dyn_threshold = 1.5
        else:
            if market_regime == "BAD":
                tp_dyn_threshold = 2.0
            else:
                tp_dyn_threshold = TP_DYN_MIN_PROFIT_PCT  # default 3%

        # ---------- 5) emergency SL follow tp_dyn_threshold ----------
        SL_FOLLOW_TP_MULT = 1.1
        sl_cap_pnl = min(MAX_EMERGENCY_SL_PNL_PCT, tp_dyn_threshold * SL_FOLLOW_TP_MULT)
        sl_cap_pnl = max(3.0, sl_cap_pnl)

        # ---------- 6) update peak pnl realtime ----------
        prev_peak = TP_TRAIL_PEAK_PNL.get(pos_key, None)
        if prev_peak is None:
            TP_TRAIL_PEAK_PNL[pos_key] = pnl_pct
        else:
            TP_TRAIL_PEAK_PNL[pos_key] = max(float(prev_peak), float(pnl_pct))
        peak_pnl = float(TP_TRAIL_PEAK_PNL.get(pos_key, pnl_pct))

        # ---------- 7) timeout 120' pnl < 5% ----------
        open_ms = int(p.get("cTime", "0") or 0)
        if open_ms > 0:
            age_min = (time.time() * 1000 - open_ms) / 60000.0
            if age_min >= 120 and pnl_pct < 5.0:
                logging.warning("[TIMEOUT] %s %s age=%.0f' pnl=%.2f%% < 5%% => CLOSE",
                                instId, posSide, age_min, pnl_pct)
                try:
                    mark_symbol_sl(instId, "timeout_120m")
                    okx.close_swap_position(instId, posSide)
                except Exception as e:
                    logging.error("[TIMEOUT] Lỗi đóng lệnh %s: %s", instId, e)
                _reset_state(pos_key)
                continue

        # ---------- 8) soft SL theo trend ----------

        # ---------- 9) EARLY FAIL-SAFE ----------
        if (not EARLY_FAIL_REACHED_PROFIT.get(pos_key, False)) and (peak_pnl >= EARLY_FAIL_NEVER_REACHED_PROFIT_PCT):
            EARLY_FAIL_REACHED_PROFIT[pos_key] = True

        # 9.2) EARLY 2-NHỊP: chạm ngưỡng lần 1 -> ARM, lần 2 liên tiếp -> CLOSE
        if (not EARLY_FAIL_REACHED_PROFIT.get(pos_key, False)) and (pnl_pct <= EARLY_FAIL_CUT_LOSS_PCT):
            # nhịp 1: chỉ ARM, không đóng ngay
            if not EARLY_FAIL_REACHED_PROFIT.get((pos_key + "_ARM"), False):
                EARLY_FAIL_REACHED_PROFIT[(pos_key + "_ARM")] = True
                logging.warning(
                    "[EARLY-ARM] %s %s peak=%.2f%% pnl=%.2f%% (chưa lên +%.1f%% mà đã xuống %.1f%%) -> WAIT 1 CYCLE",
                    instId, posSide, peak_pnl, pnl_pct,
                    EARLY_FAIL_NEVER_REACHED_PROFIT_PCT, EARLY_FAIL_CUT_LOSS_PCT
                )
            else:
                # nhịp 2: vẫn dưới ngưỡng ở vòng kế tiếp -> CLOSE
                logging.warning(
                    "[EARLY-FAIL] %s %s peak=%.2f%% pnl=%.2f%% (2nd hit) => CLOSE",
                    instId, posSide, peak_pnl, pnl_pct
                )
                try:
                    mark_symbol_sl(instId, "early_fail")
                    maker_close_position_with_timeout(
                        okx=okx,
                        inst_id=instId,
                        pos_side=posSide,
                        sz=sz,
                        last_px=c_now,
                        offset_bps=6.0,
                        timeout_sec=3,
                    )
                    logging.info("[CLOSE] reason=EARLY pnl=%.2f%% inst=%s side=%s", pnl_pct, instId, posSide)
                except Exception as e:
                    logging.error("[EARLY-FAIL] Lỗi đóng lệnh %s: %s", instId, e)
                _reset_state(pos_key)
                continue
        else:
            # reset ARM khi đã hồi lên khỏi ngưỡng EARLY
            if EARLY_FAIL_REACHED_PROFIT.get((pos_key + "_ARM"), False):
                EARLY_FAIL_REACHED_PROFIT[(pos_key + "_ARM")] = False

        # ---------- 10) emergency SL theo pnl (follow tp_dyn_threshold) ----------
        if pnl_pct <= -sl_cap_pnl:
            logging.info("[TP-DYN] %s lỗ %.2f%% <= -%.2f%% PnL -> CẮT LỖ KHẨN CẤP.", instId, pnl_pct, sl_cap_pnl)
            logging.info("[CLOSE] reason=EMERGENCY_SL pnl=%.2f%% inst=%s side=%s", pnl_pct, inst_id, pos_side)
            try:
                mark_symbol_sl(instId, "emergency_sl")
                okx.close_swap_position(instId, posSide)
            except Exception as e:
                logging.error("[TP-DYN] Lỗi đóng lệnh %s: %s", instId, e)
            _reset_state(pos_key)
            continue

        # ---------- 11) LADDER + BE (<10%) ----------
        ladder_closed = False
        pos_key = f"{instId}_{posSide}"
        if PROFIT_LOCK_ENABLED and pnl_pct < TP_LADDER_SERVER_THRESHOLD:
            # (A) Infer BE trực tiếp từ OCO để chống cron restart (dict reset vẫn nhận ra đã BE)
            is_be, inferred_tier, sl_now = infer_be_from_oco(okx, instId, posSide, avg_px)
            if is_be:
                TP_LADDER_BE_MOVED[pos_key] = True
                TP_BE_TIER[pos_key] = max(int(TP_BE_TIER.get(pos_key, 0) or 0), int(inferred_tier or 0))
        
            current_tier = int(TP_BE_TIER.get(pos_key, 0) or 0)
        
            # (B) Nếu đã BE rồi thì SKIP (chỉ nâng khi lên tier cao hơn)
            if TP_LADDER_BE_MOVED.get(pos_key, False):
                logging.info(
                    "[BE] %s %s SKIP | already_BE tier=%s | pnl=%.2f%% | sl_now=%.8f",
                    instId, posSide, current_tier, pnl_pct, float(sl_now or 0.0)
                )
            else:
                # (C) Chưa BE: nếu pnl đủ trigger thì move về tier tương ứng
                if pnl_pct >= TP_LADDER_BE_TRIGGER_PNL_PCT:
                    desired_tier = 0
                    desired_offset = TP_LADDER_BE_OFFSET_PCT
                    for i, (thr, off) in enumerate(TP_BE_TIERS, start=1):
                        if pnl_pct >= thr:
                            desired_tier = i
                            desired_offset = off
        
                    # chỉ move nếu tier > current (và current lúc này là 0)
                    if desired_tier > current_tier:
                        moved = move_oco_sl_to_be(okx, instId, posSide, sz, avg_px, desired_offset)
                        if moved:
                            TP_LADDER_BE_MOVED[pos_key] = True
                            TP_BE_TIER[pos_key] = desired_tier
                            logging.warning(
                                "[BE] %s %s moved SL->BE tier=%s (pnl=%.2f%%, offset=%.2f%%)",
                                instId, posSide, desired_tier, pnl_pct, desired_offset
                            )
            # LOG POS
            logging.info(
                "[POS] %s %s | pnl=%.2f%% | peak=%.2f%% | BE=%s(tier=%s)",
                instId, posSide,
                pnl_pct,
                TP_TRAIL_PEAK_PNL.get(pos_key, pnl_pct),
                "YES" if TP_LADDER_BE_MOVED.get(pos_key, False) else "NO",
                TP_BE_TIER.get(pos_key, 0),
            ) 
            # 11.2) Ladder close theo peak_pnl
            closed_by_ladder = False
            for peak_thr, floor_thr in TP_LADDER_RULES:
                if peak_pnl >= peak_thr and pnl_pct <= floor_thr:
                    logging.warning(
                        "[LADDER] %s peak=%.2f%% pnl=%.2f%% hit rule (>=%.1f then <=%.1f) => CLOSE",
                        instId, peak_pnl, pnl_pct, peak_thr, floor_thr
                    )
                    try:
                        mark_symbol_tp(instId)
                        maker_close_position_with_timeout(
                            okx=okx,
                            inst_id=instId,
                            pos_side=posSide,
                            sz=sz,
                            last_px=c_now,
                            offset_bps=6.0,
                            timeout_sec=3,
                        )
                        closed_by_ladder = True
                    except Exception as e:
                        logging.error("[LADDER] Lỗi đóng lệnh %s: %s", instId, e)
                    break

            if closed_by_ladder:
                _reset_state(pos_key)
                ladder_closed = True
                continue  # đóng rồi thì qua vị thế khác

        # ---------- 12) compute max_pnl_window (để dùng trailing peak->tụt) ----------
        max_pnl_window = 0.0
        for close_px in closes[-TP_TRAIL_LOOKBACK_BARS:]:
            if posSide == "long":
                price_pct_i = (close_px - avg_px) / avg_px * 100.0
            else:
                price_pct_i = (avg_px - close_px) / avg_px * 100.0
            pnl_pct_i = price_pct_i * FUT_LEVERAGE
            if pnl_pct_i > max_pnl_window:
                max_pnl_window = pnl_pct_i

        # ---------- 13) server-side trailing khi pnl >= 8% ----------
        # ---------- 13.1) trailing (pump-mode: bot-managed + protections A/B) ----------
        now_ms = int(time.time() * 1000)

        is_pump, pump_meta = detect_pump_dump_2of3(closes, highs, lows, vols)
        if is_pump:
            until = now_ms + int(PUMP_WINDOW_SEC * 1000)
            prev_until = PUMP_MODE_UNTIL_MS.get(pos_key, 0)
            PUMP_MODE_UNTIL_MS[pos_key] = max(prev_until, until)

        pump_mode = now_ms < PUMP_MODE_UNTIL_MS.get(pos_key, 0)

        if pump_mode and pnl_pct >= TP_TRAIL_SERVER_MIN_PNL_PCT:
            # 1) current price (giữ đúng logic bạn đang dùng)
            current_px = mark_px if mark_px is not None else (last_px if last_px is not None else (c_now if c_now else closes[-1]))

            # 2) callback pump (nới có kiểm soát + cap)
            pump_cb = min(callback_pct * PUMP_CB_MULT, PUMP_CB_MAX_PCT)

            # 3) peak như cũ: có đỉnh mới là update
            if posSide == "long":
                peak_now = max(highs[-TP_TRAIL_LOOKBACK_BARS:]) if len(highs) >= TP_TRAIL_LOOKBACK_BARS else max(highs)
                prev_peak = SOFT_TRAIL_PEAK_PX.get(pos_key, 0.0)
                peak_px = max(prev_peak, peak_now, current_px)
                SOFT_TRAIL_PEAK_PX[pos_key] = peak_px

                stop_px = peak_px * (1.0 - pump_cb / 100.0)
                hard_floor = peak_px * (1.0 - HARD_STOP_PCT / 100.0)

                hard_breach = current_px <= hard_floor
                under_stop = current_px <= stop_px
                safe_again = current_px > stop_px

            else:  # short
                peak_now = min(lows[-TP_TRAIL_LOOKBACK_BARS:]) if len(lows) >= TP_TRAIL_LOOKBACK_BARS else min(lows)
                prev_peak = SOFT_TRAIL_PEAK_PX.get(pos_key, 0.0)
                peak_px = peak_now if prev_peak == 0.0 else min(prev_peak, peak_now, current_px)
                SOFT_TRAIL_PEAK_PX[pos_key] = peak_px

                stop_px = peak_px * (1.0 + pump_cb / 100.0)
                hard_floor = peak_px * (1.0 + HARD_STOP_PCT / 100.0)

                hard_breach = current_px >= hard_floor
                under_stop = current_px >= stop_px
                safe_again = current_px < stop_px

            logging.info(
                "[PUMP-TRAIL] %s pump_mode=YES score=%s chg=%.2f%% volR=%.2f rangeR=%.2f | cb=%.2f%% peak=%.6f stop=%.6f hard=%.6f px=%.6f",
                instId,
                pump_meta.get("score"),
                pump_meta.get("chg_5m_pct", 0.0),
                pump_meta.get("vol_ratio", 0.0),
                pump_meta.get("range_ratio", 0.0),
                pump_cb, peak_px, stop_px, hard_floor, current_px
            )

            # 4) Nếu trước đó đã đặt trailing server (hoặc còn algo), hủy để tránh sàn đóng sớm
            try:
                exists, info = has_active_trailing_for_position(okx, instId, posSide, return_info=True)
                if exists and info:
                    old_algo_id = info.get("algoId")
                    try:
                        okx.cancel_algo_order(instId, old_algo_id)
                        logging.info("[PUMP-TRAIL] cancel trailing server cũ %s algoId=%s", instId, old_algo_id)
                    except Exception as e:
                        logging.error("[PUMP-TRAIL] cancel trailing server cũ lỗi %s algoId=%s: %s", instId, old_algo_id, e)
            except Exception:
                pass

            # 5) (Tuỳ bạn) hủy OCO để khỏi bị chốt sớm trong pump
            try:
                cancel_oco_before_trailing(okx, instId, posSide)
            except Exception as e:
                logging.error("[PUMP-TRAIL] lỗi khi hủy OCO trước soft-trail %s (%s): %s", instId, posSide, e)

            # ===== A) HARD FLOOR: breach là đóng ngay =====
            if hard_breach:
                logging.warning("[PUMP-TRAIL] HARD_FLOOR breach => CLOSE NOW %s %s (px=%.6f hard=%.6f)",
                                instId, posSide, current_px, hard_floor)
                try:
                    mark_symbol_tp(instId)
                    maker_close_position_with_timeout(
                        okx=okx,
                        inst_id=instId,
                        pos_side=posSide,
                        sz=sz,
                        last_px=current_px,
                        timeout_sec=3,
                    )
                except Exception as e:
                    logging.error("[PUMP-TRAIL] close HARD_FLOOR error %s: %s", instId, e)
                _reset_state(pos_key)
                continue

            # ===== B) TIME-UNDER-STOP: dưới stop liên tục TIME_UNDER_STOP_SEC mới đóng =====
            if under_stop:
                since = UNDER_STOP_SINCE_MS.get(pos_key, 0)
                if since <= 0:
                    UNDER_STOP_SINCE_MS[pos_key] = now_ms
                    logging.info("[PUMP-TRAIL] UNDER_STOP hit1 mark %s %s (px=%.6f stop=%.6f)",
                                 instId, posSide, current_px, stop_px)
                else:
                    dt_sec = (now_ms - since) / 1000.0
                    if dt_sec >= TIME_UNDER_STOP_SEC:
                        logging.warning("[PUMP-TRAIL] UNDER_STOP %.1fs => CLOSE %s %s (px=%.6f stop=%.6f)",
                                        dt_sec, instId, posSide, current_px, stop_px)
                        try:
                            mark_symbol_tp(instId)
                            maker_close_position_with_timeout(
                                okx=okx,
                                inst_id=instId,
                                pos_side=posSide,
                                sz=sz,
                                last_px=current_px,
                                timeout_sec=3,
                            )
                        except Exception as e:
                            logging.error("[PUMP-TRAIL] close UNDER_STOP error %s: %s", instId, e)
                        _reset_state(pos_key)
                        UNDER_STOP_SINCE_MS.pop(pos_key, None)
                        continue
            elif safe_again:
                # hồi lên lại trên stop => reset mark
                if UNDER_STOP_SINCE_MS.get(pos_key, 0) > 0:
                    logging.info("[PUMP-TRAIL] UNDER_STOP reset %s %s (px=%.6f stop=%.6f)",
                                 instId, posSide, current_px, stop_px)
                UNDER_STOP_SINCE_MS.pop(pos_key, None)

            # pump mode: không đặt trailing server, để soft-trail tiếp tục quản lý
            continue

        if pnl_pct >= TP_TRAIL_SERVER_MIN_PNL_PCT:
            callback_pct = dynamic_trail_callback_pct(pnl_pct)
        
            # >>> lấy giá realtime để set activePx (ưu tiên mark, fallback last, cuối cùng mới fallback candle close)
            mark_px = okx.get_mark_price(inst_id)
            last_px = okx.get_last_price(inst_id) if mark_px is None else None
            current_px = mark_px if mark_px is not None else (last_px if last_px is not None else (c_now if c_now else closes[-1]))
        
            logging.info(
                "[TP-TRAIL] %s vào vùng trailing server (pnl=%.2f%% >= %.2f%%). callback=%.2f%%, activePx=%.6f (mark=%s, last=%s)",
                inst_id, pnl_pct, TP_TRAIL_SERVER_MIN_PNL_PCT, callback_pct, current_px,
                f"{mark_px:.6f}" if mark_px is not None else "None",
                f"{last_px:.6f}" if last_px is not None else "None",
            )
        
            # >>> nếu đã có trailing, kiểm tra có cần UPDATE không
            exists, info = has_active_trailing_for_position(okx, inst_id, pos_side, return_info=True)
            if exists and info:
                old_algo_id = info.get("algoId")
                old_active = safe_float(info.get("activePx") or 0.0)
                old_cb = safe_float(info.get("callbackRatio") or 0.0)
        
                # ngưỡng update: activePx lệch > 0.2% hoặc callback lệch > 0.05%
                need_update = False
                if old_active > 0 and abs(old_active - current_px) / current_px > 0.002:
                    need_update = True
                if abs(old_cb - callback_pct) > 0.05:
                    need_update = True
        
                if not need_update:
                    logging.info(
                        "[TP-TRAIL] Đã có trailing (%s) và vẫn hợp lệ -> không update (old_active=%.6f old_cb=%.2f%%).",
                        inst_id, old_active, old_cb
                    )
                    continue
        
                # >>> HỦY trailing cũ để đặt trailing mới (update thực sự)
                try:
                    okx.cancel_algos(inst_id=inst_id, algo_ids=[old_algo_id])
                    logging.info("[TP-TRAIL] Cancel trailing cũ algoId=%s để update (%s).", old_algo_id, inst_id)
                except Exception as e:
                    logging.error("[TP-TRAIL] Lỗi cancel trailing cũ %s algoId=%s: %s", inst_id, old_algo_id, e)
                    # nếu không hủy được thì thôi tránh spam đặt mới
                    continue
        
            # Hủy OCO trước khi đặt trailing mới
            try:
                cancel_oco_before_trailing(okx, inst_id, pos_side)
            except Exception as e:
                logging.error("[TP-TRAIL] lỗi khi hủy OCO trước trailing %s (%s): %s", inst_id, pos_side, e)
        
            side_close = "sell" if pos_side == "long" else "buy"
            try:
                # >>> IMPORTANT: đổi triggerPxType sang MARK trong hàm OKXClient.place_trailing_stop (PATCH 3b bên dưới)
                okx.place_trailing_stop(
                    inst_id=inst_id,
                    pos_side=pos_side,
                    side_close=side_close,
                    sz=sz,
                    callback_ratio_pct=callback_pct,
                    active_px=current_px,
                    td_mode="isolated",
                )
                logging.info("[TP-TRAIL] ĐÃ ĐẶT trailing server cho %s (pnl=%.2f%%, callback=%.2f%%, activePx=%.6f).",
                             inst_id, pnl_pct, callback_pct, current_px)
            except Exception as e:
                logging.error("[TP-TRAIL] Exception khi đặt trailing server cho %s: %s", inst_id, e)
        
            continue  # đã giao cho sàn


        # ---------- 14) 4 tín hiệu TP-DYN ----------
        if posSide == "long":
            flat_move = not (c_now > c_prev1 > c_prev2)
        else:
            flat_move = not (c_now < c_prev1 < c_prev2)

        body_now = abs(c_now - o_now)
        body_prev = abs(c_prev1 - o_prev1)
        if posSide == "long":
            engulfing = (c_now < o_now) and (body_now > body_prev) and (c_now < l_prev1)
        else:
            engulfing = (c_now > o_now) and (body_now > body_prev) and (c_now > h_prev1)

        vols_before = vols[-(TP_DYN_FLAT_BARS + 10):-1]
        avg_vol10 = sum(vols_before) / max(1, len(vols_before))
        vol_drop = (avg_vol10 > 0) and ((vol_now / avg_vol10) < TP_DYN_VOL_DROP_RATIO)

        ema5 = calc_ema(closes[-(TP_DYN_EMA_LEN + 5):], TP_DYN_EMA_LEN)
        ema_break = False
        if ema5:
            if posSide == "long":
                ema_break = c_now < ema5
            else:
                ema_break = c_now > ema5

        logging.info(
            "[TP-DYN] %s pnl=%.2f%% (thr=%.2f%%, max_window=%.2f%%) | flat=%s | engulf=%s | vol_drop=%s | ema_break=%s",
            instId, pnl_pct, tp_dyn_threshold, max_pnl_window,
            flat_move, engulfing, vol_drop, ema_break,
        )

        # ---------- 15) TP-DYN2: CHỈ khi đã BE ----------
        if not TP_LADDER_BE_MOVED.get(pos_key, False):
            continue

        if (not ladder_closed) and (not above_10) and TP_LADDER_BE_MOVED.get(pos_key, False):
            dyn_hits = (1 if flat_move else 0) + (1 if engulfing else 0) + (1 if vol_drop else 0) + (1 if ema_break else 0)
            if dyn_hits >= 2:
                logging.warning(
                    "[TP-DYN2] %s %s pnl=%.2f%% hit=%d/4 (flat=%s engulf=%s vol=%s ema=%s) => CLOSE",
                    instId, posSide, pnl_pct, dyn_hits, flat_move, engulfing, vol_drop, ema_break
                )
                try:
                    mark_symbol_tp(instId)
                    maker_close_position_with_timeout(
                        okx=okx,
                        inst_id=instId,
                        pos_side=posSide,
                        sz=sz,
                        last_px=c_now,
                        offset_bps=6.0,
                        timeout_sec=3,
                    )
                except Exception as e:
                    logging.error("[TP-DYN2] Lỗi đóng lệnh %s: %s", instId, e)
                _reset_state(pos_key)
                continue

        # ---------- 16) #7: chỉ trailing peak->tụt (KHÔNG đóng theo 1/4 tín hiệu) ----------
        should_close_trailing = (max_pnl_window >= TP_TRAIL_START_PNL_PCT) and (pnl_pct <= TP_TRAIL_EXIT_PNL_PCT)
        if should_close_trailing:
            logging.info(
                "[TP-TRAIL] %s đã từng lời >= %.2f%% (max=%.2f%%) nhưng hiện còn %.2f%% → CHỐT THEO TRAILING.",
                instId, TP_TRAIL_START_PNL_PCT, max_pnl_window, pnl_pct
            )
            try:
                mark_symbol_tp(instId)
                used = maker_close_position_with_timeout(
                    okx=okx,
                    inst_id=instId,
                    pos_side=posSide,
                    sz=sz,
                    last_px=c_now,
                    offset_bps=6.0,
                    timeout_sec=3,
                )
                logging.info("[CLOSE] reason=TRAILING pnl=%.2f%% inst=%s side=%s via=%s", pnl_pct, instId, posSide, used)
            except Exception as e:
                logging.error("[TP-TRAIL] Lỗi đóng lệnh %s: %s", instId, e)
            _reset_state(pos_key)
            continue
        logging.info("[TP-DYN] Giữ lệnh %s – chưa đến điểm thoát.", instId)
    logging.info("[TP-DYN] ===== DYNAMIC TP DONE =====")

def detect_market_regime(okx: "OKXClient"):
    # GOOD/BAD dựa trên BTC 5m + đồng pha trend 15m
    try:
        # lấy dư 1 cây để chắc có 2 cây ĐÃ ĐÓNG
        c5  = okx.get_candles("BTC-USDT-SWAP", bar="5m",  limit=4)
        c15 = okx.get_candles("BTC-USDT-SWAP", bar="15m", limit=4)
    except:
        return "BAD"

    if not c5 or len(c5) < 3:
        return "BAD"

    # ==== 5m (dùng nến đã đóng) ====
    c5_s = sorted(c5, key=lambda x: int(x[0]))
    k = -2   # nến 5m đã đóng gần nhất
    p = -3   # nến trước đó (đã đóng)

    o5 = safe_float(c5_s[k][1])
    h5 = safe_float(c5_s[k][2])
    l5 = safe_float(c5_s[k][3])
    c5_close = safe_float(c5_s[k][4])

    body = abs(c5_close - o5)
    rng  = max(h5 - l5, 1e-8)
    body_ratio = body / rng

    upper_wick = h5 - max(o5, c5_close)
    lower_wick = min(o5, c5_close) - l5
    wick_ratio = (upper_wick + lower_wick) / rng

    # trend 5m: đóng-đóng
    c5_prev_close = safe_float(c5_s[p][4])
    trend_5_up = c5_close > c5_prev_close
    trend_5_dn = c5_close < c5_prev_close

    # ==== 15m trend (cũng dùng nến đã đóng) ====
    trend_15_up = trend_15_dn = False
    if c15 and len(c15) >= 3:
        c15_s = sorted(c15, key=lambda x: int(x[0]))
        c15_close = safe_float(c15_s[-2][4])   # đã đóng
        c15_prev  = safe_float(c15_s[-3][4])   # đã đóng
        trend_15_up = c15_close > c15_prev
        trend_15_dn = c15_close < c15_prev

    if (
        body_ratio > 0.55 and
        wick_ratio < 0.45 and
        ((trend_5_up and trend_15_up) or (trend_5_dn and trend_15_dn))
    ):
        return "GOOD"

    return "BAD"


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
    # 1.5) HARD STOP theo ngày (USDT)
    ok_day, day_reason = check_day_hard_stop(okx)
    if not ok_day:
        logging.warning(f"[LOCK] DAY HARD STOP -> {day_reason}. Chỉ chạy quản lý lệnh (TP-DYN) thôi.")
        return
    
    # 1.6) DEADZONE hard lock + MARKET soft lock/unlock
    ok_mkt, mkt_reason = check_market_lock_unlock(okx)
    if not ok_mkt:
        logging.warning(f"[LOCK] MARKET/DEADZONE -> {mkt_reason}. Không SCAN/MỞ lệnh mới.")
        return
    else:
        logging.info(f"[UNLOCK] -> {mkt_reason}")

    regime = detect_market_regime(okx)
    # ===== WINRATE FILTER: ngoài deadzone chỉ trade khi market GOOD =====
    # Ngoài deadzone: nếu market BAD thì vẫn trade nhưng giảm rủi ro mạnh
    if (not is_deadzone_time_vn()) and regime != "GOOD":
        global NOTIONAL_PER_TRADE
        NOTIONAL_PER_TRADE = 6
        logging.warning("[FILTER] regime != GOOD ngoài deadzone -> BAD TRADE NHẸ: NOTIONAL_PER_TRADE=6")
    else:
        # regime GOOD -> dùng notional như cấu hình bình thường (nếu bạn set ở đâu đó)
        logging.info("[FILTER] regime GOOD -> trade normal")

    logging.info(f"[REGIME] Thị trường hiện tại: {regime}")
    if regime == "GOOD":
        current_notional = 30
    else:
        current_notional = 10
    # 🔒 1b) Anti-sweep lock
    logging.info("[BOT] >>> BẮT ĐẦU KIỂM TRA ANTI-SWEEP <<<")
    if is_anti_sweep_locked():
        logging.warning(
            "[BOT] ANTI-SWEEP lock tới %s -> KHÔNG scan/mở lệnh mới.",
            ANTI_SWEEP_LOCK_UNTIL,
        )
        return
    logging.info("[BOT] Anti-sweep check OK -> tiếp tục scan/mở lệnh.")
    
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
    
    #logging.info("[SCHED] %02d' -> CHẠY FULL BOT", minute)
    #run_full_bot(okx)

    # 2) Các mốc 5 - 20 - 35 - 50 phút thì chạy thêm FULL BOT
    if minute in (5, 20, 35, 50):
        logging.info("[SCHED] %02d' -> CHẠY FULL BOT", minute)
        run_full_bot(okx)
    else:
        logging.info("[SCHED] %02d' -> CHỈ CHẠY TP DYNAMIC", minute)

if __name__ == "__main__":
    main()
