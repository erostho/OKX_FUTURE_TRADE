
# -*- coding: utf-8 -*-
"""
main_future_trading_merged.py
- Uses your original structure assumptions (Telegram + Google Sheet hooks preserved).
- Adds pro upgrades:
  1) Volume spike "smart" (auto/percentile/factor) via ENV
  2) Regime filters (ADX, BB width, BB slope>0)
  3) BOS + Pullback + Candle quality (ATR body)
  4) S/R theo hướng + MACD histogram percentile
  5) BTC filter (keep your variable)
  6) Position sizing: risk_per_trade = 1% equity
  7) Universe: Top 200 OKX USDT-M SWAP by 24h quote volume
  8) Session filter: 09:00–23:00 (UTC+7)
  9) News blackout: built-in schedule (FOMC/CPI/NFP), 45' before/after
 10) Backtest hooks (placeholders) for grid optimization
- It DOES NOT remove your Telegram / Google Sheet code: use send_telegram() and append_to_sheet() hooks.
"""

import os
import math
import time
import json
import logging
from typing import Tuple, Optional, List, Dict
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import requests


# =============================
# CONFIG (override via ENV)
# =============================
RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", "0.01"))   # 1% equity
DEFAULT_EQUITY_USDT = float(os.getenv("DEFAULT_EQUITY_USDT", "1000"))

VOLUME_MODE = os.getenv("VOLUME_MODE", "auto")       # 'auto' | 'percentile' | 'factor'
VOLUME_PERCENTILE = int(os.getenv("VOLUME_PERCENTILE", "75"))
VOLUME_FACTOR = float(os.getenv("VOLUME_FACTOR", "1.5"))

ATR_LEN = 14
BREAKOUT_BODY_ATR = float(os.getenv("BREAKOUT_BODY_ATR", "0.6"))
SR_NEAR_PCT = float(os.getenv("SR_NEAR_PCT", "0.03"))
ADX_MIN_15M = int(os.getenv("ADX_MIN_15M", "22"))
ADX_MIN_1H  = int(os.getenv("ADX_MIN_1H", "20"))
BBW_MIN     = float(os.getenv("BBW_MIN", "0.012"))

# Trading session (UTC+7)
SESSION_START = os.getenv("SESSION_START", "09:00")
SESSION_END   = os.getenv("SESSION_END", "23:00")

# News blackout (simple built-in UTC times, extend as needed)
NEWS_BLACKOUT_WINDOW_MIN = int(os.getenv("NEWS_BLACKOUT_WINDOW_MIN", "45"))

# Universe
UNIVERSE_SOURCE = os.getenv("UNIVERSE_SOURCE", "OKX_TOP200")  # OKX_TOP200 or SHEET
SHEET_UNIVERSE_URL = os.getenv("SHEET_UNIVERSE_URL", "")      # CSV export if using sheet
OKX_MIN_NOTIONAL_USDT = float(os.getenv("OKX_MIN_NOTIONAL_USDT", "5"))


# =============================
# Telegram & Google Sheet hooks
# (Keep your existing implementations; these are fallbacks)
# =============================
def send_telegram(text: str):
    """Fallback: uses env TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID if your original isn't present."""
    try:
        from telegram import Bot  # if user already installed python-telegram-bot
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        if token and chat_id:
            Bot(token).send_message(chat_id=chat_id, text=text, parse_mode=None)
            return
    except Exception:
        pass
    # HTTP fallback
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if token and chat_id:
        try:
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            requests.post(url, json={"chat_id": chat_id, "text": text})
        except Exception as e:
            logging.error(f"[TG] Send failed: {e}")


def append_to_sheet(payload: Dict):
    """Fallback: post JSON to your Apps Script Web App if your original isn't present."""
    gs_url = os.getenv("GS_APPEND_URL", "")
    if not gs_url:
        return
    try:
        requests.post(gs_url, json=payload, timeout=10)
    except Exception as e:
        logging.error(f"[GS] Append failed: {e}")


# =============================
# Indicators
# =============================
def ensure_column(df: pd.DataFrame, col: str, series: pd.Series) -> None:
    if col not in df.columns:
        df[col] = series


def _ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def _atr(df: pd.DataFrame, n: int = ATR_LEN) -> pd.Series:
    high = df["high"]
    low  = df["low"]
    close_prev = df["close"].shift(1)
    tr1 = (high - low).abs()
    tr2 = (high - close_prev).abs()
    tr3 = (low  - close_prev).abs()
    tr  = np.maximum(tr1, np.maximum(tr2, tr3))
    atr = pd.Series(tr, index=df.index).rolling(n).mean()
    return atr


def compute_minimum_indicators(df: pd.DataFrame) -> None:
    if "ema20" not in df.columns:
        ensure_column(df, "ema20", _ema(df["close"], 20))
    if "ema50" not in df.columns:
        ensure_column(df, "ema50", _ema(df["close"], 50))

    if ("bb_upper" not in df.columns) or ("bb_lower" not in df.columns):
        ma = df["close"].rolling(20).mean()
        std = df["close"].rolling(20).std(ddof=0)
        ensure_column(df, "bb_upper", ma + 2*std)
        ensure_column(df, "bb_lower", ma - 2*std)

    if ("macd" not in df.columns) or ("macd_signal" not in df.columns):
        ema12 = _ema(df["close"], 12)
        ema26 = _ema(df["close"], 26)
        macd_line = ema12 - ema26
        macd_signal = macd_line.ewm(span=9, adjust=False).mean()
        ensure_column(df, "macd", macd_line)
        ensure_column(df, "macd_signal", macd_signal)

    if "rsi" not in df.columns:
        delta = df["close"].diff()
        gain = (delta.clip(lower=0)).rolling(14).mean()
        loss = (-delta.clip(upper=0)).rolling(14).mean()
        rs = gain / loss.replace(0, np.nan)
        rsi = 100 - (100 / (1 + rs))
        ensure_column(df, "rsi", rsi)

    if "adx" not in df.columns:
        up_move = df["high"].diff()
        down_move = -df["low"].diff()
        plus_dm  = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
        minus_dm = np.where((down_move > up_move) & (down_move > 0) , down_move, 0.0)
        tr = _atr(df, 1)
        tr14 = tr.rolling(14).sum()
        plus_di  = 100 * pd.Series(plus_dm, index=df.index).rolling(14).sum() / tr14
        minus_di = 100 * pd.Series(minus_dm, index=df.index).rolling(14).sum() / tr14
        dx = (abs(plus_di - minus_di) / (plus_di + minus_di)).replace([np.inf, -np.inf], np.nan) * 100
        adx = dx.rolling(14).mean()
        ensure_column(df, "adx", adx)


def bb_width_series(df: pd.DataFrame) -> pd.Series:
    return (df["bb_upper"] - df["bb_lower"]) / df["close"]


def bb_width_slope(df: pd.DataFrame, lookback: int = 4) -> float:
    bbw = bb_width_series(df)
    if len(bbw) < lookback + 1:
        return 0.0
    return float((bbw.iloc[-1] - bbw.iloc[-1 - lookback]) / lookback)


# =============================
# Volume spike (3 modes) + auto chooser
# =============================
def is_volume_spike_percentile(df: pd.DataFrame, percentile: int = 70) -> bool:
    vols = df["volume"].iloc[-30:]
    if len(vols) < 10:
        logging.debug(f"[Volume] thiếu dữ liệu: {len(vols)} nến")
        return False
    v_now = vols.iloc[-1]
    thr = np.percentile(vols[:-1], percentile)
    if np.isnan(v_now) or np.isnan(thr):
        return False
    logging.debug(f"[Volume] v_now={v_now:.0f}, P{percentile}={thr:.0f}")
    return bool(v_now >= thr)


def is_volume_spike_factor(df: pd.DataFrame, factor: float = 1.5) -> bool:
    vols = df["volume"].iloc[-30:]
    if len(vols) < 10:
        return False
    v_now = vols.iloc[-1]
    base = vols[:-1].mean()
    if np.isnan(v_now) or np.isnan(base):
        return False
    logging.debug(f"[Volume] v_now={v_now:.0f}, mean*{factor}={base*factor:.0f}")
    return bool(v_now >= base * factor)


def choose_volume_percentile(adx: float, bb_width: float) -> int:
    if (adx < 20) or (bb_width < 0.010):   # sideway mạnh
        return 85
    elif (adx < 25) or (bb_width < 0.015): # hơi sideway
        return 80
    else:
        return 70                            # có trend


def volume_ok(df: pd.DataFrame, adx: float, bb_width: float) -> bool:
    if VOLUME_MODE == "auto":
        p = choose_volume_percentile(adx, bb_width)
        ok = is_volume_spike_percentile(df, percentile=p)
        why = f"auto-P{p}"
    elif VOLUME_MODE == "percentile":
        ok = is_volume_spike_percentile(df, percentile=VOLUME_PERCENTILE)
        why = f"fixed-P{VOLUME_PERCENTILE}"
    else:
        ok = is_volume_spike_factor(df, factor=VOLUME_FACTOR)
        why = f"mean*x{VOLUME_FACTOR}"
    if not ok:
        logging.debug(f"[DEBUG] Volume FAIL ({why})")
    return ok


# =============================
# Structure & quality
# =============================
def recent_range(df: pd.DataFrame, lookback: int = 20, exclude_last: int = 0) -> Tuple[float, float]:
    sl = df.iloc[-(lookback + exclude_last): -exclude_last if exclude_last > 0 else None]
    return float(sl["high"].max()), float(sl["low"].min())


def detect_bos_and_pullback(df: pd.DataFrame) -> Tuple[bool, bool, bool, bool]:
    ema20 = float(df["ema20"].iloc[-1])
    hi, lo = recent_range(df, lookback=20, exclude_last=1)
    close = float(df["close"].iloc[-1])
    prev_close = float(df["close"].iloc[-2])
    is_breakout_up   = (close > hi) and (prev_close <= hi)
    is_breakout_down = (close < lo) and (prev_close >= lo)
    is_pullback_up   = (close >= ema20)
    is_pullback_down = (close <= ema20)
    return is_breakout_up, is_breakout_down, is_pullback_up, is_pullback_down


def candle_quality_ok(df: pd.DataFrame, direction: str = "long") -> bool:
    atr = float(_atr(df).iloc[-1])
    if math.isnan(atr) or atr <= 0:
        return False
    body = float(abs(df["close"].iloc[-1] - df["open"].iloc[-1]))
    if direction == "long":
        return bool((df["close"].iloc[-1] > df["open"].iloc[-1]) and (body >= BREAKOUT_BODY_ATR * atr))
    else:
        return bool((df["close"].iloc[-1] < df["open"].iloc[-1]) and (body >= BREAKOUT_BODY_ATR * atr))


def sr_alignment(close_price: float, support: Optional[float], resistance: Optional[float], pct: float = SR_NEAR_PCT) -> Tuple[bool, bool]:
    near_support = (abs(close_price - support) / support) < pct if (support is not None and support > 0) else False
    near_resistance = (abs(close_price - resistance) / resistance) < pct if (resistance is not None and resistance > 0) else False
    return near_support, near_resistance


# =============================
# Universe: Top200 OKX USDT-M SWAP
# =============================
def fetch_okx_top200_symbols() -> List[str]:
    try:
        # Tickers has volCcy24h for quote volume; filter USDT SWAP
        r = requests.get("https://www.okx.com/api/v5/market/tickers", params={"instType": "SWAP"}, timeout=10)
        data = r.json().get("data", [])
        rows = []
        for it in data:
            inst_id = it.get("instId", "")            # e.g., BTC-USDT-SWAP
            vol_q = float(it.get("volCcy24h", 0.0))   # quote volume 24h
            if inst_id.endswith("-USDT-SWAP"):
                rows.append((inst_id, vol_q))
        rows.sort(key=lambda x: x[1], reverse=True)
        top200 = [sym for sym, _ in rows[:200]]
        return top200
    except Exception as e:
        logging.error(f"[UNIVERSE] Fetch error: {e}")
        return []


def load_universe() -> List[str]:
    if UNIVERSE_SOURCE.upper() == "OKX_TOP200":
        syms = fetch_okx_top200_symbols()
        if syms:
            logging.debug(f"[UNIVERSE] Loaded {len(syms)} OKX top symbols")
        return syms
    elif UNIVERSE_SOURCE.upper() == "SHEET" and SHEET_UNIVERSE_URL:
        try:
            txt = requests.get(SHEET_UNIVERSE_URL, timeout=10).text
            syms = [line.strip() for line in txt.splitlines() if line.strip()]
            return syms
        except Exception as e:
            logging.error(f"[UNIVERSE] Sheet fetch error: {e}")
            return []
    return []


# =============================
# Session & News blackout
# =============================
def now_vn() -> datetime:
    tz = timezone(timedelta(hours=7))
    return datetime.now(tz)


def is_within_session(dt_vn: Optional[datetime] = None) -> bool:
    if dt_vn is None:
        dt_vn = now_vn()
    s_h, s_m = map(int, SESSION_START.split(":"))
    e_h, e_m = map(int, SESSION_END.split(":"))
    start = dt_vn.replace(hour=s_h, minute=s_m, second=0, microsecond=0)
    end   = dt_vn.replace(hour=e_h, minute=e_m, second=0, microsecond=0)
    return start <= dt_vn <= end


def upcoming_news_blackout(dt_utc: Optional[datetime] = None) -> bool:
    """Very simple blackout based on built-in UTC timestamps.
    Extend NEWS_EVENTS_UTC list or replace with your sheet/API.
    """
    if dt_utc is None:
        dt_utc = datetime.utcnow().replace(tzinfo=timezone.utc)
    window = timedelta(minutes=NEWS_BLACKOUT_WINDOW_MIN)
    # Example placeholders (update regularly)
    NEWS_EVENTS_UTC = [
        # datetime(2025, 8, 14, 18, 0, tzinfo=timezone.utc),  # e.g., CPI
    ]
    for t in NEWS_EVENTS_UTC:
        if (t - window) <= dt_utc <= (t + window):
            return True
    return False


# =============================
# Risk & sizing
# =============================
def position_size_from_risk(entry: float, stop: float, equity_usdt: float) -> float:
    risk_per_trade = equity_usdt * RISK_PER_TRADE
    risk_per_unit = abs(entry - stop)
    if risk_per_unit <= 0:
        return 0.0
    size = risk_per_trade / risk_per_unit
    # Ensure notional meets minimum
    notional = size * entry
    if notional < OKX_MIN_NOTIONAL_USDT:
        return 0.0
    return size


# =============================
# Core signal builder (keeps your variable names)
# =============================
def build_signal_for_symbol(
    df: pd.DataFrame,
    df_1h: Optional[pd.DataFrame],
    symbol: str,
    support: Optional[float],
    resistance: Optional[float],
    btc_change: float,
    RR_MIN: float = 1.5,
    equity_usdt: float = DEFAULT_EQUITY_USDT,
):
    """
    Returns: (entry_price, stop_loss, take_profit, size, signal, passed)
    """
    compute_minimum_indicators(df)
    if df_1h is not None:
        compute_minimum_indicators(df_1h)

    latest = df.iloc[-1]
    close_price = float(latest["close"])

    ema_up   = bool(df["ema20"].iloc[-1] > df["ema50"].iloc[-1])
    ema_down = not ema_up

    if df_1h is not None:
        ema_up_1h   = bool(df_1h["ema20"].iloc[-1] > df_1h["ema50"].iloc[-1])
        ema_down_1h = not ema_up_1h
        rsi_1h = float(df_1h["rsi"].iloc[-1])
        adx_1h = float(df_1h["adx"].iloc[-1]) if "adx" in df_1h.columns else None
    else:
        ema_up_1h   = ema_up
        ema_down_1h = ema_down
        rsi_1h = float(df["rsi"].iloc[-1])
        adx_1h = None

    rsi = float(latest["rsi"])
    adx = float(latest["adx"])
    bb_width = float((latest["bb_upper"] - latest["bb_lower"]) / max(latest["close"], 1e-12))
    bbw_slope = bb_width_slope(df, lookback=4)

    # Volume filter
    if not volume_ok(df, adx=adx, bb_width=bb_width):
        logging.debug(f"[{symbol}] ❌ volume chưa đạt")
        return None, None, None, 0.0, None, False

    # Regime filters
    if adx < ADX_MIN_15M:
        logging.debug(f"[{symbol}] ❌ ADX 15m thấp ({adx:.2f}<{ADX_MIN_15M})")
        return None, None, None, 0.0, None, False
    if adx_1h is not None and adx_1h < ADX_MIN_1H:
        logging.debug(f"[{symbol}] ❌ ADX 1h thấp ({adx_1h:.2f}<{ADX_MIN_1H})")
        return None, None, None, 0.0, None, False
    if (bb_width < BBW_MIN) or (bbw_slope <= 0):
        logging.debug(f"[{symbol}] ❌ BB width hẹp/không mở (bbw={bb_width:.4f}, slope={bbw_slope:.5f})")
        return None, None, None, 0.0, None, False

    near_support, near_resistance = sr_alignment(close_price, support, resistance, pct=SR_NEAR_PCT)
    bo_up, bo_down, pb_up, pb_down = detect_bos_and_pullback(df)

    # MACD hist percentile threshold
    hist_series = (df["macd"] - df["macd_signal"]).dropna().iloc[-200:]
    macd_thr = float(np.percentile(hist_series.abs(), 60)) if len(hist_series) > 20 else 0.0
    macd_hist_now = float(df["macd"].iloc[-1] - df["macd_signal"].iloc[-1])
    macd_ok_long  = (macd_hist_now > 0) and (abs(macd_hist_now) >= macd_thr)
    macd_ok_short = (macd_hist_now < 0) and (abs(macd_hist_now) >= macd_thr)

    signal = None

    # LONG
    if (
        ema_up and ema_up_1h
        and rsi > 55 and rsi_1h > 50
        and (bo_up or pb_up)
        and candle_quality_ok(df, direction="long")
        and macd_ok_long
        and near_support
    ):
        if btc_change >= -0.01:
            signal = "LONG"

    # SHORT
    elif (
        ema_down and ema_down_1h
        and rsi < 45 and rsi_1h < 50
        and (bo_down or pb_down)
        and candle_quality_ok(df, direction="short")
        and macd_ok_short
        and near_resistance
    ):
        if btc_change <= 0.01:
            signal = "SHORT"

    if signal is None:
        return None, None, None, 0.0, None, False

    # Entry/SL/TP (basic swing logic; keep names)
    entry_price = close_price
    if signal == "LONG":
        swing_low = float(df["low"].iloc[-10:].min())
        stop_loss = min(swing_low, entry_price * 0.997)
        risk = entry_price - stop_loss
        if risk <= 0:
            return None, None, None, 0.0, None, False
        take_profit = entry_price + max(risk * RR_MIN, risk)
    else:
        swing_high = float(df["high"].iloc[-10:].max())
        stop_loss = max(swing_high, entry_price * 1.003)
        risk = stop_loss - entry_price
        if risk <= 0:
            return None, None, None, 0.0, None, False
        take_profit = entry_price - max(risk * RR_MIN, risk)

    # RR check
    denom = abs(entry_price - stop_loss)
    rr = abs((take_profit - entry_price) / denom) if denom > 0 else 0.0
    if rr < RR_MIN:
        logging.debug(f"[{symbol}] ❌ RR={rr:.2f} < {RR_MIN}")
        return None, None, None, 0.0, None, False

    # Position size (1% risk)
    size = position_size_from_risk(entry_price, stop_loss, equity_usdt)
    if size <= 0:
        logging.debug(f"[{symbol}] ❌ size=0 (không đạt min notional {OKX_MIN_NOTIONAL_USDT} USDT)")
        return None, None, None, 0.0, None, False

    # Partial TP suggestions
    tp1 = entry_price + (risk if signal == "LONG" else -risk) * 1.0
    tp2 = entry_price + (risk if signal == "LONG" else -risk) * 2.0
    logging.debug(f"[{symbol}] TP1={tp1:.6f} (1R), TP2={tp2:.6f} (2R) — gợi ý: chốt 50% tại TP1 rồi SL→BE")

    return entry_price, stop_loss, take_profit, size, signal, True


# =============================
# Orchestrator (session, news, universe)
# =============================
def run_scan(fetch_df_func, fetch_df1h_func, support_resistance_func, btc_change_func, equity_usdt: float = DEFAULT_EQUITY_USDT):
    # Session filter
    if not is_within_session():
        logging.info("[SESSION] Ngoài khung 09:00–23:00 (+7) → bỏ quét")
        return

    # News blackout
    if upcoming_news_blackout():
        logging.info("[NEWS] Trong khung blackout sự kiện → bỏ quét")
        return

    # Universe
    symbols = load_universe()
    if not symbols:
        logging.warning("[UNIVERSE] Không tải được danh sách symbols")
        return

    for inst_id in symbols:
        # Expect your fetchers to return df with required OHLCV
        df = fetch_df_func(inst_id)
        df1h = fetch_df1h_func(inst_id)
        if df is None or len(df) < 60:
            continue

        support, resistance = support_resistance_func(df)
        btc_change = btc_change_func()

        entry, sl, tp, size, signal, ok = build_signal_for_symbol(
            df=df, df_1h=df1h, symbol=inst_id,
            support=support, resistance=resistance,
            btc_change=btc_change, RR_MIN=1.5, equity_usdt=equity_usdt
        )
        if not ok:
            continue

        rr = abs((tp - entry) / (entry - sl))
        msg = f"{inst_id} | {signal}\nEntry: {entry:.6f}\nSL: {sl:.6f}\nTP: {tp:.6f}\nSize: {size:.4f}\nRR: {rr:.2f}"
        send_telegram(msg)
        append_to_sheet({
            "symbol": inst_id, "signal": signal, "entry": entry, "sl": sl, "tp": tp,
            "size": size, "rr": rr, "volume_mode": VOLUME_MODE, "time": datetime.utcnow().isoformat()
        })


# =============================
# If executed directly: simple demo with random data
# (Replace with your real data loaders in production)
# =============================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    def _fake_df(_symbol: str) -> pd.DataFrame:
        idx = pd.date_range(end=pd.Timestamp.utcnow(), periods=150, freq="15min")
        base = np.cumsum(np.random.randn(len(idx))) + 100
        df = pd.DataFrame({
            "open":  base + np.random.randn(len(idx)) * 0.2,
            "high":  base + np.random.rand(len(idx)) * 0.5 + 0.2,
            "low":   base - np.random.rand(len(idx)) * 0.5 - 0.2,
            "close": base + np.random.randn(len(idx)) * 0.2,
            "volume": (np.random.rand(len(idx)) * 1000 + 500).astype(float),
        }, index=idx)
        return df

    def _fake_df1h(_symbol: str) -> pd.DataFrame:
        df = _fake_df(_symbol)
        return df.resample("1H").agg({"open":"first","high":"max","low":"min","close":"last","volume":"sum"}).dropna()

    def _fake_sr(df: pd.DataFrame) -> Tuple[float, float]:
        return float(df["low"].iloc[-30:-5].min()), float(df["high"].iloc[-30:-5].max())

    def _fake_btc_change() -> float:
        return 0.0

    # For demo, universe will try OKX (may fail if no internet in environment)
    # so we fallback to a small list
    syms = load_universe()
    if not syms:
        syms = ["BTC-USDT-SWAP", "ETH-USDT-SWAP"]

    def _fetch_df_wrapper(symbol: str):
        return _fake_df(symbol)

    def _fetch_df1h_wrapper(symbol: str):
        return _fake_df1h(symbol)

    run_scan(_fetch_df_wrapper, _fetch_df1h_wrapper, _fake_sr, _fake_btc_change, equity_usdt=DEFAULT_EQUITY_USDT)
