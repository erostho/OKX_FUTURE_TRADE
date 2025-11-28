#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bot lọc PUMP / DUMP Binance + ghi Google Sheet + vào lệnh Futures + TP/SL + Telegram.

Flow mỗi lần cron chạy:
1. Lọc coin PUMP/DUMP theo 5 logic điểm số (0-5).
2. Ghi thêm các tín hiệu mới vào Google Sheet (và dọn dữ liệu >24h).
3. Chọn 1 vài coin score cao để vào lệnh (10 USDT, x5).
4. Đặt TP/SL cho từng lệnh vừa mở.
5. Gửi Telegram thông tin lệnh.
"""

import os
import math
from datetime import datetime, timedelta, timezone

import requests
import pandas as pd
import numpy as np

from binance.client import Client
from binance.enums import SIDE_BUY, SIDE_SELL

import gspread
from oauth2client.service_account import ServiceAccountCredentials

# =========================
# CONFIG (CHỈNH LẠI TRƯỚC KHI CHẠY)
# =========================

# Binance API (Futures REAL, không phải testnet)
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "YOUR_BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "YOUR_BINANCE_API_SECRET")

# Google Sheets
GOOGLE_SPREADSHEET_ID = os.getenv("GOOGLE_SPREADSHEET_ID", "YOUR_SHEET_ID")
GOOGLE_SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "service_account.json")
GOOGLE_WORKSHEET_NAME = os.getenv("GOOGLE_WORKSHEET_NAME", "Signals")

# Telegram
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "YOUR_TELEGRAM_CHAT_ID")

# Logic lọc & trade
QUOTE = "USDT"
INTERVAL = "15m"
KLINE_LIMIT = 100

TOP_N_FOR_TA = 40          # Số coin top biến động đem đi phân tích kỹ
MIN_SCORE_FOR_TRADE = 3    # Điểm tối thiểu để được phép trade (0–5)
MAX_TRADES_PER_RUN = 4     # Mỗi lần cron chỉ vào tối đa bao nhiêu lệnh

BASE_MARGIN_USDT = 10      # Mỗi lệnh dùng 10 USDT margin
LEVERAGE = 5               # Đòn bẩy 5x
TP_PCT = 0.01              # 1% TP
SL_PCT = 0.005             # 0.5% SL

SHEET_TTL_HOURS = 24       # Dữ liệu trên Sheet chỉ giữ 24h


# =========================
# TELEGRAM
# =========================

def notify_telegram(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID or "YOUR_TELEGRAM" in TELEGRAM_BOT_TOKEN:
        print("[WARN] Telegram chưa cấu hình. Bỏ qua gửi.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "Markdown"
    }
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print("[ERROR] Gửi telegram lỗi:", e)


# =========================
# GOOGLE SHEETS
# =========================

def get_gs_client():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/drive.file",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_SERVICE_ACCOUNT_FILE, scope)
    client = gspread.authorize(creds)
    return client


def cleanup_and_append_to_sheet(df_signals: pd.DataFrame) -> None:
    """
    - Dọn dữ liệu cũ > 24h trong sheet.
    - Ghi THÊM các tín hiệu mới vào sheet (append).
    """
    client = get_gs_client()
    sh = client.open_by_key(GOOGLE_SPREADSHEET_ID)

    try:
        ws = sh.worksheet(GOOGLE_WORKSHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=GOOGLE_WORKSHEET_NAME, rows="2000", cols="20")
        # tạo header lần đầu
        ws.append_row(list(df_signals.columns))
        ws.append_rows(df_signals.astype(str).values.tolist())
        return

    # Lấy toàn bộ dữ liệu hiện tại
    values = ws.get_all_values()

    if not values:
        # Nếu sheet rỗng thì tạo header và append
        ws.append_row(list(df_signals.columns))
        ws.append_rows(df_signals.astype(str).values.tolist())
        return

    header = values[0]
    rows = values[1:]

    # Dọn dữ liệu > 24h (col "timestamp" là cột 0)
    now_utc = datetime.now(timezone.utc)
    cutoff = now_utc - timedelta(hours=SHEET_TTL_HOURS)

    filtered_rows = []
    for row in rows:
        try:
            ts_str = row[0]
            ts = datetime.fromisoformat(ts_str)
        except Exception:
            # nếu parse lỗi thì bỏ
            continue
        if ts >= cutoff:
            filtered_rows.append(row)

    # Ghi lại phần dữ liệu còn sống + append thêm df_signals mới
    new_values = [header] + filtered_rows + df_signals.astype(str).values.tolist()
    ws.clear()
    ws.update("A1", new_values)


# =========================
# BINANCE – client & helpers
# =========================

binance_client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)

BINANCE_BASE = "https://api.binance.com"


def get_futures_symbols_usdt() -> set:
    """Lấy danh sách symbol USDT-M Futures."""
    info = binance_client.futures_exchange_info()
    symbols = set()
    for s in info["symbols"]:
        if s.get("contractType") == "PERPETUAL" and s.get("quoteAsset") == "USDT":
            symbols.add(s["symbol"])
    return symbols


def get_futures_symbol_filters() -> dict:
    """
    Trả về dict: {symbol: {'minQty':..., 'stepSize':..., 'minNotional':...}}
    để tính quantity hợp lệ.
    """
    info = binance_client.futures_exchange_info()
    filters_map = {}
    for s in info["symbols"]:
        symbol = s["symbol"]
        lot_size = [f for f in s["filters"] if f["filterType"] == "LOT_SIZE"][0]
        min_notional = [f for f in s["filters"] if f["filterType"] == "MIN_NOTIONAL"][0]
        filters_map[symbol] = {
            "minQty": float(lot_size["minQty"]),
            "stepSize": float(lot_size["stepSize"]),
            "minNotional": float(min_notional["notional"])
        }
    return filters_map


def round_qty(qty: float, step_size: float) -> float:
    if step_size <= 0:
        return qty
    return math.floor(qty / step_size) * step_size


# =========================
# TA – RSI, score
# =========================

def compute_rsi(closes, period=14):
    if len(closes) < period + 1:
        return 50.0
    deltas = np.diff(closes)
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    avg_gain = np.mean(gains[-period:])
    avg_loss = np.mean(losses[-period:])
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return float(rsi)


def get_klines(symbol: str, interval: str = INTERVAL, limit: int = KLINE_LIMIT):
    url = f"{BINANCE_BASE}/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


def score_symbol(symbol: str, ticker: dict) -> dict:
    """
    Trả về dict:
    {
        'symbol': ...,
        'change_pct': ...,
        'abs_change': ...,
        'last_price': ...,
        'pump_score': 0-5,
        'dump_score': 0-5,
    }
    5 logic:
      1) Độ biến động 24h (change_pct)
      2) Biến động 1h gần nhất
      3) Volume spike
      4) Breakout high/low 20 nến
      5) RSI cực trị
    """
    change_pct = float(ticker["priceChangePercent"])
    last_price = float(ticker["lastPrice"])

    try:
        klines = get_klines(symbol)
    except Exception as e:
        print(f"[WARN] Lỗi get_klines {symbol}: {e}")
        return {
            "symbol": symbol,
            "change_pct": change_pct,
            "abs_change": abs(change_pct),
            "last_price": last_price,
            "pump_score": 0,
            "dump_score": 0,
        }

    closes = [float(k[4]) for k in klines]
    volumes = [float(k[5]) for k in klines]

    if len(closes) < 25:
        return {
            "symbol": symbol,
            "change_pct": change_pct,
            "abs_change": abs(change_pct),
            "last_price": last_price,
            "pump_score": 0,
            "dump_score": 0,
        }

    last_close = closes[-1]
    last_vol = volumes[-1]
    prev_closes = closes[:-1]
    prev_vols = volumes[:-1]

    # Biến động 1h gần nhất (với 15m candle: 4 nến)
    if len(closes) > 4:
        close_1h_ago = closes[-5]
        change_1h = (last_close / close_1h_ago - 1) * 100
    else:
        change_1h = 0.0

    # Volume spike vs trung bình 20 nến trước
    vol_avg_20 = np.mean(prev_vols[-20:]) if len(prev_vols) >= 20 else np.mean(prev_vols)
    vol_spike_ratio = last_vol / vol_avg_20 if vol_avg_20 > 0 else 1.0

    highest_20 = max(prev_closes[-20:])
    lowest_20 = min(prev_closes[-20:])

    rsi = compute_rsi(closes, period=14)

    # ---- PUMP SCORE ----
    pump_score = 0
    if change_pct > 0:
        # 1) Độ biến động 24h
        if change_pct >= 3:
            pump_score += 1
        # 2) Biến động 1h
        if change_1h >= 1:
            pump_score += 1
        # 3) Volume spike
        if vol_spike_ratio >= 2:
            pump_score += 1
        # 4) Breakout high 20 nến
        if last_close >= highest_20:
            pump_score += 1
        # 5) RSI cao
        if rsi >= 55:
            pump_score += 1

    # ---- DUMP SCORE ----
    dump_score = 0
    if change_pct < 0:
        if change_pct <= -3:
            dump_score += 1
        if change_1h <= -1:
            dump_score += 1
        if vol_spike_ratio >= 2:
            dump_score += 1
        if last_close <= lowest_20:
            dump_score += 1
        if rsi <= 45:
            dump_score += 1

    return {
        "symbol": symbol,
        "change_pct": change_pct,
        "abs_change": abs(change_pct),
        "last_price": last_price,
        "pump_score": pump_score,
        "dump_score": dump_score,
    }


# =========================
# SCAN & BUILD SIGNALS DF
# =========================

def scan_market_and_build_signals() -> pd.DataFrame:
    """
    1) Lấy 24h tickers tất cả coin.
    2) Lọc USDT + có Futures.
    3) Chọn TOP_N_FOR_TA abs biến động lớn nhất.
    4) Chấm điểm pump/dump (0–5).
    5) Tạo DataFrame với cột:
       timestamp, symbol, signal, direction, score, change_pct, abs_change, last_price, notes
    """
    # 24h tickers (spot)
    url = f"{BINANCE_BASE}/api/v3/ticker/24hr"
    resp = requests.get(url, timeout=10)
    tickers = resp.json()

    futures_symbols = get_futures_symbols_usdt()

    records = []
    for t in tickers:
        symbol = t["symbol"]
        if not symbol.endswith(QUOTE):
            continue
        if symbol not in futures_symbols:
            continue
        change_pct = float(t["priceChangePercent"])
        records.append({
            "symbol": symbol,
            "ticker": t,
            "abs_change": abs(change_pct)
        })

    # chọn top N theo độ biến động để giảm số lần gọi klines
    records_sorted = sorted(records, key=lambda x: x["abs_change"], reverse=True)[:TOP_N_FOR_TA]

    scored = []
    print(f"[INFO] Đang chấm điểm {len(records_sorted)} symbol...")
    for rec in records_sorted:
        score_info = score_symbol(rec["symbol"], rec["ticker"])
        scored.append(score_info)

    now_utc = datetime.now(timezone.utc).replace(microsecond=0)

    rows = []
    for s in scored:
        symbol = s["symbol"]
        change_pct = s["change_pct"]
        abs_change = s["abs_change"]
        last_price = s["last_price"]
        pump_score = s["pump_score"]
        dump_score = s["dump_score"]

        # quyết định signal và direction
        if pump_score == 0 and dump_score == 0:
            continue

        if pump_score >= dump_score:
            signal = "PUMP"
            direction = "LONG"
            score = pump_score
        else:
            signal = "DUMP"
            direction = "SHORT"
            score = dump_score

        row = {
            "timestamp": now_utc.isoformat(),
            "symbol": symbol,
            "signal": signal,
            "direction": direction,
            "score": score,
            "change_pct": round(change_pct, 2),
            "abs_change": round(abs_change, 2),
            "last_price": last_price,
            "notes": f"pump_score={pump_score},dump_score={dump_score}"
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    if df.empty:
        print("[INFO] Không có tín hiệu nào đủ điều kiện.")
        return df

    # sort theo score > abs_change
    df = df.sort_values(by=["score", "abs_change"], ascending=[False, False]).reset_index(drop=True)
    return df


# =========================
# TRADE – mở lệnh + TP/SL
# =========================

def ensure_leverage(symbol: str, leverage: int):
    try:
        binance_client.futures_change_leverage(symbol=symbol, leverage=leverage)
    except Exception as e:
        print(f"[WARN] Không set được leverage {symbol}: {e}")


def open_futures_trade(symbol: str, direction: str, filters_map: dict):
    """
    Mỗi lệnh:
      - margin = 10 USDT
      - leverage = 5x => position notional = 50 USDT
    direction: LONG -> BUY, SHORT -> SELL
    """
    # Lấy giá hiện tại Futures
    ticker = binance_client.futures_symbol_ticker(symbol=symbol)
    price = float(ticker["price"])

    position_notional = BASE_MARGIN_USDT * LEVERAGE
    raw_qty = position_notional / price

    filt = filters_map.get(symbol)
    if not filt:
        print(f"[WARN] Không tìm thấy filter cho {symbol}, bỏ qua.")
        return None

    step = filt["stepSize"]
    min_qty = filt["minQty"]
    min_notional = filt["minNotional"]

    qty = round_qty(raw_qty, step)
    if qty < min_qty or qty * price < min_notional:
        print(f"[WARN] Qty quá nhỏ {symbol}: qty={qty}, price={price}")
        return None

    side = SIDE_BUY if direction == "LONG" else SIDE_SELL

    ensure_leverage(symbol, LEVERAGE)

    order = binance_client.futures_create_order(
        symbol=symbol,
        side=side,
        type="MARKET",
        quantity=qty
    )

    entry_price = float(order["avgPrice"]) if order.get("avgPrice") not in (None, "0.0000") else price
    return {
        "order": order,
        "entry_price": entry_price,
        "qty": qty,
        "side": side
    }


def set_tp_sl_for_trade(symbol: str, side: str, entry_price: float, qty: float):
    """
    Đặt TP/SL:
      LONG:
        TP = entry * (1 + TP_PCT)
        SL = entry * (1 - SL_PCT)
      SHORT:
        TP = entry * (1 - TP_PCT)
        SL = entry * (1 + SL_PCT)

    Dùng closePosition=True + reduceOnly.
    Khi 1 lệnh khớp sẽ đóng toàn bộ vị thế, lệnh kia nếu kích hoạt sau sẽ bị reject,
    KHÔNG mở ngược chiều.
    """
    if side == SIDE_BUY:  # LONG
        tp_price = round(entry_price * (1 + TP_PCT), 4)
        sl_price = round(entry_price * (1 - SL_PCT), 4)
        tp_side = SIDE_SELL
        sl_side = SIDE_SELL
    else:  # SHORT
        tp_price = round(entry_price * (1 - TP_PCT), 4)
        sl_price = round(entry_price * (1 + SL_PCT), 4)
        tp_side = SIDE_BUY
        sl_side = SIDE_BUY

    # TP
    try:
        tp_order = binance_client.futures_create_order(
            symbol=symbol,
            side=tp_side,
            type="TAKE_PROFIT_MARKET",
            stopPrice=tp_price,
            closePosition=True,
            reduceOnly=True
        )
    except Exception as e:
        print(f"[WARN] Lỗi đặt TP {symbol}: {e}")
        tp_order = None

    # SL
    try:
        sl_order = binance_client.futures_create_order(
            symbol=symbol,
            side=sl_side,
            type="STOP_MARKET",
            stopPrice=sl_price,
            closePosition=True,
            reduceOnly=True
        )
    except Exception as e:
        print(f"[WARN] Lỗi đặt SL {symbol}: {e}")
        sl_order = None

    return tp_order, sl_order, tp_price, sl_price


# =========================
# MAIN FLOW
# =========================

def main():
    print("====== CRON RUN START ======")
    # 1) Scan & build signals
    df_signals = scan_market_and_build_signals()
    if df_signals.empty:
        print("[INFO] Không có tín hiệu nào. Kết thúc.")
        return

    print(df_signals.head())

    # 2) Ghi thêm vào Google Sheet & dọn dữ liệu cũ > 24h
    try:
        cleanup_and_append_to_sheet(df_signals)
        print("[INFO] Đã cập nhật Google Sheet.")
    except Exception as e:
        print("[ERROR] Lỗi Google Sheet:", e)
        notify_telegram(f"⚠️ Lỗi Google Sheet: {e}")

    # 3) Lọc những tín hiệu đủ score để trade
    df_trades = df_signals[df_signals["score"] >= MIN_SCORE_FOR_TRADE].copy()
    if df_trades.empty:
        print("[INFO] Không có tín hiệu đủ score để vào lệnh.")
        return

    df_trades = df_trades.head(MAX_TRADES_PER_RUN)
    print("[INFO] Sẽ vào lệnh cho:")
    print(df_trades[["symbol", "direction", "score", "change_pct"]])

    filters_map = get_futures_symbol_filters()

    for _, row in df_trades.iterrows():
        symbol = row["symbol"]
        direction = row["direction"]
        score = row["score"]
        change_pct = row["change_pct"]

        try:
            trade_info = open_futures_trade(symbol, direction, filters_map)
            if not trade_info:
                continue

            entry_price = trade_info["entry_price"]
            qty = trade_info["qty"]
            side = trade_info["side"]

            tp_order, sl_order, tp_price, sl_price = set_tp_sl_for_trade(
                symbol, side, entry_price, qty
            )

            msg = (
                f"🚀 *NEW TRADE*\n"
                f"Symbol: `{symbol}`\n"
                f"Direction: *{direction}*\n"
                f"Score: `{score}` | 24h: `{change_pct}%`\n"
                f"Entry: `{entry_price}`\n"
                f"TP: `{tp_price}`\n"
                f"SL: `{sl_price}`\n"
                f"Qty: `{qty}` (margin ~{BASE_MARGIN_USDT} USDT, x{LEVERAGE})"
            )
            print(msg)
            notify_telegram(msg)

        except Exception as e:
            print(f"[ERROR] Lỗi vào lệnh {symbol}: {e}")
            notify_telegram(f"❌ Lỗi vào lệnh {symbol}: {e}")

    print("====== CRON RUN END ======")


if __name__ == "__main__":
    main()
