#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bot Binance Futures:
1) Lọc PUMP / DUMP (5 logic).
2) Ghi tín hiệu vào Google Sheet (Coin / Tín hiệu / Entry / SL / TP / Ngày).
3) Chỉ trade các dòng mới vừa ghi (mỗi lệnh 10 USDT, x5).
4) Đặt TP/SL dạng closePosition + reduceOnly (không mở lệnh ngược).
5) Gửi Telegram: coin // long/short // Entry/TP/SL.

Thiết kế để chạy bằng cron (ví dụ: mỗi 30 phút) trên Render.
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

# ============================================================
# CONFIG (CHỈNH LẠI TRƯỚC KHI CHẠY)
# ============================================================

# Binance API (Futures REAL)
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "YOUR_BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "YOUR_BINANCE_API_SECRET")

# Google Sheets
GOOGLE_SPREADSHEET_ID = os.getenv("GOOGLE_SPREADSHEET_ID", "YOUR_SHEET_ID")
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
GOOGLE_WORKSHEET_NAME = os.getenv("GOOGLE_WORKSHEET_NAME", "Signals")

# Telegram
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "YOUR_TELEGRAM_CHAT_ID")

# Tham số logic lọc & trade
QUOTE = "USDT"
INTERVAL = "15m"
KLINE_LIMIT = 100

TOP_N_FOR_TA = 40          # số coin top biến động đem đi phân tích
MIN_SCORE_FOR_TRADE = 3    # điểm tối thiểu để vào lệnh (0–5)
MAX_TRADES_PER_RUN = 4     # tối đa số lệnh mỗi lần cron

BASE_MARGIN_USDT = 10      # mỗi lệnh dùng 10 USDT margin
LEVERAGE = 5               # đòn bẩy 5x
TP_PCT = 0.01              # TP 2%
SL_PCT = 0.005             # SL 1%

SHEET_TTL_HOURS = 24       # dữ liệu trên Sheet chỉ giữ 24h

# múi giờ hiển thị trong cột "Ngày" (UTC+7)
LOCAL_TZ = timezone(timedelta(hours=7))

BINANCE_BASE = "https://api.binance.com"

# ============================================================
# TELEGRAM
# ============================================================

def notify_telegram(text: str) -> None:
    if (not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID
            or "YOUR_TELEGRAM" in TELEGRAM_BOT_TOKEN):
        print("[WARN] Telegram chưa cấu hình, bỏ qua gửi.")
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


# ============================================================
# GOOGLE SHEETS
# ============================================================

def get_gs_client():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/drive.file",
    ]

    json_str = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not json_str:
        raise Exception("GOOGLE_SERVICE_ACCOUNT_JSON is not set")

    # parse JSON từ env
    json_str = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    import json
    info = json.loads(json_str)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(info, scope)

    client = gspread.authorize(creds)
    return client



def prepare_sheet_and_cleanup():
    """
    - Mở (hoặc tạo) worksheet.
    - Đảm bảo header: Coin / Tín hiệu / Entry / SL / TP / Ngày.
    - Xoá các dòng có Ngày > 24h.
    - Trả về:
        ws: worksheet
        existing_signals: set((Coin, Tín hiệu)) còn trong 24h (để tránh trade trùng).
    """
    client = get_gs_client()
    sh = client.open_by_key(GOOGLE_SPREADSHEET_ID)

    try:
        ws = sh.worksheet(GOOGLE_WORKSHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=GOOGLE_WORKSHEET_NAME, rows="2000", cols="20")
        ws.append_row(["Coin", "Tín hiệu", "Entry", "SL", "TP", "Ngày"])
        return ws, set()

    values = ws.get_all_values()
    if not values:
        ws.append_row(["Coin", "Tín hiệu", "Entry", "SL", "TP", "Ngày"])
        return ws, set()

    header = values[0]
    rows = values[1:]

    # Nếu header sai thì reset
    expected_header = ["Coin", "Tín hiệu", "Entry", "SL", "TP", "Ngày"]
    if header != expected_header:
        ws.clear()
        ws.append_row(expected_header)
        return ws, set()

    now_local = datetime.now(LOCAL_TZ)
    cutoff = now_local - timedelta(hours=SHEET_TTL_HOURS)

    kept_rows = []
    existing_signals = set()

    for r in rows:
        if len(r) < 6:
            continue
        date_str = r[5]
        try:
            dt = datetime.strptime(date_str, "%d/%m/%Y %H:%M")
            # dt được xem theo LOCAL_TZ
            dt = dt.replace(tzinfo=LOCAL_TZ)
        except Exception:
            continue

        if dt >= cutoff:
            kept_rows.append(r)
            existing_signals.add((r[0], r[1]))  # (Coin, Tín hiệu)

    ws.clear()
    ws.append_row(expected_header)
    if kept_rows:
        ws.append_rows(kept_rows)

    return ws, existing_signals


def append_trades_to_sheet(ws, planned_trades):
    """
    planned_trades: list dict gồm:
       Coin, Tín hiệu, Entry, SL, TP, Ngày
    """
    if not planned_trades:
        return

    rows_to_append = []
    for t in planned_trades:
        rows_to_append.append([
            t["Coin"],
            t["Tín hiệu"],
            f'{t["Entry"]:.6f}',
            f'{t["SL"]:.6f}',
            f'{t["TP"]:.6f}',
            t["Ngày"],
        ])

    ws.append_rows(rows_to_append)


def get_trades_for_timestamp(ws, date_str):
    """
    Đọc lại sheet và lấy các dòng có Ngày == date_str
    (tức là các lệnh vừa được tạo trong lần cron này).
    """
    records = ws.get_all_records()
    new_trades = []
    for rec in records:
        if rec.get("Ngày") == date_str:
            try:
                new_trades.append({
                    "Coin": rec["Coin"].strip(),
                    "Tín hiệu": rec["Tín hiệu"].strip().upper(),
                    "Entry": float(rec["Entry"]),
                    "SL": float(rec["SL"]),
                    "TP": float(rec["TP"]),
                    "Ngày": rec["Ngày"],
                })
            except Exception as e:
                print(f"[WARN] Không parse được dòng mới: {rec} -> {e}")
                continue
    return new_trades


# ============================================================
# BINANCE – client & helpers
# ============================================================

binance_client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)


def get_futures_symbols_usdt() -> set:
    info = binance_client.futures_exchange_info()
    symbols = set()
    for s in info["symbols"]:
        if s.get("contractType") == "PERPETUAL" and s.get("quoteAsset") == "USDT":
            symbols.add(s["symbol"])
    return symbols


def get_futures_symbol_filters() -> dict:
    """
    mapping: {symbol: {'minQty':..., 'stepSize':..., 'minNotional':...}}
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
            "minNotional": float(min_notional["notional"]),
        }
    return filters_map


def round_qty(qty: float, step_size: float) -> float:
    if step_size <= 0:
        return qty
    return math.floor(qty / step_size) * step_size


# ============================================================
# TA – RSI & scoring
# ============================================================

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
    Chấm điểm pump/dump (0–5) cho 5 logic:
      1) Biến động 24h (change_pct)
      2) Biến động 1h (từ 4 nến 15m)
      3) Volume spike so với 20 nến trước
      4) Breakout high/low 20 nến
      5) RSI cực trị

    Trả về dict:
      symbol, change_pct, abs_change, last_price, pump_score, dump_score
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

    # Volume spike vs trung bình 20 nến
    vol_avg_20 = np.mean(prev_vols[-20:]) if len(prev_vols) >= 20 else np.mean(prev_vols)
    vol_spike_ratio = last_vol / vol_avg_20 if vol_avg_20 > 0 else 1.0

    highest_20 = max(prev_closes[-20:])
    lowest_20 = min(prev_closes[-20:])

    rsi = compute_rsi(closes, period=14)

    # ---- PUMP SCORE ----
    pump_score = 0
    if change_pct > 0:
        if change_pct >= 3:        # 1) 24h tăng mạnh
            pump_score += 1
        if change_1h >= 1:         # 2) 1h vừa qua tăng
            pump_score += 1
        if vol_spike_ratio >= 2:   # 3) volume gấp >=2 lần
            pump_score += 1
        if last_close >= highest_20:  # 4) breakout high 20 nến
            pump_score += 1
        if rsi >= 55:              # 5) RSI cao
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


# ============================================================
# SCAN & PLAN TRADES
# ============================================================

def scan_market_and_signals():
    """
    1) Lấy 24h tickers.
    2) Lọc USDT + có Futures.
    3) Chọn TOP_N_FOR_TA theo abs_change.
    4) Chấm điểm PUMP/DUMP.
    5) Trả về DataFrame: symbol, direction, score, change_pct, abs_change, last_price.
    """
    url = f"{BINANCE_BASE}/api/v3/ticker/24hr"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    tickers = resp.json()

    futures_symbols = get_futures_symbols_usdt()

    tmp = []
    for t in tickers:
        symbol = t["symbol"]
        if not symbol.endswith(QUOTE):
            continue
        if symbol not in futures_symbols:
            continue
        change_pct = float(t["priceChangePercent"])
        tmp.append({
            "symbol": symbol,
            "ticker": t,
            "abs_change": abs(change_pct),
        })

    tmp_sorted = sorted(tmp, key=lambda x: x["abs_change"], reverse=True)[:TOP_N_FOR_TA]

    scored = []
    print(f"[INFO] Đang chấm điểm {len(tmp_sorted)} symbol...")
    for rec in tmp_sorted:
        s = score_symbol(rec["symbol"], rec["ticker"])
        scored.append(s)

    rows = []
    for s in scored:
        pump_score = s["pump_score"]
        dump_score = s["dump_score"]
        if pump_score == 0 and dump_score == 0:
            continue

        if pump_score >= dump_score:
            direction = "LONG"
            score = pump_score
        else:
            direction = "SHORT"
            score = dump_score

        rows.append({
            "symbol": s["symbol"],
            "direction": direction,
            "score": score,
            "change_pct": round(s["change_pct"], 2),
            "abs_change": round(s["abs_change"], 2),
            "last_price": s["last_price"],
        })

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df = df.sort_values(by=["score", "abs_change"], ascending=[False, False]).reset_index(drop=True)
    return df


def plan_trades_from_signals(df_signals, existing_signals):
    """
    Từ df_signals => chọn các lệnh sẽ trade:
      - score >= MIN_SCORE_FOR_TRADE
      - (Coin, Tín hiệu) chưa từng xuất hiện trong 24h (dựa trên sheet)
      - tối đa MAX_TRADES_PER_RUN

    Trả về:
      planned_trades: list dict (Coin, Tín hiệu, Entry, SL, TP, Ngày, score, change_pct)
      date_str: chuỗi Ngày (dd/MM/yyyy HH:mm)
    """
    if df_signals.empty:
        return [], None

    now_local = datetime.now(LOCAL_TZ)
    date_str = now_local.strftime("%d/%m/%Y %H:%M")

    candidates = df_signals[df_signals["score"] >= MIN_SCORE_FOR_TRADE].copy()
    if candidates.empty:
        return [], date_str

    # bỏ các lệnh đã có trong sheet (24h)
    filtered = []
    for _, row in candidates.iterrows():
        coin = row["symbol"]
        direction = row["direction"]
        if (coin, direction) in existing_signals:
            continue
        filtered.append(row)

    if not filtered:
        return [], date_str

    df = pd.DataFrame(filtered)
    df = df.sort_values(by=["score", "abs_change"], ascending=[False, False])
    df = df.head(MAX_TRADES_PER_RUN)

    planned = []
    for _, row in df.iterrows():
        symbol = row["symbol"]
        direction = row["direction"]
        price = float(row["last_price"])

        if direction == "LONG":
            tp = price * (1 + TP_PCT)
            sl = price * (1 - SL_PCT)
        else:  # SHORT
            tp = price * (1 - TP_PCT)
            sl = price * (1 + SL_PCT)

        planned.append({
            "Coin": symbol,
            "Tín hiệu": direction,
            "Entry": price,
            "SL": sl,
            "TP": tp,
            "Ngày": date_str,
            "score": int(row["score"]),
            "change_pct": float(row["change_pct"]),
        })

    return planned, date_str


# ============================================================
# TRADE – vào lệnh & TP/SL
# ============================================================

def ensure_leverage(symbol: str, leverage: int):
    try:
        binance_client.futures_change_leverage(symbol=symbol, leverage=leverage)
    except Exception as e:
        print(f"[WARN] Không set được leverage {symbol}: {e}")


def open_futures_trade(symbol: str, direction: str, filters_map: dict):
    """
    Mỗi lệnh:
      margin = 10 USDT
      leverage = 5x  => notional ~ 50 USDT
    direction: LONG -> BUY, SHORT -> SELL
    """
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
    if qty <= 0 or qty < min_qty or qty * price < min_notional:
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
        "side": side,
    }


def set_tp_sl_for_trade(symbol: str, side: str, tp_price: float, sl_price: float):
    """
    Đặt TP/SL:
      LONG  -> TP/SL bán (SELL)
      SHORT -> TP/SL mua (BUY)

    Dùng closePosition=True + reduceOnly=True để:
      - Khi TP khớp thì SL tự vô hiệu (vì không còn position).
      - Khi SL khớp thì TP cũng không còn hiệu lực.
    """
    if side == SIDE_BUY:
        tp_side = SIDE_SELL
        sl_side = SIDE_SELL
    else:
        tp_side = SIDE_BUY
        sl_side = SIDE_BUY

    tp_order = sl_order = None

    try:
        tp_order = binance_client.futures_create_order(
            symbol=symbol,
            side=tp_side,
            type="TAKE_PROFIT_MARKET",
            stopPrice=float(f"{tp_price:.6f}"),
            closePosition=True,
            reduceOnly=True,
        )
    except Exception as e:
        print(f"[WARN] Lỗi đặt TP {symbol}: {e}")

    try:
        sl_order = binance_client.futures_create_order(
            symbol=symbol,
            side=sl_side,
            type="STOP_MARKET",
            stopPrice=float(f"{sl_price:.6f}"),
            closePosition=True,
            reduceOnly=True,
        )
    except Exception as e:
        print(f"[WARN] Lỗi đặt SL {symbol}: {e}")

    return tp_order, sl_order


def execute_trades(new_trades):
    """
    new_trades: list dict đọc từ sheet (Coin, Tín hiệu, Entry, SL, TP, Ngày)
    """
    if not new_trades:
        print("[INFO] Không có lệnh mới trong sheet để vào.")
        return

    filters_map = get_futures_symbol_filters()

    for t in new_trades:
        symbol = t["Coin"]
        direction = t["Tín hiệu"]
        entry_planned = t["Entry"]
        sl_price = t["SL"]
        tp_price = t["TP"]

        try:
            trade_info = open_futures_trade(symbol, direction, filters_map)
            if not trade_info:
                continue

            entry_real = trade_info["entry_price"]
            qty = trade_info["qty"]
            side = trade_info["side"]

            set_tp_sl_for_trade(symbol, side, tp_price, sl_price)

            msg = (
                f"🚀 *NEW TRADE*\n"
                f"Coin: `{symbol}`\n"
                f"Tín hiệu: *{direction}*\n"
                f"Entry (sheet): `{entry_planned:.6f}`\n"
                f"Entry (real): `{entry_real:.6f}`\n"
                f"TP: `{tp_price:.6f}`\n"
                f"SL: `{sl_price:.6f}`\n"
                f"Số lượng: `{qty}` (margin ~{BASE_MARGIN_USDT} USDT, x{LEVERAGE})"
            )
            print(msg)
            notify_telegram(msg)

        except Exception as e:
            print(f"[ERROR] Lỗi vào lệnh {symbol}: {e}")
            notify_telegram(f"❌ Lỗi vào lệnh {symbol}: {e}")


# ============================================================
# MAIN FLOW
# ============================================================

def main():
    print("====== CRON RUN START ======")

    # 1) Chuẩn bị sheet & lấy các lệnh đã trade trong 24h để tránh trùng
    try:
        ws, existing_signals = prepare_sheet_and_cleanup()
    except Exception as e:
        print("[ERROR] Lỗi chuẩn bị Google Sheet:", e)
        notify_telegram(f"⚠️ Lỗi Google Sheet (prepare): {e}")
        return

    # 2) Scan market & chấm điểm
    df_signals = scan_market_and_signals()
    if df_signals.empty:
        print("[INFO] Không có tín hiệu PUMP/DUMP.")
        return

    print("[INFO] Top signals:")
    print(df_signals.head())

    # 3) Lên kế hoạch các lệnh sẽ trade
    planned_trades, date_str = plan_trades_from_signals(df_signals, existing_signals)
    if not planned_trades:
        print("[INFO] Không có lệnh nào đạt score / chưa trùng sheet.")
        return

    print("[INFO] Planned trades:")
    for t in planned_trades:
        print(
            f"{t['Coin']} - {t['Tín hiệu']} - "
            f"Entry={t['Entry']:.6f} TP={t['TP']:.6f} SL={t['SL']:.6f} "
            f"score={t['score']} 24h={t['change_pct']}%"
        )

    # 4) Ghi lệnh vào Google Sheet (append + đã cleanup ở bước 1)
    try:
        append_trades_to_sheet(ws, planned_trades)
        print("[INFO] Đã append lệnh mới vào Google Sheet.")
    except Exception as e:
        print("[ERROR] Lỗi append Google Sheet:", e)
        notify_telegram(f"⚠️ Lỗi Google Sheet (append): {e}")
        return

    # 5) Đọc lại sheet & lấy đúng các dòng mới (Ngày == date_str) để bot vào lệnh
    try:
        new_trades = get_trades_for_timestamp(ws, date_str)
    except Exception as e:
        print("[ERROR] Lỗi đọc sheet cho timestamp mới:", e)
        notify_telegram(f"⚠️ Lỗi Google Sheet (read new trades): {e}")
        return

    # 6) Vào lệnh Futures + đặt TP/SL + gửi Telegram
    execute_trades(new_trades)

    print("====== CRON RUN END ======")


if __name__ == "__main__":
    main()
