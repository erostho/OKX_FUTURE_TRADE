#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
OKX PUMP/DUMP BOT (SPOT)

- Quét SPOT trên OKX (instType=SPOT) tìm coin PUMP/DUMP.
- 5 logic cho điểm: biến động 24h, 1h, volume spike, breakout, RSI.
- Xuất Google Sheet định dạng:
    Coin | Tín hiệu | Entry | SL | TP | Ngày
- Dữ liệu sheet auto xoá > 24h.
- Chỉ trade các lệnh mới tạo trong lần cron hiện tại.
- Trade SPOT trên OKX:
    + LONG  -> BUY market ~10 USDT
    + SHORT -> SELL market ~10 USDT (nếu có đủ coin)
- TP/SL hiện tại:
    + Tính & ghi vào Sheet + Telegram (logic tham chiếu),
    + Chưa đặt TP/SL tự động trên OKX (có thể nâng cấp sau bằng order-algo).
- Hỗ trợ chạy DEMO (simulated) bằng header x-simulated-trading.

Khuyến nghị:
- Test với OKX Demo (OKX_SIMULATED_TRADING=1) trước,
  sau đó chuyển sang real nếu bạn đã hiểu rõ hành vi.
"""

import os
import math
import time
import hmac
import base64
import json
from datetime import datetime, timedelta, timezone

import requests
import pandas as pd
import numpy as np

import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ============================================================
# CONFIG
# ============================================================

OKX_API_KEY = os.getenv("OKX_API_KEY", "")
OKX_API_SECRET = os.getenv("OKX_API_SECRET", "")
OKX_API_PASSPHRASE = os.getenv("OKX_API_PASSPHRASE", "")
OKX_SIMULATED_TRADING = os.getenv("OKX_SIMULATED_TRADING", "1")  # "1" demo, "0" real

OKX_BASE_URL = "https://www.okx.com"

# Google Sheets
GOOGLE_SPREADSHEET_ID = os.getenv("GOOGLE_SPREADSHEET_ID")
GOOGLE_WORKSHEET_NAME = os.getenv("GOOGLE_WORKSHEET_NAME", "OKX_BOT")

# Telegram
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Scan settings
QUOTE = "USDT"
INTERVAL = "15m"
KLINE_LIMIT = 100

TOP_N_FOR_TA = 40
MIN_SCORE_FOR_TRADE = 3
MAX_TRADES_PER_RUN = 4

BASE_MARGIN_USDT = 10.0  # mỗi lệnh khoảng 10 USDT (SPOT, không leverage)
TP_PCT = 0.01            # 1%
SL_PCT = 0.005           # 0.5%

SHEET_TTL_HOURS = 24
LOCAL_TZ = timezone(timedelta(hours=7))


# ============================================================
# TELEGRAM
# ============================================================

def notify_telegram(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
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
        print("[ERROR] Gửi Telegram lỗi:", e)


# ============================================================
# GOOGLE SHEETS (dùng GOOGLE_SERVICE_ACCOUNT_JSON)
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

    info = json.loads(json_str)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(info, scope)
    client = gspread.authorize(creds)
    return client


def prepare_sheet_and_cleanup():
    """
    - Mở (hoặc tạo) worksheet.
    - Đảm bảo header: Coin / Tín hiệu / Entry / SL / TP / Ngày.
    - Xoá các dòng cũ > 24h.
    - Trả về: ws, existing_signals (set (Coin, Tín hiệu)).
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
    expected_header = ["Coin", "Tín hiệu", "Entry", "SL", "TP", "Ngày"]

    if header != expected_header:
        ws.clear()
        ws.append_row(expected_header)
        return ws, set()

    rows = values[1:]
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
            dt = dt.replace(tzinfo=LOCAL_TZ)
        except Exception:
            continue
        if dt >= cutoff:
            kept_rows.append(r)
            existing_signals.add((r[0].strip(), r[1].strip().upper()))

    ws.clear()
    ws.append_row(expected_header)
    if kept_rows:
        ws.append_rows(kept_rows)

    return ws, existing_signals


def append_trades_to_sheet(ws, planned_trades):
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
                print(f"[WARN] Không parse được dòng sheet: {rec} -> {e}")
    return new_trades


# ============================================================
# OKX CLIENT (REST V5)
# ============================================================

class OKXClient:
    def __init__(self, api_key, api_secret, passphrase, simulated=True):
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        self.simulated = simulated

        def _headers(self, method, path, body=""):
            ts = datetime.utcnow().isoformat(timespec="milliseconds") + "Z"
            msg = ts + method.upper() + path + body
    
            signature = hmac.new(
                self.api_secret.encode(),
                msg.encode(),
                digestmod="sha256"
            ).digest()
    
            sign_base64 = base64.b64encode(signature).decode()
    
            headers = {
                "OK-ACCESS-KEY": self.api_key,
                "OK-ACCESS-SIGN": sign_base64,
                "OK-ACCESS-TIMESTAMP": ts,
                "OK-ACCESS-PASSPHRASE": self.passphrase,
                "Content-Type": "application/json",
            }
    
            if self.simulated:
                headers["x-simulated-trading"] = "1"
    
            # 🔥 LOG thêm để debug signature lỗi
            print("---- OKX HEADER DEBUG ----")
            print("Method:", method)
            print("Path:", path)
            print("Timestamp:", ts)
            print("Message for HMAC:", msg)
            print("Signature:", sign_base64)
            print("Headers:", headers)
            print("---------------------------")
    
            return headers


    # ---------- PUBLIC ----------

    def get_spot_tickers(self):
        url = f"{OKX_BASE_URL}/api/v5/market/tickers"
        params = {"instType": "SPOT"}
        try:
            r.raise_for_status()
        except Exception:
            print("❌ OKX REQUEST FAILED")
            print("URL:", r.url)
            print("Status Code:", r.status_code)
            print("Response:", r.text)
            raise

        data = r.json()
        return data.get("data", [])

    def get_candles(self, inst_id, bar="15m", limit=KLINE_LIMIT):
        url = f"{OKX_BASE_URL}/api/v5/market/candles"
        params = {"instId": inst_id, "bar": bar, "limit": limit}
        try:
            r.raise_for_status()
        except Exception:
            print("❌ OKX REQUEST FAILED")
            print("URL:", r.url)
            print("Status Code:", r.status_code)
            print("Response:", r.text)
            raise
        data = r.json()
        return data.get("data", [])

    def get_spot_instruments(self):
        # để lấy minSz, lotSz, ...
        url = f"{OKX_BASE_URL}/api/v5/public/instruments"
        params = {"instType": "SPOT"}
        try:
            r.raise_for_status()
        except Exception:
            print("❌ OKX REQUEST FAILED")
            print("URL:", r.url)
            print("Status Code:", r.status_code)
            print("Response:", r.text)
            raise
        data = r.json()
        return data.get("data", [])

    # ---------- PRIVATE (SPOT TRADE) ----------

    def place_spot_market_order(self, inst_id, side, sz):
        """
        side: 'buy' hoặc 'sell'
        sz: quantity (coin), string
        """
        path = "/api/v5/trade/order"
        url = OKX_BASE_URL + path
        body_dict = {
            "instId": inst_id,
            "tdMode": "cash",
            "side": side,
            "ordType": "market",
            "sz": str(sz),
        }
        body = json.dumps(body_dict)
        headers = self._headers("POST", path, body)
        r = requests.post(url, headers=headers, data=body, timeout=15)
        r.raise_for_status()
        return r.json()
        def place_oco_tp_sl(self, inst_id, side, sz, tp_trigger_px, sl_trigger_px):
            """
            Đặt OCO TP/SL cho SPOT:
              - inst_id: 'BTC-USDT'
              - side: 'sell' nếu đang LONG (tức TP/SL đều là lệnh bán)
                      'buy'  nếu đang SHORT (TP/SL đều là lệnh mua)
              - sz: khối lượng coin (giống lệnh vào)
              - tp_trigger_px: giá kích hoạt TP
              - sl_trigger_px: giá kích hoạt SL
    
            ordType = 'oco' => khi TP khớp thì SL bị hủy và ngược lại.
            tpOrdPx = -1, slOrdPx = -1 => dùng MARKET price khi trigger.
            """
            path = "/api/v5/trade/order-algo"
            url = OKX_BASE_URL + path
    
            body_dict = {
                "instId": inst_id,
                "tdMode": "cash",
                "side": side,            # 'sell' hoặc 'buy'
                "ordType": "oco",
                "sz": str(sz),
    
                # TP
                "tpTriggerPx": str(tp_trigger_px),
                "tpTriggerPxType": "last",
                "tpOrdPx": "-1",         # -1 = market khi trigger
    
                # SL
                "slTriggerPx": str(sl_trigger_px),
                "slTriggerPxType": "last",
                "slOrdPx": "-1",         # -1 = market khi trigger
            }
    
            body = json.dumps(body_dict)
            headers = self._headers("POST", path, body)
    
            r = requests.post(url, headers=headers, data=body, timeout=15)
            r.raise_for_status()
            return r.json()

    def get_balance(self, ccy):
        path = f"/api/v5/account/balance?ccy={ccy}"
        url = OKX_BASE_URL + path
        headers = self._headers("GET", path)
        r = requests.get(url, headers=headers, timeout=15)
        r.raise_for_status()
        data = r.json()
        details = data.get("data", [])
        if not details:
            return 0.0
        # totalEq hoặc availEq
        detail = details[0]
        for d in detail.get("details", []):
            if d.get("ccy") == ccy:
                return float(d.get("availBal", d.get("cashBal", "0")))
        return 0.0


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


def score_symbol_okx(inst_id, ticker, client: OKXClient):
    """
    inst_id: ví dụ 'PEPE-USDT'
    ticker: object từ /market/tickers
    Trả về dict: change_pct, abs_change, last_price, pump_score, dump_score
    """
    last = float(ticker["last"])
    open24h = float(ticker.get("open24h", "0") or "0")
    if open24h <= 0:
        change_pct = 0.0
    else:
        change_pct = (last / open24h - 1) * 100.0

    try:
        candles = client.get_candles(inst_id, bar=INTERVAL, limit=KLINE_LIMIT)
    except Exception as e:
        print(f"[WARN] Lỗi get_candles {inst_id}: {e}")
        return {
            "instId": inst_id,
            "change_pct": change_pct,
            "abs_change": abs(change_pct),
            "last_price": last,
            "pump_score": 0,
            "dump_score": 0,
        }

    # OKX trả list [ts, o, h, l, c, vol, volCcy, ...] và mới nhất ở index 0
    if not candles or len(candles) < 25:
        return {
            "instId": inst_id,
            "change_pct": change_pct,
            "abs_change": abs(change_pct),
            "last_price": last,
            "pump_score": 0,
            "dump_score": 0,
        }

    candles_sorted = list(reversed(candles))  # cũ -> mới
    closes = [float(c[4]) for c in candles_sorted]
    vols = [float(c[5]) for c in candles_sorted]

    last_close = closes[-1]
    last_vol = vols[-1]
    prev_closes = closes[:-1]
    prev_vols = vols[:-1]

    # 1h change (4 nến 15m)
    if len(closes) > 4:
        close_1h_ago = closes[-5]
        change_1h = (last_close / close_1h_ago - 1) * 100
    else:
        change_1h = 0.0

    # volume spike
    vol_avg_20 = np.mean(prev_vols[-20:]) if len(prev_vols) >= 20 else np.mean(prev_vols)
    vol_spike_ratio = last_vol / vol_avg_20 if vol_avg_20 > 0 else 1.0

    highest_20 = max(prev_closes[-20:])
    lowest_20 = min(prev_closes[-20:])

    rsi = compute_rsi(closes, period=14)

    pump_score = 0
    dump_score = 0

    # PUMP score
    if change_pct > 0:
        if change_pct >= 3:
            pump_score += 1
        if change_1h >= 1:
            pump_score += 1
        if vol_spike_ratio >= 2:
            pump_score += 1
        if last_close >= highest_20:
            pump_score += 1
        if rsi >= 55:
            pump_score += 1

    # DUMP score
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
        "instId": inst_id,
        "change_pct": change_pct,
        "abs_change": abs(change_pct),
        "last_price": last,
        "pump_score": pump_score,
        "dump_score": dump_score,
    }


# ============================================================
# SCAN & PLAN TRADES
# ============================================================

def scan_okx_market(client: OKXClient):
    """
    1) Lấy toàn bộ tickers SPOT.
    2) Lọc instId kết thúc bằng -USDT.
    3) Chọn top theo abs_change.
    4) Chấm điểm PUMP/DUMP.
    """
    tickers = client.get_spot_tickers()
    tmp = []
    for t in tickers:
        inst_id = t["instId"]
        if not inst_id.endswith(f"-{QUOTE}"):
            continue
        last = float(t["last"])
        open24h = float(t.get("open24h", "0") or "0")
        if open24h <= 0:
            change_pct = 0.0
        else:
            change_pct = (last / open24h - 1) * 100.0
        tmp.append({
            "instId": inst_id,
            "ticker": t,
            "abs_change": abs(change_pct),
        })

    if not tmp:
        return pd.DataFrame()

    tmp_sorted = sorted(tmp, key=lambda x: x["abs_change"], reverse=True)[:TOP_N_FOR_TA]

    results = []
    print(f"[INFO] Đang chấm điểm {len(tmp_sorted)} cặp trên OKX SPOT...")
    for rec in tmp_sorted:
        s = score_symbol_okx(rec["instId"], rec["ticker"], client)
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

        results.append({
            "instId": s["instId"],
            "direction": direction,
            "score": score,
            "change_pct": round(s["change_pct"], 2),
            "abs_change": round(s["abs_change"], 2),
            "last_price": s["last_price"],
        })

    if not results:
        return pd.DataFrame()

    df = pd.DataFrame(results)
    df = df.sort_values(by=["score", "abs_change"], ascending=[False, False]).reset_index(drop=True)
    return df


def plan_trades_from_signals(df_signals, existing_signals):
    if df_signals.empty:
        return [], None

    now_local = datetime.now(LOCAL_TZ)
    date_str = now_local.strftime("%d/%m/%Y %H:%M")

    candidates = df_signals[df_signals["score"] >= MIN_SCORE_FOR_TRADE].copy()
    if candidates.empty:
        return [], date_str

    filtered = []
    for _, row in candidates.iterrows():
        coin = row["instId"]
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
        inst_id = row["instId"]
        direction = row["direction"]
        price = float(row["last_price"])

        if direction == "LONG":
            tp = price * (1 + TP_PCT)
            sl = price * (1 - SL_PCT)
        else:
            tp = price * (1 - TP_PCT)
            sl = price * (1 + SL_PCT)

        planned.append({
            "Coin": inst_id,
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
# TRADE SPOT ON OKX
# ============================================================

def build_spot_instrument_map(client: OKXClient):
    data = client.get_spot_instruments()
    mp = {}
    for d in data:
        inst_id = d["instId"]
        min_sz = float(d.get("minSz", "0.0") or "0.0")
        lot_sz = float(d.get("lotSz", "0.0") or "0.0")
        mp[inst_id] = {
            "minSz": min_sz,
            "lotSz": lot_sz,
        }
    return mp


def round_sz(sz, lot_sz, min_sz):
    if lot_sz > 0:
        sz = math.floor(sz / lot_sz) * lot_sz
    if sz < min_sz:
        return 0.0
    return float(f"{sz:.12f}".rstrip("0").rstrip("."))


def execute_trades_okx_spot(client: OKXClient, new_trades):
    if not new_trades:
        print("[INFO] Không có lệnh mới trong sheet để vào (OKX SPOT).")
        return

    # Lấy thông tin instrument để biết minSz, lotSz
    inst_map = build_spot_instrument_map(client)

    # Số USDT khả dụng
    usdt_avail = client.get_balance("USDT")
    print(f"[INFO] USDT khả dụng: {usdt_avail}")

    for t in new_trades:
        inst_id = t["Coin"]
        direction = t["Tín hiệu"]
        entry_planned = t["Entry"]
        sl_price = t["SL"]
        tp_price = t["TP"]

        if inst_id not in inst_map:
            print(f"[WARN] Không tìm thấy instrument SPOT cho {inst_id}, bỏ qua.")
            continue

        inst_info = inst_map[inst_id]
        min_sz = inst_info["minSz"]
        lot_sz = inst_info["lotSz"] if inst_info["lotSz"] > 0 else min_sz

        # lấy giá hiện tại
        try:
            candles = client.get_candles(inst_id, bar=INTERVAL, limit=1)
            if candles:
                last_price = float(candles[0][4])
            else:
                last_price = entry_planned
        except Exception:
            last_price = entry_planned

        side = "buy" if direction == "LONG" else "sell"

        if side == "buy":
            notional = min(BASE_MARGIN_USDT, usdt_avail)
            if notional < BASE_MARGIN_USDT * 0.5:
                print(f"[WARN] USDT quá ít ({usdt_avail}), bỏ qua mua {inst_id}.")
                continue
            sz_raw = notional / last_price
            sz = round_sz(sz_raw, lot_sz, min_sz)
            if sz <= 0:
                print(f"[WARN] Sz quá nhỏ cho {inst_id}, bỏ.")
                continue
        else:  # sell
            # coin base, ví dụ BTC-USDT -> BTC
            base = inst_id.split("-")[0]
            base_avail = client.get_balance(base)
            if base_avail <= 0:
                print(f"[WARN] Không có {base} để bán, bỏ SHORT {inst_id}.")
                continue
            # bán khoảng 10 USDT worth hoặc toàn bộ nếu ít
            target_sz = BASE_MARGIN_USDT / last_price
            sz_raw = min(target_sz, base_avail)
            sz = round_sz(sz_raw, lot_sz, min_sz)
            if sz <= 0:
                print(f"[WARN] Sz sell quá nhỏ cho {inst_id}, bỏ.")
                continue

        try:
            # 1) Vào lệnh market
            resp_order = client.place_spot_market_order(inst_id, side, sz)
            usdt_avail = client.get_balance("USDT")  # update sau mỗi lệnh

            # 2) Đặt OCO TP/SL cho cùng khối lượng
            try:
                # LONG -> TP/SL là lệnh SELL ; SHORT -> lệnh BUY
                oco_side = "sell" if direction == "LONG" else "buy"
                resp_oco = client.place_oco_tp_sl(
                    inst_id=inst_id,
                    side=oco_side,
                    sz=sz,
                    tp_trigger_px=tp_price,
                    sl_trigger_px=sl_price,
                )
                oco_info = "TP/SL OCO đặt thành công."
            except Exception as e2:
                oco_info = f"Đặt TP/SL OCO bị lỗi: {e2}"
                print("[WARN]", oco_info)
                resp_oco = None

            # 3) Log + Telegram
            msg = (
                f"🚀 *OKX SPOT TRADE*\n"
                f"Coin: `{inst_id}`\n"
                f"Tín hiệu: *{direction}*\n"
                f"Side vào lệnh: `{side}`\n"
                f"Qty: `{sz}`\n"
                f"Entry (sheet): `{entry_planned:.6f}`\n"
                f"Giá hiện tại: `{last_price:.6f}`\n"
                f"TP: `{tp_price:.6f}`\n"
                f"SL: `{sl_price:.6f}`\n"
                f"TP/SL: *OCO tự động trên OKX* (1 khớp thì lệnh kia tự huỷ)\n"
                f"Chi tiết OCO: {oco_info}"
            )
            print(msg)
            notify_telegram(msg)
            print("[OKX ORDER RESP]", resp_order)
            if resp_oco is not None:
                print("[OKX OCO RESP]", resp_oco)

        except Exception as e:
            print(f"[ERROR] Lỗi vào lệnh SPOT {inst_id}: {e}")
            notify_telegram(f"❌ Lỗi vào lệnh OKX SPOT {inst_id}: {e}")


# ============================================================
# MAIN
# ============================================================

def main():
    print("====== OKX BOT CRON START ======")

    simulated = (OKX_SIMULATED_TRADING == "1")
    okx_client = OKXClient(
        api_key=OKX_API_KEY,
        api_secret=OKX_API_SECRET,
        passphrase=OKX_API_PASSPHRASE,
        simulated=simulated
    )

    # 1) Prepare sheet & get existing signals
    try:
        ws, existing_signals = prepare_sheet_and_cleanup()
    except Exception as e:
        print("[ERROR] Google Sheet prepare lỗi:", e)
        notify_telegram(f"⚠️ Lỗi Google Sheet (prepare): {e}")
        return

    # 2) Scan OKX market
    try:
        df_signals = scan_okx_market(okx_client)
    except Exception as e:
        print("[ERROR] Scan thị trường OKX lỗi:", e)
        notify_telegram(f"⚠️ Lỗi scan OKX: {e}")
        return

    if df_signals.empty:
        print("[INFO] Không có tín hiệu PUMP/DUMP trên OKX.")
        return

    print("[INFO] Top signals:")
    print(df_signals.head())

    # 3) Plan trades
    planned_trades, date_str = plan_trades_from_signals(df_signals, existing_signals)
    if not planned_trades:
        print("[INFO] Không có lệnh mới đạt điều kiện.")
        return

    print("[INFO] Planned trades:")
    for t in planned_trades:
        print(
            f"{t['Coin']} - {t['Tín hiệu']} - "
            f"Entry={t['Entry']:.6f} TP={t['TP']:.6f} SL={t['SL']:.6f} "
            f"score={t['score']} 24h={t['change_pct']}%"
        )

    # 4) Append to Google Sheet
    try:
        append_trades_to_sheet(ws, planned_trades)
        print("[INFO] Đã append lệnh mới vào Google Sheet.")
    except Exception as e:
        print("[ERROR] Append Google Sheet lỗi:", e)
        notify_telegram(f"⚠️ Lỗi Google Sheet (append): {e}")
        return

    # 5) Read back new trades (by timestamp)
    try:
        new_trades = get_trades_for_timestamp(ws, date_str)
    except Exception as e:
        print("[ERROR] Đọc sheet theo timestamp lỗi:", e)
        notify_telegram(f"⚠️ Lỗi Google Sheet (read new trades): {e}")
        return

    # 6) Execute SPOT trades on OKX
    try:
        execute_trades_okx_spot(okx_client, new_trades)
    except Exception as e:
        print("[ERROR] Lỗi execute trades OKX:", e)
        notify_telegram(f"⚠️ Lỗi execute trades OKX: {e}")
        return

    print("====== OKX BOT CRON END ======")


if __name__ == "__main__":
    main()
