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

# Trading config
FUT_LEVERAGE = 6              # x5 isolated
NOTIONAL_PER_TRADE = 25.0     # 25 USDT position size (ký quỹ ~5$ với x5)
MAX_TRADES_PER_RUN = 5        # tối đa 5 lệnh / 1 lần cron

# Scanner config
MIN_ABS_CHANGE_PCT = 5.0      # chỉ lấy coin |24h change| >= 5%
MIN_VOL_USDT = 100000         # min 24h volume quote
TOP_N_BY_CHANGE = 40          # universe: top 40 theo độ biến động

# Google Sheet headers
SHEET_HEADERS = ["Coin", "Tín hiệu", "Entry", "SL", "TP", "Ngày"]


# ========== HELPERS ==========

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%b %d %I:%M:%S %p",
    )


def now_str_vn():
    # Render dùng UTC -> +7h cho giờ VN
    return (datetime.utcnow() + timedelta(hours=7)).strftime("%d/%m/%Y %H:%M")


# ========== OKX REST CLIENT ==========

class OKXClient:
    def __init__(self, api_key, api_secret, passphrase, simulated_trading=True):
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

    def get_swap_instruments(self):
        path = "/api/v5/public/instruments"
        params = {"instType": "SWAP"}
        data = self._request("GET", path, params=params)
        return data.get("data", [])

    # ---------- PRIVATE ----------

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

def build_signals_from_tickers(tickers):
    rows = []
    for t in tickers:
        inst_id = t.get("instId")
        if not inst_id or not inst_id.endswith("-USDT"):
            continue

        last = float(t.get("last", "0"))
        open24h = float(t.get("open24h", "0"))
        vol_quote = float(t.get("volCcy24h", "0"))

        if open24h <= 0 or last <= 0:
            continue

        change_pct = (last - open24h) / open24h * 100.0
        abs_change = abs(change_pct)

        if abs_change < MIN_ABS_CHANGE_PCT:
            continue
        if vol_quote < MIN_VOL_USDT:
            continue

        direction = "LONG" if change_pct > 0 else "SHORT"
        rows.append(
            {
                "instId": inst_id,
                "direction": direction,
                "change_pct": change_pct,
                "abs_change": abs_change,
                "last_price": last,
                "vol_quote": vol_quote,
            }
        )

    if not rows:
        return pd.DataFrame(
            columns=[
                "instId",
                "direction",
                "change_pct",
                "abs_change",
                "last_price",
                "vol_quote",
            ]
        )

    df = pd.DataFrame(rows)
    # score đơn giản theo abs_change
    df["score"] = pd.qcut(
        df["abs_change"], q=5, labels=[1, 2, 3, 4, 5]
    ).astype(int)
    df = df.sort_values(["score", "abs_change"], ascending=[False, False])
    df = df.head(TOP_N_BY_CHANGE)
    return df


def plan_trades_from_signals(df, existing_keys):
    """
    df: signals dataframe
    existing_keys: set of (coin, signal)
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
        key = (row.instId, row.direction)
        if key in existing_keys:
            continue

        entry = row.last_price
        # 5% TP, 2% SL
        if row.direction == "LONG":
            tp = entry * 1.05
            sl = entry * 0.98
        else:
            tp = entry * 0.95
            sl = entry * 1.02

        planned.append(
            {
                "coin": row.instId,
                "signal": row.direction,
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


# ========== EXECUTE FUTURES TRADES ==========

def execute_futures_trades(okx: OKXClient, trades):
    if not trades:
        logging.info("[INFO] Không có lệnh futures nào để vào.")
        return

    # metadata SWAP
    swap_ins = okx.get_swap_instruments()
    swap_meta = build_swap_meta_map(swap_ins)

    # kiểm tra balance (margin ~ NOTIONAL / leverage)
    avail_usdt = okx.get_usdt_balance()
    margin_per_trade = NOTIONAL_PER_TRADE / FUT_LEVERAGE
    max_trades_by_balance = int(avail_usdt // margin_per_trade)
    if max_trades_by_balance <= 0:
        logging.warning("[WARN] Không đủ USDT để vào bất kỳ lệnh nào.")
        return

    allowed_trades = trades[: max_trades_by_balance]

    for t in allowed_trades:
        coin = t["coin"]         # ví dụ NEIRO-USDT
        signal = t["signal"]     # LONG / SHORT
        entry = t["entry"]
        tp = t["tp"]
        sl = t["sl"]

        # Spot -> Perp SWAP
        swap_inst = coin.replace("-USDT", "-USDT-SWAP")
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

        # 2) Market order
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
            text = (
                "❌ *OKX FUTURES TRADE FAILED*\n"
                f"Coin: {coin}\n"
                f"Tín hiệu: {signal}\n"
                f"Lỗi: {msg}"
            )
            send_telegram_message(text)
            continue

        # 3) OCO TP/SL
        oco_resp = okx.place_oco_tp_sl(
            inst_id=swap_inst,
            pos_side=pos_side,
            side_close=side_close,
            sz=contracts,
            tp_px=tp,
            sl_px=sl,
            td_mode="isolated",
        )

        oco_msg = oco_resp.get("msg", "")
        oco_code = oco_resp.get("code")
        success_oco = oco_code == "0"

        # 4) Telegram
        text_lines = [
            "🚀 OKX FUTURES TRADE",
            f"Coin: {coin}",
            f"Future: {swap_inst}",
            f"Tín hiệu: {signal}",
            f"PosSide: {pos_side}",
            f"Leverage: x{FUT_LEVERAGE} isolated",
            f"Qty (contracts): {contracts}",
            f"Entry (sheet): {entry:.8f}",
            f"TP: {tp:.8f}",
            f"SL: {sl:.8f}",
            "TP/SL: OCO tự động trên OKX (1 khớp thì lệnh kia tự hủy)",
        ]
        if success_oco:
            text_lines.append("Chi tiết OCO: TP/SL OCO đặt *thành công*.")
        else:
            text_lines.append(
                f"Chi tiết OCO: LỖI đặt OCO code={oco_code} msg={oco_msg}"
            )

        send_telegram_message("\n".join(text_lines))


# ========== MAIN ==========

def main():
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

    # 1) Scan market
    try:
        tickers = okx.get_spot_tickers()
    except Exception as e:
        logging.error("[ERROR] Scan thị trường OKX lỗi: %s", e)
        return

    df_signals = build_signals_from_tickers(tickers)
    logging.info(
        "[INFO] Đang chấm điểm %d cặp SPOT trên OKX ...", len(df_signals)
    )

    # 2) Google Sheet
    try:
        ws = prepare_worksheet()
        existing = get_recent_signals(ws)
    except Exception as e:
        logging.error("[ERROR] Google Sheet prepare lỗi: %s", e)
        return

    # 3) Plan trades
    planned_trades = plan_trades_from_signals(df_signals, existing)

    # 4) Append sheet
    append_signals(ws, planned_trades)

    # 5) Futures + Telegram
    execute_futures_trades(okx, planned_trades)


if __name__ == "__main__":
    main()
