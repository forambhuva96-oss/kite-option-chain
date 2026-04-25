"""
NSE Option Chain Public API fetcher.
No authentication required. Works after market hours too — returns 3:30 PM snapshot.
"""

import requests
import logging
import pytz
from datetime import datetime

logger = logging.getLogger("app")
IST = pytz.timezone("Asia/Kolkata")

NSE_BASE    = "https://www.nseindia.com"
NSE_API_URL = "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY"
NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer":         "https://www.nseindia.com/option-chain",
    "Connection":      "keep-alive",
}


def _get_session() -> requests.Session:
    """Create a cookie-enabled session by visiting NSE home page first."""
    s = requests.Session()
    s.headers.update(NSE_HEADERS)
    try:
        s.get(NSE_BASE, timeout=8)                        # seed cookies
        s.get("https://www.nseindia.com/option-chain", timeout=8)  # warm referer
    except Exception as e:
        logger.warning(f"NSE session warm-up failed: {e}")
    return s


def fetch_nse_option_chain(symbol: str = "NIFTY", expiry_filter: str = None) -> dict | None:
    """
    Fetch option chain from NSE public API.

    Returns a dict in the same shape as background_task.STATE so it can be
    dropped into STATE directly:
        {
            "latest_data": [...],
            "spot_price":  float,
            "atm_strike":  int,
            "expiry":      str,          # "YYYY-MM-DD"
            "all_expiries": [{"label":..,"value":..}],
            "last_updated": str,         # "HH:MM:SS"
            "snapshot_time": str,        # human readable
        }
    Returns None on failure.
    """
    url = f"https://www.nseindia.com/api/option-chain-indices?symbol={symbol}"
    try:
        session = _get_session()
        r = session.get(url, timeout=10)
        r.raise_for_status()
        raw = r.json()
    except Exception as e:
        logger.error(f"NSE option chain fetch failed: {e}")
        return None

    try:
        records   = raw.get("records", {})
        spot_price = float(records.get("underlyingValue", 0))
        atm_strike = round(spot_price / 50) * 50

        # Expiry list
        expiry_dates_raw = records.get("expiryDates", [])
        all_expiries = []
        for ex in expiry_dates_raw[:6]:
            try:
                dt = datetime.strptime(ex, "%d-%b-%Y")
                all_expiries.append({
                    "label": dt.strftime("%d %b %Y"),
                    "value": dt.strftime("%Y-%m-%d")
                })
            except Exception:
                pass

        # Pick target expiry
        target_expiry_label = expiry_filter  # "YYYY-MM-DD" or None
        if target_expiry_label is None and all_expiries:
            target_expiry_label = all_expiries[0]["value"]  # nearest

        # Map expiry label back to raw string for filtering
        target_expiry_raw = None
        for ex in expiry_dates_raw:
            try:
                dt = datetime.strptime(ex, "%d-%b-%Y")
                if dt.strftime("%Y-%m-%d") == target_expiry_label:
                    target_expiry_raw = ex
                    break
            except Exception:
                pass

        data_rows = records.get("data", [])

        # Collect all strikes for target expiry
        strike_map: dict[int, dict] = {}
        for row in data_rows:
            if target_expiry_raw and row.get("expiryDate") != target_expiry_raw:
                continue
            strike = int(row.get("strikePrice", 0))
            entry = strike_map.setdefault(strike, {"strike": strike, "CE": None, "PE": None})
            for side in ("CE", "PE"):
                if side in row:
                    d = row[side]
                    entry[side] = {
                        "ltp":                d.get("lastPrice", 0),
                        "oi":                 d.get("openInterest", 0),
                        "volume":             d.get("totalTradedVolume", 0),
                        "intraday_oi_change": d.get("changeinOpenInterest", None),
                        "iv":                 d.get("impliedVolatility", None),
                        "signal":             "",
                        "strength":           "",
                        "action":             "",
                        "alert":              False,
                    }

        # Keep only strikes near ATM (atm ± 15 strikes of 50)
        target_strikes = set(atm_strike + i * 50 for i in range(-15, 16))
        chain_data = sorted(
            [v for k, v in strike_map.items() if k in target_strikes],
            key=lambda x: x["strike"]
        )

        now_ist = datetime.now(IST)
        return {
            "latest_data":   chain_data,
            "spot_price":    spot_price,
            "atm_strike":    atm_strike,
            "expiry":        target_expiry_label or "",
            "all_expiries":  all_expiries,
            "last_updated":  now_ist.strftime("%H:%M:%S"),
            "snapshot_time": now_ist.strftime("%Y-%m-%d %H:%M:%S IST"),
        }

    except Exception as e:
        logger.error(f"NSE option chain parse error: {e}")
        return None
