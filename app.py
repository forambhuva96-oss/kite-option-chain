import os
import math
import json
import secrets
import pyotp
import requests as req
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
import pytz
import pandas as pd
from flask import Flask, request, redirect, jsonify, render_template, session
from kiteconnect import KiteConnect
from dotenv import load_dotenv
import oi_tracker   # our local SQLite-based OI tracker

load_dotenv()

# Remove legacy JSON snapshot path
IST = pytz.timezone("Asia/Kolkata")

# ─────────────────────────────────────────────
# Black-Scholes Greeks  (stdlib only, no scipy)
# ─────────────────────────────────────────────
RISK_FREE_RATE = 0.065   # ~6.5% Indian T-bill

def _ncdf(x):
    return (1.0 + math.erf(x / math.sqrt(2.0))) / 2.0

def _npdf(x):
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)

def _bs_price(S, K, T, r, sigma, kind):
    if T <= 0 or sigma <= 0:
        return max(0.0, (S - K) if kind == 'CE' else (K - S))
    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    if kind == 'CE':
        return S * _ncdf(d1) - K * math.exp(-r * T) * _ncdf(d2)
    return K * math.exp(-r * T) * _ncdf(-d2) - S * _ncdf(-d1)

def _implied_vol(S, K, T, r, market_price, kind):
    if T <= 0 or market_price <= 0:
        return 0.0
    lo, hi = 1e-4, 10.0
    for _ in range(200):
        mid = (lo + hi) / 2.0
        diff = _bs_price(S, K, T, r, mid, kind) - market_price
        if abs(diff) < 0.005:
            return mid
        lo, hi = (mid, hi) if diff < 0 else (lo, mid)
    return (lo + hi) / 2.0

def compute_greeks(S, K, T, ltp, kind):
    """Return {iv, delta, gamma, theta, vega} or None on failure."""
    if T <= 0 or ltp <= 0 or S <= 0 or K <= 0:
        return None
    try:
        r     = RISK_FREE_RATE
        sigma = _implied_vol(S, K, T, r, ltp, kind)
        if sigma <= 0:
            return None
        sqT  = math.sqrt(T)
        d1   = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * sqT)
        d2   = d1 - sigma * sqT
        pd1  = _npdf(d1)
        delta = _ncdf(d1) if kind == 'CE' else _ncdf(d1) - 1.0
        gamma = pd1 / (S * sigma * sqT)
        if kind == 'CE':
            theta = (-(S * pd1 * sigma) / (2 * sqT)
                     - r * K * math.exp(-r * T) * _ncdf(d2)) / 365.0
        else:
            theta = (-(S * pd1 * sigma) / (2 * sqT)
                     + r * K * math.exp(-r * T) * _ncdf(-d2)) / 365.0
        vega = S * pd1 * sqT / 100.0   # per 1% vol move
        return {
            'iv':    round(sigma * 100, 2),
            'delta': round(delta, 3),
            'gamma': round(gamma, 4),
            'theta': round(theta, 2),
            'vega':  round(vega, 2),
        }
    except Exception:
        return None

# ─────────────────────────────────────────────
# Flask setup
# ─────────────────────────────────────────────
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", secrets.token_hex(32))
app.config["SESSION_PERMANENT"] = False
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = os.environ.get("RENDER") is not None

KITE_API_KEY        = os.getenv("KITE_API_KEY")
KITE_API_SECRET     = os.getenv("KITE_API_SECRET")
ZERODHA_USER_ID     = os.getenv("ZERODHA_USER_ID")
ZERODHA_PASSWORD    = os.getenv("ZERODHA_PASSWORD")
ZERODHA_TOTP_SECRET = os.getenv("ZERODHA_TOTP_SECRET")

_server_access_token = None
instruments_cache    = {"date": None, "data": None}

# In-memory fallback: tracks first-seen live OI per symbol per day.
# Used when no 09:15 OPEN snapshot exists in SQLite yet (e.g. first run).
_intraday_fallback = {"date": None, "data": {}}


def _seed_intraday_fallback(opt_quotes: dict, intraday_db: dict, today_str: str):
    """
    If there is no 09:15 AM OI snapshot in the DB yet, seed an in-memory
    fallback baseline using the FIRST observed live OI of today's session.
    Never overwrites a symbol once seeded.
    """
    global _intraday_fallback
    if _intraday_fallback["date"] != today_str:
        # New day — reset fallback
        _intraday_fallback["date"] = today_str
        _intraday_fallback["data"] = {}

    # Only use fallback when DB snapshot is absent
    if intraday_db:
        return  # DB has data — no need for fallback

    for sym, q in opt_quotes.items():
        if sym not in _intraday_fallback["data"]:
            _intraday_fallback["data"][sym] = q.get("oi", 0)


# ─────────────────────────────────────────────
# OI Snapshot wrappers  (delegates to oi_tracker)
# ─────────────────────────────────────────────
def take_snapshot(label: str):
    """
    Called by the scheduler:
      - 'OPEN' at 09:15 IST  → used for intraday OI change
      - 'EOD'  at 15:29 IST  → used as next day's overnight baseline
    """
    kite = get_kite_client()
    if kite is None:
        print(f"[scheduler] No kite session — skipping {label} snapshot.")
        return
    oi_tracker.save_snapshot(kite, label)








# ─────────────────────────────────────────────
# Auto-login (TOTP)
# ─────────────────────────────────────────────
def auto_login():
    global _server_access_token
    if not all([ZERODHA_USER_ID, ZERODHA_PASSWORD, ZERODHA_TOTP_SECRET]):
        print("[auto_login] Credentials not set — skipping.")
        return False
    try:
        print("[auto_login] Starting automated login…")
        s = req.Session()
        r1 = s.post("https://kite.zerodha.com/api/login",
                    data={"user_id": ZERODHA_USER_ID, "password": ZERODHA_PASSWORD},
                    timeout=15)
        data1 = r1.json()
        if data1.get("status") != "success":
            print("[auto_login] Password step failed:", data1.get("message"))
            return False
        totp = pyotp.TOTP(ZERODHA_TOTP_SECRET).now()
        r2 = s.post("https://kite.zerodha.com/api/twofa",
                    data={"user_id": ZERODHA_USER_ID,
                          "request_id": data1["data"]["request_id"],
                          "twofa_value": totp, "twofa_type": "totp"},
                    timeout=15)
        if r2.json().get("status") != "success":
            print("[auto_login] 2FA step failed:", r2.json().get("message"))
            return False
        r3 = s.get(f"https://kite.zerodha.com/connect/login?api_key={KITE_API_KEY}&v=3",
                   allow_redirects=True, timeout=15)
        from urllib.parse import urlparse, parse_qs
        params = parse_qs(urlparse(r3.url).query)
        req_token = params.get("request_token", [None])[0]
        if not req_token:
            print("[auto_login] Could not extract request_token from:", r3.url)
            return False
        kite = KiteConnect(api_key=KITE_API_KEY)
        sess = kite.generate_session(req_token, api_secret=KITE_API_SECRET)
        _server_access_token = sess["access_token"]
        print(f"[auto_login] ✅ Done at {datetime.now().strftime('%H:%M:%S')}")
        return True
    except Exception as e:
        import traceback; traceback.print_exc()
        print("[auto_login] ❌ Error:", e)
        return False

def start_scheduler():
    tz = pytz.timezone("Asia/Kolkata")
    scheduler = BackgroundScheduler(timezone=tz)
    # Auto-login every morning before market opens
    scheduler.add_job(auto_login, "cron", hour=8, minute=45,
                      id="daily_login", replace_existing=True)
    # Save market-OPEN OI at 09:15 IST → used for intraday OI change during the day
    scheduler.add_job(lambda: take_snapshot("OPEN"), "cron",
                      hour=9, minute=15, day_of_week="mon-fri",
                      id="oi_open_snapshot", replace_existing=True)
    # Save EOD OI at 15:29 IST → used as next day's overnight baseline
    scheduler.add_job(lambda: take_snapshot("EOD"), "cron",
                      hour=15, minute=29, day_of_week="mon-fri",
                      id="oi_eod_snapshot", replace_existing=True)
    scheduler.start()
    print("[scheduler] Jobs: login@08:45, OPEN-snapshot@09:15, EOD-snapshot@15:29 (Mon-Fri)")

# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────
def get_kite_client():
    if _server_access_token:
        return KiteConnect(api_key=KITE_API_KEY, access_token=_server_access_token)
    if "access_token" in session:
        return KiteConnect(api_key=KITE_API_KEY, access_token=session["access_token"])
    return None

def get_nfo_instruments(kite):
    global instruments_cache
    today = datetime.now().date()
    if instruments_cache["date"] != today or instruments_cache["data"] is None:
        try:
            instruments_cache["data"] = pd.DataFrame(kite.instruments("NFO"))
            instruments_cache["date"] = today
        except Exception as e:
            print("Instrument fetch error:", e)
            return None
    return instruments_cache["data"]

# ─────────────────────────────────────────────
# Routes
# ─────────────────────────────────────────────
@app.route("/")
def index():
    req_token = request.args.get("request_token")
    if req_token:
        kite = KiteConnect(api_key=KITE_API_KEY)
        try:
            data = kite.generate_session(req_token, api_secret=KITE_API_SECRET)
            session["access_token"] = data["access_token"]
            return redirect("/")
        except Exception:
            pass
    if _server_access_token or "access_token" in session:
        return render_template("index.html")
    return render_template("login.html",
                           error=request.args.get("error"),
                           auto_login_enabled=bool(ZERODHA_TOTP_SECRET))

@app.route("/login")
def login():
    return redirect(KiteConnect(api_key=KITE_API_KEY).login_url())

@app.route("/callback")
def callback():
    req_token = request.args.get("request_token")
    if not req_token:
        return redirect("/?error=No+request+token+received")
    kite = KiteConnect(api_key=KITE_API_KEY)
    try:
        data = kite.generate_session(req_token, api_secret=KITE_API_SECRET)
        session["access_token"] = data["access_token"]
        return redirect("/")
    except Exception as e:
        return redirect(f"/?error={e}")

@app.route("/logout")
def logout():
    session.clear()
    return redirect("/")

@app.route("/api/status")
def api_status():
    snap = oi_tracker.snapshot_status()
    return jsonify({
        "auto_login_active":  bool(_server_access_token),
        "session_active":     "access_token" in session,
        "snapshot_status":    snap,
    })

@app.route("/api/debug-hi")
def api_debug_hi():
    kite = get_kite_client()
    if not kite: return jsonify({"error": "No kite"})
    try:
        inst = kite.instruments("NFO")
        token = [i["instrument_token"] for i in inst if i["name"]=="NIFTY" and i["segment"]=="NFO-OPT"][0]
        from datetime import datetime, timedelta
        f = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        t = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        hist = kite.historical_data(token, f, t, "day", oi=True)
        return jsonify({"token": token, "hist": hist, "f": f, "t": t})
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "trace": traceback.format_exc()})


@app.route("/api/expiries")
def api_expiries():
    """Return available expiry dates for a symbol."""
    kite = get_kite_client()
    if not kite:
        return jsonify({"error": "Unauthorized"}), 401
    symbol = request.args.get("symbol", "NIFTY")
    if symbol not in ["NIFTY", "BANKNIFTY"]:
        return jsonify({"error": "Invalid symbol"}), 400
    df = get_nfo_instruments(kite)
    if df is None:
        return jsonify({"error": "Failed to load instruments"}), 500
    df_sym = df[(df["name"] == symbol) & (df["segment"] == "NFO-OPT")]
    expiries = sorted(df_sym["expiry"].dropna().unique())
    return jsonify({
        "success": True,
        "expiries": [e.strftime("%Y-%m-%d") for e in expiries[:10]],
        "labels":   [e.strftime("%d %b '%y") for e in expiries[:10]],
    })


@app.route("/api/option-chain")
def api_option_chain():
    kite = get_kite_client()
    if not kite:
        return jsonify({"error": "Unauthorized"}), 401

    symbol = request.args.get("symbol", "NIFTY")
    if symbol not in ["NIFTY", "BANKNIFTY"]:
        return jsonify({"error": "Invalid symbol"}), 400

    spot_sym = "NSE:NIFTY 50" if symbol == "NIFTY" else "NSE:NIFTY BANK"

    try:
        quote = kite.quote([spot_sym])
        if spot_sym not in quote:
            return jsonify({"error": f"Could not fetch spot for {spot_sym}"}), 500
        spot_price = quote[spot_sym]["last_price"]

        df = get_nfo_instruments(kite)
        if df is None:
            return jsonify({"error": "Failed to load NFO instruments"}), 500

        df_sym = df[(df["name"] == symbol) & (df["segment"] == "NFO-OPT")]
        if df_sym.empty:
            return jsonify({"error": f"No options for {symbol}"}), 404

        expiries = sorted(df_sym["expiry"].dropna().unique())
        if not expiries:
            return jsonify({"error": "No expiries"}), 404

        # Use expiry from query param if provided and valid, else use nearest
        expiry_param = request.args.get("expiry")   # "YYYY-MM-DD"
        selected_expiry = expiries[0]               # default = nearest
        if expiry_param:
            from datetime import date as _date
            try:
                req_date = _date.fromisoformat(expiry_param)
                if req_date in [e for e in expiries]:
                    selected_expiry = req_date
            except ValueError:
                pass

        df_exp = df_sym[df_sym["expiry"] == selected_expiry].copy()

        # Time to expiry in years
        now = datetime.now()
        expiry_dt = datetime.combine(selected_expiry, datetime.min.time()).replace(hour=15, minute=30)
        T = max((expiry_dt - now).total_seconds() / (365.25 * 24 * 3600), 0)

        strike_diff    = 50 if symbol == "NIFTY" else 100
        atm_strike     = round(spot_price / strike_diff) * strike_diff
        target_strikes = [atm_strike + i * strike_diff for i in range(-10, 11)]  # ATM ±10

        df_filtered  = df_exp[df_exp["strike"].isin(target_strikes)]

        opt_syms   = ["NFO:" + s for s in df_filtered["tradingsymbol"].tolist()]
        opt_quotes = kite.quote(opt_syms) if opt_syms else {}

        # ── Load baseline dicts from SQLite via oi_tracker ──────────────────
        # overnight_base : yesterday's EOD OI  → Overnight OI Change
        # intraday_base  : today's 09:15 OI    → Intraday OI Change
        expiry_str       = selected_expiry.strftime("%Y-%m-%d")
        overnight_base   = oi_tracker.get_eod_snapshot(symbol, expiry_str)
        intraday_base    = oi_tracker.get_open_snapshot(symbol, expiry_str)

        # ── In-memory live session fallback for intraday ──────────────────────
        # If no 09:15 snapshot yet (e.g. app just started), we track live OI
        # from first fetch. Stored in a module-level dict so it persists between
        # requests without being overwritten.
        today_str = datetime.now(IST).date().isoformat()
        _seed_intraday_fallback(opt_quotes, intraday_base, today_str)

        chain_data = []
        for strike in target_strikes:
            entry = {"strike": strike, "CE": None, "PE": None}
            for kind in ["CE", "PE"]:
                row = df_filtered[
                    (df_filtered["strike"] == strike) &
                    (df_filtered["instrument_type"] == kind)
                ]
                if not row.empty:
                    sym = "NFO:" + row.iloc[0]["tradingsymbol"]
                    if sym in opt_quotes:
                        q        = opt_quotes[sym]
                        ltp      = round(q.get("last_price", 0), 2)
                        curr_oi  = q.get("oi", 0)

                        # Overnight OI Change = current OI − yesterday's EOD OI
                        o_base   = overnight_base.get(sym, None)
                        o_chg    = (curr_oi - o_base) if o_base is not None else None

                        # Intraday OI Change = current OI − today's 09:15 AM OI
                        # Falls back to live session baseline if no DB snapshot yet
                        i_base   = intraday_base.get(sym) or _intraday_fallback["data"].get(sym)
                        i_chg    = (curr_oi - i_base) if i_base is not None else None

                        greeks   = compute_greeks(spot_price, strike, T, ltp, kind)
                        entry[kind] = {
                            "ltp":              ltp,
                            "volume":           q.get("volume", 0),
                            "oi":               curr_oi,
                            "oi_change":        o_chg,   # overnight (vs yesterday EOD)
                            "intraday_oi_chg":  i_chg,   # intraday  (vs 09:15 AM)
                            **(greeks or {}),
                        }

            chain_data.append(entry)

        return jsonify({
            "success":    True,
            "spot_price": spot_price,
            "atm_strike": atm_strike,
            "expiry":     selected_expiry.strftime("%d %b %Y"),
            "expiry_val": selected_expiry.strftime("%Y-%m-%d"),
            "all_expiries": [
                {"value": e.strftime("%Y-%m-%d"), "label": e.strftime("%d %b '%y")}
                for e in expiries[:10]
            ],
            "chain":      chain_data,
            "auto_login": bool(_server_access_token),
        })

    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500

# ─────────────────────────────────────────────
# Startup
# ─────────────────────────────────────────────
oi_tracker.init_db()   # ensure SQLite tables exist
auto_login()
start_scheduler()

if __name__ == "__main__":
    port  = int(os.environ.get("PORT", 5000))
    is_dev = os.environ.get("FLASK_ENV") == "development"
    if is_dev:
        app.config["SESSION_COOKIE_SECURE"] = False
    app.run(host="0.0.0.0", port=port, debug=is_dev)
