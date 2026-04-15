import os
import math
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

load_dotenv()

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

# OI baseline: keyed by NFO tradingsymbol, value = previous day's closing OI
# Rebuilt once per calendar day via Kite historical API (survives cold restarts)
oi_baseline = {"date": None, "data": {}}   # {"NFO:XXXXX": prev_close_oi}


def build_oi_baseline(kite, df_filtered, today):
    """
    Fetch the most-recent trading day's closing OI for each option
    in df_filtered. Called once per calendar day; skips if already built.
    """
    global oi_baseline
    if oi_baseline["date"] == today:
        return  # already fresh

    from datetime import timedelta

    # Walk back to find the last trading weekday (Mon-Fri)
    prev_day = datetime.now().date() - timedelta(days=1)
    while prev_day.weekday() >= 5:           # 5=Sat, 6=Sun
        prev_day -= timedelta(days=1)

    from_str = prev_day.strftime("%Y-%m-%d")
    to_str   = today.strftime("%Y-%m-%d")

    new_baseline = {}
    for _, row in df_filtered.iterrows():
        try:
            token = int(row["instrument_token"])
            sym   = "NFO:" + row["tradingsymbol"]
            hist  = kite.historical_data(token, from_str, to_str, "day")
            if hist:
                new_baseline[sym] = hist[-1].get("oi", 0)   # last completed candle
        except Exception as e:
            print(f"[oi_baseline] {row['tradingsymbol']}: {e}")
            new_baseline["NFO:" + row["tradingsymbol"]] = 0

    oi_baseline["data"] = new_baseline
    oi_baseline["date"] = today
    print(f"[oi_baseline] Built for {len(new_baseline)} symbols on {today}")



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
    scheduler.add_job(auto_login, "cron", hour=8, minute=45,
                      id="daily_login", replace_existing=True)
    scheduler.start()
    print("[scheduler] Daily auto-login at 08:45 IST")

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
    return jsonify({"auto_login_active": bool(_server_access_token),
                    "session_active": "access_token" in session})

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

        nearest_expiry = expiries[0]
        df_exp = df_sym[df_sym["expiry"] == nearest_expiry].copy()

        # Time to expiry in years
        now = datetime.now()
        expiry_dt = datetime.combine(nearest_expiry, datetime.min.time()).replace(hour=15, minute=30)
        T = max((expiry_dt - now).total_seconds() / (365.25 * 24 * 3600), 0)

        strike_diff    = 50 if symbol == "NIFTY" else 100
        atm_strike     = round(spot_price / strike_diff) * strike_diff
        target_strikes = [atm_strike + i * strike_diff for i in range(-10, 11)]  # ATM ±10

        df_filtered  = df_exp[df_exp["strike"].isin(target_strikes)]

        opt_syms   = ["NFO:" + s for s in df_filtered["tradingsymbol"].tolist()]
        opt_quotes = kite.quote(opt_syms) if opt_syms else {}

        # ── Build yesterday's OI baseline (once per day, survives cold restart) ──
        today = datetime.now(pytz.timezone("Asia/Kolkata")).date()
        build_oi_baseline(kite, df_filtered, today)

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
                        q    = opt_quotes[sym]
                        ltp  = round(q.get("last_price", 0), 2)
                        curr_oi  = q.get("oi", 0)
                        base_oi  = oi_baseline["data"].get(sym, curr_oi)
                        greeks   = compute_greeks(spot_price, strike, T, ltp, kind)
                        entry[kind] = {
                            "ltp":       ltp,
                            "volume":    q.get("volume", 0),
                            "oi":        curr_oi,
                            "oi_change": curr_oi - base_oi,   # real intraday OI δ
                            **(greeks or {}),
                        }

            chain_data.append(entry)

        return jsonify({
            "success":    True,
            "spot_price": spot_price,
            "atm_strike": atm_strike,
            "expiry":     nearest_expiry.strftime("%d %b %Y"),
            "chain":      chain_data,
            "auto_login": bool(_server_access_token),
        })

    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500

# ─────────────────────────────────────────────
# Startup
# ─────────────────────────────────────────────
auto_login()
start_scheduler()

if __name__ == "__main__":
    port  = int(os.environ.get("PORT", 5000))
    is_dev = os.environ.get("FLASK_ENV") == "development"
    if is_dev:
        app.config["SESSION_COOKIE_SECURE"] = False
    app.run(host="0.0.0.0", port=port, debug=is_dev)
