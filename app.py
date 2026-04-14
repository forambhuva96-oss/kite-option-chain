import os
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

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", secrets.token_hex(32))
app.config["SESSION_PERMANENT"] = False
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = os.environ.get("RENDER") is not None

KITE_API_KEY    = os.getenv("KITE_API_KEY")
KITE_API_SECRET = os.getenv("KITE_API_SECRET")

# Optional: auto-login credentials (stored in Render env vars)
ZERODHA_USER_ID     = os.getenv("ZERODHA_USER_ID")
ZERODHA_PASSWORD    = os.getenv("ZERODHA_PASSWORD")
ZERODHA_TOTP_SECRET = os.getenv("ZERODHA_TOTP_SECRET")

# Global server-side access token (used when auto-login is configured)
_server_access_token = None

# Instrument cache
instruments_cache = {"date": None, "data": None}


# ─────────────────────────────────────────────
# Auto-login logic (no browser needed)
# ─────────────────────────────────────────────

def auto_login():
    """Automatically log in to Zerodha using credentials + TOTP and cache the access token."""
    global _server_access_token

    if not all([ZERODHA_USER_ID, ZERODHA_PASSWORD, ZERODHA_TOTP_SECRET]):
        print("[auto_login] Credentials not set — skipping auto-login.")
        return False

    try:
        print("[auto_login] Starting automated Zerodha login...")
        s = req.Session()

        # Step 1: Password login
        login_resp = s.post("https://kite.zerodha.com/api/login", data={
            "user_id":  ZERODHA_USER_ID,
            "password": ZERODHA_PASSWORD,
        }, timeout=15)
        login_data = login_resp.json()

        if login_data.get("status") != "success":
            print("[auto_login] Password login failed:", login_data.get("message"))
            return False

        request_id = login_data["data"]["request_id"]

        # Step 2: TOTP 2FA
        totp_code = pyotp.TOTP(ZERODHA_TOTP_SECRET).now()
        twofa_resp = s.post("https://kite.zerodha.com/api/twofa", data={
            "user_id":    ZERODHA_USER_ID,
            "request_id": request_id,
            "twofa_value": totp_code,
            "twofa_type": "totp",
        }, timeout=15)
        twofa_data = twofa_resp.json()

        if twofa_data.get("status") != "success":
            print("[auto_login] 2FA failed:", twofa_data.get("message"))
            return False

        # Step 3: Get request_token via Kite Connect redirect
        connect_resp = s.get(
            f"https://kite.zerodha.com/connect/login?api_key={KITE_API_KEY}&v=3",
            allow_redirects=True,
            timeout=15,
        )

        # The final URL after redirect contains request_token
        final_url = connect_resp.url
        from urllib.parse import urlparse, parse_qs
        params = parse_qs(urlparse(final_url).query)
        request_token = params.get("request_token", [None])[0]

        if not request_token:
            print("[auto_login] Could not extract request_token from:", final_url)
            return False

        # Step 4: Exchange for access_token
        kite = KiteConnect(api_key=KITE_API_KEY)
        session_data = kite.generate_session(request_token, api_secret=KITE_API_SECRET)
        _server_access_token = session_data["access_token"]

        print(f"[auto_login] ✅ Auto-login successful at {datetime.now().strftime('%H:%M:%S')}")
        return True

    except Exception as e:
        import traceback
        print("[auto_login] ❌ Error:", e)
        traceback.print_exc()
        return False


def get_kite_client():
    """Return a Kite client using server token (auto-login) or user's session token."""
    # Prefer server-side auto-login token
    if _server_access_token:
        return KiteConnect(api_key=KITE_API_KEY, access_token=_server_access_token)
    # Fall back to user's session token
    if "access_token" in session:
        return KiteConnect(api_key=KITE_API_KEY, access_token=session["access_token"])
    return None


def get_nfo_instruments(kite):
    global instruments_cache
    today = datetime.now().date()
    if instruments_cache["date"] != today or instruments_cache["data"] is None:
        try:
            raw = kite.instruments(exchange="NFO")
            instruments_cache["data"] = pd.DataFrame(raw)
            instruments_cache["date"] = today
        except Exception as e:
            print("Instrument fetch error:", e)
            return None
    return instruments_cache["data"]


# ─────────────────────────────────────────────
# Daily scheduler — fires at 8:45 AM IST
# ─────────────────────────────────────────────

def start_scheduler():
    scheduler = BackgroundScheduler(timezone=pytz.timezone("Asia/Kolkata"))
    scheduler.add_job(auto_login, "cron", hour=8, minute=45,
                      id="daily_login", replace_existing=True)
    scheduler.start()
    print("[scheduler] Daily auto-login scheduled at 08:45 AM IST")


# ─────────────────────────────────────────────
# Flask Routes
# ─────────────────────────────────────────────

@app.route("/")
def index():
    # Catch request_token if Kite redirects to "/" instead of "/callback"
    request_token = request.args.get("request_token")
    if request_token:
        kite = KiteConnect(api_key=KITE_API_KEY)
        try:
            data = kite.generate_session(request_token, api_secret=KITE_API_SECRET)
            session["access_token"] = data["access_token"]
            return redirect("/")
        except Exception:
            pass

    # If auto-login is active, go straight to dashboard — no manual login needed
    if _server_access_token or "access_token" in session:
        return render_template("index.html")

    return render_template("login.html", error=request.args.get("error"),
                           auto_login_enabled=bool(ZERODHA_TOTP_SECRET))


@app.route("/login")
def login():
    kite = KiteConnect(api_key=KITE_API_KEY)
    return redirect(kite.login_url())


@app.route("/callback")
def callback():
    request_token = request.args.get("request_token")
    if not request_token:
        return redirect("/?error=No+request+token+received")

    kite = KiteConnect(api_key=KITE_API_KEY)
    try:
        data = kite.generate_session(request_token, api_secret=KITE_API_SECRET)
        session["access_token"] = data["access_token"]
        return redirect("/")
    except Exception as e:
        return redirect(f"/?error={str(e)}")


@app.route("/logout")
def logout():
    session.clear()
    return redirect("/")


@app.route("/api/status")
def api_status():
    return jsonify({
        "auto_login_active": bool(_server_access_token),
        "session_active": "access_token" in session,
    })


@app.route("/api/option-chain")
def api_option_chain():
    kite = get_kite_client()
    if not kite:
        return jsonify({"error": "Unauthorized"}), 401

    symbol = request.args.get("symbol", "NIFTY")
    if symbol not in ["NIFTY", "BANKNIFTY"]:
        return jsonify({"error": "Invalid symbol"}), 400

    spot_symbol = "NSE:NIFTY 50" if symbol == "NIFTY" else "NSE:NIFTY BANK"

    try:
        quote = kite.quote([spot_symbol])
        if spot_symbol not in quote:
            return jsonify({"error": f"Could not fetch spot for {spot_symbol}"}), 500
        spot_price = quote[spot_symbol]["last_price"]

        df = get_nfo_instruments(kite)
        if df is None:
            return jsonify({"error": "Failed to load NFO instruments"}), 500

        df_sym = df[(df["name"] == symbol) & (df["segment"] == "NFO-OPT")]
        if df_sym.empty:
            return jsonify({"error": f"No options found for {symbol}"}), 404

        expiries = sorted(df_sym["expiry"].dropna().unique())
        if not expiries:
            return jsonify({"error": "No expiries found"}), 404
        nearest_expiry = expiries[0]
        df_exp = df_sym[df_sym["expiry"] == nearest_expiry].copy()

        strike_diff = 50 if symbol == "NIFTY" else 100
        atm_strike = round(spot_price / strike_diff) * strike_diff
        target_strikes = [atm_strike + (i * strike_diff) for i in range(-5, 6)]
        df_filtered = df_exp[df_exp["strike"].isin(target_strikes)]

        opt_symbols = ["NFO:" + s for s in df_filtered["tradingsymbol"].tolist()]
        opt_quotes = kite.quote(opt_symbols) if opt_symbols else {}

        chain_data = []
        for strike in target_strikes:
            strike_data = {"strike": strike, "CE": None, "PE": None}
            for opt_type in ["CE", "PE"]:
                row = df_filtered[
                    (df_filtered["strike"] == strike) &
                    (df_filtered["instrument_type"] == opt_type)
                ]
                if not row.empty:
                    sym = "NFO:" + row.iloc[0]["tradingsymbol"]
                    if sym in opt_quotes:
                        q = opt_quotes[sym]
                        strike_data[opt_type] = {
                            "ltp":       round(q.get("last_price", 0), 2),
                            "volume":    q.get("volume", 0),
                            "oi":        q.get("oi", 0),
                            "oi_change": q.get("oi_day_change", q.get("net_change", 0)),
                        }
            chain_data.append(strike_data)

        return jsonify({
            "success":     True,
            "spot_price":  spot_price,
            "atm_strike":  atm_strike,
            "expiry":      nearest_expiry.strftime("%d %b %Y"),
            "chain":       chain_data,
            "auto_login":  bool(_server_access_token),
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


# ─────────────────────────────────────────────
# Startup
# ─────────────────────────────────────────────

# Auto-login once at startup (so the dashboard works immediately after deploy)
auto_login()

# Schedule daily re-login at 8:45 AM IST
start_scheduler()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    is_dev = os.environ.get("FLASK_ENV") == "development"
    if is_dev:
        app.config["SESSION_COOKIE_SECURE"] = False
    app.run(host="0.0.0.0", port=port, debug=is_dev)
