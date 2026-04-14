import os
import secrets
from datetime import datetime
import pandas as pd
from flask import Flask, request, redirect, jsonify, render_template, session
from kiteconnect import KiteConnect
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
# Use env var secret key, or generate one per process (good enough for single-instance Render free tier)
app.secret_key = os.getenv("FLASK_SECRET_KEY", secrets.token_hex(32))
app.config["SESSION_PERMANENT"] = False
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = True  # Required for HTTPS on Render

KITE_API_KEY = os.getenv("KITE_API_KEY")
KITE_API_SECRET = os.getenv("KITE_API_SECRET")

# In-memory instrument cache (resets daily)
instruments_cache = {'date': None, 'data': None}


def get_kite_client():
    if "access_token" in session:
        return KiteConnect(api_key=KITE_API_KEY, access_token=session["access_token"])
    return None


def get_nfo_instruments(kite):
    global instruments_cache
    today = datetime.now().date()
    if instruments_cache['date'] != today or instruments_cache['data'] is None:
        try:
            raw = kite.instruments(exchange="NFO")
            instruments_cache['data'] = pd.DataFrame(raw)
            instruments_cache['date'] = today
        except Exception as e:
            print("Instrument fetch error:", e)
            return None
    return instruments_cache['data']


@app.route("/")
def index():
    # Gracefully catch request_token if Kite redirects to "/" instead of "/callback"
    request_token = request.args.get("request_token")
    if request_token:
        kite = KiteConnect(api_key=KITE_API_KEY)
        try:
            data = kite.generate_session(request_token, api_secret=KITE_API_SECRET)
            session["access_token"] = data["access_token"]
            return redirect("/")
        except Exception:
            pass

    if "access_token" not in session:
        return render_template("login.html", error=request.args.get("error"))
    return render_template("index.html")


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


@app.route("/api/option-chain")
def api_option_chain():
    kite = get_kite_client()
    if not kite:
        return jsonify({"error": "Unauthorized"}), 401

    symbol = request.args.get("symbol", "NIFTY")
    if symbol not in ["NIFTY", "BANKNIFTY"]:
        return jsonify({"error": "Invalid symbol"}), 400

    # Correct NSE index quote symbols
    spot_symbol = "NSE:NIFTY 50" if symbol == "NIFTY" else "NSE:NIFTY BANK"

    try:
        # 1. Spot price
        quote = kite.quote([spot_symbol])
        if spot_symbol not in quote:
            return jsonify({"error": f"Could not fetch spot for {spot_symbol}"}), 500
        spot_price = quote[spot_symbol]['last_price']

        # 2. Instruments (cached daily)
        df = get_nfo_instruments(kite)
        if df is None:
            return jsonify({"error": "Failed to load NFO instruments"}), 500

        df_sym = df[(df['name'] == symbol) & (df['segment'] == 'NFO-OPT')]
        if df_sym.empty:
            return jsonify({"error": f"No options found for {symbol}"}), 404

        # 3. Nearest expiry
        expiries = sorted(df_sym['expiry'].dropna().unique())
        if not expiries:
            return jsonify({"error": "No expiries found"}), 404
        nearest_expiry = expiries[0]
        df_exp = df_sym[df_sym['expiry'] == nearest_expiry].copy()

        # 4. ATM ±5 strikes
        strike_diff = 50 if symbol == "NIFTY" else 100
        atm_strike = round(spot_price / strike_diff) * strike_diff
        target_strikes = [atm_strike + (i * strike_diff) for i in range(-5, 6)]
        df_filtered = df_exp[df_exp['strike'].isin(target_strikes)]

        # 5. Batch quote fetch
        opt_symbols = ["NFO:" + s for s in df_filtered['tradingsymbol'].tolist()]
        opt_quotes = kite.quote(opt_symbols) if opt_symbols else {}

        # 6. Build chain
        chain_data = []
        for strike in target_strikes:
            strike_data = {'strike': strike, 'CE': None, 'PE': None}
            for opt_type in ['CE', 'PE']:
                row = df_filtered[
                    (df_filtered['strike'] == strike) &
                    (df_filtered['instrument_type'] == opt_type)
                ]
                if not row.empty:
                    sym = "NFO:" + row.iloc[0]['tradingsymbol']
                    if sym in opt_quotes:
                        q = opt_quotes[sym]
                        strike_data[opt_type] = {
                            'ltp': round(q.get('last_price', 0), 2),
                            'volume': q.get('volume', 0),
                            'oi': q.get('oi', 0),
                            'oi_change': q.get('oi_day_change', q.get('net_change', 0)),
                        }
            chain_data.append(strike_data)

        return jsonify({
            'success': True,
            'spot_price': spot_price,
            'atm_strike': atm_strike,
            'expiry': nearest_expiry.strftime('%d %b %Y'),
            'chain': chain_data
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    is_dev = os.environ.get("FLASK_ENV") == "development"
    # Disable secure cookie for local HTTP dev
    if is_dev:
        app.config["SESSION_COOKIE_SECURE"] = False
    app.run(host="0.0.0.0", port=port, debug=is_dev)
