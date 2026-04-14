import os
import secrets
from datetime import datetime
import pandas as pd
from flask import Flask, request, redirect, jsonify, render_template, session
from flask_session import Session
from kiteconnect import KiteConnect
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
# Secure secret key for session management
app.secret_key = os.getenv("FLASK_SECRET_KEY", secrets.token_hex(16))
# Using filesystem so it persists across local reloads (though Render ephemeral disk deletes this on new deploys, it's fine for runtime)
app.config["SESSION_TYPE"] = "filesystem"
app.config["SESSION_PERMANENT"] = False
Session(app)

KITE_API_KEY = os.getenv("KITE_API_KEY")
KITE_API_SECRET = os.getenv("KITE_API_SECRET")

# Simple in-memory cache for instruments
instruments_cache = {
    'date': None,
    'data': None
}

def get_kite_client():
    if "access_token" in session:
        return KiteConnect(api_key=KITE_API_KEY, access_token=session["access_token"])
    return None

def get_nfo_instruments(kite):
    global instruments_cache
    today = datetime.now().date()
    # Cache instruments for the day
    if instruments_cache['date'] != today or instruments_cache['data'] is None:
        try:
            raw_instruments = kite.instruments(exchange="NFO")
            instruments_cache['data'] = pd.DataFrame(raw_instruments)
            instruments_cache['date'] = today
        except Exception as e:
            print("Error fetching instruments:", e)
            return None
    return instruments_cache['data']

@app.route("/")
def index():
    # If the user accidentally set their Kite redirect URL to "/" instead of "/callback",
    # we can intercept the token here to make their life easier!
    request_token = request.args.get("request_token")
    if request_token:
        kite = KiteConnect(api_key=KITE_API_KEY)
        try:
            data = kite.generate_session(request_token, api_secret=KITE_API_SECRET)
            session["access_token"] = data["access_token"]
            return redirect("/") # Redirect to clean URL
        except Exception as e:
            pass # Ignore and fall through to login

    if "access_token" not in session:
        return render_template("login.html")
    return render_template("index.html")

@app.route("/login")
def login():
    kite = KiteConnect(api_key=KITE_API_KEY)
    return redirect(kite.login_url())

@app.route("/callback")
def callback():
    request_token = request.args.get("request_token")
    if not request_token:
        return "No request token found in URL", 400

    kite = KiteConnect(api_key=KITE_API_KEY)
    try:
        data = kite.generate_session(request_token, api_secret=KITE_API_SECRET)
        session["access_token"] = data["access_token"]
        return redirect("/")
    except Exception as e:
        return f"Authentication failed: {str(e)}", 500

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
        
    spot_symbol = f"NSE:{symbol} 50" if symbol == "NIFTY" else "NSE:NIFTY BANK"
    
    try:
        # 1. Fetch Spot Price
        quote = kite.quote([spot_symbol])
        if spot_symbol not in quote:
            return jsonify({"error": "Failed to fetch spot price"}), 500
            
        spot_price = quote[spot_symbol]['last_price']
        
        # 2. Get Instruments mapping
        df = get_nfo_instruments(kite)
        if df is None:
             return jsonify({"error": "Failed to fetch instruments"}), 500
             
        df_sym = df[(df['name'] == symbol) & (df['segment'] == 'NFO-OPT')]
        
        if df_sym.empty:
            return jsonify({"error": "No options found"}), 404
            
        # Get nearest expiry
        expiries = sorted(df_sym['expiry'].unique())
        if not expiries:
            return jsonify({"error": "No expiries found"}), 404
        nearest_expiry = expiries[0]
        
        # Filter by nearest expiry
        df_exp = df_sym[df_sym['expiry'] == nearest_expiry]
        
        # 3. Find ATM and filter strikes +/- 5
        strike_diff = 50 if symbol == "NIFTY" else 100
        atm_strike = round(spot_price / strike_diff) * strike_diff
        
        target_strikes = []
        for i in range(-5, 6):
            target_strikes.append(atm_strike + (i * strike_diff))
            
        df_filtered = df_exp[df_exp['strike'].isin(target_strikes)]
        
        # 4. Fetch quotes for these specific symbols
        opt_symbols = ["NFO:" + s for s in df_filtered['tradingsymbol'].tolist()]
        opt_quotes = kite.quote(opt_symbols)
        
        # 5. Structure data for the frontend
        chain_data = []
        for strike in target_strikes:
            strike_data = {'strike': strike, 'CE': None, 'PE': None}
            
            # CE Data
            ce_row = df_filtered[(df_filtered['strike'] == strike) & (df_filtered['instrument_type'] == 'CE')]
            if not ce_row.empty:
                sym = "NFO:" + ce_row.iloc[0]['tradingsymbol']
                if sym in opt_quotes:
                    q = opt_quotes[sym]
                    strike_data['CE'] = {
                        'ltp': q.get('last_price', 0),
                        'volume': q.get('volume', 0),
                        'oi': q.get('oi', 0),
                        'oi_change_pct': q.get('net_change', 0) # Provide net price change as a substitute if oi change missing
                    }
            
            # PE Data
            pe_row = df_filtered[(df_filtered['strike'] == strike) & (df_filtered['instrument_type'] == 'PE')]
            if not pe_row.empty:
                sym = "NFO:" + pe_row.iloc[0]['tradingsymbol']
                if sym in opt_quotes:
                    q = opt_quotes[sym]
                    strike_data['PE'] = {
                        'ltp': q.get('last_price', 0),
                        'volume': q.get('volume', 0),
                        'oi': q.get('oi', 0),
                        'oi_change_pct': q.get('net_change', 0)
                    }
                    
            chain_data.append(strike_data)
            
        return jsonify({
            'success': True,
            'spot_price': spot_price,
            'atm_strike': atm_strike,
            'expiry': nearest_expiry.strftime('%Y-%m-%d'),
            'chain': chain_data
        })
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    # Ensure port is binding correctly for Render (defaults to 10000 usually, but app.run is for local dev anyway)
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
