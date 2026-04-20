import os
import asyncio
import pytz
import pandas as pd
from datetime import datetime
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from contextlib import asynccontextmanager
from kiteconnect import KiteConnect
from dotenv import load_dotenv

import oi_tracker

load_dotenv()
IST = pytz.timezone("Asia/Kolkata")

# Global State Container
STATE = {
    "access_token": None,
    "active": False,
    "latest_data": [],
    "last_updated": None
}

background_task = None
KITE_API_KEY = os.getenv("KITE_API_KEY")
KITE_API_SECRET = os.getenv("KITE_API_SECRET")

# Background polling process
async def poll_option_chain():
    print("[Task] Background polling started.")
    kite = KiteConnect(api_key=KITE_API_KEY, access_token=STATE["access_token"])
    
    try:
        print("[Task] Caching NFO instruments...")
        all_inst = await asyncio.to_thread(kite.instruments, "NFO")
        df_all = pd.DataFrame(all_inst)
    except Exception as e:
        print("[Task] Initial instruments fetch failed:", e)
        STATE["active"] = False
        return

    while STATE["active"]:
        try:
            spot_sym = "NSE:NIFTY 50"
            quote = await asyncio.to_thread(kite.quote, [spot_sym])
            spot_price = quote[spot_sym]["last_price"]

            # ATM +/- 25 strikes
            atm_strike = round(spot_price / 50) * 50
            target_strikes = [atm_strike + i * 50 for i in range(-25, 26)] 

            df_sym = df_all[(df_all["name"] == "NIFTY") & (df_all["segment"] == "NFO-OPT")]
            expiries = sorted(df_sym["expiry"].dropna().unique())
            
            if not expiries:
                continue
            nearest_expiry = expiries[0]
            
            df_filtered = df_sym[
                (df_sym["expiry"] == nearest_expiry) &
                (df_sym["strike"].isin(target_strikes))
            ]

            opt_syms = ["NFO:" + s for s in df_filtered["tradingsymbol"].tolist()]
            
            if opt_syms:
                opt_quotes = await asyncio.to_thread(kite.quote, opt_syms)
            else:
                opt_quotes = {}

            expiry_str = nearest_expiry.strftime("%Y-%m-%d")
            overnight_base = await asyncio.to_thread(oi_tracker.get_eod_snapshot, kite, "NIFTY", expiry_str)

            chain_data = []
            for strike in target_strikes:
                entry = {"time": datetime.now(IST).strftime("%H:%M:%S"), "strike": strike, "CE": None, "PE": None}
                for kind in ["CE", "PE"]:
                    row = df_filtered[(df_filtered["strike"] == strike) & (df_filtered["instrument_type"] == kind)]
                    if not row.empty:
                        sym = "NFO:" + row.iloc[0]["tradingsymbol"]
                        if sym in opt_quotes:
                            q = opt_quotes[sym]
                            ltp = q.get("last_price", 0)
                            curr_oi = q.get("oi", 0)
                            
                            o_base = overnight_base.get(sym, None)
                            o_chg = (curr_oi - o_base) if o_base is not None else None
                            
                            entry[kind] = {
                                "ltp": ltp,
                                "oi": curr_oi,
                                "daily_oi_change": o_chg
                            }
                chain_data.append(entry)

            STATE["latest_data"] = chain_data
            STATE["last_updated"] = datetime.now(IST).strftime("%H:%M:%S")

        except Exception as e:
            print("[Task] Error during fetching:", e)
            import traceback; traceback.print_exc()
            
        await asyncio.sleep(5)  # Requirements: fetch every 5-10s

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("[Lifespan] Booting application.")
    # Initialize our dependent sqlite db wrapper
    await asyncio.to_thread(oi_tracker.init_db)
    
    # Optional: Automatically reload previous access_token if persists
    if os.path.exists("access_token.txt"):
        with open("access_token.txt", "r") as f:
            token = f.read().strip()
            if token:
                STATE["access_token"] = token
                # Could optionally set active=True and start loop here if desired...
                
    yield
    
    print("[Lifespan] Shutting down.")
    STATE["active"] = False
    if background_task:
        background_task.cancel()

app = FastAPI(lifespan=lifespan)
templates = Jinja2Templates(directory="templates")

# You can statically mount CSS here if standard folder exists
# app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/", response_class=HTMLResponse)
async def serve_ui(request: Request):
    """Serve the sleek, mobile-friendly interface"""
    return templates.TemplateResponse("mobile_login.html", {
        "request": request, 
        "active": STATE["active"],
        "last_updated": STATE["last_updated"]
    })

@app.post("/login")
async def process_login(request_token: str = Form(...)):
    global background_task
    kite = KiteConnect(api_key=KITE_API_KEY)
    
    try:
        # Blocking network call -> threaded to avoid locking FastAPI
        data = await asyncio.to_thread(kite.generate_session, request_token, api_secret=KITE_API_SECRET)
        token = data["access_token"]
        
        # Store heavily locally so restarting the server preserves it
        with open("access_token.txt", "w") as f:
            f.write(token)
            
        STATE["access_token"] = token
        STATE["active"] = True
        
        # Kill previous active loop if re-entered
        if background_task:
            background_task.cancel()
            
        background_task = asyncio.create_task(poll_option_chain())
        print(f"[System] Success! System Started.")
        
        return JSONResponse({"status": "success", "message": "System Started Successfully!"})
        
    except Exception as e:
        print("[System] Login failed:", str(e))
        return JSONResponse({"status": "error", "message": str(e)}, status_code=400)

@app.post("/stop")
async def stop_system():
    STATE["active"] = False
    global background_task
    if background_task:
        background_task.cancel()
    return JSONResponse({"status": "success", "message": "Background task halted."})

@app.get("/data")
async def get_data():
    """Return exactly the ATM ± 25 processed array payload"""
    if not STATE["latest_data"]:
        return JSONResponse({"error": "No data available yet or system inactive."}, status_code=400)
        
    return {
        "status": "success",
        "timestamp": STATE["last_updated"],
        "data": STATE["latest_data"]
    }
