import asyncio
import pandas as pd
import pytz
from datetime import datetime
import traceback

from services.kite_auth import get_kite_client
from utils import oi_tracker

IST = pytz.timezone("Asia/Kolkata")

# Global State Container Shared purely within this service
STATE = {
    "active": False,
    "latest_data": [],
    "last_updated": None
}

_active_task = None

def get_latest_data() -> dict:
    if not STATE["active"] and not STATE["latest_data"]:
        return {"error": "System is idle or inactive", "status": "error"}
        
    return {
        "status": "success",
        "timestamp": STATE["last_updated"],
        "data": STATE["latest_data"]
    }

def get_system_status() -> dict:
    return {
        "active": STATE["active"],
        "last_updated": STATE["last_updated"]
    }

async def _poll_option_chain(access_token: str):
    print("Background Task Running")
    kite = get_kite_client(access_token)
    
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

            atm_strike = round(spot_price / 50) * 50
            target_strikes = [atm_strike + i * 50 for i in range(-25, 26)] 

            df_sym = df_all[(df_all["name"] == "NIFTY") & (df_all["segment"] == "NFO-OPT")]
            expiries = sorted(df_sym["expiry"].dropna().unique())
            
            if not expiries:
                await asyncio.sleep(5)
                continue
                
            nearest_expiry = expiries[0]
            df_filtered = df_sym[
                (df_sym["expiry"] == nearest_expiry) &
                (df_sym["strike"].isin(target_strikes))
            ]

            opt_syms = ["NFO:" + s for s in df_filtered["tradingsymbol"].tolist()]
            opt_quotes = await asyncio.to_thread(kite.quote, opt_syms) if opt_syms else {}

            expiry_str = nearest_expiry.strftime("%Y-%m-%d")
            open_base = await asyncio.to_thread(oi_tracker.get_open_snapshot, "NIFTY", expiry_str)
            if not open_base:
                print("[Task] Initializing today's baseline OI...")
                await asyncio.to_thread(oi_tracker.save_snapshot, kite, "OPEN")
                open_base = await asyncio.to_thread(oi_tracker.get_open_snapshot, "NIFTY", expiry_str)

            chain_data = []
            now_time = datetime.now(IST).strftime("%H:%M:%S")
            
            for strike in target_strikes:
                entry = {"time": now_time, "strike": strike, "CE": None, "PE": None}
                for kind in ["CE", "PE"]:
                    row = df_filtered[(df_filtered["strike"] == strike) & (df_filtered["instrument_type"] == kind)]
                    if not row.empty:
                        sym = "NFO:" + row.iloc[0]["tradingsymbol"]
                        if sym in opt_quotes:
                            q = opt_quotes[sym]
                            ltp = q.get("last_price", 0)
                            curr_oi = q.get("oi", 0)
                            
                            o_base = open_base.get(sym)
                            baseline_oi = o_base if o_base is not None else curr_oi
                            o_chg = curr_oi - baseline_oi
                            
                            entry[kind] = {
                                "ltp": ltp,
                                "oi": curr_oi,
                                "daily_oi_change": o_chg
                            }
                chain_data.append(entry)

            STATE["latest_data"] = chain_data
            STATE["last_updated"] = now_time

        except Exception as e:
            print("[Task] Error during fetching:", e)
            traceback.print_exc()
            
        # Requirement: fetch every 5-10 seconds natively
        await asyncio.sleep(5) 

def start_polling(access_token: str):
    global _active_task
    STATE["active"] = True
    
    # Singleton check to prevent multiple active loops natively
    if _active_task and not _active_task.done():
        _active_task.cancel()
        
    _active_task = asyncio.create_task(_poll_option_chain(access_token))

def stop_polling():
    global _active_task
    STATE["active"] = False
    if _active_task:
        _active_task.cancel()
        _active_task = None
