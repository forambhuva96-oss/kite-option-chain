import asyncio
import pandas as pd
import pytz
from datetime import datetime
import traceback
import logging

from services.kite_auth import get_kite_client
from utils import oi_tracker

IST = pytz.timezone("Asia/Kolkata")

logger = logging.getLogger("app")

# Global State Container Shared purely within this service
STATE = {
    "status": "idle",
    "latest_data": [],
    "last_updated": None
}

_active_task = None

def get_latest_data() -> dict:
    if STATE["status"] != "running" and not STATE["latest_data"]:
        return {"error": f"System is inactive. Status: {STATE['status']}", "status": "error", "system_state": STATE["status"]}
        
    return {
        "status": "success",
        "timestamp": STATE["last_updated"],
        "data": STATE["latest_data"],
        "system_state": STATE["status"]
    }

def get_system_status() -> dict:
    return {
        "active": STATE["status"] == "running",
        "status": STATE["status"],
        "last_updated": STATE["last_updated"]
    }

def compute_signal(price_change, oi_change):
    # Pure exact mathematical mapping
    if price_change > 0 and oi_change > 0:
        return "Long Buildup"
    elif price_change < 0 and oi_change > 0:
        return "Short Buildup"
    elif price_change > 0 and oi_change < 0:
        return "Short Covering"
    elif price_change < 0 and oi_change < 0:
        return "Long Unwinding"
    return "Neutral"

async def _poll_option_chain(access_token: str):
    logger.info("Background Task Running")
    kite = get_kite_client(access_token)
    
    try:
        all_inst = await asyncio.to_thread(kite.instruments, "NFO")
        df_all = pd.DataFrame(all_inst)
    except Exception as e:
        logger.error(f"Initial instruments fetch failed: {e}")
        STATE["status"] = "error"
        return

    # In-memory dictionary to store the preceding loop's raw parameters
    # Layout: { "NFO:NIFTY...": {"oi": X, "ltp": Y} }
    previous_state = {}

    while STATE["status"] == "running":
        try:
            spot_sym = "NSE:NIFTY 50"
            quote = await asyncio.to_thread(kite.quote, [spot_sym])
            spot_price = quote[spot_sym]["last_price"]

            # Optimize API limits (ATM +/- 15 strikes securely)
            atm_strike = round(spot_price / 50) * 50
            target_strikes = [atm_strike + i * 50 for i in range(-15, 16)] 

            df_sym = df_all[(df_all["name"] == "NIFTY") & (df_all["segment"] == "NFO-OPT")]
            expiries = sorted(df_sym["expiry"].dropna().unique())
            
            if not expiries:
                await asyncio.sleep(8)
                continue
                
            nearest_expiry = expiries[0]
            df_filtered = df_sym[
                (df_sym["expiry"] == nearest_expiry) &
                (df_sym["strike"].isin(target_strikes))
            ]

            opt_syms = ["NFO:" + s for s in df_filtered["tradingsymbol"].tolist()]
            opt_quotes = await asyncio.to_thread(kite.quote, opt_syms) if opt_syms else {}

            expiry_str = nearest_expiry.strftime("%Y-%m-%d")
            # Baseline logic rigidly checking date mapping inside get_open_snapshot natively
            open_base = await asyncio.to_thread(oi_tracker.get_open_snapshot, "NIFTY", expiry_str)
            if not open_base:
                logger.info("Initializing today's baseline OI explicitly (locked strictly to today's date).")
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
                            
                            # 1. Daily Intraday OI Change (Current - Fixed Morning Baseline)
                            o_base = open_base.get(sym)
                            baseline_oi = o_base if o_base is not None else curr_oi
                            intraday_chg = curr_oi - baseline_oi

                            # 2. Momentum OI & Signal Engine
                            prev = previous_state.get(sym, {"oi": curr_oi, "ltp": ltp})
                            momentum_oi = curr_oi - prev["oi"]
                            price_chg = ltp - prev["ltp"]
                            
                            signal = compute_signal(price_chg, momentum_oi)
                            
                            entry[kind] = {
                                "ltp": ltp,
                                "oi": curr_oi,
                                "intraday_oi_change": intraday_chg,
                                "momentum_oi_change": momentum_oi,
                                "signal": signal
                            }
                            
                            # Update cycle memory
                            previous_state[sym] = {"oi": curr_oi, "ltp": ltp}
                            
                chain_data.append(entry)

            STATE["latest_data"] = chain_data
            STATE["last_updated"] = now_time

        except Exception as e:
            err_str = str(e).lower()
            # Intercepts TokenExceptions explicitly
            if "token" in err_str or "forbidden" in err_str or "unauthorized" in err_str or "invalid" in err_str:
                logger.error(f"Critical Auth Error: Token Invalid or Expired. Halting loop natively. {e}")
                STATE["status"] = "login_required"
                break
            else:
                logger.error(f"API Error during fetching: {e}. Retrying securely in 3 seconds to prevent crash.")
                await asyncio.sleep(3)
                continue
            
        # Requirement: fetch every 8-10 seconds natively to relieve API pressure
        await asyncio.sleep(8) 

def start_polling(access_token: str):
    global _active_task
    STATE["status"] = "running"
    
    # Singleton check to prevent multiple active loops natively
    if _active_task and not _active_task.done():
        logger.warning("Singleton prevention: Old loop detected, isolating.")
        _active_task.cancel()
        
    _active_task = asyncio.create_task(_poll_option_chain(access_token))

def stop_polling():
    global _active_task
    STATE["status"] = "idle"
    if _active_task:
        _active_task.cancel()
        _active_task = None
