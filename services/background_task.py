import asyncio
import pandas as pd
import pytz
from datetime import datetime
import traceback
import logging
import os
from dotenv import load_dotenv

from core.redis_layer import publish_delta
from services.kite_auth import get_kite_client
from services.nse_bhavcopy import GLOBAL_NSE_CACHE
from utils import oi_tracker

load_dotenv()
IST = pytz.timezone("Asia/Kolkata")
logger = logging.getLogger("app")

STATE = {
    "status": "idle",
    "latest_data": [],
    "last_updated": None
}

# In-Memory Cache to eliminate SQLite I/O bottlenecks during live ticks
EOD_CACHE = {}

_active_task = None

def get_latest_data() -> dict:
    if STATE["status"] != "running" and not STATE["latest_data"]:
        return {"error": f"System is inactive. Status: {STATE['status']}", "status": "error", "system_state": STATE["status"]}
        
    return {
        "status": "success",
        "timestamp": STATE["last_updated"],
        "data": STATE["latest_data"],
        "spot_price": STATE.get("spot_price", 0),
        "atm_strike": STATE.get("atm_strike", 0),
        "expiry": STATE.get("expiry", "Unknown"),
        "all_expiries": STATE.get("all_expiries", []),
        "system_state": STATE["status"]
    }

def get_system_status() -> dict:
    return {
        "active": STATE["status"] == "running",
        "status": STATE["status"],
        "last_updated": STATE["last_updated"]
    }

def get_thresholds(current_time: datetime):
    """
    Extracts dynamic thresholds from .env file explicitly based on native time.
    Time phases: 9:15-9:30 (High), 9:30-12:00 (Normal), 12:00-13:30 (Low), 13:30+ (Normal)
    """
    h = current_time.hour
    m = current_time.minute
    total_mins = h * 60 + m
    
    # Defaults from .env
    norm_oi = int(os.getenv("DEFAULT_OI_THRESH", "4000"))
    norm_px = float(os.getenv("DEFAULT_PX_THRESH", "0.5"))
    high_oi = int(os.getenv("HIGH_VOL_OI_THRESH", "8000"))
    high_px = float(os.getenv("HIGH_VOL_PX_THRESH", "1.0"))
    low_oi = int(os.getenv("LOW_VOL_OI_THRESH", "7000"))
    low_px = float(os.getenv("LOW_VOL_PX_THRESH", "0.8"))
    
    if total_mins < (9*60 + 30):
        return high_oi, high_px
    elif (12*60) <= total_mins < (13*60 + 30):
        return low_oi, low_px
    else:
        return norm_oi, norm_px

def compute_signal_and_action(price_change, oi_change, oi_thr, px_thr):
    """
    Classifies magnitude into Weak, Moderate, Strong.
    Output: signal, strength, action
    """
    if abs(price_change) < px_thr or abs(oi_change) < oi_thr:
        return "No Trade", "weak", "NO TRADE"
        
    strength = "strong" if (abs(price_change) >= 2*px_thr and abs(oi_change) >= 2*oi_thr) else "moderate"
    
    signal = "Neutral"
    action = "NO TRADE"
    
    if price_change > 0 and oi_change > 0:
        signal = "Long Buildup"
        if strength == "strong": action = "BUY CALL"
    elif price_change < 0 and oi_change > 0:
        signal = "Short Buildup"
        if strength == "strong": action = "BUY PUT"
    elif price_change > 0 and oi_change < 0:
        signal = "Short Covering"
        if strength == "strong": action = "EXIT PUT"
    elif price_change < 0 and oi_change < 0:
        signal = "Long Unwinding"
        if strength == "strong": action = "EXIT CALL"
        
    return signal, strength, action

def _compute_delta(old_state: dict, new_state: dict, seq_id: int) -> dict:
    if not old_state:
        return None
        
    new_timestamp = new_state.get("last_updated")
    if new_timestamp == old_state.get("last_updated"):
        return None 
        
    delta = {
        "type": "DELTA",
        "seq_id": seq_id,
        "timestamp": new_timestamp,
        "spot_price": new_state.get("spot_price"),
        "atm_strike": new_state.get("atm_strike"),
        "expiry": new_state.get("expiry"),
        "chain_updates": {}
    }
    
    old_chain = {str(item["strike"]): item for item in old_state.get("latest_data", [])}
    new_chain = new_state.get("latest_data", [])
    
    updates = False
    
    for item in new_chain:
        strike = str(item["strike"])
        old_item = old_chain.get(strike)
        if not old_item:
            delta["chain_updates"][strike] = item
            updates = True
            continue
            
        strike_updates = {}
        for side in ["CE", "PE"]:
            if item.get(side) and old_item.get(side):
                side_diff = {}
                for k, v in item[side].items():
                    if old_item[side].get(k) != v:
                        side_diff[k] = v
                if side_diff:
                    strike_updates[side] = side_diff
            elif item.get(side):
                strike_updates[side] = item[side]
                
        if strike_updates:
            delta["chain_updates"][strike] = strike_updates
            updates = True
            
    if not updates:
        return None
        
    return delta

async def _poll_option_chain(access_token: str):
    logger.info("Algorithmic Signal Engine Task Running")
    kite = get_kite_client(access_token)
    
    try:
        all_inst = await asyncio.to_thread(kite.instruments, "NFO")
        df_all = pd.DataFrame(all_inst)
    except Exception as e:
        logger.error(f"Initial instruments fetch failed: {e}")
        STATE["status"] = "error"
        return

    # Memory Matrices
    previous_state = {}         # Stores previous tick's OI/Price
    confirmation_state = {}     # Stores previous cycle's ACTION (for consecutive alerting)
    last_saved_minute = None
    
    _last_full_state = {}
    master_seq_id = 1

    while STATE["status"] == "running":
        try:
            spot_sym = "NSE:NIFTY 50"
            quote = await asyncio.to_thread(kite.quote, [spot_sym])
            spot_price = quote[spot_sym]["last_price"]

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
            
            # Smart Cache Layer: Load from SQLite ONCE per expiry
            if expiry_str not in EOD_CACHE or not EOD_CACHE[expiry_str]:
                baseline_data = await asyncio.to_thread(oi_tracker.get_eod_snapshot, kite, "NIFTY", expiry_str)
                if not baseline_data:
                    # Fallback to OPEN snapshot if yesterday's historical was completely empty
                    baseline_data = await asyncio.to_thread(oi_tracker.get_open_snapshot, "NIFTY", expiry_str)
                    if not baseline_data:
                        await asyncio.to_thread(oi_tracker.save_snapshot, kite, "OPEN")
                        baseline_data = await asyncio.to_thread(oi_tracker.get_open_snapshot, "NIFTY", expiry_str)
                
                # Only cache if data was successfully fetched to avoid caching network failures permanently
                if baseline_data:
                    EOD_CACHE[expiry_str] = baseline_data
            
            baseline_data = EOD_CACHE.get(expiry_str, {})

            chain_data = []
            hist_records = []
            
            now_dt = datetime.now(IST)
            now_time = now_dt.strftime("%H:%M:%S")
            current_minute = now_dt.minute
            
            # Fetch Dynamic Base Thresholds cleanly from env
            oi_thr, px_thr = get_thresholds(now_dt)
            
            for strike in target_strikes:
                entry = {"strike": strike, "CE": None, "PE": None}
                for kind in ["CE", "PE"]:
                    row = df_filtered[(df_filtered["strike"] == strike) & (df_filtered["instrument_type"] == kind)]
                    if not row.empty:
                        sym = "NFO:" + row.iloc[0]["tradingsymbol"]
                        if sym in opt_quotes:
                            q = opt_quotes[sym]
                            ltp = q.get("last_price", 0)
                            curr_oi = q.get("oi", 0)
                            volume = q.get("volume", 0)
                            
                            o_base = None
                            
                            # STEP 1: Deep Query Institutional NSE Memory Array
                            if "NIFTY" in GLOBAL_NSE_CACHE and expiry_str in GLOBAL_NSE_CACHE["NIFTY"]:
                                if str(strike) in GLOBAL_NSE_CACHE["NIFTY"][expiry_str]:
                                    nse_map = GLOBAL_NSE_CACHE["NIFTY"][expiry_str][str(strike)].get(kind)
                                    if nse_map:
                                        o_base = nse_map["open_interest"]
                            
                            # STEP 2: Pure Database Integrity Fallback Layer (Offline API Shield)
                            if o_base is None:
                                o_base = baseline_data.get(sym)
                            
                            # STEP 3: Safe Null Fallback Boundary to Prevent Ghost 0 Plots
                            if o_base is None:
                                baseline_oi = None
                                intraday_chg = None
                            else:
                                baseline_oi = o_base
                                intraday_chg = curr_oi - baseline_oi

                            prev = previous_state.get(sym, {"oi": curr_oi, "ltp": ltp})
                            momentum_oi = curr_oi - prev["oi"]
                            price_chg = ltp - prev["ltp"]
                            
                            # Filter Engine Execution
                            signal, strength, action = compute_signal_and_action(price_chg, momentum_oi, oi_thr, px_thr)
                            
                            # Cache Consecutivity System (2-Cycle Exact Validation)
                            prev_action = confirmation_state.get(sym, "NO TRADE")
                            alert = False
                            if action != "NO TRADE" and action == prev_action:
                                alert = True
                                logger.info(f"🚨 ALERT! Strong confirmed {action} signal dynamically mapped on {sym}!")

                            entry[kind] = {
                                "ltp": ltp,
                                "oi": curr_oi,
                                "volume": volume,
                                "intraday_oi_change": intraday_chg,
                                "momentum_oi_change": momentum_oi,
                                "signal": signal,
                                "strength": strength,
                                "action": action,
                                "alert": alert
                            }
                            
                            previous_state[sym] = {"oi": curr_oi, "ltp": ltp}
                            confirmation_state[sym] = action
                            
                            # Queue 1-minute historical DB intercept
                            if current_minute != last_saved_minute and action != "NO TRADE":
                                hist_records.append((now_dt.isoformat(), strike, curr_oi, ltp, signal, strength, action))
                                
                chain_data.append(entry)

            # Dump Historical Intercept dynamically
            if current_minute != last_saved_minute and hist_records:
                await asyncio.to_thread(oi_tracker.save_signal_snapshot, hist_records)
                last_saved_minute = current_minute

            STATE["latest_data"] = chain_data
            STATE["last_updated"] = now_time
            STATE["spot_price"] = spot_price
            STATE["atm_strike"] = atm_strike
            STATE["expiry"] = expiry_str
            STATE["all_expiries"] = [{"label": e.strftime("%d %b %Y"), "value": e.strftime("%Y-%m-%d")} for e in expiries[:5]]

            # Execute Local Engine Sync Calculation
            delta_chunk = _compute_delta(_last_full_state, STATE, master_seq_id)
            _last_full_state = dict(STATE)

            try:
                from core.broadcaster import manager
                # Always broadcast in-process (works without Redis)
                asyncio.create_task(manager.broadcast(STATE))

                # Also publish to Redis if available (for multi-node scaling)
                from core.redis_layer import redis_client
                if redis_client and delta_chunk:
                    master_seq_id += 1
                    full_payload = {
                        "type": "FULL",
                        "seq_id": master_seq_id,
                        "timestamp": STATE.get("last_updated", ""),
                        "spot_price": STATE.get("spot_price", 0),
                        "atm_strike": STATE.get("atm_strike", 0),
                        "expiry": STATE.get("expiry", ""),
                        "chain": STATE.get("latest_data", [])
                    }
                    from core.redis_layer import publish_delta
                    asyncio.create_task(publish_delta("nifty:stream", delta_chunk, full_payload))
            except Exception as broadcast_err:
                logger.error(f"Broadcast error: {broadcast_err}")

        except Exception as e:
            err_str = str(e).lower()
            if "token" in err_str or "forbidden" in err_str or "unauthorized" in err_str or "invalid" in err_str:
                logger.error(f"Critical Auth Error: {e}")
                STATE["status"] = "login_required"
                break
            else:
                logger.error(f"API Error during fetching: {e}.")
                await asyncio.sleep(3)
                continue
            
        await asyncio.sleep(8) 

def start_polling(access_token: str):
    global _active_task
    STATE["status"] = "running"
    if _active_task and not _active_task.done():
        _active_task.cancel()
    _active_task = asyncio.create_task(_poll_option_chain(access_token))

def stop_polling():
    global _active_task
    STATE["status"] = "idle"
    if _active_task:
        _active_task.cancel()
        _active_task = None
