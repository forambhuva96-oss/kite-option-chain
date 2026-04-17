"""
oi_tracker.py  –  OI Snapshot Storage & Retrieval
===================================================
Stores option chain OI in a local SQLite database.

Two types of snapshots are saved each trading day:
  - 'OPEN'  : Saved at 09:15 AM  -> used for Intraday OI Change
  - 'EOD'   : Saved at 03:29 PM  -> used as next day's Overnight OI Change baseline

Usage (from app.py):
  import oi_tracker
  oi_tracker.init_db()
  oi_tracker.save_snapshot(kite, label='EOD')
  overnight = oi_tracker.get_eod_snapshot(symbol='NIFTY')
  open_snap = oi_tracker.get_open_snapshot(symbol='NIFTY')
"""

import os
import sqlite3
import pandas as pd
import pytz
import time
from datetime import datetime, timedelta

# -- Database file lives in the same folder as this script -------------------
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "oi_data.db")
IST = pytz.timezone("Asia/Kolkata")


# ----------------------------------------------------------------------------
# 1.  Database setup
# ----------------------------------------------------------------------------
def init_db():
    """
    Create the SQLite database and tables if they don't already exist.
    Call this once when the Flask app starts.
    """
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS oi_snapshots (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            date            TEXT NOT NULL,   -- 'YYYY-MM-DD'  trading date
            label           TEXT NOT NULL,   -- 'EOD' or 'OPEN'
            symbol          TEXT NOT NULL,   -- 'NIFTY' or 'BANKNIFTY'
            expiry          TEXT NOT NULL,   -- 'YYYY-MM-DD'
            strike          REAL NOT NULL,
            instrument_type TEXT NOT NULL,   -- 'CE' or 'PE'
            tradingsymbol   TEXT NOT NULL,   -- e.g. 'NFO:NIFTY2441721750CE'
            oi              INTEGER NOT NULL,
            saved_at        TEXT NOT NULL    -- full ISO timestamp
        )
    """)
    # Index for fast lookups by date + label + symbol
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_lookup
        ON oi_snapshots (date, label, symbol, expiry, strike, instrument_type)
    """)
    conn.commit()
    conn.close()
    print("[oi_tracker] DB ready:", DB_PATH)


# ----------------------------------------------------------------------------
# 2.  Save a snapshot
# ----------------------------------------------------------------------------
def save_snapshot(kite, label: str):
    """
    Fetch live OI for NIFTY + BANKNIFTY (ATM ± 15 strikes, nearest expiry)
    and store it in the database.

    Parameters
    ----------
    kite  : KiteConnect instance (already authenticated)
    label : 'EOD'  – run at 15:29; used as tomorrow's overnight baseline
            'OPEN' – run at 09:15; used for intraday change during the day
    """
    import pandas as pd

    now      = datetime.now(IST)
    today    = now.date().isoformat()
    saved_at = now.isoformat()

    rows = []  # will collect all rows to insert in one batch

    # Fetch all NFO instruments once (usually cached in app.py already)
    all_inst = kite.instruments("NFO")
    df_all   = pd.DataFrame(all_inst)

    for symbol in ["NIFTY", "BANKNIFTY"]:
        # -- Get spot price --------------------------------------------------
        spot_sym = "NSE:NIFTY 50" if symbol == "NIFTY" else "NSE:NIFTY BANK"
        try:
            spot_price = kite.quote([spot_sym])[spot_sym]["last_price"]
        except Exception as e:
            print(f"[oi_tracker] Spot fetch failed for {symbol}: {e}")
            continue

        # -- Find ATM ±15 strikes --------------------------------------------
        strike_diff = 50 if symbol == "NIFTY" else 100
        atm_strike  = round(spot_price / strike_diff) * strike_diff
        strikes     = [atm_strike + i * strike_diff for i in range(-15, 16)]

        # -- Filter options for nearest expiry -------------------------------
        df_sym  = df_all[(df_all["name"] == symbol) & (df_all["segment"] == "NFO-OPT")]
        expiries = sorted(df_sym["expiry"].dropna().unique())
        if not expiries:
            continue
        nearest_expiry = expiries[0]
        expiry_str     = nearest_expiry.strftime("%Y-%m-%d") if hasattr(nearest_expiry, "strftime") else str(nearest_expiry)

        df_exp  = df_sym[df_sym["expiry"] == nearest_expiry]
        df_filt = df_exp[df_exp["strike"].isin(strikes)]

        opt_syms = ["NFO:" + s for s in df_filt["tradingsymbol"].tolist()]
        if not opt_syms:
            continue

        # -- Fetch live OI quotes ---------------------------------------------
        try:
            quotes = kite.quote(opt_syms)
        except Exception as e:
            print(f"[oi_tracker] Quote fetch failed for {symbol}: {e}")
            continue

        # -- Build rows for DB ------------------------------------------------
        for _, inst_row in df_filt.iterrows():
            nfo_sym = "NFO:" + inst_row["tradingsymbol"]
            if nfo_sym not in quotes:
                continue
            oi = quotes[nfo_sym].get("oi", 0)
            rows.append((
                today,
                label,
                symbol,
                expiry_str,
                float(inst_row["strike"]),
                inst_row["instrument_type"],   # CE or PE
                nfo_sym,
                int(oi),
                saved_at,
            ))

    # -- Batch insert into SQLite ---------------------------------------------
    if rows:
        conn = sqlite3.connect(DB_PATH)
        conn.executemany("""
            INSERT INTO oi_snapshots
              (date, label, symbol, expiry, strike, instrument_type, tradingsymbol, oi, saved_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        conn.commit()
        conn.close()
        print(f"[oi_tracker] OK {label} snapshot saved - {len(rows)} rows on {today}")
    else:
        print(f"[oi_tracker] WARN  No rows saved for {label} on {today}")


# ----------------------------------------------------------------------------
# 3.  Load snapshots as dicts  {tradingsymbol: oi}
# ----------------------------------------------------------------------------
def _last_trading_day(today_date):
    """Return the most recent Mon–Fri before today_date."""
    d = today_date - timedelta(days=1)
    while d.weekday() >= 5:   # 5=Sat, 6=Sun
        d -= timedelta(days=1)
    return d


def seed_yesterday_eod(kite, symbol: str, expiry_str: str | None = None) -> bool:
    """
    Backfills yesterday's EOD OI snapshot by querying Kite's historical_data API.
    Returns True if successfully seeded, False otherwise.
    """
    today     = datetime.now(IST).date()
    prev_day  = _last_trading_day(today)
    prev_str  = prev_day.isoformat()
    
    print(f"[oi_tracker] Missing EOD baseline for {symbol} on {prev_str}. Starting backfill API fetch...")
    
    all_inst = kite.instruments("NFO")
    df_all   = pd.DataFrame(all_inst)
    
    spot_sym = "NSE:NIFTY 50" if symbol == "NIFTY" else "NSE:NIFTY BANK"
    try:
        spot_price = kite.quote([spot_sym])[spot_sym]["last_price"]
    except Exception as e:
        print(f"[oi_tracker] Spot fetch failed: {e}")
        return False
        
    strike_diff = 50 if symbol == "NIFTY" else 100
    atm_strike  = round(spot_price / strike_diff) * strike_diff
    # We only backfill ATM ±10 strikes to save API calls (14 sec total)
    strikes     = [atm_strike + i * strike_diff for i in range(-10, 11)]
    
    df_sym = df_all[(df_all["name"] == symbol) & (df_all["segment"] == "NFO-OPT")]
    if df_sym.empty: return False

    if expiry_str:
        from datetime import date
        try:
            req_date = date.fromisoformat(expiry_str)
            df_sym = df_sym[df_sym["expiry"] == req_date]
        except:
            pass

    if df_sym.empty: return False
    
    expiries = sorted(df_sym["expiry"].dropna().unique())
    if not expiries: return False
    target_expiry = expiries[0]

    df_filt = df_sym[(df_sym["expiry"] == target_expiry) & (df_sym["strike"].isin(strikes))]
    if df_filt.empty: return False

    date_formatted = prev_day.strftime("%Y-%m-%d 00:00:00")
    
    rows = []
    saved_at = datetime.now().isoformat()
    
    for _, inst_row in df_filt.iterrows():
        token = inst_row["instrument_token"]
        nfo_sym = "NFO:" + inst_row["tradingsymbol"]
        try:
            # Respect 3 req/sec kite connect rate limit
            time.sleep(0.35)
            hist = kite.historical_data(token, date_formatted, date_formatted, "day", oi=True)
            if hist and len(hist) > 0:
                closing_oi = hist[-1].get("oi", 0)
                if closing_oi > 0:
                    rows.append((
                        prev_str,
                        "EOD",
                        symbol,
                        target_expiry.strftime("%Y-%m-%d"),
                        float(inst_row["strike"]),
                        inst_row["instrument_type"],
                        nfo_sym,
                        int(closing_oi),
                        saved_at
                    ))
        except Exception as e:
            print(f"[oi_tracker] Hist API error for {nfo_sym}: {e}")
            break

    if rows:
        conn = sqlite3.connect(DB_PATH)
        conn.executemany("""
            INSERT INTO oi_snapshots
              (date, label, symbol, expiry, strike, instrument_type, tradingsymbol, oi, saved_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        conn.commit()
        conn.close()
        print(f"[oi_tracker] Successfully backfilled {len(rows)} rows for EOD on {prev_str}")
        return True
    return False

def get_eod_snapshot(kite, symbol: str, expiry_str: str | None = None) -> dict:
    """
    Return previous trading day's EOD OI as  { 'NFO:XXXXX': oi, ... }
    Used for: Daily OI Change = Today's live OI − Yesterday's EOD OI
    """
    from datetime import date
    today     = datetime.now(IST).date()
    prev_day  = _last_trading_day(today)
    prev_str  = prev_day.isoformat()
    data = _load_snapshot(prev_str, "EOD", symbol, expiry_str)
    
    if not data and kite is not None:
        if seed_yesterday_eod(kite, symbol, expiry_str):
            data = _load_snapshot(prev_str, "EOD", symbol, expiry_str)
            
    return data


def get_open_snapshot(symbol: str, expiry_str: str | None = None) -> dict:
    """
    Return today's 9:15 AM OI as  { 'NFO:XXXXX': oi, ... }
    Used for: Intraday OI Change = Current OI − 9:15 AM OI
    """
    today = datetime.now(IST).date().isoformat()
    return _load_snapshot(today, "OPEN", symbol, expiry_str)


def _load_snapshot(date_str: str, label: str, symbol: str,
                   expiry_str: str | None) -> dict:
    """Internal: query DB and return { tradingsymbol: oi }."""
    if not os.path.exists(DB_PATH):
        return {}
    conn   = sqlite3.connect(DB_PATH)
    query  = """
        SELECT tradingsymbol, oi FROM oi_snapshots
        WHERE date = ? AND label = ? AND symbol = ?
    """
    params = [date_str, label, symbol]
    if expiry_str:
        query  += " AND expiry = ?"
        params.append(expiry_str)
    # If multiple rows exist for the same key (re-runs), take the latest
    query += " GROUP BY tradingsymbol HAVING MAX(saved_at)"

    cur  = conn.cursor()
    cur.execute(query, params)
    rows = cur.fetchall()
    conn.close()
    return {row[0]: row[1] for row in rows}


# ----------------------------------------------------------------------------
# 4.  Snapshot status  (used by /api/status endpoint)
# ----------------------------------------------------------------------------
def snapshot_status() -> dict:
    """Returns info about what snapshots exist for today/yesterday."""
    if not os.path.exists(DB_PATH):
        return {"db": False}

    today    = datetime.now(IST).date().isoformat()
    prev_day = _last_trading_day(datetime.now(IST).date()).isoformat()

    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()
    def count(date, label):
        cur.execute("SELECT COUNT(*) FROM oi_snapshots WHERE date=? AND label=?", (date, label))
        return cur.fetchone()[0]

    result = {
        "db":            True,
        "today_open":    count(today,    "OPEN"),
        "today_eod":     count(today,    "EOD"),
        "prev_eod":      count(prev_day, "EOD"),
        "today":         today,
        "prev_day":      prev_day,
    }
    conn.close()
    return result
