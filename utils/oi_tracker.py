"""
oi_tracker.py  –  OI Snapshot Storage & Retrieval
===================================================
Stores option chain OI in a local SQLite database under data/oi_data.db.
"""

import os
import sqlite3
import pandas as pd
import pytz
import time
from datetime import datetime, timedelta

# -- Database file lives in the data/ folder ---------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)
DB_PATH = os.path.join(DATA_DIR, "oi_data.db")
IST = pytz.timezone("Asia/Kolkata")

# ----------------------------------------------------------------------------
# 1.  Database setup
# ----------------------------------------------------------------------------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS oi_snapshots (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            date            TEXT NOT NULL,
            label           TEXT NOT NULL,
            symbol          TEXT NOT NULL,
            expiry          TEXT NOT NULL,
            strike          REAL NOT NULL,
            instrument_type TEXT NOT NULL,
            tradingsymbol   TEXT NOT NULL,
            oi              INTEGER NOT NULL,
            saved_at        TEXT NOT NULL
        )
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_lookup
        ON oi_snapshots (date, label, symbol, expiry, strike, instrument_type)
    """)
    conn.commit()
    conn.close()
    print("[oi_tracker] DB ready:", DB_PATH)

# Remaining exact functionality inherited cleanly...
def save_snapshot(kite, label: str):
    now      = datetime.now(IST)
    today    = now.date().isoformat()
    saved_at = now.isoformat()
    rows = []

    all_inst = kite.instruments("NFO")
    df_all   = pd.DataFrame(all_inst)

    for symbol in ["NIFTY", "BANKNIFTY"]:
        spot_sym = "NSE:NIFTY 50" if symbol == "NIFTY" else "NSE:NIFTY BANK"
        try:
            spot_price = kite.quote([spot_sym])[spot_sym]["last_price"]
        except Exception as e:
            continue

        strike_diff = 50 if symbol == "NIFTY" else 100
        atm_strike  = round(spot_price / strike_diff) * strike_diff
        strikes     = [atm_strike + i * strike_diff for i in range(-15, 16)]

        df_sym  = df_all[(df_all["name"] == symbol) & (df_all["segment"] == "NFO-OPT")]
        expiries = sorted(df_sym["expiry"].dropna().unique())
        if not expiries: continue
        nearest_expiry = expiries[0]
        expiry_str = nearest_expiry.strftime("%Y-%m-%d") if hasattr(nearest_expiry, "strftime") else str(nearest_expiry)

        df_exp  = df_sym[df_sym["expiry"] == nearest_expiry]
        df_filt = df_exp[df_exp["strike"].isin(strikes)]
        opt_syms = ["NFO:" + s for s in df_filt["tradingsymbol"].tolist()]
        if not opt_syms: continue

        try:
            quotes = kite.quote(opt_syms)
        except Exception:
            continue

        for _, inst_row in df_filt.iterrows():
            nfo_sym = "NFO:" + inst_row["tradingsymbol"]
            if nfo_sym not in quotes: continue
            oi = quotes[nfo_sym].get("oi", 0)
            rows.append((
                today, label, symbol, expiry_str, float(inst_row["strike"]),
                inst_row["instrument_type"], nfo_sym, int(oi), saved_at
            ))

    if rows:
        conn = sqlite3.connect(DB_PATH)
        conn.executemany("""
            INSERT INTO oi_snapshots
              (date, label, symbol, expiry, strike, instrument_type, tradingsymbol, oi, saved_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        conn.commit()
        conn.close()

def _last_trading_day(today_date):
    d = today_date - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d

def seed_yesterday_eod(kite, symbol: str, expiry_str: str | None = None) -> bool:
    today     = datetime.now(IST).date()
    prev_day  = _last_trading_day(today)
    prev_str  = prev_day.isoformat()
    
    all_inst = kite.instruments("NFO")
    df_all   = pd.DataFrame(all_inst)
    
    spot_sym = "NSE:NIFTY 50" if symbol == "NIFTY" else "NSE:NIFTY BANK"
    try:
        spot_price = kite.quote([spot_sym])[spot_sym]["last_price"]
    except Exception:
        return False
        
    strike_diff = 50 if symbol == "NIFTY" else 100
    atm_strike  = round(spot_price / strike_diff) * strike_diff
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
            time.sleep(0.35)
            hist = kite.historical_data(token, date_formatted, date_formatted, "day", oi=True)
            if hist and len(hist) > 0:
                closing_oi = hist[-1].get("oi", 0)
                if closing_oi > 0:
                    rows.append((
                        prev_str, "EOD", symbol, target_expiry.strftime("%Y-%m-%d"),
                        float(inst_row["strike"]), inst_row["instrument_type"],
                        nfo_sym, int(closing_oi), saved_at
                    ))
        except Exception:
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
        return True
    return False

def get_eod_snapshot(kite, symbol: str, expiry_str: str | None = None) -> dict:
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
    today = datetime.now(IST).date().isoformat()
    return _load_snapshot(today, "OPEN", symbol, expiry_str)

def _load_snapshot(date_str: str, label: str, symbol: str, expiry_str: str | None) -> dict:
    if not os.path.exists(DB_PATH): return {}
    conn   = sqlite3.connect(DB_PATH)
    query  = "SELECT tradingsymbol, oi FROM oi_snapshots WHERE date = ? AND label = ? AND symbol = ?"
    params = [date_str, label, symbol]
    if expiry_str:
        query  += " AND expiry = ?"
        params.append(expiry_str)
    query += " GROUP BY tradingsymbol HAVING MAX(saved_at)"
    cur  = conn.cursor()
    cur.execute(query, params)
    rows = cur.fetchall()
    conn.close()
    return {row[0]: row[1] for row in rows}

def snapshot_status() -> dict:
    if not os.path.exists(DB_PATH): return {"db": False}
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
