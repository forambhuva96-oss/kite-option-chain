import math
import random
from datetime import datetime, timedelta

def _ncdf(x):
    return (1.0 + math.erf(x / math.sqrt(2.0))) / 2.0

def _bs_price(S, K, T, r, sigma, kind):
    if T <= 0 or sigma <= 0:
        return max(0.0, (S - K) if kind == 'CE' else (K - S))
    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    if kind == 'CE':
        return S * _ncdf(d1) - K * math.exp(-r * T) * _ncdf(d2)
    return K * math.exp(-r * T) * _ncdf(-d2) - S * _ncdf(-d1)

def get_mock_fallback(symbol, requested_expiry=None):
    base_spot = 22250.0 if symbol == "NIFTY" else 47800.0
    
    # Add a bit of random walk to spot price so it looks live
    # It walks around the base spot
    noise = random.uniform(-0.001, 0.001)
    spot_price = round(base_spot * (1 + noise), 2)
    
    now = datetime.now()
    days_ahead = 3 - now.weekday()
    if days_ahead <= 0: days_ahead += 7
    mock_expiry = now.date() + timedelta(days=days_ahead)
    
    strike_diff = 50 if symbol == "NIFTY" else 100
    atm_strike = round(spot_price / strike_diff) * strike_diff
    target_strikes = [atm_strike + i * strike_diff for i in range(-10, 11)]
    
    chain_data = []
    T = 5.0 / 365.0
    r = 0.065
    
    for strike in target_strikes:
        entry = {"strike": strike}
        
        for kind in ["CE", "PE"]:
            # Basic smile logic: OTM options have slightly higher IV
            otm_amount = (strike - spot_price) if kind == "CE" else (spot_price - strike)
            otm_pct = max(0, otm_amount) / spot_price
            vol = 0.12 + (otm_pct * 1.5) 
            
            price = _bs_price(spot_price, strike, T, r, vol, kind)
            # Add micro noise to price
            ltp = price * random.uniform(0.99, 1.01)
            
            # Simple greek approximation for mock
            if kind == 'CE':
                delta = 0.5 if strike == atm_strike else (0.8 if strike < atm_strike else 0.2)
            else:
                delta = -0.5 if strike == atm_strike else (-0.8 if strike > atm_strike else -0.2)
                
            entry[kind] = {
                "ltp": max(0.05, round(ltp, 2)),
                "volume": random.randint(1000, 500000),
                "oi": random.randint(5000, 2000000),
                "oi_change": random.randint(-50000, 50000),
                "intraday_oi_chg": random.randint(-150000, 150000),
                "iv": round(vol * 100, 2),
                "delta": round(delta, 3),
                "gamma": 0.0125,
                "theta": -8.50,
                "vega": 12.20
            }
        
        chain_data.append(entry)
        
    return {
        "success": True,
        "is_mock": True,
        "spot_price": spot_price,
        "atm_strike": atm_strike,
        "expiry": mock_expiry.strftime("%d %b %Y"),
        "expiry_val": mock_expiry.strftime("%Y-%m-%d"),
        "all_expiries": [
             {"value": mock_expiry.strftime("%Y-%m-%d"), "label": mock_expiry.strftime("%d %b '%y")}
        ],
        "chain": chain_data,
        "auto_login": False
    }
