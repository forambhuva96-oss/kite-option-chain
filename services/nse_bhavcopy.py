import pandas as pd
import requests
from io import BytesIO
import zipfile
import logging
from datetime import datetime, timedelta
import pytz

logger = logging.getLogger("nse_engine")

# Global RAM Cache to strictly enforce "Zero I/O loops" during 50ms tick processing
# Target structure: { "NIFTY": { expiry: { strike: { "CE": { oi: X }, "PE": { oi: X } } } } }
GLOBAL_NSE_CACHE = {}

class NSEBhavcopyEngine:
    def __init__(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        }
        self.IST = pytz.timezone("Asia/Kolkata")

    def get_last_trading_day_str(self):
        """Returns the last NSE trading day in 'DD-MM-YYYY' format cleanly."""
        d = datetime.now(self.IST).date() - timedelta(days=1)
        while d.weekday() >= 5: # Skip weekends
            d -= timedelta(days=1)
        return d.strftime("%d-%m-%Y")

    def fetch_current_bhavcopy(self):
        """
        Orchestrates grabbing the most recent acceptable NSE market date and loading it directly into memory.
        """
        target_date_str = self.get_last_trading_day_str()
        
        parsed_data = self._fetch_and_normalize(target_date_str)
        if not parsed_data:
            # Try one day earlier (to handle generic public local holidays seamlessly without external calendar APIs)
            target_dt = datetime.strptime(target_date_str, "%d-%m-%Y") - timedelta(days=1)
            while target_dt.weekday() >= 5:
                target_dt -= timedelta(days=1)
            parsed_data = self._fetch_and_normalize(target_dt.strftime("%d-%m-%Y"))

        if parsed_data:
            GLOBAL_NSE_CACHE.clear()
            GLOBAL_NSE_CACHE.update({"NIFTY": parsed_data})
            logger.info("🟢 NSE Bhavcopy Successfully Extracted into GLOBAL RAM CACHE")
            return True
        else:
            logger.error("🔴 Failed to initialize NSE Bhavcopy directly. Broker Fallbacks remain active.")
            return False

    def _fetch_and_normalize(self, date_str: str) -> dict:
        """
        Fetches official NSE Bhavcopy ZIP for `date_str`, dumps to memory, and normalizes NIFTY Options.
        """
        try:
            dd, mm, yyyy = date_str.split('-')[0], date_str.split('-')[1], date_str.split('-')[2]
            month_map = {"01":"JAN", "02":"FEB", "03":"MAR", "04":"APR", "05":"MAY", "06":"JUN", 
                         "07":"JUL", "08":"AUG", "09":"SEP", "10":"OCT", "11":"NOV", "12":"DEC"}
            mm_str = month_map[mm]
            
            url = f"https://archives.nseindia.com/content/historical/DERIVATIVES/{yyyy}/{mm_str}/fo{dd}{mm_str}{yyyy}bhav.csv.zip"
            logger.info(f"Downloading Official NSE Metrics from: {url}")
            
            res = requests.get(url, headers=self.headers, timeout=10)
            if res.status_code != 200:
                logger.warning(f"NSE returned {res.status_code} for {dd}-{mm_str}-{yyyy}")
                return None
                
            with zipfile.ZipFile(BytesIO(res.content)) as z:
                filename = z.namelist()[0]
                with z.open(filename) as f:
                    df = pd.read_csv(f)
                    
            # Native normalization pipeline bounded strictly to NIFTY Opts
            df = df[(df['SYMBOL'] == 'NIFTY') & (df['INSTRUMENT'] == 'OPTIDX')]
            
            normalized_cache = {}
            for _, row in df.iterrows():
                # NSE provides EXPIRY_DT natively as '27-Jun-2024' typically, standardizing to YYYY-MM-DD
                expiry = pd.to_datetime(row['EXPIRY_DT']).strftime("%Y-%m-%d")
                strike = str(int(float(row['STRIKE_PR'])))
                option_type = row['OPTION_TYP'] # Exact 'CE' or 'PE' mapped natively
                oi = int(row['OPEN_INT'])
                
                if expiry not in normalized_cache:
                    normalized_cache[expiry] = {}
                if strike not in normalized_cache[expiry]:
                    normalized_cache[expiry][strike] = {}
                    
                normalized_cache[expiry][strike][option_type] = {
                    "open_interest": oi,
                    "previous_close": row['CLOSE']
                }
            return normalized_cache
            
        except Exception as e:
            logger.error(f"NSE Bhavcopy parser exception: {str(e)}")
            return None

# Singleton orchestrator for direct system integration
nse_engine = NSEBhavcopyEngine()
