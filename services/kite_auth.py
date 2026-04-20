import os
from kiteconnect import KiteConnect

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
TOKEN_FILE = os.path.join(DATA_DIR, "access_token.txt")

KITE_API_KEY = os.getenv("KITE_API_KEY")
KITE_API_SECRET = os.getenv("KITE_API_SECRET")

def get_kite_client(access_token: str | None = None) -> KiteConnect:
    if access_token:
        return KiteConnect(api_key=KITE_API_KEY, access_token=access_token)
    return KiteConnect(api_key=KITE_API_KEY)

def generate_session_from_token(request_token: str) -> str:
    kite = KiteConnect(api_key=KITE_API_KEY)
    # This is a synchronous network call
    data = kite.generate_session(request_token, api_secret=KITE_API_SECRET)
    token = data["access_token"]
    
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
        
    with open(TOKEN_FILE, "w") as f:
        f.write(token)
    return token

def load_saved_token() -> str | None:
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "r") as f:
            token = f.read().strip()
            if token:
                return token
    return None
