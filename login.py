import os
import sys
from kiteconnect import KiteConnect
from dotenv import load_dotenv, set_key

def get_login_url():
    load_dotenv()
    api_key = os.getenv("KITE_API_KEY")
    api_secret = os.getenv("KITE_API_SECRET")

    if not api_key or not api_secret:
        raise ValueError("Please provide KITE_API_KEY and KITE_API_SECRET in .env file")

    kite = KiteConnect(api_key=api_key)
    print("\n" + "="*50)
    print("Please login to this link to generate your request token:")
    print(kite.login_url())
    print("="*50 + "\n")
    return kite, api_secret

def generate_session(kite, api_secret, request_token):
    try:
        data = kite.generate_session(request_token, api_secret=api_secret)
        access_token = data["access_token"]
        print(f"\nLogin successful! Access token generated: {access_token}")
        return access_token
    except Exception as e:
        print(f"Error generating session: {e}")
        return None

if __name__ == "__main__":
    kite, api_secret = get_login_url()
    
    if len(sys.argv) > 1:
        # User passed the request token as an argument
        request_token = sys.argv[1]
        access_token = generate_session(kite, api_secret, request_token)
        if access_token:
            # Store access token in .env
            env_path = os.path.join(os.path.dirname(__file__), '.env')
            set_key(env_path, "KITE_ACCESS_TOKEN", access_token)
            print("Access token saved to .env file successfully.")
    else:
        print("INSTRUCTIONS:")
        print("1. Click the link above to log in to Kite")
        print("2. Authenticate and it will redirect to your redirect_url")
        print("3. Copy the 'request_token' from the URL bar (e.g. ?request_token=YOUR_TOKEN&action=login)")
        print("4. Run this script again with the request_token as an argument:")
        print("   python login.py <request_token>\n")
