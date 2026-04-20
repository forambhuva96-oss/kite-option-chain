import asyncio
import os
import uvicorn
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
from dotenv import load_dotenv
import logging

os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/app.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("app")

from services import kite_auth
from services import background_task
from utils import oi_tracker

load_dotenv()

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Server Started")
    
    # 1. Initialize DB safely
    await asyncio.to_thread(oi_tracker.init_db)
    
    # 2. Check Auto-Resume Capability
    saved_token = kite_auth.load_saved_token()
    if saved_token:
        logger.info("System Auto-Resumed")
        background_task.start_polling(saved_token)
    else:
        logger.info("[Lifespan] No active token found. Awaiting mobile login.")

    yield
    logger.info("[Lifespan] Shutting down gracefully...")
    background_task.stop_polling()

app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

@app.get("/dashboard", response_class=HTMLResponse)
async def serve_dashboard(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={}
    )

@app.get("/", response_class=HTMLResponse)
async def serve_mobile_controller(request: Request):
    status = background_task.get_system_status()
    # Pull KITE_API_KEY directly from environment for frontend integration
    kite_api_key = os.getenv("KITE_API_KEY", "")
    return templates.TemplateResponse(
        request=request,
        name="mobile_login.html", 
        context={
            "active": status["active"],
            "system_state": status["status"],
            "last_updated": status["last_updated"],
            "kite_api_key": kite_api_key
        }
    )

@app.post("/login")
async def process_login(request_token: str = Form(...)):
    try:
        # Generate and save access_token via kite_auth service wrapper
        token = await asyncio.to_thread(kite_auth.generate_session_from_token, request_token)
        
        # Enforce purely isolated looping thread logic natively
        background_task.start_polling(token)
        
        logger.info("Login Successful")
        return JSONResponse({"status": "success", "message": "System Started Successfully!"})
        
    except Exception as e:
        logger.error(f"[System] Login failed: {str(e)}")
        return JSONResponse({"status": "error", "message": str(e)}, status_code=400)

@app.post("/stop")
async def process_stop():
    background_task.stop_polling()
    return JSONResponse({"status": "success", "message": "Background task halted natively."})

@app.get("/data")
async def api_get_data():
    data = background_task.get_latest_data()
    if data.get("status") == "error":
        return JSONResponse(data, status_code=400)
    return data

@app.get("/api/option-chain")
async def api_option_chain_frontend(symbol: str = "NIFTY", expiry: str = None):
    # Native bridge converting Algorithmic backend to the specific Dashboard Javascript format
    backend_val = background_task.get_latest_data()
    
    if backend_val.get("status") == "error":
        return {"success": False, "error": "System not running"}

    # Extract dynamic root metrics naturally attached to state in background_task
    # (Since background_task.STATE just returns 'data', I will map it perfectly)
    data_list = backend_val.get("data", [])
    if not data_list:
        return {"success": False, "error": "No chain data natively cached yet"}
    
    return {
        "success": True,
        "spot_price": backend_val.get("spot_price", 0),
        "atm_strike": backend_val.get("atm_strike", 0),
        "expiry": backend_val.get("expiry", "Unknown"),
        "all_expiries": backend_val.get("all_expiries", []),
        "chain": data_list
    }

@app.get("/health")
async def api_health_check():
    status = background_task.get_system_status()
    return {
        "status": status["status"],
        "system_active": status["active"],
        "last_ping": status["last_updated"]
    }

if __name__ == "__main__":
    # Natively standardizes booting on absolutely explicitly 8000
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
