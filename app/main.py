import logging
import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from app.interfaces.api.pii_scan_router import router as pii_router
from app.domain.models.memory_manager import MemoryManager
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app/logs/app.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="PII Scanner Service",
    description="Memory-efficient PII scanning service for large datasets",
    version="2.0.0"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(pii_router, prefix="/api/v2/pii", tags=["PII Scanner"])

# Initialize memory manager
memory_manager = MemoryManager()

@app.on_event("startup")
async def startup_event():
    """
    Initialize services and verify environment on startup.
    """
    try:
        # Ensure required environment variables are set
        required_vars = [
            "CLICKHOUSE_HOST",
            "CLICKHOUSE_PORT",
            "CLICKHOUSE_USER",
            "CLICKHOUSE_PASSWORD",
            "CLICKHOUSE_DATABASE"
        ]
        
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

        # Set memory limits
        memory_limit = int(os.getenv("MEMORY_LIMIT", 1.5 * 1024 * 1024 * 1024))  # Default 1.5GB
        memory_manager.set_resource_limits()
        
        logger.info(f"Service initialized with memory limit: {memory_limit / (1024*1024*1024):.2f}GB")

    except Exception as e:
        logger.error(f"Failed to initialize service: {e}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    """
    Clean up resources on shutdown.
    """
    try:
        memory_manager.cleanup()
        logger.info("Service shutdown completed successfully")
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")

@app.get("/health")
async def health_check():
    """
    Health check endpoint to verify service status.
    Also checks memory usage.
    """
    try:
        if not memory_manager.check_memory():
            raise HTTPException(
                status_code=503,
                detail="Service under high memory pressure"
            )
        
        return {
            "status": "healthy",
            "memory_usage": {
                "current": memory_manager.get_memory_usage() / (1024*1024),
                "limit": memory_manager._memory_limit / (1024*1024)
            }
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        workers=1,  # Use single worker for better memory management
        limit_max_requests=1000,  # Restart worker after processing 1000 requests
        timeout_keep_alive=30
    )