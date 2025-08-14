from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging
from guppy_consumer.api.endpoints import router
from guppy_consumer.api.query_endpoints import query_router
from guppy_consumer.api.admin_endpoints import admin_router
from guppy_consumer.services.mongodb_service import mongodb_service

# setup logging so we can see whats happening
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """handle startup and shutdown stuff for the app"""
    # startup stuff
    logger.info("Starting Guppy Consumer service...")
    try:
        await mongodb_service.connect()
        logger.info("Application startup completed successfully")
    except Exception as e:
        logger.error(f"Failed to start application: {e}")
        raise
    
    yield
    
    # shutdown stuff
    logger.info("Shutting down Guppy Consumer service...")
    await mongodb_service.disconnect()
    logger.info("Application shutdown completed")

app = FastAPI(
    title="Guppy Consumer API",
    description="""
    Enterprise-grade CSV data processor and MongoDB ingestion service for Guppy Funds.
    
    ## Features
    
    * **CSV Processing**: Automatic detection and parsing of Amex and Wells Fargo CSV formats
    * **Duplicate Detection**: Intelligent hash-based duplicate prevention
    * **Bulk Operations**: High-performance batch processing
    * **Data Validation**: Pydantic-based type validation and error handling
    * **MongoDB Integration**: Async MongoDB operations with connection pooling
    * **Health Monitoring**: Comprehensive health checks and system statistics
    
    ## Collections
    
    * **amex_raw**: Raw Amex transaction data
    * **wells_raw**: Raw Wells Fargo transaction data
    
    ## Authentication
    
    No authentication required for local development.
    """,
    version="0.1.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    contact={
        "name": "Guppy Funds Development Team",
        "email": "dev@guppyfunds.local",
    },
    license_info={
        "name": "MIT License",
        "url": "https://opensource.org/licenses/MIT",
    },
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:8501"],  # react and streamlit ports
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router, prefix="/api")
app.include_router(query_router, prefix="/api/query")
app.include_router(admin_router, prefix="/api/admin")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)