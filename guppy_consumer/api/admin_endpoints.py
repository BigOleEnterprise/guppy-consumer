from fastapi import APIRouter, Depends
from typing import Dict, Any
import logging
import platform
import psutil
import time
from datetime import datetime
from ..services.mongodb_service import mongodb_service
from ..config.settings import settings

logger = logging.getLogger(__name__)
admin_router = APIRouter(tags=["System Administration"])

@admin_router.get("/system/info", summary="Get system information")
async def get_system_info() -> Dict[str, Any]:
    """
    get info about the system - cpu, memory, disk usage etc
    useful for checking if everything is running ok
    """
    try:
        # grab system stats
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        return {
            "status": "success",
            "timestamp": datetime.utcnow().isoformat(),
            "system": {
                "platform": platform.platform(),
                "python_version": platform.python_version(),
                "cpu_cores": psutil.cpu_count(),
                "cpu_usage_percent": cpu_percent,
                "memory": {
                    "total_gb": round(memory.total / 1024**3, 2),
                    "available_gb": round(memory.available / 1024**3, 2),
                    "used_percent": memory.percent
                },
                "disk": {
                    "total_gb": round(disk.total / 1024**3, 2),
                    "free_gb": round(disk.free / 1024**3, 2),
                    "used_percent": round((disk.used / disk.total) * 100, 1)
                }
            },
            "application": {
                "name": "Guppy Consumer",
                "version": "0.1.0",
                "environment": settings.environment,
                "log_level": settings.log_level,
                "uptime_seconds": int(time.time() - start_time)
            },
            "database": {
                "mongodb_url": settings.mongodb_url.split("@")[1] if "@" in settings.mongodb_url else "***",
                "database_name": settings.database_name,
                "collections": {
                    "amex": settings.amex_collection,
                    "wells_fargo": settings.wells_collection
                }
            }
        }
        
    except Exception as e:
        logger.error(f"Error getting system info: {e}")
        return {
            "status": "error",
            "error": str(e)
        }

@admin_router.get("/database/indexes", summary="List all database indexes")
async def get_database_indexes() -> Dict[str, Any]:
    """
    Get information about all database indexes for performance monitoring.
    """
    try:
        amex_indexes = []
        wells_indexes = []
        
        # get amex collection indexes
        async for index in mongodb_service.amex_collection.list_indexes():
            amex_indexes.append(index)
            
        # get wells fargo collection indexes too
        async for index in mongodb_service.wells_collection.list_indexes():
            wells_indexes.append(index)
        
        return {
            "status": "success",
            "collections": {
                "amex_raw": {
                    "index_count": len(amex_indexes),
                    "indexes": amex_indexes
                },
                "wells_raw": {
                    "index_count": len(wells_indexes),
                    "indexes": wells_indexes
                }
            }
        }
        
    except Exception as e:
        logger.error(f"Error getting database indexes: {e}")
        return {
            "status": "error",
            "error": str(e)
        }

@admin_router.post("/database/reindex", summary="Rebuild database indexes")
async def rebuild_indexes() -> Dict[str, Any]:
    """
    Rebuild all database indexes for performance optimization.
    Use with caution in production.
    """
    try:
        logger.info("Starting database index rebuild...")
        
        # rebuild indexes
        await mongodb_service._create_indexes()
        
        logger.info("Database index rebuild completed")
        
        return {
            "status": "success",
            "message": "Database indexes rebuilt successfully"
        }
        
    except Exception as e:
        logger.error(f"Error rebuilding indexes: {e}")
        return {
            "status": "error",
            "error": str(e)
        }

# track when the app started so we can show uptime
start_time = time.time()