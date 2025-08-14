from fastapi import APIRouter, Depends, Query
from typing import Dict, Any, List
import logging
from ..services.mongodb_service import mongodb_service

logger = logging.getLogger(__name__)
query_router = APIRouter(tags=["Data Query"])

@query_router.get("/amex/sample")
async def get_amex_sample(limit: int = Query(default=3, le=10)) -> Dict[str, Any]:
    """Get sample Amex transactions to inspect data types"""
    try:
        collection = mongodb_service.amex_collection
        
        # Get sample documents
        cursor = collection.find({}).limit(limit)
        documents = await cursor.to_list(length=limit)
        
        # Convert ObjectId to string for JSON serialization
        for doc in documents:
            if "_id" in doc:
                doc["_id"] = str(doc["_id"])
        
        return {
            "status": "success",
            "count": len(documents),
            "sample_transactions": documents
        }
        
    except Exception as e:
        logger.error(f"Error querying Amex collection: {e}")
        return {
            "status": "error",
            "error": str(e)
        }

@query_router.get("/wells/sample")
async def get_wells_sample(limit: int = Query(default=3, le=10)) -> Dict[str, Any]:
    """Get sample Wells Fargo transactions to inspect data types"""
    try:
        collection = mongodb_service.wells_collection
        
        cursor = collection.find({}).limit(limit)
        documents = await cursor.to_list(length=limit)
        
        for doc in documents:
            if "_id" in doc:
                doc["_id"] = str(doc["_id"])
        
        return {
            "status": "success", 
            "count": len(documents),
            "sample_transactions": documents
        }
        
    except Exception as e:
        logger.error(f"Error querying Wells collection: {e}")
        return {
            "status": "error",
            "error": str(e)
        }

@query_router.get("/collections/stats")
async def get_all_collection_stats() -> Dict[str, Any]:
    """Get detailed stats for both collections"""
    try:
        amex_count = await mongodb_service.amex_collection.count_documents({})
        wells_count = await mongodb_service.wells_collection.count_documents({})
        
        return {
            "status": "success",
            "amex_collection": {
                "total_documents": amex_count,
                "collection_name": "amex_raw"
            },
            "wells_collection": {
                "total_documents": wells_count, 
                "collection_name": "wells_raw"
            }
        }
        
    except Exception as e:
        logger.error(f"Error getting collection stats: {e}")
        return {
            "status": "error",
            "error": str(e)
        }