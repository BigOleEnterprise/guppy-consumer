from fastapi import APIRouter, UploadFile, File, HTTPException, BackgroundTasks, Depends
from fastapi.responses import JSONResponse
import pandas as pd
import io
import logging
from typing import Dict, Any
from pydantic import BaseModel
from ..services.mongodb_service import mongodb_service
from ..services.raw import RawProcessingService, ProcessingResult

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Core Operations"])

class UploadResponse(BaseModel):
    """what we send back when someone uploads a csv"""
    status: str
    message: str
    bank_type: str
    processing_details: Dict[str, Any]
    processing_time_ms: int

class HealthResponse(BaseModel):
    """health check response format"""
    status: str
    mongodb_connected: bool
    collections_accessible: bool
    version: str = "0.1.0"

async def get_processing_service() -> RawProcessingService:
    """get the processing service when we need it"""
    return RawProcessingService(mongodb_service)

@router.post("/v1/upload/", response_model=UploadResponse)
async def upload_csv(
    file: UploadFile = File(..., description="CSV file to process"),
    processing_service: RawProcessingService = Depends(get_processing_service)
) -> UploadResponse:
    """
    main endpoint that takes a csv and does all the processing:
    1. make sure the file is good
    2. figure out if its amex or wells fargo
    3. parse the csv and validate data
    4. check for duplicates
    5. insert into mongo
    """
    
    # basic file checks first
    if not file.filename:
        raise HTTPException(status_code=400, detail="No file provided")
    
    if not file.filename.lower().endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are supported")
    
    logger.info(f"Processing CSV upload: {file.filename}")
    
    try:
        # read the csv file
        contents = await file.read()
        
        # dont let huge files crash the server
        if len(contents) > 50 * 1024 * 1024:  # 50MB limit
            raise HTTPException(
                status_code=413, 
                detail="File too large. Maximum size is 50MB"
            )
        
        # try to parse the csv with pandas
        try:
            df = pd.read_csv(io.StringIO(contents.decode('utf-8')))
        except UnicodeDecodeError:
            # if utf-8 fails try latin-1
            try:
                df = pd.read_csv(io.StringIO(contents.decode('latin-1')))
            except Exception as e:
                raise HTTPException(
                    status_code=400,
                    detail=f"Unable to decode CSV file. Please ensure it's UTF-8 or Latin-1 encoded: {str(e)}"
                )
        except Exception as e:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid CSV format: {str(e)}"
            )
        
        # make sure csv isnt empty
        if df.empty:
            raise HTTPException(
                status_code=400,
                detail="CSV file is empty"
            )
        
        logger.info(f"CSV loaded successfully: {len(df)} rows, {len(df.columns)} columns")
        
        # run the csv through our processing pipeline
        result: ProcessingResult = await processing_service.process_csv(df)
        
        # figure out what to send back based on what happened
        if result.parsing_successful and result.insertion_result.total_inserted > 0:
            return UploadResponse(
                status="success",
                message=f"Successfully processed {result.insertion_result.total_inserted} new transactions",
                bank_type=result.bank_type,
                processing_details={
                    "rows_in_csv": result.total_rows_processed,
                    "transactions_parsed": result.insertion_result.total_submitted,
                    "new_transactions_inserted": result.insertion_result.total_inserted,
                    "duplicates_skipped": result.insertion_result.total_duplicates,
                    "errors": result.insertion_result.total_errors,
                    "error_details": result.insertion_result.error_details
                },
                processing_time_ms=result.insertion_result.processing_time_ms
            )
        
        elif not result.bank_detected:
            raise HTTPException(
                status_code=422,
                detail=f"Unsupported CSV format: {result.error_message}"
            )
        
        elif not result.parsing_successful:
            raise HTTPException(
                status_code=422,
                detail=f"CSV parsing failed: {result.error_message}"
            )
        
        elif result.insertion_result.total_inserted == 0:
            return UploadResponse(
                status="success",
                message="No new transactions to insert - all were duplicates",
                bank_type=result.bank_type,
                processing_details={
                    "rows_in_csv": result.total_rows_processed,
                    "transactions_parsed": result.insertion_result.total_submitted,
                    "new_transactions_inserted": 0,
                    "duplicates_skipped": result.insertion_result.total_duplicates,
                    "errors": result.insertion_result.total_errors
                },
                processing_time_ms=result.insertion_result.processing_time_ms
            )
        
        else:
            raise HTTPException(
                status_code=500,
                detail="Processing completed but with unknown status"
            )
            
    except HTTPException:
        # let http exceptions bubble up
        raise
    except Exception as e:
        logger.error(f"Unexpected error processing {file.filename}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error during CSV processing: {str(e)}"
        )

@router.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """
    check if everything is working properly
    """
    try:
        # see if mongo is up
        mongodb_healthy = await mongodb_service.health_check()
        
        # make sure we can actually use the collections
        collections_accessible = False
        if mongodb_healthy:
            try:
                # try to count docs in both collections
                amex_count = await mongodb_service.amex_collection.count_documents({})
                wells_count = await mongodb_service.wells_collection.count_documents({})
                collections_accessible = True
                logger.debug(f"Collections accessible - Amex: {amex_count}, Wells: {wells_count}")
            except Exception as e:
                logger.warning(f"Collections not accessible: {e}")
        
        overall_status = "healthy" if mongodb_healthy and collections_accessible else "degraded"
        
        return HealthResponse(
            status=overall_status,
            mongodb_connected=mongodb_healthy,
            collections_accessible=collections_accessible
        )
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return HealthResponse(
            status="unhealthy",
            mongodb_connected=False,
            collections_accessible=False
        )

@router.get("/stats")
async def get_system_stats(
    processing_service: RawProcessingService = Depends(get_processing_service)
) -> Dict[str, Any]:
    """
    get some stats about the system
    """
    try:
        stats = await processing_service.get_processing_summary()
        return {
            "status": "success",
            "statistics": stats
        }
    except Exception as e:
        logger.error(f"Error retrieving system stats: {e}")
        return {
            "status": "error",
            "error": str(e)
        }