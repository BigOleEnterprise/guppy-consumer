from typing import List, Union, Dict, Any
from pydantic import BaseModel
from pymongo.errors import BulkWriteError, DuplicateKeyError
from ...models.raw import AmexRawTransaction, WellsRawTransaction
from .raw_duplicate_service import RawDuplicateService
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class InsertionResult(BaseModel):
    """Result model for bulk insertion operations"""
    total_submitted: int
    total_inserted: int
    total_duplicates: int
    total_errors: int
    insert_ids: List[str] = []
    error_details: List[Dict[str, Any]] = []
    processing_time_ms: int

class RawInsertionService:
    """Enterprise-grade service for bulk transaction insertion"""
    
    def __init__(self, mongodb_service):
        self.mongodb_service = mongodb_service
        self.duplicate_service = RawDuplicateService(mongodb_service)
    
    async def bulk_insert_transactions(
        self,
        transactions: List[Union[AmexRawTransaction, WellsRawTransaction]], 
        bank_type: str
    ) -> InsertionResult:
        """
        Perform enterprise-grade bulk insertion with duplicate filtering,
        error handling, and detailed reporting
        """
        start_time = datetime.utcnow()
        total_submitted = len(transactions)
        
        logger.info(f"Starting bulk insertion of {total_submitted} {bank_type} transactions")
        
        try:
            # Step 1: Filter out duplicates using batch detection
            new_transactions = await self.duplicate_service.filter_duplicates(
                transactions, bank_type
            )
            
            duplicates_filtered = total_submitted - len(new_transactions)
            
            if not new_transactions:
                logger.info("No new transactions to insert after duplicate filtering")
                return InsertionResult(
                    total_submitted=total_submitted,
                    total_inserted=0,
                    total_duplicates=duplicates_filtered,
                    total_errors=0,
                    processing_time_ms=self._get_processing_time(start_time)
                )
            
            # Step 2: Perform bulk insertion
            insertion_result = await self._perform_bulk_insert(new_transactions, bank_type)
            
            # Step 3: Compile final results
            result = InsertionResult(
                total_submitted=total_submitted,
                total_inserted=insertion_result["inserted_count"],
                total_duplicates=duplicates_filtered + insertion_result["duplicate_errors"],
                total_errors=insertion_result["other_errors"],
                insert_ids=insertion_result["insert_ids"],
                error_details=insertion_result["error_details"],
                processing_time_ms=self._get_processing_time(start_time)
            )
            
            logger.info(
                f"Bulk insertion completed: {result.total_inserted} inserted, "
                f"{result.total_duplicates} duplicates, {result.total_errors} errors"
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Critical error in bulk insertion: {e}")
            return InsertionResult(
                total_submitted=total_submitted,
                total_inserted=0,
                total_duplicates=0,
                total_errors=total_submitted,
                error_details=[{"error": str(e), "type": "critical_failure"}],
                processing_time_ms=self._get_processing_time(start_time)
            )
    
    async def _perform_bulk_insert(
        self, 
        transactions: List[Union[AmexRawTransaction, WellsRawTransaction]], 
        bank_type: str
    ) -> Dict[str, Any]:
        """Perform optimized bulk insertion with error handling"""
        
        collection = self.mongodb_service.get_collection(bank_type)
        
        # Convert Pydantic models to dictionaries for MongoDB
        documents = [transaction.dict() for transaction in transactions]
        
        try:
            # Use ordered=False for better performance - continues on errors
            result = await collection.insert_many(documents, ordered=False)
            
            return {
                "inserted_count": len(result.inserted_ids),
                "insert_ids": [str(id) for id in result.inserted_ids],
                "duplicate_errors": 0,
                "other_errors": 0,
                "error_details": []
            }
            
        except BulkWriteError as bwe:
            # Handle partial success scenarios
            return self._handle_bulk_write_error(bwe)
            
        except Exception as e:
            logger.error(f"Unexpected error during bulk insert: {e}")
            return {
                "inserted_count": 0,
                "insert_ids": [],
                "duplicate_errors": 0,
                "other_errors": len(transactions),
                "error_details": [{"error": str(e), "type": "bulk_insert_failure"}]
            }
    
    def _handle_bulk_write_error(self, bwe: BulkWriteError) -> Dict[str, Any]:
        """Handle partial failures in bulk operations"""
        
        # MongoDB bulk operations can partially succeed
        inserted_count = bwe.details.get("nInserted", 0)
        insert_ids = []
        
        # Extract inserted IDs if available
        if "insertedIds" in bwe.details:
            insert_ids = [str(id) for id in bwe.details["insertedIds"]]
        
        # Categorize errors
        duplicate_errors = 0
        other_errors = 0
        error_details = []
        
        for error in bwe.details.get("writeErrors", []):
            if error.get("code") == 11000:  # Duplicate key error
                duplicate_errors += 1
            else:
                other_errors += 1
                error_details.append({
                    "error": error.get("errmsg", "Unknown error"),
                    "code": error.get("code"),
                    "index": error.get("index")
                })
        
        logger.warning(
            f"Bulk write partial failure: {inserted_count} inserted, "
            f"{duplicate_errors} duplicates, {other_errors} other errors"
        )
        
        return {
            "inserted_count": inserted_count,
            "insert_ids": insert_ids,
            "duplicate_errors": duplicate_errors,
            "other_errors": other_errors,
            "error_details": error_details
        }
    
    def _get_processing_time(self, start_time: datetime) -> int:
        """Calculate processing time in milliseconds"""
        delta = datetime.utcnow() - start_time
        return int(delta.total_seconds() * 1000)
    
    async def get_collection_stats(self, bank_type: str) -> Dict[str, Any]:
        """Get collection statistics for monitoring"""
        try:
            collection = self.mongodb_service.get_collection(bank_type)
            stats = await self.mongodb_service.database.command("collStats", collection.name)
            
            return {
                "document_count": stats.get("count", 0),
                "storage_size_mb": round(stats.get("storageSize", 0) / 1024 / 1024, 2),
                "index_count": stats.get("nindexes", 0),
                "avg_document_size": stats.get("avgObjSize", 0)
            }
        except Exception as e:
            logger.error(f"Error getting collection stats: {e}")
            return {"error": str(e)}