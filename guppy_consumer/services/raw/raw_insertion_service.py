from typing import List, Union, Dict, Any
from pydantic import BaseModel
from pymongo.errors import BulkWriteError, DuplicateKeyError
from ...models.raw import AmexRawTransaction, WellsRawTransaction
from .raw_duplicate_service import RawDuplicateService
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class InsertionResult(BaseModel):
    """what we get back from bulk insert operations"""
    total_submitted: int
    total_inserted: int
    total_duplicates: int
    total_errors: int
    insert_ids: List[str] = []
    error_details: List[Dict[str, Any]] = []
    processing_time_ms: int

class RawInsertionService:
    """handles inserting lots of transactions at once"""
    
    def __init__(self, mongodb_service):
        self.mongodb_service = mongodb_service
        self.duplicate_service = RawDuplicateService(mongodb_service)
    
    async def bulk_insert_transactions(
        self,
        transactions: List[Union[AmexRawTransaction, WellsRawTransaction]], 
        bank_type: str
    ) -> InsertionResult:
        """
        insert a bunch of transactions with duplicate checking
        and good error handling
        """
        start_time = datetime.utcnow()
        total_submitted = len(transactions)
        
        logger.info(f"Starting bulk insertion of {total_submitted} {bank_type} transactions")
        
        try:
            # step 1: filter out duplicates using batch checking
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
            
            # step 2: do the actual bulk insert
            insertion_result = await self._perform_bulk_insert(new_transactions, bank_type)
            
            # step 3: put together the final results
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
        """do the actual bulk insert with error handling"""
        
        collection = self.mongodb_service.get_collection(bank_type)
        
        # turn pydantic objects into dicts for mongo
        documents = [transaction.dict() for transaction in transactions]
        
        try:
            # use ordered=False so it keeps going if some fail
            result = await collection.insert_many(documents, ordered=False)
            
            return {
                "inserted_count": len(result.inserted_ids),
                "insert_ids": [str(id) for id in result.inserted_ids],
                "duplicate_errors": 0,
                "other_errors": 0,
                "error_details": []
            }
            
        except BulkWriteError as bwe:
            # handle when some work but others dont
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
        """deal with when some inserts work and others dont"""
        
        # mongo bulk ops can partially work
        inserted_count = bwe.details.get("nInserted", 0)
        insert_ids = []
        
        # get the ids that actually got inserted
        if "insertedIds" in bwe.details:
            insert_ids = [str(id) for id in bwe.details["insertedIds"]]
        
        # figure out what kind of errors we got
        duplicate_errors = 0
        other_errors = 0
        error_details = []
        
        for error in bwe.details.get("writeErrors", []):
            if error.get("code") == 11000:  # duplicate key error
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
        """figure out how long this took in milliseconds"""
        delta = datetime.utcnow() - start_time
        return int(delta.total_seconds() * 1000)
    
    async def get_collection_stats(self, bank_type: str) -> Dict[str, Any]:
        """get stats about the collection for monitoring"""
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