from typing import List, Set, Union
from motor.motor_asyncio import AsyncIOMotorCollection
from ...models.raw import AmexRawTransaction, WellsRawTransaction
from .hash_service import HashService
import logging

logger = logging.getLogger(__name__)

class RawDuplicateService:
    """finds and filters out duplicate transactions"""
    
    def __init__(self, mongodb_service):
        self.mongodb_service = mongodb_service
    
    async def filter_duplicates(
        self, 
        transactions: List[Union[AmexRawTransaction, WellsRawTransaction]],
        bank_type: str
    ) -> List[Union[AmexRawTransaction, WellsRawTransaction]]:
        """remove duplicate transactions using batch checking"""
        
        if not transactions:
            return []
        
        # step 1: add hashes to all transactions
        transactions_with_hashes = HashService.add_hashes_to_transactions(transactions)
        
        # step 2: get all the hashes for batch lookup
        hashes = [t.raw_hash for t in transactions_with_hashes]
        
        # step 3: check which hashes already exist in db
        existing_hashes = await self._get_existing_hashes(hashes, bank_type)
        
        # step 4: only keep transactions we haven't seen before
        new_transactions = [
            t for t in transactions_with_hashes 
            if t.raw_hash not in existing_hashes
        ]
        
        logger.info(
            f"Duplicate detection: {len(transactions)} total, "
            f"{len(existing_hashes)} duplicates, "
            f"{len(new_transactions)} new transactions"
        )
        
        return new_transactions
    
    async def _get_existing_hashes(self, hashes: List[str], bank_type: str) -> Set[str]:
        """check db for existing hashes all at once"""
        try:
            collection = self.mongodb_service.get_collection(bank_type)
            
            # one query to find all existing hashes
            cursor = collection.find(
                {"raw_hash": {"$in": hashes}},
                {"raw_hash": 1, "_id": 0}  # only need the hash field
            )
            
            existing_docs = await cursor.to_list(length=None)
            existing_hashes = {doc["raw_hash"] for doc in existing_docs}
            
            return existing_hashes
            
        except Exception as e:
            logger.error(f"Error checking for duplicate hashes: {e}")
            # if error, assume no duplicates so we dont lose data
            return set()
    
    async def check_single_duplicate(self, transaction_hash: str, bank_type: str) -> bool:
        """check if one transaction hash exists (for debugging)"""
        try:
            collection = self.mongodb_service.get_collection(bank_type)
            result = await collection.find_one({"raw_hash": transaction_hash})
            return result is not None
            
        except Exception as e:
            logger.error(f"Error checking single duplicate: {e}")
            return False