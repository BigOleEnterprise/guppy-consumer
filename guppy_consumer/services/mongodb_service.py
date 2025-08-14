from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase, AsyncIOMotorCollection
from typing import Optional, List, Dict, Any
import logging
from ..config.settings import settings

logger = logging.getLogger(__name__)

class MongoDBService:
    """handles all our mongo database stuff"""
    
    def __init__(self):
        self.client: Optional[AsyncIOMotorClient] = None
        self.database: Optional[AsyncIOMotorDatabase] = None
        self.amex_collection: Optional[AsyncIOMotorCollection] = None
        self.wells_collection: Optional[AsyncIOMotorCollection] = None
    
    async def connect(self) -> None:
        """connect to mongo and set up our collections"""
        try:
            self.client = AsyncIOMotorClient(settings.mongodb_url)
            self.database = self.client[settings.database_name]
            
            # get references to our collections
            self.amex_collection = self.database[settings.amex_collection]
            self.wells_collection = self.database[settings.wells_collection]
            
            # create indexes so queries are fast
            await self._create_indexes()
            
            logger.info("MongoDB connection established successfully")
            
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
    
    async def disconnect(self) -> None:
        """close the mongo connection"""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")
    
    async def _create_indexes(self) -> None:
        """create indexes to make queries fast"""
        try:
            # index on hash so we can find duplicates fast
            await self.amex_collection.create_index("raw_hash", unique=True, sparse=True)
            await self.wells_collection.create_index("raw_hash", unique=True, sparse=True)
            
            # index on date so we can sort by time
            await self.amex_collection.create_index("created_at")
            await self.wells_collection.create_index("created_at")
            
            # indexes just for amex
            await self.amex_collection.create_index("reference")  # Amex reference field
            await self.amex_collection.create_index("date")
            
            # indexes just for wells  
            await self.wells_collection.create_index("date")
            
            logger.info("Database indexes created successfully")
            
        except Exception as e:
            logger.warning(f"Index creation failed (may already exist): {e}")
    
    def get_collection(self, bank_type: str) -> AsyncIOMotorCollection:
        """get the right collection for this bank"""
        if bank_type == "amex":
            return self.amex_collection
        elif bank_type == "wells_fargo":
            return self.wells_collection
        else:
            raise ValueError(f"Unknown bank type: {bank_type}")
    
    async def health_check(self) -> bool:
        """check if mongo is still working"""
        try:
            await self.client.admin.command('ping')
            return True
        except Exception as e:
            logger.error(f"MongoDB health check failed: {e}")
            return False

# single mongo service we use everywhere
mongodb_service = MongoDBService()