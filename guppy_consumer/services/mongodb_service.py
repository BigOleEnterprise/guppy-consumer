from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase, AsyncIOMotorCollection
from typing import Optional, List, Dict, Any
import logging
from ..config.settings import settings

logger = logging.getLogger(__name__)

class MongoDBService:
    """MongoDB service for raw transaction operations"""
    
    def __init__(self):
        self.client: Optional[AsyncIOMotorClient] = None
        self.database: Optional[AsyncIOMotorDatabase] = None
        self.amex_collection: Optional[AsyncIOMotorCollection] = None
        self.wells_collection: Optional[AsyncIOMotorCollection] = None
    
    async def connect(self) -> None:
        """Establish MongoDB connection and setup collections"""
        try:
            self.client = AsyncIOMotorClient(settings.mongodb_url)
            self.database = self.client[settings.database_name]
            
            # Get collection references
            self.amex_collection = self.database[settings.amex_collection]
            self.wells_collection = self.database[settings.wells_collection]
            
            # Create indexes for performance
            await self._create_indexes()
            
            logger.info("MongoDB connection established successfully")
            
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
    
    async def disconnect(self) -> None:
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")
    
    async def _create_indexes(self) -> None:
        """Create database indexes for optimal performance"""
        try:
            # Index on raw_hash for duplicate detection
            await self.amex_collection.create_index("raw_hash", unique=True, sparse=True)
            await self.wells_collection.create_index("raw_hash", unique=True, sparse=True)
            
            # Index on created_at for chronological queries
            await self.amex_collection.create_index("created_at")
            await self.wells_collection.create_index("created_at")
            
            # Amex-specific indexes
            await self.amex_collection.create_index("reference")  # Amex reference field
            await self.amex_collection.create_index("date")
            
            # Wells Fargo-specific indexes  
            await self.wells_collection.create_index("date")
            
            logger.info("Database indexes created successfully")
            
        except Exception as e:
            logger.warning(f"Index creation failed (may already exist): {e}")
    
    def get_collection(self, bank_type: str) -> AsyncIOMotorCollection:
        """Get appropriate collection based on bank type"""
        if bank_type == "amex":
            return self.amex_collection
        elif bank_type == "wells_fargo":
            return self.wells_collection
        else:
            raise ValueError(f"Unknown bank type: {bank_type}")
    
    async def health_check(self) -> bool:
        """Check if MongoDB connection is healthy"""
        try:
            await self.client.admin.command('ping')
            return True
        except Exception as e:
            logger.error(f"MongoDB health check failed: {e}")
            return False

# Global MongoDB service instance
mongodb_service = MongoDBService()