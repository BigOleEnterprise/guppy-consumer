from typing import List, Union, Tuple
import pandas as pd
from ...parsers.detector import BankDetector, BankType
from ...models.raw import AmexRawTransaction, WellsRawTransaction
from .raw_insertion_service import RawInsertionService, InsertionResult
from pydantic import BaseModel
import logging

logger = logging.getLogger(__name__)

class ProcessingResult(BaseModel):
    """Comprehensive result for CSV processing pipeline"""
    bank_type: str
    bank_detected: bool
    parsing_successful: bool
    total_rows_processed: int
    insertion_result: InsertionResult
    error_message: str = ""

class RawProcessingService:
    """
    High-level orchestrator service that combines:
    Bank detection → Parsing → Duplicate filtering → Bulk insertion
    """
    
    def __init__(self, mongodb_service):
        self.mongodb_service = mongodb_service
        self.bank_detector = BankDetector()
        self.insertion_service = RawInsertionService(mongodb_service)
    
    async def process_csv(self, df: pd.DataFrame) -> ProcessingResult:
        """
        Complete CSV processing pipeline:
        1. Detect bank type
        2. Parse CSV to Pydantic models
        3. Bulk insert with duplicate detection
        """
        logger.info("=== STARTING CSV PROCESSING PIPELINE ===")
        logger.info(f"CSV contains {len(df)} rows and {len(df.columns)} columns")
        logger.info(f"CSV columns: {list(df.columns)}")
        
        try:
            # Step 1: Bank Detection
            logger.info("Step 1: Detecting bank type...")
            bank_type = self.bank_detector.detect_bank_type(df)
            
            if bank_type == BankType.UNKNOWN:
                logger.warning("Bank detection failed - unknown format")
                return ProcessingResult(
                    bank_type="unknown",
                    bank_detected=False,
                    parsing_successful=False,
                    total_rows_processed=0,
                    insertion_result=InsertionResult(
                        total_submitted=0,
                        total_inserted=0,
                        total_duplicates=0,
                        total_errors=0,
                        processing_time_ms=0
                    ),
                    error_message="Unable to detect bank format. Supported formats: Amex, Wells Fargo"
                )
            
            logger.info(f"Detected bank type: {bank_type.value}")
            
            # Step 2: Parse CSV
            logger.info("Step 2: Parsing CSV with bank-specific parser...")
            parser = self.bank_detector.get_parser(df)
            transactions = parser.parse_raw(df)
            
            if not transactions:
                logger.warning("Parsing failed - no valid transactions found")
                return ProcessingResult(
                    bank_type=bank_type.value,
                    bank_detected=True,
                    parsing_successful=False,
                    total_rows_processed=len(df),
                    insertion_result=InsertionResult(
                        total_submitted=0,
                        total_inserted=0,
                        total_duplicates=0,
                        total_errors=0,
                        processing_time_ms=0
                    ),
                    error_message="No valid transactions could be parsed from CSV"
                )
            
            logger.info(f"Successfully parsed {len(transactions)} transactions")
            
            # Step 3: Bulk insertion with duplicate detection
            logger.info("Step 3: Starting bulk insertion with duplicate detection...")
            insertion_result = await self.insertion_service.bulk_insert_transactions(
                transactions, bank_type.value
            )
            
            logger.info(f"Processing completed - Inserted: {insertion_result.total_inserted}, Duplicates: {insertion_result.total_duplicates}, Errors: {insertion_result.total_errors}")
            logger.info("=== CSV PROCESSING PIPELINE COMPLETED ===")
            logger.info("")
            
            return ProcessingResult(
                bank_type=bank_type.value,
                bank_detected=True,
                parsing_successful=True,
                total_rows_processed=len(df),
                insertion_result=insertion_result
            )
            
        except Exception as e:
            logger.error(f"Critical error in CSV processing: {e}")
            return ProcessingResult(
                bank_type="unknown",
                bank_detected=False,
                parsing_successful=False,
                total_rows_processed=len(df) if df is not None else 0,
                insertion_result=InsertionResult(
                    total_submitted=0,
                    total_inserted=0,
                    total_duplicates=0,
                    total_errors=0,
                    processing_time_ms=0
                ),
                error_message=f"Processing failed: {str(e)}"
            )
    
    async def get_processing_summary(self) -> dict:
        """Get summary statistics across all collections"""
        try:
            amex_stats = await self.insertion_service.get_collection_stats("amex")
            wells_stats = await self.insertion_service.get_collection_stats("wells_fargo")
            
            return {
                "amex_collection": amex_stats,
                "wells_fargo_collection": wells_stats,
                "mongodb_healthy": await self.mongodb_service.health_check()
            }
        except Exception as e:
            logger.error(f"Error getting processing summary: {e}")
            return {"error": str(e)}