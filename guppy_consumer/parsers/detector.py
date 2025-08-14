import pandas as pd
from typing import Optional
import logging
from .base import BaseParser, BankType
from .amex import AmexParser
from .wells_fargo import WellsFargoParser

logger = logging.getLogger(__name__)

class BankDetector:
    """Factory class to detect bank type and return appropriate parser"""
    
    def __init__(self):
        self.parsers = [
            AmexParser(),
            WellsFargoParser()
        ]
    
    def detect_bank_type(self, df: pd.DataFrame) -> BankType:
        """Detect which bank format the CSV matches"""
        logger.debug(f"Checking CSV format with {len(df.columns)} columns: {list(df.columns)}")
        
        for parser in self.parsers:
            parser_name = parser.__class__.__name__
            logger.debug(f"Testing {parser_name}...")
            if parser.can_parse(df):
                bank_type = parser.get_bank_type()
                logger.info(f"Bank detection successful: {parser_name} -> {bank_type.value}")
                return bank_type
            logger.debug(f"{parser_name} cannot parse this CSV format")
        
        logger.warning("No matching bank parser found for this CSV format")
        return BankType.UNKNOWN
    
    def get_parser(self, df: pd.DataFrame) -> Optional[BaseParser]:
        """Get the appropriate parser for the CSV format"""
        for parser in self.parsers:
            if parser.can_parse(df):
                return parser
        return None