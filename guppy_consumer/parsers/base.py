from abc import ABC, abstractmethod
from typing import List, Union
from enum import Enum
import pandas as pd
from pydantic import BaseModel

class BankType(Enum):
    AMEX = "amex"
    WELLS_FARGO = "wells_fargo"
    UNKNOWN = "unknown"

class BaseParser(ABC):
    
    @abstractmethod
    def can_parse(self, df: pd.DataFrame) -> bool:
        """Check if this parser can handle the given DataFrame"""
        pass
    
    @abstractmethod
    def get_bank_type(self) -> BankType:
        """Return the bank type this parser handles"""
        pass
    
    @abstractmethod
    def parse_raw(self, df: pd.DataFrame) -> List[BaseModel]:
        """Parse DataFrame to raw transaction Pydantic models"""
        pass