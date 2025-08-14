from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime

class WellsRawTransaction(BaseModel):
    """Raw Wells Fargo transaction model matching CSV structure exactly"""
    
    # Core transaction fields (5 columns from Wells Fargo CSV)
    date: str = Field(..., description="Transaction date as string from CSV")
    amount: float = Field(..., description="Transaction amount (negative for debits)")
    status: str = Field(..., description="Transaction status field")
    unknown_field: Optional[str] = Field(None, description="Unknown fourth field")
    description: str = Field(..., description="Transaction description")
    
    # Metadata
    bank_type: str = Field(default="wells_fargo", description="Bank identifier")
    raw_hash: Optional[str] = Field(None, description="Hash for duplicate detection")
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Record creation timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }