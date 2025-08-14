from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime

class WellsRawTransaction(BaseModel):
    """raw wells fargo transaction - matches their csv format exactly"""
    
    # main transaction stuff (5 columns from wells csv)
    date: str = Field(..., description="Transaction date as string from CSV")
    amount: float = Field(..., description="Transaction amount (negative for debits)")
    status: str = Field(..., description="Transaction status field")
    unknown_field: Optional[str] = Field(None, description="Unknown fourth field")
    description: str = Field(..., description="Transaction description")
    
    # our internal tracking stuff
    bank_type: str = Field(default="wells_fargo", description="Bank identifier")
    raw_hash: Optional[str] = Field(None, description="Hash for duplicate detection")
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Record creation timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }