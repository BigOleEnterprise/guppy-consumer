from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime

class AmexRawTransaction(BaseModel):
    """Raw Amex transaction model matching CSV structure exactly"""
    
    # Core transaction fields
    date: str = Field(..., description="Transaction date as string from CSV")
    description: str = Field(..., description="Transaction description")
    card_member: str = Field(..., description="Card member name")
    account_number: str = Field(..., description="Account number")
    amount: float = Field(..., description="Transaction amount")
    
    # Extended Amex-specific fields
    extended_details: Optional[str] = Field(None, description="Extended transaction details")
    appears_on_statement_as: Optional[str] = Field(None, description="How it appears on statement")
    address: Optional[str] = Field(None, description="Merchant address")
    city_state: Optional[str] = Field(None, description="Merchant city/state")
    zip_code: Optional[str] = Field(None, description="Merchant zip code")
    country: Optional[str] = Field(None, description="Merchant country")
    reference: Optional[str] = Field(None, description="Transaction reference ID")
    category: Optional[str] = Field(None, description="Transaction category")
    
    # Metadata
    bank_type: str = Field(default="amex", description="Bank identifier")
    raw_hash: Optional[str] = Field(None, description="Hash for duplicate detection")
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Record creation timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }