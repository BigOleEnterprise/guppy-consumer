import pandas as pd
from typing import List
from .base import BaseParser, BankType
from ..models.raw import WellsRawTransaction

class WellsFargoParser(BaseParser):
    
    def can_parse(self, df: pd.DataFrame) -> bool:
        """Check if DataFrame matches Wells Fargo CSV format"""
        # Wells Fargo has no headers, exactly 5 columns, and quoted values
        if len(df.columns) == 5:
            # Check if first row looks like Wells Fargo data pattern
            first_row = df.iloc[0] if not df.empty else None
            if first_row is not None:
                # Look for date pattern in first column and quoted format
                first_val = str(first_row.iloc[0])
                # Wells Fargo dates are quoted: "06/06/2025"
                return '/' in first_val and ('"' in first_val or len(first_val) == 10)
        
        return False
    
    def get_bank_type(self) -> BankType:
        return BankType.WELLS_FARGO
    
    def parse_raw(self, df: pd.DataFrame) -> List[WellsRawTransaction]:
        """Parse Wells Fargo CSV to WellsRawTransaction Pydantic models"""
        transactions = []
        
        # Wells Fargo has no headers, assign column names
        df.columns = ['date', 'amount', 'status', 'unknown_field', 'description']
        
        for _, row in df.iterrows():
            try:
                transaction = WellsRawTransaction(
                    date=str(row['date']).strip('"'),  # Remove quotes
                    amount=float(row['amount']),
                    status=str(row['status']),
                    unknown_field=str(row['unknown_field']) if pd.notna(row['unknown_field']) else None,
                    description=str(row['description']).strip('"')  # Remove quotes
                )
                transactions.append(transaction)
            except Exception as e:
                # Log the error and skip this row
                print(f"Error parsing Wells Fargo row: {e}")
                continue
        
        return transactions