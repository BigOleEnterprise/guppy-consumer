import pandas as pd
from typing import List
from .base import BaseParser, BankType
from ..models.raw import AmexRawTransaction

class AmexParser(BaseParser):
    
    def can_parse(self, df: pd.DataFrame) -> bool:
        """Check if DataFrame matches Amex CSV format"""
        expected_columns = {
            'Date', 'Description', 'Card Member', 'Account #', 
            'Amount', 'Extended Details', 'Reference', 'Category'
        }
        
        # Check if has headers and contains key Amex columns
        if len(df.columns) >= 10:  # Amex has 13 columns
            column_set = set(df.columns)
            # Look for unique Amex identifiers
            return 'Card Member' in column_set and 'Reference' in column_set
        
        return False
    
    def get_bank_type(self) -> BankType:
        return BankType.AMEX
    
    def parse_raw(self, df: pd.DataFrame) -> List[AmexRawTransaction]:
        """Parse Amex CSV to AmexRawTransaction Pydantic models"""
        transactions = []
        
        for _, row in df.iterrows():
            try:
                transaction = AmexRawTransaction(
                    date=str(row.get('Date', '')),
                    description=str(row.get('Description', '')),
                    card_member=str(row.get('Card Member', '')),
                    account_number=str(row.get('Account #', '')),
                    amount=float(row.get('Amount', 0)),
                    extended_details=str(row.get('Extended Details', '')) if pd.notna(row.get('Extended Details')) else None,
                    appears_on_statement_as=str(row.get('Appears On Your Statement As', '')) if pd.notna(row.get('Appears On Your Statement As')) else None,
                    address=str(row.get('Address', '')) if pd.notna(row.get('Address')) else None,
                    city_state=str(row.get('City/State', '')) if pd.notna(row.get('City/State')) else None,
                    zip_code=str(row.get('Zip Code', '')) if pd.notna(row.get('Zip Code')) else None,
                    country=str(row.get('Country', '')) if pd.notna(row.get('Country')) else None,
                    reference=str(row.get('Reference', '')) if pd.notna(row.get('Reference')) else None,
                    category=str(row.get('Category', '')) if pd.notna(row.get('Category')) else None
                )
                transactions.append(transaction)
            except Exception as e:
                # Log the error and skip this row
                print(f"Error parsing Amex row: {e}")
                continue
        
        return transactions