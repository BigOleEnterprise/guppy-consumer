import pandas as pd
from typing import List
from .base import BaseParser, BankType
from ..models.raw import WellsRawTransaction

class WellsFargoParser(BaseParser):
    
    def can_parse(self, df: pd.DataFrame) -> bool:
        """check if this csv looks like wells fargo"""
        # wells fargo csvs dont have headers, just 5 columns with quotes
        if len(df.columns) == 5:
            # check if the first row looks like wells data
            first_row = df.iloc[0] if not df.empty else None
            if first_row is not None:
                # look for date format in first column
                first_val = str(first_row.iloc[0])
                # wells dates look like "06/06/2025"
                return '/' in first_val and ('"' in first_val or len(first_val) == 10)
        
        return False
    
    def get_bank_type(self) -> BankType:
        return BankType.WELLS_FARGO
    
    def parse_raw(self, df: pd.DataFrame) -> List[WellsRawTransaction]:
        """turn wells fargo csv rows into our transaction objects"""
        transactions = []
        
        # wells doesnt give us headers so we assign them ourselves
        df.columns = ['date', 'amount', 'status', 'unknown_field', 'description']
        
        for _, row in df.iterrows():
            try:
                transaction = WellsRawTransaction(
                    date=str(row['date']).strip('"'),  # remove quotes
                    amount=float(row['amount']),
                    status=str(row['status']),
                    unknown_field=str(row['unknown_field']) if pd.notna(row['unknown_field']) else None,
                    description=str(row['description']).strip('"')  # remove quotes
                )
                transactions.append(transaction)
            except Exception as e:
                # skip bad rows but keep going
                print(f"Error parsing Wells Fargo row: {e}")
                continue
        
        return transactions