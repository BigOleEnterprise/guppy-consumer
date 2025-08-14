import hashlib
from typing import List, Union
from ...models.raw import AmexRawTransaction, WellsRawTransaction

class HashService:
    """Service for generating transaction hashes for duplicate detection"""
    
    @staticmethod
    def generate_amex_hash(transaction: AmexRawTransaction) -> str:
        """Generate hash for Amex transaction using key fields"""
        # Use reference (unique transaction ID) + date + amount for Amex
        composite_key = f"{transaction.date}|{transaction.amount}|{transaction.reference}"
        return hashlib.sha256(composite_key.encode('utf-8')).hexdigest()
    
    @staticmethod
    def generate_wells_hash(transaction: WellsRawTransaction) -> str:
        """Generate hash for Wells Fargo transaction using key fields"""
        # Use date + amount + description (no unique ID available)
        normalized_description = transaction.description.strip().lower() if transaction.description else ""
        composite_key = f"{transaction.date}|{transaction.amount}|{normalized_description}"
        return hashlib.sha256(composite_key.encode('utf-8')).hexdigest()
    
    @staticmethod
    def generate_hash(transaction: Union[AmexRawTransaction, WellsRawTransaction]) -> str:
        """Generate appropriate hash based on transaction type"""
        if isinstance(transaction, AmexRawTransaction):
            return HashService.generate_amex_hash(transaction)
        elif isinstance(transaction, WellsRawTransaction):
            return HashService.generate_wells_hash(transaction)
        else:
            raise ValueError(f"Unknown transaction type: {type(transaction)}")
    
    @staticmethod
    def add_hashes_to_transactions(
        transactions: List[Union[AmexRawTransaction, WellsRawTransaction]]
    ) -> List[Union[AmexRawTransaction, WellsRawTransaction]]:
        """Add raw_hash field to all transactions"""
        for transaction in transactions:
            hash_value = HashService.generate_hash(transaction)
            transaction.raw_hash = hash_value
        return transactions