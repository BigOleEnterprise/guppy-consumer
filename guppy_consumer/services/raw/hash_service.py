import hashlib
from typing import List, Union
from ...models.raw import AmexRawTransaction, WellsRawTransaction

class HashService:
    """makes hashes for transactions so we can spot duplicates"""
    
    @staticmethod
    def generate_amex_hash(transaction: AmexRawTransaction) -> str:
        """make a hash for amex transactions"""
        # use reference + date + amount for amex
        composite_key = f"{transaction.date}|{transaction.amount}|{transaction.reference}"
        return hashlib.sha256(composite_key.encode('utf-8')).hexdigest()
    
    @staticmethod
    def generate_wells_hash(transaction: WellsRawTransaction) -> str:
        """make a hash for wells transactions"""
        # use date + amount + description (wells doesnt have unique ids)
        normalized_description = transaction.description.strip().lower() if transaction.description else ""
        composite_key = f"{transaction.date}|{transaction.amount}|{normalized_description}"
        return hashlib.sha256(composite_key.encode('utf-8')).hexdigest()
    
    @staticmethod
    def generate_hash(transaction: Union[AmexRawTransaction, WellsRawTransaction]) -> str:
        """figure out what kind of transaction and make the right hash"""
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
        """add hash to every transaction in the list"""
        for transaction in transactions:
            hash_value = HashService.generate_hash(transaction)
            transaction.raw_hash = hash_value
        return transactions