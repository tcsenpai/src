from typing import Dict, List, Any
from enum import Enum
import threading
from datetime import datetime

class TransactionState(Enum):
    ACTIVE = "ACTIVE"
    COMMITTED = "COMMITTED"
    ROLLED_BACK = "ROLLED_BACK"

class Transaction:
    def __init__(self, id: str):
        self.id = id
        self.state = TransactionState.ACTIVE
        self.changes: List[Dict[str, Any]] = []
        self.locks = set()
        self.timestamp = datetime.utcnow()

class TransactionManager:
    def __init__(self):
        self.transactions: Dict[str, Transaction] = {}
        self.lock = threading.Lock()
    
    def begin_transaction(self) -> str:
        """Start a new transaction"""
        with self.lock:
            tx_id = str(len(self.transactions) + 1)
            self.transactions[tx_id] = Transaction(tx_id)
            return tx_id
    
    def commit(self, tx_id: str):
        """Commit a transaction"""
        with self.lock:
            if tx_id not in self.transactions:
                raise ValueError(f"Transaction {tx_id} not found")
            
            tx = self.transactions[tx_id]
            if tx.state != TransactionState.ACTIVE:
                raise ValueError(f"Transaction {tx_id} is not active")
            
            # Apply changes
            self._apply_changes(tx)
            tx.state = TransactionState.COMMITTED
    
    def rollback(self, tx_id: str):
        """Rollback a transaction"""
        with self.lock:
            if tx_id not in self.transactions:
                raise ValueError(f"Transaction {tx_id} not found")
            
            tx = self.transactions[tx_id]
            if tx.state != TransactionState.ACTIVE:
                raise ValueError(f"Transaction {tx_id} is not active")
            
            # Discard changes
            tx.changes.clear()
            tx.state = TransactionState.ROLLED_BACK
    
    def _apply_changes(self, transaction: Transaction):
        """Apply transaction changes"""
        for change in transaction.changes:
            # Implementation of applying changes
            pass