from typing import Dict, Any, Optional, Set, List
from enum import Enum
import time
import uuid
import threading


class TransactionState(Enum):
    ACTIVE = "active"
    COMMITTED = "committed"
    ROLLED_BACK = "rolled_back"


class Transaction:
    def __init__(self, tx_id: str):
        self.id = tx_id
        self.state = TransactionState.ACTIVE
        self.start_time = time.time()
        self.locks: Set[str] = set()  # Set of table names that are locked
        self.changes: List[Dict[str, Any]] = (
            []
        )  # List of changes made during transaction


class TransactionManager:
    def __init__(self):
        self.transactions: Dict[str, Transaction] = {}
        self.lock = threading.Lock()

    def begin_transaction(self) -> str:
        """Start a new transaction"""
        with self.lock:
            tx_id = str(uuid.uuid4())
            self.transactions[tx_id] = Transaction(tx_id)
            return tx_id

    def commit(self, tx_id: str) -> bool:
        """Commit a transaction"""
        with self.lock:
            if tx_id not in self.transactions:
                raise ValueError(f"Transaction {tx_id} not found")

            tx = self.transactions[tx_id]
            if tx.state != TransactionState.ACTIVE:
                raise ValueError(f"Transaction {tx_id} is not active")

            # Apply changes
            tx.state = TransactionState.COMMITTED

            # Release locks
            tx.locks.clear()

            return True

    def rollback(self, tx_id: str) -> bool:
        """Rollback a transaction"""
        with self.lock:
            if tx_id not in self.transactions:
                raise ValueError(f"Transaction {tx_id} not found")

            tx = self.transactions[tx_id]
            if tx.state != TransactionState.ACTIVE:
                raise ValueError(f"Transaction {tx_id} is not active")

            # Revert changes
            tx.state = TransactionState.ROLLED_BACK
            tx.changes.clear()

            # Release locks
            tx.locks.clear()

            return True

    def get_transaction(self, tx_id: str) -> Optional[Transaction]:
        """Get transaction by ID"""
        return self.transactions.get(tx_id)

    def is_active(self, tx_id: str) -> bool:
        """Check if a transaction is active"""
        tx = self.get_transaction(tx_id)
        return tx is not None and tx.state == TransactionState.ACTIVE
