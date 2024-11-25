from typing import Dict, List, Any, Optional
from .table import Table
from .sql.parser import SQLParser, SelectStatement, InsertStatement
from .sql.executor import QueryExecutor
from .sql.optimizer import QueryOptimizer
from .sql.statistics import TableStatistics
from .transaction import TransactionManager


class PyFlareDB:
    def __init__(self, db_path: str):
        """Initialize the database"""
        self.db_path = db_path
        self.tables: Dict[str, Table] = {}
        self.parser = SQLParser()
        self.statistics = TableStatistics()
        self.optimizer = QueryOptimizer(self.tables, self.statistics)
        self.executor = QueryExecutor(self.tables)
        self.transaction_manager = TransactionManager()
        self._query_cache = {}

    def begin_transaction(self) -> str:
        """Begin a new transaction"""
        return self.transaction_manager.begin_transaction()

    def commit_transaction(self, tx_id: str) -> bool:
        """Commit a transaction"""
        return self.transaction_manager.commit(tx_id)

    def rollback_transaction(self, tx_id: str) -> bool:
        """Rollback a transaction"""
        return self.transaction_manager.rollback(tx_id)

    def create_table(self, table: Table) -> None:
        """Create a new table"""
        if table.name in self.tables:
            raise ValueError(f"Table {table.name} already exists")
        self.tables[table.name] = table
        self.statistics.collect_statistics(table)

    def drop_table(self, table_name: str) -> None:
        """Drop a table"""
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist")
        del self.tables[table_name]

    def execute(
        self, sql: str, tx_id: Optional[str] = None
    ) -> Optional[List[Dict[str, Any]]]:
        """Execute a SQL query"""
        try:
            # Check query cache for non-transactional SELECT queries
            if tx_id is None and sql in self._query_cache:
                return self._query_cache[sql]

            # Parse SQL
            if sql.strip().upper().startswith("SELECT"):
                statement = self.parser.parse_select(sql)
            elif sql.strip().upper().startswith("INSERT"):
                statement = self.parser.parse_insert(sql)
            else:
                raise ValueError("Unsupported SQL statement type")

            # Get transaction if provided
            tx = None
            if tx_id:
                tx = self.transaction_manager.get_transaction(tx_id)
                if not tx:
                    raise ValueError(f"Transaction {tx_id} does not exist")

            # Optimize query plan
            optimized_plan = self.optimizer.optimize(statement)

            # Execute query
            result = self.executor.execute(optimized_plan, transaction=tx)

            # Cache SELECT results for non-transactional queries
            if tx_id is None and isinstance(statement, SelectStatement):
                self._query_cache[sql] = result

            return result

        except Exception as e:
            # Clear cache on error
            self._query_cache.clear()
            raise e

    def clear_cache(self) -> None:
        """Clear the query cache"""
        self._query_cache.clear()
