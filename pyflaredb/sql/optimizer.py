from typing import List, Dict, Any, Union
from dataclasses import dataclass
from enum import Enum
from .parser import SelectStatement, InsertStatement

from pyflaredb.sql.statistics import TableStatistics
from pyflaredb.table import Table


class JoinStrategy(Enum):
    NESTED_LOOP = "nested_loop"
    HASH_JOIN = "hash_join"
    MERGE_JOIN = "merge_join"


class ScanType(Enum):
    SEQUENTIAL = "sequential"
    INDEX = "index"


@dataclass
class QueryPlan:
    operation: str
    strategy: Union[JoinStrategy, ScanType]
    estimated_cost: float
    children: List["QueryPlan"] = None


class QueryOptimizer:
    def __init__(self, tables: Dict[str, "Table"], statistics: "TableStatistics"):
        self.tables = tables
        self.statistics = statistics

    def optimize(self, statement) -> Any:
        """Generate an optimized query plan"""
        if isinstance(statement, SelectStatement):
            return self._optimize_select(statement)
        elif isinstance(statement, InsertStatement):
            return statement  # No optimization needed for simple inserts
        return statement  # Return original statement if no optimization is needed

    def _optimize_select(self, stmt: SelectStatement) -> SelectStatement:
        """Optimize SELECT query execution"""
        # For now, return the original statement
        # TODO: Implement actual optimization strategies
        return stmt

    def _estimate_cost(self, plan: QueryPlan) -> float:
        """Estimate the cost of a query plan"""
        # Implementation for cost estimation
        pass
