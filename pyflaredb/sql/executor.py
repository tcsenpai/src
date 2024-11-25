from typing import List, Dict, Any, Callable, Tuple, Optional
import operator
from ..table import Table
from .parser import SelectStatement, InsertStatement
from ..transaction import Transaction


class QueryExecutor:
    def __init__(self, tables: Dict[str, Table]):
        self.tables = tables
        self._compiled_conditions = {}
        self._comparison_ops = {
            '>': operator.gt,
            '<': operator.lt,
            '>=': operator.ge,
            '<=': operator.le,
            '=': operator.eq,
            '!=': operator.ne
        }

    def _parse_where_clause(self, where_clause: str) -> List[Tuple[str, str, str]]:
        """Parse WHERE clause into list of (field, operator, value) tuples"""
        conditions = []
        # Split on AND if present
        subclauses = [c.strip() for c in where_clause.split(' AND ')]
        
        for subclause in subclauses:
            # Find the operator
            operator_found = None
            for op in ['>=', '<=', '>', '<', '=', '!=']:
                if op in subclause:
                    operator_found = op
                    field, value = subclause.split(op)
                    conditions.append((field.strip(), op, value.strip()))
                    break
            
            if not operator_found:
                raise ValueError(f"Invalid condition: {subclause}")
        
        return conditions
    
    def execute(self, statement, transaction: Optional[Transaction] = None):
        """Execute a parsed SQL statement"""
        if isinstance(statement, SelectStatement):
            return self._execute_select(statement, transaction)
        elif isinstance(statement, InsertStatement):
            return self._execute_insert(statement, transaction)
        elif statement is None:
            raise ValueError("No statement to execute")
        else:
            raise ValueError(f"Unsupported statement type: {type(statement)}")

    def _execute_select(self, stmt: SelectStatement, transaction: Optional[Transaction] = None) -> List[Dict[str, Any]]:
        if stmt.table_name not in self.tables:
            raise ValueError(f"Table {stmt.table_name} does not exist")
        
        table = self.tables[stmt.table_name]
        
        # If in transaction, check for locks
        if transaction and table.name in transaction.locks:
            # Handle transaction isolation level logic here
            pass
        
        # Handle COUNT(*) separately
        if len(stmt.columns) == 1 and stmt.columns[0].lower() == "count(*)":
            return [{"count": len(table.data)}]
        
        # Try to use index for WHERE clause
        if stmt.where_clause:
            try:
                conditions = self._parse_where_clause(stmt.where_clause)
                
                # Check if we can use an index for any condition
                for field, op, value in conditions:
                    if field in table._indexes:
                        # Convert value to proper type
                        column = next((col for col in table.columns if col.name == field), None)
                        if column:
                            try:
                                if column.data_type == "integer":
                                    value = int(value)
                                elif column.data_type == "float":
                                    value = float(value)
                            except (ValueError, TypeError):
                                continue
                        
                        # Use index for lookup
                        if op == '=':
                            results = table.find_by_index(field, value)
                        elif op in {'>', '>='}:
                            results = table.range_search(field, value, None)
                        elif op in {'<', '<='}:
                            results = table.range_search(field, None, value)
                        else:  # op == '!='
                            # For inequality, we still need to scan
                            results = table.data
                        
                        # Apply remaining conditions
                        filtered_results = []
                        for row in results:
                            if self._matches_all_conditions(row, conditions):
                                filtered_results.append(row)
                        
                        return self._process_results(filtered_results, stmt)
            except ValueError:
                # If WHERE clause parsing fails, fall back to table scan
                pass
        
        # Fall back to full table scan
        return self._table_scan(table, stmt)
    
    def _matches_all_conditions(self, row: Dict[str, Any], conditions: List[Tuple[str, str, str]]) -> bool:
        """Check if row matches all conditions"""
        for field, op, value in conditions:
            row_value = row.get(field)
            if row_value is None:
                return False
            
            # Convert value to proper type based on row_value
            try:
                if isinstance(row_value, int):
                    value = int(value)
                elif isinstance(row_value, float):
                    value = float(value)
            except (ValueError, TypeError):
                return False
            
            # Apply comparison
            op_func = self._comparison_ops[op]
            try:
                if not op_func(row_value, value):
                    return False
            except TypeError:
                return False
        
        return True
    
    def _table_scan(self, table: Table, stmt: SelectStatement) -> List[Dict[str, Any]]:
        """Perform a full table scan with filtering"""
        results = []
        
        # Parse WHERE conditions if present
        conditions = []
        if stmt.where_clause:
            try:
                conditions = self._parse_where_clause(stmt.where_clause)
            except ValueError:
                # If parsing fails, return empty result
                return []
        
        # Process rows
        for row in table.data:
            # Apply WHERE clause
            if conditions and not self._matches_all_conditions(row, conditions):
                continue
            
            # Select requested columns
            if "*" in stmt.columns:
                results.append(row.copy())
            else:
                filtered_row = {}
                for col in stmt.columns:
                    if "count(" in col.lower():
                        filtered_row[col] = len(results)
                    else:
                        filtered_row[col] = row.get(col)
                results.append(filtered_row)
        
        return self._process_results(results, stmt)
    
    def _process_results(self, rows: List[Dict[str, Any]], stmt: SelectStatement) -> List[Dict[str, Any]]:
        """Process result rows according to SELECT statement"""
        results = []
        for row in rows:
            if "*" in stmt.columns:
                results.append(row.copy())
            else:
                filtered_row = {}
                for col in stmt.columns:
                    if "count(" in col.lower():
                        filtered_row[col] = len(results)
                    else:
                        filtered_row[col] = row.get(col)
                results.append(filtered_row)
        
        # Handle ORDER BY
        if stmt.order_by:
            for order_clause in stmt.order_by:
                reverse = order_clause.direction.value == "DESC"
                results.sort(
                    key=lambda x: (x.get(order_clause.column) is None, x.get(order_clause.column)),
                    reverse=reverse
                )
        
        # Handle LIMIT
        if stmt.limit is not None:
            results = results[:stmt.limit]
        
        return results
    
    def _execute_insert(self, stmt: InsertStatement, transaction: Optional[Transaction] = None) -> bool:
        if stmt.table_name not in self.tables:
            raise ValueError(f"Table {stmt.table_name} does not exist")
        
        table = self.tables[stmt.table_name]
        
        # If in transaction, acquire lock and track changes
        if transaction:
            transaction.locks.add(table.name)
            # Track the changes for potential rollback
            transaction.changes.append({
                'type': 'INSERT',
                'table': table.name,
                'data': dict(zip(stmt.columns, stmt.values))
            })
        
        # Create dictionary of column-value pairs
        row_data = {}
        for col_name, value in zip(stmt.columns, stmt.values):
            # Find the column definition
            column = next((col for col in table.columns if col.name == col_name), None)
            if not column:
                raise ValueError(f"Column {col_name} does not exist")
            
            # Convert value based on column type
            if value is not None:
                try:
                    if column.data_type == "integer":
                        row_data[col_name] = int(value)
                    elif column.data_type == "float":
                        row_data[col_name] = float(value)
                    elif column.data_type == "boolean":
                        if isinstance(value, str):
                            row_data[col_name] = value.lower() == 'true'
                        else:
                            row_data[col_name] = bool(value)
                    else:  # string type
                        row_data[col_name] = str(value)
                except (ValueError, TypeError):
                    raise ValueError(f"Invalid value for column {column.name}: {value}")
            else:
                row_data[col_name] = None
        
        # Insert the data
        return table.insert(row_data)
