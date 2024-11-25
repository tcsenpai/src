from dataclasses import dataclass
from typing import List, Optional, Any
from enum import Enum

class OrderDirection(Enum):
    ASC = "ASC"
    DESC = "DESC"

@dataclass
class OrderByClause:
    column: str
    direction: OrderDirection = OrderDirection.ASC

@dataclass
class SelectStatement:
    table_name: str
    columns: List[str]
    where_clause: Optional[str] = None
    group_by: Optional[List[str]] = None
    order_by: Optional[List[OrderByClause]] = None
    limit: Optional[int] = None

@dataclass
class InsertStatement:
    table_name: str
    columns: List[str]
    values: List[Any]

class SQLParser:
    @staticmethod
    def parse_insert(sql: str) -> InsertStatement:
        """Parse INSERT statement"""
        # Remove newlines and extra spaces
        sql = ' '.join(sql.split())
        
        # Extract table name
        table_start = sql.find("INTO") + 4
        table_end = sql.find("(", table_start)
        table_name = sql[table_start:table_end].strip()
        
        # Extract columns
        cols_start = sql.find("(", table_end) + 1
        cols_end = sql.find(")", cols_start)
        columns = [col.strip() for col in sql[cols_start:cols_end].split(",")]
        
        # Extract values
        values_start = sql.find("VALUES", cols_end) + 6
        values_start = sql.find("(", values_start) + 1
        values_end = sql.find(")", values_start)
        values_str = sql[values_start:values_end]
        
        # Parse values while respecting quotes
        values = []
        current_value = ""
        in_quotes = False
        quote_char = None
        
        for char in values_str:
            if char in ["'", '"']:
                if not in_quotes:
                    in_quotes = True
                    quote_char = char
                elif quote_char == char:
                    in_quotes = False
                    quote_char = None
                current_value += char
            elif char == ',' and not in_quotes:
                values.append(current_value.strip())
                current_value = ""
            else:
                current_value += char
        
        if current_value:
            values.append(current_value.strip())
        
        # Clean up values
        cleaned_values = []
        for value in values:
            value = value.strip()
            if value.startswith(("'", '"')) and value.endswith(("'", '"')):
                # String value - keep quotes
                cleaned_values.append(value)
            elif value.lower() == 'true':
                cleaned_values.append(True)
            elif value.lower() == 'false':
                cleaned_values.append(False)
            elif value.lower() == 'null':
                cleaned_values.append(None)
            else:
                try:
                    # Try to convert to number if possible
                    if '.' in value:
                        cleaned_values.append(float(value))
                    else:
                        cleaned_values.append(int(value))
                except ValueError:
                    # If not a number, keep as is
                    cleaned_values.append(value)
        
        if len(columns) != len(cleaned_values):
            raise ValueError(f"Column count ({len(columns)}) doesn't match value count ({len(cleaned_values)})")
        
        return InsertStatement(table_name=table_name, columns=columns, values=cleaned_values)
    
    @staticmethod
    def parse_select(sql: str) -> SelectStatement:
        """Parse SELECT statement"""
        # Remove newlines and extra spaces
        sql = ' '.join(sql.split())
        
        # Extract table name
        from_idx = sql.upper().find("FROM")
        if from_idx == -1:
            raise ValueError("Invalid SELECT statement: missing FROM clause")
        
        # Extract columns
        columns_str = sql[6:from_idx].strip()
        columns = [col.strip() for col in columns_str.split(",")]
        
        # Find all clause positions
        where_idx = sql.upper().find("WHERE")
        group_idx = sql.upper().find("GROUP BY")
        order_idx = sql.upper().find("ORDER BY")
        limit_idx = sql.upper().find("LIMIT")
        
        # Find table name end position
        table_end = min(x for x in [where_idx, group_idx, order_idx, limit_idx] if x != -1) if any(x != -1 for x in [where_idx, group_idx, order_idx, limit_idx]) else len(sql)
        table_name = sql[from_idx + 4:table_end].strip()
        
        # Parse WHERE clause
        where_clause = None
        if where_idx != -1:
            where_end = min(x for x in [group_idx, order_idx, limit_idx] if x != -1) if any(x != -1 for x in [group_idx, order_idx, limit_idx]) else len(sql)
            where_clause = sql[where_idx + 5:where_end].strip()
        
        # Parse GROUP BY clause
        group_by = None
        if group_idx != -1:
            group_end = min(x for x in [order_idx, limit_idx] if x != -1) if any(x != -1 for x in [order_idx, limit_idx]) else len(sql)
            group_by_str = sql[group_idx + 8:group_end].strip()
            group_by = [col.strip() for col in group_by_str.split(",")]
        
        # Parse ORDER BY clause
        order_by = None
        if order_idx != -1:
            order_end = limit_idx if limit_idx != -1 else len(sql)
            order_str = sql[order_idx + 8:order_end].strip()
            order_parts = order_str.split(",")
            order_by = []
            for part in order_parts:
                part = part.strip()
                if " DESC" in part.upper():
                    column = part[:part.upper().find(" DESC")].strip()
                    direction = OrderDirection.DESC
                else:
                    column = part.replace(" ASC", "").strip()
                    direction = OrderDirection.ASC
                order_by.append(OrderByClause(column=column, direction=direction))
        
        # Parse LIMIT clause
        limit = None
        if limit_idx != -1:
            limit_str = sql[limit_idx + 5:].strip()
            try:
                limit = int(limit_str)
            except ValueError:
                raise ValueError(f"Invalid LIMIT value: {limit_str}")
        
        return SelectStatement(
            table_name=table_name,
            columns=columns,
            where_clause=where_clause,
            group_by=group_by,
            order_by=order_by,
            limit=limit
        )