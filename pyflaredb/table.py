from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from datetime import datetime
from collections import defaultdict
from .indexing.btree import BTreeIndex

@dataclass
class Column:
    name: str
    data_type: str
    nullable: bool = True
    unique: bool = False
    primary_key: bool = False
    default: Any = None

class Table:
    def __init__(self, name: str, columns: List[Column]):
        self.name = name
        self.columns = columns
        self.data: List[Dict[str, Any]] = []
        self._unique_indexes: Dict[str, Dict[Any, int]] = defaultdict(dict)
        self._compiled_conditions = {}
        self._indexes: Dict[str, BTreeIndex] = {}
        
        # Validate column definitions
        self._validate_columns()
    
    def _validate_columns(self):
        """Validate column definitions"""
        # Ensure only one primary key
        primary_keys = [col for col in self.columns if col.primary_key]
        if len(primary_keys) > 1:
            raise ValueError("Table can only have one primary key")
        
        # Validate data types
        valid_types = {"string", "integer", "float", "boolean", "datetime"}
        for col in self.columns:
            if col.data_type.lower() not in valid_types:
                raise ValueError(f"Invalid data type for column {col.name}: {col.data_type}")
    
    def create_index(self, column_name: str) -> None:
        """Create a B-tree index for a column"""
        if column_name not in {col.name for col in self.columns}:
            raise ValueError(f"Column {column_name} does not exist")
        
        # Create new index
        index = BTreeIndex()
        
        # Build index from existing data
        for row_id, row in enumerate(self.data):
            if column_name in row:
                index.insert(row[column_name], row_id)
        
        self._indexes[column_name] = index
    
    def batch_insert(self, rows: List[Dict[str, Any]]) -> bool:
        """Efficiently insert multiple rows with index updates"""
        # Pre-validate all rows
        validated_rows = []
        unique_values = defaultdict(set)
        
        # Check unique constraints across all new rows
        for row in rows:
            converted_row = {}
            # Validate required columns and defaults
            for column in self.columns:
                if not column.nullable and column.name not in row and column.default is None:
                    raise ValueError(f"Required column {column.name} is missing")
                
                value = row.get(column.name, column.default)
                
                # Type conversion
                if value is not None:
                    try:
                        if column.data_type == "integer":
                            value = int(value)
                        elif column.data_type == "float":
                            value = float(value)
                        elif column.data_type == "boolean":
                            value = bool(value)
                        else:  # string and datetime
                            value = str(value)
                    except (ValueError, TypeError):
                        raise ValueError(f"Invalid value for column {column.name}: {value}")
                
                converted_row[column.name] = value
                
                # Track unique values
                if column.unique and value is not None:
                    if value in unique_values[column.name] or value in self._unique_indexes[column.name]:
                        raise ValueError(f"Unique constraint violated for column {column.name}")
                    unique_values[column.name].add(value)
            
            validated_rows.append(converted_row)
        
        # All rows validated, perform batch insert
        start_id = len(self.data)
        for i, row in enumerate(validated_rows):
            row_id = start_id + i
            
            # Update indexes
            for column_name, index in self._indexes.items():
                if column_name in row:
                    index.insert(row[column_name], row_id)
            
            # Update unique indexes
            for column in self.columns:
                if column.unique:
                    value = row.get(column.name)
                    if value is not None:
                        self._unique_indexes[column.name][value] = row_id
            
            self.data.append(row)
        
        return True

    def insert(self, row: Dict[str, Any]) -> bool:
        """Insert a single row (now uses batch_insert)"""
        return self.batch_insert([row])

    def to_dict(self) -> dict:
        """Convert table to dictionary for serialization"""
        return {
            "name": self.name,
            "columns": [
                {
                    "name": col.name,
                    "data_type": col.data_type,
                    "nullable": col.nullable,
                    "unique": col.unique,
                    "primary_key": col.primary_key
                }
                for col in self.columns
            ],
            "data": self.data
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'Table':
        """Create table from dictionary"""
        columns = [
            Column(**col_data)
            for col_data in data["columns"]
        ]
        table = cls(data["name"], columns)
        table.data = data["data"]
        return table

    def _validate_type(self, value: Any, expected_type: str) -> bool:
        """Validate that a value matches the expected data type"""
        type_mapping = {
            "string": str,
            "integer": int,
            "float": float,
            "boolean": bool,
            "datetime": datetime,
        }
        
        if expected_type not in type_mapping:
            raise ValueError(f"Unsupported data type: {expected_type}")
        
        expected_python_type = type_mapping[expected_type]
        
        if not isinstance(value, expected_python_type):
            try:
                # Attempt to convert the value
                expected_python_type(value)
            except (ValueError, TypeError):
                raise ValueError(
                    f"Value {value} is not of expected type {expected_type}"
                )
        
        return True
    
    def find_by_index(self, column_name: str, value: Any) -> List[Dict[str, Any]]:
        """Find rows using an index"""
        if column_name not in self._indexes:
            raise ValueError(f"No index exists for column {column_name}")
        
        index = self._indexes[column_name]
        row_ids = index.search(value)
        return [self.data[row_id] for row_id in row_ids]
    
    def range_search(self, column_name: str, start_value: Any, end_value: Any) -> List[Dict[str, Any]]:
        """Perform a range search using an index"""
        if column_name not in self._indexes:
            raise ValueError(f"No index exists for column {column_name}")
        
        index = self._indexes[column_name]
        row_ids = index.range_search(start_value, end_value)
        return [self.data[row_id] for row_id in row_ids]