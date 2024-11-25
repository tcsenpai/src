from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class Version:
    timestamp: datetime
    operation: str  # 'INSERT', 'UPDATE', 'DELETE'
    table_name: str
    row_id: str
    data: Dict[str, Any]
    previous_version: Optional[str] = None  # Hash of previous version


class VersionStore:
    def __init__(self):
        self.versions: List[Version] = []
        self.current_version: str = None  # Hash of current version

    def add_version(self, version: Version):
        """Add a new version to the store"""
        version_hash = self._calculate_hash(version)
        self.versions.append(version)
        self.current_version = version_hash

    def get_state_at(self, timestamp: datetime) -> Dict[str, List[Dict[str, Any]]]:
        """Reconstruct database state at given timestamp"""
        state = {}
        relevant_versions = [v for v in self.versions if v.timestamp <= timestamp]

        for version in relevant_versions:
            if version.table_name not in state:
                state[version.table_name] = []

            if version.operation == "INSERT":
                state[version.table_name].append(version.data)
            elif version.operation == "DELETE":
                state[version.table_name] = [
                    row
                    for row in state[version.table_name]
                    if row["id"] != version.row_id
                ]
            elif version.operation == "UPDATE":
                state[version.table_name] = [
                    version.data if row["id"] == version.row_id else row
                    for row in state[version.table_name]
                ]

        return state
