from typing import Dict, Any
import numpy as np

from pyflaredb.table import Table


class TableStatistics:
    def __init__(self):
        self.table_sizes: Dict[str, int] = {}
        self.column_stats: Dict[str, Dict[str, Any]] = {}

    def collect_statistics(self, table: "Table"):
        """Collect statistics for a table"""
        self.table_sizes[table.name] = len(table.data)

        for column in table.columns:
            values = [row[column.name] for row in table.data if column.name in row]

            if not values:
                continue

            stats = {
                "distinct_count": len(set(values)),
                "null_count": sum(1 for v in values if v is None),
                "min": min(values) if values and None not in values else None,
                "max": max(values) if values and None not in values else None,
            }

            if isinstance(values[0], (int, float)):
                stats.update(
                    {
                        "mean": np.mean(values),
                        "std_dev": np.std(values),
                        "histogram": np.histogram(values, bins=100),
                    }
                )

            self.column_stats[f"{table.name}.{column.name}"] = stats
