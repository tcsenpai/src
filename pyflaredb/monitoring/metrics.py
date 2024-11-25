from typing import Dict, List
import time
from collections import deque
import threading


class PerformanceMetrics:
    def __init__(self, window_size: int = 1000):
        self.window_size = window_size
        self.query_times: Dict[str, deque] = {}
        self.lock = threading.Lock()

    def record_query(self, query_type: str, execution_time: float):
        """Record query execution time"""
        with self.lock:
            if query_type not in self.query_times:
                self.query_times[query_type] = deque(maxlen=self.window_size)
            self.query_times[query_type].append(execution_time)

    def get_metrics(self) -> Dict[str, Dict[str, float]]:
        """Get performance metrics"""
        metrics = {}
        with self.lock:
            for query_type, times in self.query_times.items():
                if not times:
                    continue
                metrics[query_type] = {
                    "avg_time": sum(times) / len(times),
                    "max_time": max(times),
                    "min_time": min(times),
                    "count": len(times),
                }
        return metrics
