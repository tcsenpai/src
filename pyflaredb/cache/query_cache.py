from typing import Dict, Any, Optional
import time
import hashlib
from collections import OrderedDict

class QueryCache:
    def __init__(self, capacity: int = 1000, ttl: int = 300):
        self.capacity = capacity
        self.ttl = ttl
        self.cache = OrderedDict()
    
    def get(self, query: str) -> Optional[Any]:
        """Get cached query result"""
        query_hash = self._hash_query(query)
        if query_hash in self.cache:
            entry = self.cache[query_hash]
            if time.time() - entry['timestamp'] < self.ttl:
                self.cache.move_to_end(query_hash)
                return entry['result']
            else:
                del self.cache[query_hash]
        return None
    
    def set(self, query: str, result: Any):
        """Cache query result"""
        if len(self.cache) >= self.capacity:
            self.cache.popitem(last=False)
        
        query_hash = self._hash_query(query)
        self.cache[query_hash] = {
            'result': result,
            'timestamp': time.time()
        }
    
    def _hash_query(self, query: str) -> str:
        return hashlib.sha256(query.encode()).hexdigest()
    
    def clear(self):
        self.cache.clear()