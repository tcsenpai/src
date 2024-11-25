import time
from typing import List, Dict, Any
import random
import string
from ..core import PyFlareDB


class BenchmarkSuite:
    def __init__(self, db: PyFlareDB):
        self.db = db

    def run_benchmark(self, num_records: int = 10000):
        """Run comprehensive benchmark"""
        results = {
            "insert": self._benchmark_insert(num_records),
            "select": self._benchmark_select(num_records),
            "index": self._benchmark_index_performance(num_records),
            "complex_query": self._benchmark_complex_queries(num_records),
        }
        return results

    def _benchmark_insert(self, num_records: int) -> Dict[str, float]:
        start_time = time.time()
        batch_times = []

        for i in range(0, num_records, 1000):
            batch_start = time.time()
            self._insert_batch(min(1000, num_records - i))
            batch_times.append(time.time() - batch_start)

        total_time = time.time() - start_time
        return {
            "total_time": total_time,
            "records_per_second": num_records / total_time,
            "avg_batch_time": sum(batch_times) / len(batch_times),
        }

    def _insert_batch(self, size: int):
        """Insert a batch of random records"""
        tx_id = None
        try:
            tx_id = self.db.transaction_manager.begin_transaction()

            for _ in range(size):
                query = (
                    "INSERT INTO users (id, username, email, age) "
                    f"VALUES ('{self._random_string(10)}', "
                    f"'{self._random_string(8)}', "
                    f"'{self._random_string(8)}@example.com', "
                    f"{random.randint(18, 80)})"
                )
                try:
                    self.db.execute(query)
                except Exception as e:
                    print(f"Failed to insert record: {e}")
                    print(f"Query was: {query}")
                    raise

            self.db.transaction_manager.commit(tx_id)

        except Exception as e:
            if tx_id is not None:
                try:
                    self.db.transaction_manager.rollback(tx_id)
                except ValueError:
                    pass
            raise

    def _benchmark_select(self, num_records: int) -> Dict[str, float]:
        """Benchmark SELECT queries"""
        queries = [
            "SELECT * FROM users WHERE age > 30",
            "SELECT username, email FROM users WHERE age < 25",
            "SELECT COUNT(*) FROM users",
        ]

        results = {}
        for query in queries:
            start_time = time.time()
            try:
                self.db.execute(query)
                query_time = time.time() - start_time
                results[query] = query_time
            except Exception as e:
                results[query] = f"Error: {e}"

        return results

    def _benchmark_index_performance(self, num_records: int) -> Dict[str, float]:
        """Benchmark index performance"""
        # TODO: Implement index benchmarking
        return {"index_creation_time": 0.0}

    def _benchmark_complex_queries(self, num_records: int) -> Dict[str, float]:
        """Benchmark complex queries"""
        complex_queries = [
            """
            SELECT username, COUNT(*) as count 
            FROM users 
            GROUP BY username
            """,
            """
            SELECT * FROM users 
            WHERE age > 30 
            ORDER BY username DESC 
            LIMIT 10
            """,
        ]

        results = {}
        for query in complex_queries:
            start_time = time.time()
            try:
                self.db.execute(query)
                query_time = time.time() - start_time
                results[query.strip()] = query_time
            except Exception as e:
                results[query.strip()] = f"Error: {e}"

        return results

    @staticmethod
    def _random_string(length: int) -> str:
        """Generate a random string of specified length"""
        return "".join(random.choices(string.ascii_letters + string.digits, k=length))
