from pyflaredb.core import PyFlareDB
from pyflaredb.table import Column, Table
from pyflaredb.benchmark.suite import BenchmarkSuite
import time
from datetime import datetime
import random
import string
import json
from typing import List, Dict, Any


def generate_realistic_data(n: int) -> List[Dict[str, Any]]:
    """Generate realistic test data"""
    domains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'company.com']
    cities = ['New York', 'London', 'Tokyo', 'Paris', 'Berlin', 'Sydney', 'Toronto']
    
    data = []
    for i in range(n):
        # Generate realistic username
        username = f"{random.choice(string.ascii_lowercase)}{random.choice(string.ascii_lowercase)}"
        username += ''.join(random.choices(string.ascii_lowercase + string.digits, k=random.randint(6, 12)))
        
        # Generate realistic email
        email = f"{username}@{random.choice(domains)}"
        
        # Generate JSON metadata
        metadata = {
            "city": random.choice(cities),
            "last_login": f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
            "preferences": {
                "theme": random.choice(["light", "dark", "system"]),
                "notifications": random.choice([True, False])
            }
        }
        
        data.append({
            "id": f"usr_{i:08d}",
            "username": username,
            "email": email,
            "age": random.randint(18, 80),
            "score": round(random.uniform(0, 100), 2),
            "is_active": random.random() > 0.1,  # 90% active users
            "login_count": random.randint(1, 1000),
            "metadata": json.dumps(metadata)
        })
    
    return data


def format_value(value):
    """Format value based on its type"""
    if isinstance(value, (float, int)):
        return f"{value:.4f}"
    return str(value)


def test_database_features():
    """Test all database features with realistic workloads"""
    print("\n=== Starting Realistic Database Tests ===")
    
    # Initialize database
    db = PyFlareDB("test.db")
    
    # 1. Create test table with realistic schema
    print("\n1. Setting up test environment...")
    users_table = Table(
        name="users",
        columns=[
            Column("id", "string", nullable=False, primary_key=True),
            Column("username", "string", nullable=False, unique=True),
            Column("email", "string", nullable=False),
            Column("age", "integer", nullable=True),
            Column("score", "float", nullable=True),
            Column("is_active", "boolean", nullable=True, default=True),
            Column("login_count", "integer", nullable=True, default=0),
            Column("metadata", "string", nullable=True)  # JSON string
        ],
    )
    db.tables["users"] = users_table
    
    # Create indexes for commonly queried fields
    users_table.create_index("age")
    users_table.create_index("score")
    users_table.create_index("login_count")
    
    # 2. Performance Tests with Realistic Data
    print("\n2. Running performance tests...")
    
    # Generate test data
    test_data = generate_realistic_data(1000)  # 1000 realistic records
    
    # Insert Performance (Single vs Batch)
    print("\nInsert Performance:")
    
    # Single Insert (OLTP-style)
    start_time = time.time()
    for record in test_data[:100]:  # Test with first 100 records
        # Properly escape the metadata string
        metadata_str = record['metadata'].replace("'", "''")
        
        # Format each value according to its type
        values = [
            f"'{record['id']}'",                    # string
            f"'{record['username']}'",              # string
            f"'{record['email']}'",                 # string
            str(record['age']),                     # integer
            str(record['score']),                   # float
            str(record['is_active']).lower(),       # boolean
            str(record['login_count']),             # integer
            f"'{metadata_str}'"                     # string (JSON)
        ]
        
        query = f"""
        INSERT INTO users 
            (id, username, email, age, score, is_active, login_count, metadata)
        VALUES 
            ({', '.join(values)})
        """
        db.execute(query)
    single_insert_time = time.time() - start_time
    print(f"Single Insert (100 records, OLTP): {single_insert_time:.4f}s")
    
    # Batch Insert (OLAP-style)
    start_time = time.time()
    batch_data = test_data[100:200]  # Next 100 records
    users_table.batch_insert(batch_data)  # This should work as is
    batch_insert_time = time.time() - start_time
    print(f"Batch Insert (100 records, OLAP): {batch_insert_time:.4f}s")
    
    # 3. Query Performance Tests
    print("\nQuery Performance (OLTP vs OLAP):")
    
    # OLTP-style queries (point queries, simple filters)
    oltp_queries = [
        ("Single Record Lookup", "SELECT * FROM users WHERE id = 'usr_00000001'"),
        ("Simple Range Query", "SELECT * FROM users WHERE age > 30 LIMIT 10"),
        ("Active Users Count", "SELECT COUNT(*) FROM users WHERE is_active = true"),
        ("Recent Logins", "SELECT * FROM users WHERE login_count > 500 ORDER BY login_count DESC LIMIT 5")
    ]
    
    # OLAP-style queries (aggregations, complex filters)
    olap_queries = [
        ("Age Distribution", """
            SELECT 
                CASE 
                    WHEN age < 25 THEN 'Gen Z'
                    WHEN age < 40 THEN 'Millennial'
                    WHEN age < 55 THEN 'Gen X'
                    ELSE 'Boomer'
                END as generation,
                COUNT(*) as count
            FROM users 
            GROUP BY generation
        """),
        ("User Engagement", """
            SELECT 
                username,
                score,
                login_count
            FROM users 
            WHERE score > 75 
            AND login_count > 100
            ORDER BY score DESC
            LIMIT 10
        """),
        ("Complex Analytics", """
            SELECT 
                COUNT(*) as total_users,
                AVG(score) as avg_score,
                SUM(CASE WHEN is_active THEN 1 ELSE 0 END) as active_users
            FROM users
            WHERE age BETWEEN 25 AND 45
        """)
    ]
    
    print("\nOLTP Query Performance:")
    for query_name, query in oltp_queries:
        # First run (cold)
        start_time = time.time()
        db.execute(query)
        cold_time = time.time() - start_time
        
        # Second run (warm/cached)
        start_time = time.time()
        db.execute(query)
        warm_time = time.time() - start_time
        
        print(f"\n{query_name}:")
        print(f"  Cold run: {cold_time:.4f}s")
        print(f"  Warm run: {warm_time:.4f}s")
        print(f"  Cache improvement: {((cold_time - warm_time) / cold_time * 100):.1f}%")
    
    print("\nOLAP Query Performance:")
    for query_name, query in olap_queries:
        start_time = time.time()
        db.execute(query)
        execution_time = time.time() - start_time
        print(f"\n{query_name}: {execution_time:.4f}s")
    
    # 4. Concurrent Operations Test
    print("\nConcurrent Operations Simulation:")
    start_time = time.time()
    # Simulate mixed workload
    for _ in range(100):
        if random.random() < 0.8:  # 80% reads
            query = random.choice(oltp_queries)[1]
        else:  # 20% writes
            record = generate_realistic_data(1)[0]
            query = f"""
            INSERT INTO users (id, username, email, age, score, is_active, login_count, metadata)
            VALUES (
                '{record['id']}',
                '{record['username']}',
                '{record['email']}',
                {record['age']},
                {record['score']},
                {str(record['is_active']).lower()},
                {record['login_count']},
                '{record['metadata']}'
            )
            """
        db.execute(query)
    mixed_workload_time = time.time() - start_time
    print(f"Mixed Workload (100 operations): {mixed_workload_time:.4f}s")
    
    # 5. Memory Usage Test
    print("\nMemory Usage:")
    import sys
    memory_size = sys.getsizeof(db.tables["users"].data) / 1024  # KB
    records_count = len(db.tables["users"].data)
    print(f"Memory per record: {(memory_size / records_count):.2f} KB")
    
    # 6. Run standard benchmark suite
    print("\n6. Running standard benchmark suite...")
    benchmark = BenchmarkSuite(db)
    results = benchmark.run_benchmark(num_records=10000)
    
    print("\nBenchmark Results:")
    for test_name, metrics in results.items():
        print(f"\n{test_name.upper()}:")
        for metric, value in metrics.items():
            print(f"  {metric}: {format_value(value)}")


def main():
    try:
        test_database_features()
    except Exception as e:
        print(f"Test failed: {e}")
        raise e


if __name__ == "__main__":
    main()
