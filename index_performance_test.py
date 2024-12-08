import mysql.connector
from mysql.connector import errorcode
from concurrent.futures import ThreadPoolExecutor
import time
import random

DB_CONFIG = {
    'user': 'root',
    'password': 'rootpassword',
    'host': 'mysql',
}

def connect_with_retries(config, retries=5, delay=5):
    for attempt in range(retries):
        try:
            connection = mysql.connector.connect(**config)
            print("MySQL 서버에 성공적으로 연결되었습니다.")
            return connection
        except mysql.connector.Error as err:
            print(f"MySQL 연결 실패 (시도 {attempt + 1}/{retries}): {err}")
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                raise

def initialize_database(cursor):
    cursor.execute("DROP DATABASE IF EXISTS IndexTestComparison;")
    cursor.execute("CREATE DATABASE IndexTestComparison;")
    cursor.execute("USE IndexTestComparison;")

def create_table(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS products (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255),
        category VARCHAR(100),
        price DECIMAL(10, 2),
        stock INT,
        description TEXT
    );
    """)

def insert_batch(start, end):
    
    try:
        cnx = mysql.connector.connect(**DB_CONFIG)
        cursor = cnx.cursor()
        
        # 데이터베이스 선택
        cursor.execute("USE IndexTestComparison;")

        batch_size = 1000
        rows = []
        for i in range(start, end):
            category = ['Electronics', 'Clothing', 'Books', 'Home', 'Toys'][i % 5]
            price = round(10 + 100 * random.random(), 2)
            stock = random.randint(0, 1000)
            description = f"This is a description for product {i}"
            rows.append((f"Product {i}", category, price, stock, description))

            if len(rows) == batch_size:
                cursor.executemany(
                    """
                    INSERT INTO products (name, category, price, stock, description)
                    VALUES (%s, %s, %s, %s, %s);
                    """,
                    rows
                )
                cnx.commit()
                rows = []

        if rows:  # Remaining rows
            cursor.executemany(
                """
                INSERT INTO products (name, category, price, stock, description)
                VALUES (%s, %s, %s, %s, %s);
                """,
                rows
            )
            cnx.commit()

        cursor.close()
        cnx.close()
        print(f"Batch {start}-{end} completed.")
    except mysql.connector.Error as err:
        print(f"Error during batch {start}-{end}: {err}")

def insert_sample_data_parallel(total_records, num_threads):
    records_per_thread = total_records // num_threads
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for i in range(num_threads):
            start = i * records_per_thread + 1
            end = (i + 1) * records_per_thread + 1
            futures.append(executor.submit(insert_batch, start, end))

        for future in futures:
            future.result()

def measure_query_performance(cursor, query):
    start_time = time.time()
    cursor.execute(query)
    rows = cursor.fetchall()
    elapsed_time = time.time() - start_time
    return len(rows), elapsed_time

def run_test_scenario(cursor, description, create_index_queries, test_queries):
    print(f"==== {description} ====")
    for query in create_index_queries:
        try:
            cursor.execute(query)
        except mysql.connector.Error:
            pass  # 이미 존재하는 인덱스 무시

    results = []
    for test_query in test_queries:
        rows, elapsed_time = measure_query_performance(cursor, test_query)
        print(f"쿼리: {test_query}\n결과 수: {rows}, 실행 시간: {elapsed_time:.2f}초")
        results.append((test_query, rows, elapsed_time))
    return results

def main():
    cnx = connect_with_retries(DB_CONFIG)
    cursor = cnx.cursor()
    initialize_database(cursor)
    create_table(cursor)

    print("데이터를 병렬로 삽입 중입니다...")
    insert_sample_data_parallel(total_records=5000000, num_threads=10)

    test_queries = [
        "SELECT SQL_NO_CACHE * FROM products WHERE category = 'Electronics';",
        "SELECT SQL_NO_CACHE * FROM products WHERE category = 'Electronics' AND stock > 50;",
        "SELECT SQL_NO_CACHE * FROM products WHERE price > 50 AND stock < 500;",
    ]

    # 1. 인덱싱 없는 경우
    run_test_scenario(cursor, "인덱싱 없는 경우", [], test_queries)

    # 2. 최악의 인덱싱
    worst_indexes = [
        "CREATE INDEX idx_name ON products (name);",
        "CREATE INDEX idx_price_stock ON products (price, stock);",
    ]
    run_test_scenario(cursor, "최악의 인덱싱", worst_indexes, test_queries)

    # 3. 최선의 인덱싱
    best_indexes = [
        "CREATE INDEX idx_category ON products (category);",
        "CREATE INDEX idx_category_stock ON products (category, stock);",
        "CREATE INDEX idx_price_stock ON products (stock, price);",
    ]
    run_test_scenario(cursor, "최선의 인덱싱", best_indexes, test_queries)

    cursor.close()
    cnx.close()

if __name__ == "__main__":
    main()
