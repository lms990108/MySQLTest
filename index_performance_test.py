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

def explain_query(cursor, query):
    print(f"\n[EXPLAIN] {query}")
    cursor.execute(f"EXPLAIN {query}")
    for row in cursor.fetchall():
        print(row)

def run_test_scenario(cursor, description, create_index_queries, test_queries):
    print(f"==== {description} ====")
    
    # 인덱스 생성
    for query in create_index_queries:
        try:
            cursor.execute(query)
        except mysql.connector.Error as err:
            print(f"인덱스 생성 중 오류 발생: {err}")
    
    # 커밋하여 인덱스 생성 완료 보장
    cursor._connection.commit()

    # 쿼리 실행
    results = []
    for test_query in test_queries:
        try:
            # 실행 계획 출력
            explain_query(cursor, test_query)

            # 쿼리 실행 및 성능 측정
            rows, elapsed_time = measure_query_performance(cursor, test_query)
            print(f"쿼리: {test_query}\n결과 수: {rows}, 실행 시간: {elapsed_time:.2f}초")
            results.append((test_query, rows, elapsed_time))
        except mysql.connector.Error as err:
            print(f"쿼리 실행 중 오류 발생: {err}")
    return results

def create_partitioned_table(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS products_partitioned (
        id INT AUTO_INCREMENT,
        name VARCHAR(255),
        category VARCHAR(100),
        price DECIMAL(10, 2),
        stock INT,
        description TEXT,
        PRIMARY KEY (id, category)
    ) PARTITION BY LIST COLUMNS(category) (
        PARTITION p_electronics VALUES IN ('Electronics'),
        PARTITION p_clothing VALUES IN ('Clothing'),
        PARTITION p_books VALUES IN ('Books'),
        PARTITION p_home VALUES IN ('Home'),
        PARTITION p_toys VALUES IN ('Toys')
    );
    """)

def populate_partitioned_table(cursor):
    cursor.execute("""
    INSERT INTO products_partitioned (id, name, category, price, stock, description)
    SELECT id, name, category, price, stock, description FROM products;
    """)

def run_partition_test(cursor):
    print("==== Partitioning Test ====")
    partition_queries = [
        "SELECT SQL_NO_CACHE * FROM products_partitioned WHERE category = 'Electronics';",
        "SELECT SQL_NO_CACHE * FROM products_partitioned WHERE category = 'Electronics' AND stock > 50;",
        "SELECT SQL_NO_CACHE * FROM products_partitioned WHERE price > 50 AND stock < 500;",
    ]
    return run_test_scenario(cursor, "Partitioning", [], partition_queries)

def run_materialized_view_test(cursor):
    print("==== Materialized View Test ====")
    materialized_view_queries = [
        "SELECT SQL_NO_CACHE * FROM materialized_products WHERE category = 'Electronics';",
    ]
    return run_test_scenario(cursor, "Materialized View", [], materialized_view_queries)

def main():
    cnx = connect_with_retries(DB_CONFIG)
    cursor = cnx.cursor()
    initialize_database(cursor)
    create_table(cursor)

    print("데이터를 병렬로 삽입 중입니다...")
    insert_sample_data_parallel(total_records=1000000, num_threads=10)

    # Partitioning 테스트 준비
    create_partitioned_table(cursor)
    populate_partitioned_table(cursor)

    # Partitioning 테스트 실행
    run_partition_test(cursor)

    # 최악의 인덱싱 테스트 실행
    worst_indexes = [
        "CREATE INDEX idx_description ON products (description(255));",  # 키 길이 지정
        "CREATE INDEX idx_name_stock ON products (name, stock);",
    ]
    worst_index_test_queries = [
        "SELECT SQL_NO_CACHE * FROM products FORCE INDEX (idx_description) WHERE category = 'Electronics';",
        "SELECT SQL_NO_CACHE * FROM products FORCE INDEX (idx_name_stock) WHERE category = 'Electronics' AND stock > 50;",
        "SELECT SQL_NO_CACHE * FROM products FORCE INDEX (idx_name_stock) WHERE price > 50 AND stock < 500;",
    ]
    run_test_scenario(cursor, "최악의 인덱싱", worst_indexes, worst_index_test_queries)

    # 최선의 인덱싱 테스트 실행
    best_indexes = [
        "CREATE INDEX idx_category ON products (category);",
        "CREATE INDEX idx_category_stock ON products (category, stock);",
        "CREATE INDEX idx_price_stock ON products (price, stock);",
    ]
    best_index_test_queries = [
        "SELECT SQL_NO_CACHE * FROM products FORCE INDEX (idx_category) WHERE category = 'Electronics';",
        "SELECT SQL_NO_CACHE * FROM products FORCE INDEX (idx_category_stock) WHERE category = 'Electronics' AND stock > 50;",
        "SELECT SQL_NO_CACHE * FROM products FORCE INDEX (idx_price_stock) WHERE price > 50 AND stock < 500;",
    ]
    run_test_scenario(cursor, "최선의 인덱싱", best_indexes, best_index_test_queries)

    # 인덱싱 없는 경우 테스트 실행
    no_index_test_queries = [
        "SELECT SQL_NO_CACHE * FROM products WHERE category = 'Electronics';",
        "SELECT SQL_NO_CACHE * FROM products WHERE category = 'Electronics' AND stock > 50;",
        "SELECT SQL_NO_CACHE * FROM products WHERE price > 50 AND stock < 500;",
    ]
    run_test_scenario(cursor, "인덱싱 없는 경우", [], no_index_test_queries)

    cursor.close()
    cnx.close()

if __name__ == "__main__":
    main()
