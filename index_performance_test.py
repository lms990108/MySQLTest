import mysql.connector
from mysql.connector import errorcode
import time
import random

# MySQL 데이터베이스 연결 정보
DB_CONFIG = {
    'user': 'root',  # MySQL root 사용자
    'password': 'rootpassword',  # docker-compose.yml에 설정된 비밀번호
    'host': 'mysql',  # 도커 네트워크 상에서 MySQL 컨테이너 이름
}

def connect_with_retries(config, retries=5, delay=5):
    """
    MySQL 서버에 연결을 시도하며, 실패 시 일정 횟수 재시도합니다.
    """
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
    """
    데이터베이스와 사용자를 초기화하는 함수
    """
    try:
        cursor.execute("DROP DATABASE IF EXISTS IndexPerformanceTest;")
        print("기존 데이터베이스 'IndexPerformanceTest'가 삭제되었습니다.")
        cursor.execute("CREATE DATABASE IndexPerformanceTest;")
        print("데이터베이스 'IndexPerformanceTest'가 성공적으로 생성되었습니다.")
        cursor.execute("USE IndexPerformanceTest;")
    except mysql.connector.Error as err:
        print(f"데이터베이스 초기화 중 오류 발생: {err}")

def create_table(cursor):
    """
    데이터 저장을 위한 테이블 생성
    """
    create_table_query = """
    CREATE TABLE IF NOT EXISTS products (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        category VARCHAR(100),
        price DECIMAL(10, 2),
        stock INT,
        description TEXT
    );
    """
    try:
        cursor.execute(create_table_query)
        print("테이블 'products'가 성공적으로 생성되었습니다.")
    except mysql.connector.Error as err:
        print(f"테이블 생성 중 오류 발생: {err}")

def insert_sample_data(cursor, cnx):
    """
    샘플 데이터를 100만 개 삽입하며 삽입 시간을 측정
    """
    start_time = time.time()
    print("샘플 데이터를 삽입 중입니다. 100만 개를 삽입합니다...")
    try:
        for i in range(1, 1000001):  # 100만 개 데이터 삽입
            category = ['Electronics', 'Clothing', 'Books', 'Home', 'Toys'][i % 5]
            price = round(10 + 100 * random.random(), 2)
            stock = random.randint(0, 1000)
            description = f"This is a description for product {i}"

            cursor.execute(
                """
                INSERT INTO products (name, category, price, stock, description)
                VALUES (%s, %s, %s, %s, %s);
                """,
                (f"Product {i}", category, price, stock, description)
            )
            if i % 10000 == 0:  # 10,000개 단위로 커밋
                cnx.commit()
                print(f"{i}개의 행이 삽입되었습니다...")

        cnx.commit()
        print(f"100만 개 데이터 삽입 완료. 소요 시간: {time.time() - start_time:.2f}초")
    except mysql.connector.Error as err:
        print(f"데이터 삽입 중 오류 발생: {err}")

def create_indexes(cursor):
    """
    테이블에 인덱스를 생성하며 소요 시간을 측정
    """
    try:
        start_time = time.time()
        try:
            cursor.execute("DROP INDEX idx_category ON products;")
        except mysql.connector.Error:
            pass  # 인덱스가 없으면 무시
        cursor.execute("CREATE INDEX idx_category ON products (category);")
        print(f"인덱스 'idx_category' 생성 완료. 소요 시간: {time.time() - start_time:.2f}초")

        start_time = time.time()
        try:
            cursor.execute("DROP INDEX idx_category_price ON products;")
        except mysql.connector.Error:
            pass  # 인덱스가 없으면 무시
        cursor.execute("CREATE INDEX idx_category_price ON products (category, price);")
        print(f"인덱스 'idx_category_price' 생성 완료. 소요 시간: {time.time() - start_time:.2f}초")

        start_time = time.time()
        cursor.execute("ALTER TABLE products ADD FULLTEXT(description);")
        print(f"FULLTEXT 인덱스 생성 완료. 소요 시간: {time.time() - start_time:.2f}초")
    except mysql.connector.Error as err:
        print(f"인덱스 생성 중 오류 발생: {err}")

def explain_queries(cursor):
    """
    실행 계획(EXPLAIN)을 확인하고 상세 결과를 출력
    """
    queries = [
        "SELECT SQL_NO_CACHE * FROM products WHERE category = 'Electronics';",
        "SELECT SQL_NO_CACHE * FROM products WHERE category = 'Electronics' AND price > 50;",
        "SELECT SQL_NO_CACHE * FROM products WHERE MATCH(description) AGAINST('description');"
    ]
    for query in queries:
        try:
            print(f"쿼리 실행 계획: {query[:50]}...")
            cursor.execute(f"EXPLAIN {query}")
            result = cursor.fetchall()
            print("실행 계획 상세 결과:")
            for row in result:
                for key, value in zip(cursor.column_names, row):
                    print(f"  {key}: {value}")
        except mysql.connector.Error as err:
            print(f"쿼리 실행 계획 확인 중 오류 발생: {err}")

def main():
    """
    메인 함수
    """
    try:
        # 데이터베이스 연결
        cnx = connect_with_retries(DB_CONFIG)
        cursor = cnx.cursor()

        # 초기화 및 작업 수행
        initialize_database(cursor)
        create_table(cursor)
        insert_sample_data(cursor, cnx)
        create_indexes(cursor)
        explain_queries(cursor)

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("사용자 이름 또는 비밀번호가 잘못되었습니다.")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("데이터베이스가 존재하지 않습니다.")
        else:
            print(err)
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'cnx' in locals():
            cnx.close()

if __name__ == "__main__":
    main()
