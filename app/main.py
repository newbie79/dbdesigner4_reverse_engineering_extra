import xml.etree.ElementTree as elementTree
import mariadb
import os

from app.utilities.settings import settings

# 문자열을 CP949 코드페이지로 변환하여 \128\123 형식으로 변환하는 함수
def encode_comment(comment):
    if not comment:
        return ""
    return "".join(f"\\{ord(c)}" for c in comment.encode("cp949", "ignore").decode("cp949"))

# MariaDB에서 테이블 주석 가져오기
def fetch_table_comments(cursor):
    cursor.execute("""
        SELECT TABLE_NAME, TABLE_COMMENT 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = %s
    """, (settings.DB_DATABASE,))
    return {row[0]: row[1] for row in cursor.fetchall()}

# MariaDB에서 컬럼 주석 가져오기
def fetch_column_comments(cursor):
    cursor.execute("""
        SELECT TABLE_NAME, COLUMN_NAME, COLUMN_COMMENT 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = %s
    """, (settings.DB_DATABASE,))
    db_columns = {}
    for table, column, comment in cursor.fetchall():
        if table not in db_columns:
            db_columns[table] = {}
        db_columns[table][column] = comment
    return db_columns

# MariaDB에서 주석 정보 가져오기
def fetch_comments():
    connection = mariadb.connect(
        user=settings.DB_USERNAME,
        password=settings.DB_DATABASE,
        host=settings.DB_SERVER,
        port=settings.DB_PORT,
        database=settings.DB_DATABASE
    )
    cursor = connection.cursor()
    db_tables = fetch_table_comments(cursor)
    db_columns = fetch_column_comments(cursor)
    cursor.close()
    connection.close()
    return db_tables, db_columns

# ERD XML 파일 수정
def update_erd_with_comments(file_path, db_tables, db_columns):
    print(f"{file_path} 작업을 시작합니다.")
    tree = elementTree.parse(file_path)
    root = tree.getroot()
    
    # ERD에서 테이블 정보 수정
    for table_element in root.findall(".//TABLE"):
        table_name = table_element.get("Tablename")
        if table_name in db_tables:
            table_element.set("Comments", encode_comment(db_tables[table_name]))
        
        # 컬럼 정보 수정
        for column_element in table_element.findall(".//COLUMN"):
            column_name = column_element.get("ColName")
            if table_name in db_columns and column_name in db_columns[table_name]:
                column_element.set("Comments", encode_comment(db_columns[table_name][column_name]))
    
    # 수정된 XML 저장
    tree.write(file_path, encoding="utf-8", xml_declaration=True)
    print(f"{file_path} 성공적으로 업데이트되었습니다.")

def main():
    print(f"DB에서 정보를 읽어옵니다.")
    tables, columns = fetch_comments()

    # 폴더 내의 모든 ERD 파일 처리
    for filename in os.listdir(settings.ERD_FOLDER_PATH):
        if filename.endswith(".dxml"):
            erd_file_path = os.path.join(settings.ERD_FOLDER_PATH, filename)
            update_erd_with_comments(erd_file_path, tables, columns)

    print(f"작업을 완료하였습니다.")

if __name__ == "__main__":
    main()