from lxml import etree
import mariadb
import os
import re
from app.utilities.settings import settings

# 문자열을 CP949 코드페이지로 변환하여 \128\123 형식으로 변환하는 함수
def encode_comment(text):
    def encode_match(match):
        try:
            return "\\" + "\\".join(str(c) for c in match.group(0).encode("cp949"))
        except UnicodeEncodeError:
            return match.group(0)  # 인코딩 실패 시 원본 유지

    # 한글 유니코드 범위: 가-힣
    pattern = re.compile(r'[가-힣]+')
    encoded_text = pattern.sub(encode_match, text)

    return encoded_text

# MariaDB에서 테이블 주석 가져오기
def fetch_table_comments(cursor):
    cursor.execute("""
        SELECT TABLE_NAME, TABLE_COMMENT 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = %s
        ORDER BY TABLE_NAME ASC;
    """, (settings.DB_DATABASE,))
    return {row[0]: row[1] for row in cursor.fetchall()}

# MariaDB에서 컬럼 주석 가져오기
def fetch_column_comments(cursor):
    cursor.execute("""
        SELECT TABLE_NAME, COLUMN_NAME, COLUMN_COMMENT 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = %s;
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
        password=settings.DB_PASSWORD,
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
def update_erd_with_comments(filepath, db_tables, db_columns):
    print(f"{filepath} 작업을 시작합니다.")
    tree = etree.parse(filepath)
    root = tree.getroot()
    
    # ERD에서 테이블 정보 수정
    for table_element in root.xpath("//TABLE"):
        table_name = table_element.get("Tablename")
        if table_name in db_tables:
            table_comment = db_tables[table_name]
            if table_comment and table_name != table_comment:
                enc_table_comment = encode_comment(table_comment)

                if " " not in table_element:
                    if "Tablename" in table_element.attrib:
                        table_element.attrib["Tablename"] = table_name + ' (' + enc_table_comment + ')'
                    else:  # 속성이 없으면 추가
                        table_element.set("Tablename", table_name + ' (' + enc_table_comment + ')')

                # 테이블 주석 수정
                if "Comments" in table_element.attrib:
                    table_element.attrib["Comments"] = enc_table_comment
                else:  # 속성이 없으면 추가
                    table_element.set("Comments", enc_table_comment)

            # Table Engine을 InnoDB로 변경
            if "TableType" in table_element.attrib:
                table_element.attrib["TableType"] = "1"
            else:  # 속성이 없으면 추가
                table_element.set("TableType", "1")

        # 컬럼 정보 수정
        for column_element in table_element.findall(".//COLUMN"):
            column_name = column_element.get("ColName")
            if table_name in db_columns and column_name in db_columns[table_name]:
                column_comment = db_columns[table_name][column_name]
                if column_comment:
                    enc_column_comment = encode_comment(column_comment)
                    if "Comments" in column_element.attrib:
                        column_element.attrib["Comments"] = enc_column_comment
                    else:  # 속성이 없으면 추가
                        column_element.set("Comments", enc_column_comment)

    # 수정된 XML 저장
    tree.write(filepath, pretty_print=True, xml_declaration=True, encoding="utf-8")
    print(f"{filepath} 성공적으로 업데이트되었습니다.")

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