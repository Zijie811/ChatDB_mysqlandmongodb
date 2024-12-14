# explore_database.py

def list_mysql_tables(connection):
    try:
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        print("\nMySQL tables:")
        for (table,) in tables:
            print(f"- {table}")
        return [table[0] for table in tables]
    except Exception as err:
        print(f"MySQL failed to look up the tables: {err}")
        return []

def show_mysql_table_data(connection, table_name):
    cursor = connection.cursor()
    cursor.execute(f"DESCRIBE {table_name}")
    columns = cursor.fetchall()
    print(f"\n {table_name} field information:")
    for column in columns:
        print(column)
    cursor.execute(f"SELECT * FROM {table_name} LIMIT 10")
    data = cursor.fetchall()
    print("sample data:")
    for row in data:
        print(row)


# def list_mongodb_collections(db):
#     try:
#         collections = db.list_collection_names()
#         print("\nMongoDB 数据库中的集合:")
#         for collection in collections:
#             print(f"- {collection}")
#     except Exception as err:
#         print(f"MongoDB 集合查询失败: {err}")
