import mysql.connector
from pymongo import MongoClient

def get_mysql_connection(schema_name): ## 原本有schema_name
    """
    创建并返回 MySQL 数据库连接
    schema_name: 指定要连接的 schema
    """
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="15261526",
            database=schema_name
        )
        return connection
    except mysql.connector.Error as err:
        print(f"error in connecting database: {err}")
        return None
    
def get_available_schemas():
    """
    Fetches all available schemas (databases) in the MySQL server.
    """
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="15261526"  # Replace with your actual password
        )
        cursor = connection.cursor()
        cursor.execute("SHOW DATABASES;")
        schemas = [row[0] for row in cursor.fetchall()]
        connection.close()
        return schemas
    except mysql.connector.Error as err:
        print(f"Failed to retrieve schemas: {err}")
        return []
    