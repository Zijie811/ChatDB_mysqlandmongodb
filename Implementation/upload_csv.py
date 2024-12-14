import os
import pandas as pd
import mysql.connector


def ensure_upload_directory_exists():
    """
    Ensures the 'upload' directory exists for storing user-uploaded CSV files.
    """
    upload_dir = "upload"
    if not os.path.exists(upload_dir):
        os.makedirs(upload_dir)
    return upload_dir


def upload_csv_with_scenarios(connection, schema_name, csv_file_path):
    """
    Handles uploading a CSV file to the database with focus on:
    - Scenario 1: CSV columns match a table in the schema.
    - Scenario 4: CSV unrelated to schema, allowing creation of a new table or schema.

    Parameters:
        connection: MySQL connection object
        schema_name: Current database schema
        csv_file_path: Path to the CSV file
    """
    # Ensure the 'upload' directory exists
    upload_dir = ensure_upload_directory_exists()

    # Check if the file exists
    if not os.path.exists(csv_file_path):
        return f"File '{csv_file_path}' not found in the '{upload_dir}' directory."

    # Step 1: Read CSV
    try:
        data = pd.read_csv(csv_file_path)
    except Exception as e:
        return f"Failed to read CSV file: {e}"

    if data.empty:
        return "The CSV file is empty."

    cursor = connection.cursor()
    cursor.execute(f"USE {schema_name};")
    cursor.execute("SHOW TABLES;")
    tables = [table[0] for table in cursor.fetchall()]
    schema_info = {}

    # Step 2: Get schema information
    for table in tables:
        cursor.execute(f"DESCRIBE {table};")
        columns = cursor.fetchall()
        schema_info[table] = {col[0]: col[1] for col in columns}  # column name: type

    csv_columns = list(data.columns)

    # Step 3: Match CSV columns with tables
    matched_table = None
    for table, table_columns in schema_info.items():
        table_column_names = list(table_columns.keys())
        if set(csv_columns) == set(table_column_names):  # Full match
            matched_table = table
            break

    # Step 4: Handle matched scenario
    if matched_table:
        print(f"CSV matches table '{matched_table}' in schema '{schema_name}'.")
        placeholders = ", ".join(["%s" for _ in csv_columns])
        insert_query = f"INSERT INTO `{matched_table}` ({', '.join([f'`{col}`' for col in csv_columns])}) VALUES ({placeholders});"

        # Insert data into the matched table
        try:
            for row in data.itertuples(index=False):
                cursor.execute(insert_query, row)
            connection.commit()
            return f"Data successfully uploaded to table '{matched_table}' in schema '{schema_name}'."
        except mysql.connector.Error as e:
            return f"Failed to insert data into '{matched_table}': {e}"

    # Scenario 4: CSV unrelated to schema
    create_new = input(f"The CSV does not match any table in schema '{schema_name}'. Create a new table in this schema? (yes/no): ").strip().lower()
    if create_new == "yes":
        table_name = input("Enter the name for the new table: ").strip()
        column_definitions = ", ".join([f"`{col}` VARCHAR(255)" for col in csv_columns])
        create_table_query = f"CREATE TABLE `{table_name}` ({column_definitions});"

        try:
            cursor.execute(create_table_query)
            placeholders = ", ".join(["%s" for _ in csv_columns])
            insert_query = f"INSERT INTO `{table_name}` ({', '.join([f'`{col}`' for col in csv_columns])}) VALUES ({placeholders});"

            for row in data.itertuples(index=False):
                cursor.execute(insert_query, row)
            connection.commit()
            return f"Data successfully uploaded to new table '{table_name}' in schema '{schema_name}'."
        except mysql.connector.Error as e:
            return f"Failed to create table or insert data: {e}"

    create_new_schema = input("Would you like to create a new schema for this CSV? (yes/no): ").strip().lower()
    if create_new_schema == "yes":
        new_schema_name = input("Enter the name for the new schema: ").strip()
        try:
            cursor.execute(f"CREATE SCHEMA `{new_schema_name}`;")
            cursor.execute(f"USE `{new_schema_name}`;")
            table_name = input("Enter the name for the new table: ").strip()
            column_definitions = ", ".join([f"`{col}` VARCHAR(255)" for col in csv_columns])
            create_table_query = f"CREATE TABLE `{table_name}` ({column_definitions});"

            cursor.execute(create_table_query)
            placeholders = ", ".join(["%s" for _ in csv_columns])
            insert_query = f"INSERT INTO `{table_name}` ({', '.join([f'`{col}`' for col in csv_columns])}) VALUES ({placeholders});"

            for row in data.itertuples(index=False):
                cursor.execute(insert_query, row)
            connection.commit()
            return f"Data successfully uploaded to new schema '{new_schema_name}' and table '{table_name}'."
        except mysql.connector.Error as e:
            return f"Failed to create schema or table: {e}"

    return "Operation canceled. No changes made."
