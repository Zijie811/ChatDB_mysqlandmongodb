import sample_queries
import db_connection
import explore_db
import nlp_handler
from upload_csv import upload_csv_with_scenarios
import os


def main_menu():
    """
    主菜单
    """
    print("\nWelcome to ChatDB! Please choose a function:")
    print("1. Explore Database")
    print("2. Obtain Sample Query")
    print("3. Obtain Specific Language Construct Query")
    print("4. Process Natural Language Query")
    print("5. Upload CSV to Database")
    print("6. Returning to the main menu")
    print("0. Exit")
    return input("Please enter the function number：")


def explore_database(connection, schema_name):
    """
    交互式探索数据库结构，并查看表信息和示例数据
    """
    print(f"\nexploring database: {schema_name}")
    tables = explore_db.list_mysql_tables(connection)  # 列出所有表

    if not tables:
        print(f"{schema_name} There is no such table in the schema")
        return

    # 提示用户选择一个表
    print("\nPlease choose the table you want to check:")
    for idx, table in enumerate(tables, 1):
        print(f"{idx}. {table}")

    try:
        choice = int(input("Please enter the number of table: "))
        if 1 <= choice <= len(tables):
            table_name = tables[choice - 1]
            explore_db.show_mysql_table_data(connection, table_name)  # 显示表信息和示例数据
        else:
            print("invalid choice")
    except ValueError:
        print("please enter a valid number")

def generate_sample_query_interface(connection, schema_name): # no schema_name
    """
    生成通用 SQL 查询样例并返回自然语言描述和查询结果
    """

    while True:
        query, description, result = sample_queries.generate_sample_queries(connection, schema_name) ### sample_query no schema_name
        print("Natural language description:", description)
        print("The generateed query:", query)
        print("Query result:")
        for row in result:
            print(row)
        ans = input('Do you wanna continue generating sample queries(yes/no): \n')
        if ans.lower() == 'yes':
            continue
        else:
            break


def generate_construct_specific_query_interface(connection, schema_name):
    """
    生成带有特定语言结构的 SQL 查询样例并返回自然语言描述和查询结果
    """
    while True:
        construct_type = input("please enter the specific language construct, for instance: GROUP BY, JOIN, ORDER BY）：").lower()
        query, description, result = sample_queries.generate_construct_specific_query(connection, schema_name, construct_type)
        if query:
            # print("自然语言描述:", description)
            # print("生成的查询:", query)
            # print("查询结果:")
            print("Natural language description:", description)
            print("The generateed query:", query)
            print("Query result:")
            for row in result:
                print(row)
        else:
            print(description)

        ans = input('Do you wanna continue generating specific queries(yes/no): \n')
        if ans.lower() == 'yes':
            continue
        else:
            break


def process_nlp_query(connection, schema_name):
    """
    Process a natural language query and execute the corresponding SQL query.
    """
    natural_query = input("Enter your query in natural language: ")
    preprocessed_tokens = nlp_handler.preprocess_query(natural_query)
    # query_type = nlp_handler.match_query_pattern(preprocessed_tokens) # match_query_pattern
    query, description, result = nlp_handler.generate_sql_from_nlp(connection, schema_name,natural_query)  ##query_type, 

    if query:
        print("Natural Language Description:", description)
        print("Generated Query:", query)
        print("Query Results:")
        for row in result:
            print(row)
    else:
        print(description)


def upload_csv_interface(connection, schema_name):
    """
    User interface for uploading a CSV file to the database.
    """
    print("\n--- Upload CSV to Database ---")
    upload_dir = "upload"

    # List available CSV files in the upload directory
    csv_files = [f for f in os.listdir(upload_dir) if f.endswith(".csv")]
    if not csv_files:
        print(f"No CSV files found in the '{upload_dir}' directory.")
        print("Please place your CSV file in the 'upload' directory and try again.")
        return

    # Display available files
    print(f"Available CSV files in '{upload_dir}':")
    for idx, file_name in enumerate(csv_files, start=1):
        print(f"{idx}. {file_name}")

    # Prompt user to choose a file
    try:
        file_choice = int(input("Enter the number of the file you want to upload: "))
        if file_choice < 1 or file_choice > len(csv_files):
            print("Invalid choice. Returning to main menu.")
            return

        csv_file_path = os.path.join(upload_dir, csv_files[file_choice - 1])
        table_name = input("Enter the name of the table to upload the data to: ")

        # Upload the selected CSV file to the database
        result = upload_csv_with_scenarios(connection, schema_name, csv_file_path)
        print(result)
    except ValueError:
        print("Invalid input. Please enter a number.")



