import random

def execute_query(connection, query):
    """
    Executes an SQL query and returns the result.
    """
    cursor = connection.cursor()
    cursor.execute(query)
    return cursor.fetchall()

def get_joinable_tables(connection, schema_name):
    """
    Dynamically retrieves pairs of tables and columns for potential JOINs.
    Prioritizes foreign key relationships, then shared columns, and finally supports cartesian product (CROSS JOIN).
    """
    cursor = connection.cursor()

    # Step 1: Retrieve all foreign key relationships
    query_fk = f"""
        SELECT TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
        FROM information_schema.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = '{schema_name}' AND REFERENCED_TABLE_NAME IS NOT NULL;
    """
    cursor.execute(query_fk)
    fk_relationships = cursor.fetchall()

    if fk_relationships:
        # If foreign key relationships exist, return them
        joinable_tables = [
            (row[0], row[1], row[2], row[3]) for row in fk_relationships
        ]
        return joinable_tables

    # Step 2: Retrieve all tables and their columns
    cursor.execute("SHOW TABLES;")
    tables = [table[0] for table in cursor.fetchall()]

    columns = {}
    for table in tables:
        cursor.execute(f"DESCRIBE {table};")
        columns[table] = [col[0] for col in cursor.fetchall()]

    # Step 3: Check for shared columns between tables
    shared_pairs = []
    for table_a in tables:
        for table_b in tables:
            if table_a != table_b:
                shared_columns = set(columns[table_a]).intersection(columns[table_b])
                for shared_column in shared_columns:
                    shared_pairs.append((table_a, shared_column, table_b, shared_column))

    if shared_pairs:
        # If shared columns exist, return them
        return shared_pairs

    # Step 4: If no shared columns or foreign keys, generate cartesian product (CROSS JOIN)
    cartesian_pairs = []
    for table_a in tables:
        for table_b in tables:
            if table_a != table_b:
                cartesian_pairs.append((table_a, None, table_b, None))  # No specific columns

    return cartesian_pairs


def find_tables_with_numeric_and_group_columns(connection, schema_name):
    """
    Finds tables in the given schema that have both numeric columns and groupable columns.

    Parameters:
        connection: MySQL connection object
        schema_name: The schema name to search in

    Returns:
        A dictionary where the keys are table names, and the values are tuples:
        (numeric_columns, groupable_columns)
    """
    cursor = connection.cursor()
    cursor.execute(f"USE {schema_name};")

    # Get all tables in the schema
    cursor.execute("SHOW TABLES;")
    tables = [table[0] for table in cursor.fetchall()]

    # Initialize the result dictionary
    valid_tables = {}

    for table in tables:
        # Get column info for the table
        cursor.execute(f"DESCRIBE {table};")
        columns = cursor.fetchall()

        # Separate numeric and groupable columns
        numeric_columns = [
            col[0] for col in columns
            if ("int" in col[1] or "decimal" in col[1] or "float" in col[1]) and "_id" not in col[0].lower()
        ]
        groupable_columns = [
            col[0] for col in columns
            if "varchar" in col[1] or "char" in col[1] or "text" in col[1]
        ]

        # If both numeric and groupable columns exist, add to results
        if numeric_columns and groupable_columns:
            valid_tables[table] = (numeric_columns, groupable_columns)

    return valid_tables


def generate_construct_specific_query(connection, schema_name, construct_type): 
    """
    Generates a specific type of SQL query based on the schema and construct type.
    Returns the query, a natural language description, and the result.
    """
    cursor = connection.cursor()

    # Get all tables in the schema
    cursor.execute("SHOW TABLES;")
    tables = [table[0] for table in cursor.fetchall()]

    if construct_type in ["group by", "having"]:




        valid_tables = []
        for table_name in tables:
            cursor.execute(f"DESCRIBE {table_name};")
            columns = cursor.fetchall()

            # 筛选数值型列和分组列
            numeric_columns = [
                col[0] for col in columns
                if ("int" in col[1] or "decimal" in col[1] or "float" in col[1]) and "_id" not in col[0].lower()
            ]
            group_columns = [
                col[0] for col in columns
                if col[0] not in numeric_columns
            ]

            # 如果表满足条件（既有数值型列又有分组列），加入有效表列表
            if numeric_columns and group_columns:
                valid_tables.append((table_name, numeric_columns, group_columns))

        # 如果没有符合条件的表，返回提示
        if not valid_tables:
            return None, "No tables with both numeric and groupable columns available for the operation.", []

        # 随机选择一个符合条件的表
        table_name, numeric_columns, group_columns = random.choice(valid_tables)

        # 随机选择数值列和分组列
        group_column = random.choice(group_columns)
        numeric_column = random.choice(numeric_columns)



        
        # table_name = random.choice(tables)  # tables
        # cursor.execute(f"DESCRIBE {table_name};")
        # columns = cursor.fetchall()

        numeric_columns = [
        col[0] for col in columns
        if ("int" in col[1] or "decimal" in col[1] or "float" in col[1]) and "_id" not in col[0].lower()]
        #print(numeric_columns)
        group_columns = [col[0] for col in columns if col[0] not in numeric_columns]

        
        if not group_columns or not numeric_columns:
            # Fallback for tables without suitable columns
            query = f"SELECT COUNT(*) AS total_rows FROM {table_name};"
            description = f"Fallback query: Count the total rows in the table {table_name} \n" \
            "This fallback query generates because the randomly two attributes chosen for group by and having are not siuitable"
            result = execute_query(connection, query)
            return query, description, result

        # group_column = random.choice(group_columns)
        # numeric_column = random.choice(numeric_columns)

        if construct_type == "group by":
            numeric_func = random.choice(['SUM', 'AVG', 'MAX', 'MIN'])
            query = f"SELECT {group_column}, {numeric_func}({numeric_column}) FROM {table_name} GROUP BY {group_column};"
            description = f"Group by {group_column} and calculate the sum of {numeric_column} in the table {table_name}."

        elif construct_type == "having":
            threshold = random.randint(3, 6)
            numeric_func = random.choice(['SUM', 'AVG', 'MAX', 'MIN'])
            query = f"""
            SELECT {group_column}, {numeric_func}({numeric_column})
            FROM {table_name}
            GROUP BY {group_column}
            HAVING {numeric_func}({numeric_column}) > {threshold};
            """.strip().replace("\n", "").replace("  ", " ")
            description = f"Group by {group_column}, and filter groups where the {numeric_func} of {numeric_column} exceeds 100 in the table {table_name}."

    elif construct_type == "join":
        joinable_tables = get_joinable_tables(connection, schema_name)
        if not joinable_tables:
            return None, "No tables available for join in the current schema.", []

        # Randomly select a joinable pair
        table_a, column_a, table_b, column_b = random.choice(joinable_tables)

        if column_a is None or column_b is None:
            # No columns specified -> Cartesian product (CROSS JOIN)
            query = f"""
                SELECT *
                FROM {table_a} CROSS JOIN {table_b}
                LIMIT 5;
            """.strip().replace("\n", "").replace("  ", " ")
            description = (
                f"Perform a cartesian product (CROSS JOIN) between tables {table_a} and {table_b}. "
                "This join may produce a large result set without meaningful relationships."
            )
        else:
            # Normal join
            query = f"""
                SELECT a.{column_a}, b.*
                FROM {table_a} a
                JOIN {table_b} b ON a.{column_a} = b.{column_b}
                LIMIT 5;
            """.strip().replace("\n", "").replace("  ", " ")
            description = f"Join tables {table_a} and {table_b} on columns {column_a} and {column_b}."

        

    elif construct_type == "order by":
        table_name = random.choice(tables)
        cursor.execute(f"DESCRIBE {table_name};")
        columns = [col[0] for col in cursor.fetchall()]
        column = random.choice(columns)
        order_type = random.choice(["ASC", "DESC"])
        query = f"SELECT * FROM {table_name} ORDER BY {column} {order_type} LIMIT 5;"
        description = f"Order the rows in the table {table_name} by {column} in {order_type} order."

    elif construct_type == "where":
        table_name = random.choice(tables)
        #print(table_name)
        cursor.execute(f"DESCRIBE {table_name};")
        columns = [col[0] for col in cursor.fetchall()]
        cursor.execute(f"DESCRIBE {table_name};")
        columns_num = cursor.fetchall()

        numeric_columns = [
        col[0] for col in columns_num
        if ("int" in col[1] or "decimal" in col[1] or "float" in col[1]) and "_id" not in col[0].lower()]
        #print(numeric_columns)
        column = random.choice(columns)
        #print(column)

        if column not in numeric_columns:
            query = f"SELECT * FROM {table_name} WHERE {column} IS NOT NULL LIMIT 5;"
            description = f"Filter rows in the table {table_name} where the column {column} is not null."
       
        else:
            query = f"SELECT * FROM {table_name} WHERE {column} > 5;"
            description = f"Filter rows in the table {table_name} where the column {column} greater than 5."
        
        


    else:
        return None, f"The construct type {construct_type} is not supported.", []

    result = execute_query(connection, query)
    return query, description, result

def generate_sample_queries(connection, schema_name, num_queries=2): ## no schema name
    """
    Generates a set of sample SQL queries, covering various language constructs.
    
    Parameters:
    - connection: Database connection object
    - schema_name: The currently selected schema name
    - num_queries: Number of queries to generate (default: 5)
    
    Returns:
    - queries: List of query strings
    - descriptions: List of natural language descriptions
    - results: List of query results
    """
    queries = []
    descriptions = []
    results = []
    construct_types = ["group by", "having", "order by", "where", "join"]

    for _ in range(num_queries):
        # Randomly choose a construct type
        construct_type = random.choice(construct_types)

        # Generate a query for the selected construct type
        query, description, result = generate_construct_specific_query(connection, schema_name, construct_type) ## no schema name

        # Append successful queries to the results
        if query:
            queries.append(query)
            descriptions.append(description)
            results.append(result)
        else:
            descriptions.append(f"Failed to generate query for construct type: {construct_type}")

    return queries, descriptions, results
