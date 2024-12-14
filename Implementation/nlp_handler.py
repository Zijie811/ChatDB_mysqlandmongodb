import random
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from sample_queries import get_joinable_tables


def preprocess_query(natural_query):
    """
    Preprocesses the natural language query by tokenizing, lowercasing,
    and removing stopwords.
    """
    sql_keywords = {'having'}
    stop_words = set(stopwords.words('english')) - sql_keywords
    tokens = word_tokenize(natural_query.lower())  # Tokenize and lowercase
    preprocessed_tokens = [word for word in tokens if word not in stop_words]  # Remove stopwords
    return preprocessed_tokens


def match_query_pattern(preprocessed_tokens):
    """
    Matches the preprocessed query tokens to predefined query patterns.
    Handles combinations like 'group by' with 'having' or aggregation functions.
    """
    tokens = " ".join(preprocessed_tokens)
    print(preprocessed_tokens)
    print(tokens)


    # Combined patterns
    if "group" in tokens and "having" in tokens:
        return "group_by_having"
    if "group" in tokens and any(func in tokens for func in ["max", "min", "sum", "avg", "count",'maximum', 'minimum', 'average']):
        return "group_by_aggregation"

    # Individual patterns
    if "join" in tokens:
        return "join"
    if "group by" in tokens:
        return "group_by"
    if "having" in tokens:
        return "having"
    if "max" in tokens or "maximum" in tokens:
        return "max"
    if "min" in tokens or "minimum" in tokens:
        return "min"
    if "sum" in tokens or "total" in tokens:
        return "sum"
    if "avg" in tokens or "average" in tokens:
        return "avg"
    if "greater" in tokens or "more" in tokens:
        return "greater_than"
    if "less" in tokens or "smaller" in tokens:
        return "smaller_than"
    if "order" in tokens:
        if "desc" in tokens or "descending" in tokens:
            return "order_desc"
        return "order_asc"  # Default to ascending if "desc" is not specified

    return "unknown"


def find_table_by_column(connection, schema_name, column_name):
    """
    Searches for the table containing the specified column in the given schema.
    """
    cursor = connection.cursor()
    query = f"""
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema_name}' AND COLUMN_NAME = '{column_name}';
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result[0][0] if result else None


def execute_query(connection, query):
    """
    Executes an SQL query and returns the result.
    """
    cursor = connection.cursor()
    cursor.execute(query)
    return cursor.fetchall()


def generate_sql_from_nlp(connection, schema_name, natural_query):
    """
    Generates an SQL query from a natural language query by matching it to
    predefined query types and columns within the schema.
    """
    # Step 1: Preprocess the natural query
    preprocessed_tokens = preprocess_query(natural_query)
    query_type = match_query_pattern(preprocessed_tokens)

    # Step 2: Retrieve all tables in the schema
    cursor = connection.cursor()
    cursor.execute("SHOW TABLES;")
    tables = [table[0] for table in cursor.fetchall()]

    # Step 3: Handle different query types
    # Aggregation Queries (with or without GROUP BY)
    if query_type in ["max", "min", "sum", "avg"]:
        column_name = None
        table_name = None
        group_column = None

        for i, token in enumerate(preprocessed_tokens):
            if token in ["maximum", "max", "minimum", "min", "sum", "total", "average", "avg"]:
                if i + 1 < len(preprocessed_tokens):
                    column_name = preprocessed_tokens[i + 1]
            if token == "by" and i + 1 < len(preprocessed_tokens):
                group_column = preprocessed_tokens[i + 1]

        for table in tables:
            if table in preprocessed_tokens:
                table_name = table
                break

        if not column_name:
            return None, "Unable to determine the column for the aggregation query.", []

        if not table_name:
            table_name = find_table_by_column(connection, schema_name, column_name)

        if not table_name:
            return None, f"The column '{column_name}' does not exist in any table within the schema '{schema_name}'.", []

        # Construct query with or without GROUP BY

        # if query_type == "group_by_aggregation" or group_column:
        #     query = f"SELECT {group_column}, {query_type.upper()}({column_name}) FROM {table_name} GROUP BY {group_column};"
        #     description = f"Group by '{group_column}' and calculate the {query_type} of '{column_name}' in the table '{table_name}'."
        # else:
        query = f"SELECT {query_type.upper()}({column_name}) FROM {table_name};"
        description = f"Find the {query_type} value in the column '{column_name}' from the table '{table_name}'."
        

        result = execute_query(connection, query)
        return query, description, result

    # Greater or Smaller Comparisons
    elif query_type in ["greater_than", "smaller_than"]:
        comparison = ">" if query_type == "greater_than" else "<"
        column_name = None
        value = None
        table_name = None

        for i, token in enumerate(preprocessed_tokens):
            if token in ["greater", "more", "less", "smaller"] and i + 1 < len(preprocessed_tokens):
                column_name = preprocessed_tokens[i - 1]
                value = preprocessed_tokens[i + 1]
                print(column_name, value)

        for table in tables:
            if table in preprocessed_tokens:
                table_name = table
                break

        if not column_name or not value:
            return None, "Unable to determine the column or value for the comparison query.", []

        if not table_name:
            table_name = find_table_by_column(connection, schema_name, column_name)

        if not table_name:
            return None, f"The column '{column_name}' does not exist in any table within the schema '{schema_name}'.", []

        query = f"SELECT * FROM {table_name} WHERE {column_name} {comparison} {value};"
        description = f"Select all rows from the table '{table_name}' where '{column_name}' is {comparison} {value}."
        result = execute_query(connection, query)
        return query, description, result

    # JOIN Queries
    elif query_type == "join":
        joinable_tables = get_joinable_tables(connection, schema_name)
        if not joinable_tables:
            return None, "No tables available for join in the current schema.", []

        table_a, column_a, table_b, column_b = random.choice(joinable_tables)
        query = f"""
            SELECT a.*, b.*
            FROM {table_a} a
            JOIN {table_b} b ON a.{column_a} = b.{column_b}
            LIMIT 5;
        """.strip()
        description = f"Join tables '{table_a}' and '{table_b}' on columns '{column_a}' and '{column_b}'."
        result = execute_query(connection, query)
        return query, description, result

    # ORDER BY Queries
    elif query_type in ["order_asc", "order_desc"]:
        order = "ASC" if query_type == "order_asc" else "DESC"
        column_name = None
        table_name = None

        for table in tables:
            if table in preprocessed_tokens:
                table_name = table
                break

        for i, token in enumerate(preprocessed_tokens):
            print(token)
            if token == "order" and i + 1 < len(preprocessed_tokens): # and preprocessed_tokens[i + 1] == "order":
                column_name = preprocessed_tokens[i + 1]
                print(column_name)

        if not column_name:
            return None, "Unable to determine the column to order by.", []

        if not table_name:
            table_name = find_table_by_column(connection, schema_name, column_name)

        if not table_name:
            return None, f"The column '{column_name}' does not exist in any table within the schema '{schema_name}'.", []

        query = f"SELECT * FROM {table_name} ORDER BY {column_name} {order} LIMIT 5;"
        description = f"Order all rows in the table '{table_name}' by '{column_name}' in {order} order."
        result = execute_query(connection, query)
        return query, description, result
    
    elif query_type == "group_by_aggregation":
        column_name = None
        group_column = None
        aggregation_function = None

        for i, token in enumerate(preprocessed_tokens):
            if token in ["average", "avg", "sum", "max", "min", "count", 'maximum', 'minimum']:
                aggregation_function = token
                if i + 1 < len(preprocessed_tokens):
                    column_name = preprocessed_tokens[i + 1]
                    print(column_name)
            if token == "group" and i + 1 < len(preprocessed_tokens): #by
                group_column = preprocessed_tokens[i + 1]
                print(group_column)


        if not column_name or not group_column:
            return None, "Unable to determine both the aggregation and grouping columns.", []

        # table_name = find_table_by_column(connection, schema_name, [column_name, group_column])
        table_name = find_table_by_column(connection, schema_name, column_name)
        print(table_name)
        if not table_name:
            return None, f"No table found containing columns '{column_name}' and '{group_column}' in schema '{schema_name}'.", []
        
        aggregation_function = aggregation_function.lower()
        if aggregation_function in ["average", "avg"]:
            aggregation_function = "AVG"
        elif aggregation_function in ["sum", "total"]:
            aggregation_function = "SUM"
        elif aggregation_function in ["max", "maximum"]:
            aggregation_function = "MAX"
        elif aggregation_function in ["min", "minimum"]:
            aggregation_function = "MIN"
        elif aggregation_function == "count":
            aggregation_function = "COUNT"


        query = f"SELECT {group_column}, {aggregation_function}({column_name}) FROM {table_name} GROUP BY {group_column};"
        print(query)
        description = f"Calculate the average of '{column_name}' grouped by '{group_column}' in the table '{table_name}'."
        result = execute_query(connection, query)
        return query, description, result

    # Group By Having Queries
    elif query_type == "group_by_having":
        column_name = None
        group_column = None
        having_value = None
        aggregation_function = None

        for i, token in enumerate(preprocessed_tokens):
            if token == "group" and i + 1 < len(preprocessed_tokens):
                group_column = preprocessed_tokens[i + 1]
            if token in ["sum", "avg", "count", "max", "min", 'average', 'maximum', 'minimum']:
                aggregation_function = token
                column_name = preprocessed_tokens[i + 1] if i + 1 < len(preprocessed_tokens) else None
            if token == "greater" and i + 1 < len(preprocessed_tokens): #and preprocessed_tokens[i + 1] == "than":
                having_value = preprocessed_tokens[i + 1]

        if not column_name or not group_column or not having_value:
            return None, "Unable to determine columns or having condition for the query.", []

        table_name = find_table_by_column(connection, schema_name, column_name)
        
        if not table_name:
            return None, f"No table found containing columns '{column_name}' and '{group_column}' in schema '{schema_name}'.", []
        

        aggregation_function = aggregation_function.lower()
        if aggregation_function in ["average", "avg"]:
            aggregation_function = "AVG"
        elif aggregation_function in ["sum", "total"]:
            aggregation_function = "SUM"
        elif aggregation_function in ["max", "maximum"]:
            aggregation_function = "MAX"
        elif aggregation_function in ["min", "minimum"]:
            aggregation_function = "MIN"
        elif aggregation_function == "count":
            aggregation_function = "COUNT"

        query = f"""
            SELECT {group_column}, {aggregation_function}({column_name})
            FROM {table_name}
            GROUP BY {group_column}
            HAVING {aggregation_function}({column_name}) > {having_value};
        """.strip()
        description = f"Group by '{group_column}' and calculate the sum of '{column_name}' where the sum exceeds {having_value} in the table '{table_name}'."
        result = execute_query(connection, query)
        return query, description, result

    # Default case: Unknown Query Type
    return None, f"Unable to process the query: '{natural_query}'. Please try rephrasing.", []
