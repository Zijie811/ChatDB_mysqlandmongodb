import re
import random
import user_interface
import db_connection
import query_generator
from pymongo import MongoClient

def list_databases(client):
    # List all available MongoDB databases.
    databases = client.list_database_names()

    if not databases:
        print("No database available.")
        return None

    print("\nHere are the available MongoDB databases:")
    for i, database in enumerate(databases, 1):
        print(f"{i}. {database}")
    return databases

query_patterns = {
    # Matches "find total <A> by <B>"
    "total <A> by <B>": {
        "regex": r"total (\w+) by (\w+)",
        "handler": lambda a, b: [
            {"$group": {"_id": f"${b}", "total": {"$sum": f"${a}"}}}
        ]
    },
    # Matches "find all <A>" 
    "find all <A>": {
        "regex": r"find all (\w+)",
        "handler": lambda a: {"find": {}, "projection": {a: 1}}
    },
    # Matches "show <A> where <B>"
    "show <A> where <B>": {
        "regex": r"show (\w+) where (.+)",
        "handler": lambda a, b: [
            {"$match": {b.split('=')[0].strip(): b.split('=')[1].strip()}}
        ]
    },
    # Matches "sort <A> by <B>"
    "sort <A> by <B>": {
        "regex": r"sort (\w+) by (\w+)",
        "handler": lambda a, b: [{"$sort": {b: 1 if a.lower() == "asc" else -1}}]
    },
    # Matches "max/highest <A> by <B>"
    "max/highest <A> by <B>": {
        "regex": r"(?:max|highest) (\w+) by (\w+)",
        "handler": lambda a, b: [
            {"$group": {"_id": f"${b}", "max_value": {"$max": f"${a}"}}}
        ]
    },
}

def detect_query_pattern(user_query):
    user_query = user_query.lower()
    for pattern_name, pattern_details in query_patterns.items():
        match = re.search(pattern_details["regex"], user_query)
        if match:
            variables = match.groups()  # Extract matched variables
            return pattern_name, pattern_details["handler"], variables
    return None, None, None

def process_natural_language_query(user_query, db, table):
    pattern_name, handler, variables = detect_query_pattern(user_query)

    if pattern_name and handler:
        # Generate the MongoDB query using the handler and variables
        query = handler(*variables)

        # Execute the query
        if isinstance(query, list):  
            results = db[table].aggregate(query)
            print(f"\nQuery: db.{table}.aggregate({query})")
        elif isinstance(query, dict): 
            results = db[table].find(query["find"], query["projection"])
            print(f"\nQuery: db.{table}.find({query})")
        else:
            return "Invalid query type detected."

        # Display results
        result_list = list(results)
        if result_list:
            print("Results:")
            for doc in result_list:
                print(doc)
        else:
            print("No results found.")
    else:
        print("No matching query pattern found.")

def mongodb_workflow(client):

    # List databases
    databases = list_databases(client)
    if not databases:
        return
    
    db_choice = int(input("Select a database to explore: "))
    db_name = databases[db_choice - 1]
    db = client[db_name]

    tables = db.list_collection_names()
    print(f"\nThere are {len(tables)} tables in the {db_name} database:")
    if not tables:
        print("No collections found in this database.")
        return None

    # Iterate over collections and display their attributes and sample data
    for i, table_name in enumerate(tables, 1):
        table = db[table_name]
        sample_data = list(table.find().limit(3))
        print(f"\n{i}. {table_name} table")

        if sample_data:
            attributes = sample_data[0].keys()
            print(f"Attributes: {list(attributes)}")
            print("Sample Data:")
            for record in sample_data:
                print(record)
        else:
            print("No data found in this collection.")

    while True:
    
        print("\nAvailable features:")
        print("1. Obtain sample queries")
        print("2. Obtain queries with specific language constructs")
        print("3. Ask questions in natural language")
        print("4. Upload dataset into this database")
        print("5. Change a database to explore")
        print("0. Exit")
        choice = input("Choose an option: ")

        if choice == "1":
            for i in range(5):
                table, operation, query, representaion = query_generator.random_mongo_query(db_name)
                if operation == "distinct":
                    print(f"\n{i+1}.Query: ", f'db.{table}.{operation}("{query}")')
                    print("Representaion:", representaion)
                    results = getattr(db[table], operation)(query)
                    print("Result: ", db[table].distinct(query))
                else:
                    print(f"\n{i+1}.Query: ", f'db.{table}.{operation}({query})')
                    print("Representaion:", representaion)
                    results = getattr(db[table], operation)(query)  
                    result_list = list(results) 
                    if not result_list: 
                        print("Do not find any result.")
                    else:
                        print("Result: ") 
                        for doc in result_list:
                            print(doc)
            continue

        if choice == "2":
            table_variable_types = {}
            for table in tables:
                data = list(db[table].find())
                attributes = list(data[0].keys())
                attributes.remove('_id')
                project_stage = {attr: {"$type": f"${attr}"} for attr in attributes}
                project_stage["_id"] = 0
                table_variable_type = list(db[table].aggregate([{"$project": project_stage}, {"$limit": 1}]))[0]
                table_variable_types[table] = table_variable_type

            collections_dict = {}
            collections = db.list_collection_names()
            for collection_name in collections:
                collection = db[collection_name]
                attributes_with_values = []
                documents = collection.find({}, {"_id": 0})
                field_values = {}
                for doc in documents:
                    for field, value in doc.items():
                        if field not in field_values:
                            field_values[field] = []
                        field_values[field].append(value)
                collections_dict[collection_name] = field_values

            print("Available language constructs for mongodb query, such as sort, group by, order by, match, limit, aggregation:")
            option = input("Choose an option: ").lower()

            if option == "sort":
                table = random.choice(tables)  # Randomly choose a table
                variable = random.choice(list(table_variable_types[table]))
                query = [{"$sort": {f"{variable}": 1}}] 
                print(f'\nQuery: db.{table}.find({query})')
                print(f"Representation: Sort table {table} by {variable}.")
                results = db[table].aggregate(query)
                print("results: ")
                for doc in results:
                    print(doc)
            elif option == "group by" or option == "groupby":
                table = random.choice(tables)
                group_variable = random.choice(list(table_variable_types[table]))
                query = [
                    {"$group": {"_id": f"${group_variable}", "count": {"$sum": 1}}}
                ]
                print(f'\nQuery: db.{table}.aggregate({query})')
                print(f"Representation: Group table {table} by {group_variable}.")
                results = db[table].aggregate(query)
                print("Results: ")
                for doc in results:
                    print(doc)
            elif option == "order by" or option == "orderby":
                table = random.choice(tables)
                variable = random.choice(list(table_variable_types[table]))
                query = [{"$sort": {f"{variable}": -1}}]
                print(f'\nQuery: db.{table}.aggregate({query})')
                print(f"Representation: Order table '{table}' by {variable} in descending order.")
                results = db[table].aggregate(query)
                print("Results: ")
                for doc in results:
                    print(doc)
            elif option == "match":
                table = random.choice(tables)
                variable = random.choice(list(table_variable_types[table]))
                value = random.choice(collections_dict[table][variable])
                query = [{"$match": {f"{variable}": value}}]
                print(f'\nQuery: db.{table}.aggregate({query})')
                print(f"Representation: Match documents in table {table} where {variable} equals {value}.")
                results = db[table].aggregate(query)
                print("Results: ")
                for doc in results:
                    print(doc)
            elif option == "limit":
                table = random.choice(tables)
                limit_value = random.randint(1, 10)
                query = [{"$limit": limit_value}]
                print(f'\nQuery: db.{table}.aggregate({query})')
                print(f"Representation: Limit table {table} to {limit_value} documents.")
                results = db[table].aggregate(query)
                print("Results: ")
                for doc in results:
                    print(doc)
            elif option == "aggregation" or option == "aggregate":
                table = random.choice(tables)
                variable = random.choice(list(table_variable_types[table]))
                query = [
                    {"$group": {"_id": f"${variable}", "total": {"$sum": 1}}},
                    {"$sort": {"total": -1}}
                ]
                print(f'\nQuery: db.{table}.aggregate({query})')
                print(f"Representation: Aggregate table {table}, grouping by {variable}, and sorting by total count in descending order.")
                results = db[table].aggregate(query)
                print("Results: ")
                for doc in results:
                    print(doc)
            else:
                print("Cannot recognize. ")
            continue

        if choice == "3":
            tables = db.list_collection_names()
            print(f"\nThere are {len(tables)} tables in the {db_name} database:")
            if not tables:
                print("No collections found in this database.")
                continue
            for i, table_name in enumerate(tables, 1):
                table = db[table_name]
                sample_data = list(table.find().limit(2))
                print(f"\n{i}. {table_name}")
                if sample_data:
                    attributes = sample_data[0].keys()
                    print(f"Attributes: {list(attributes)}")
                    print("Sample Data:")
                    for record in sample_data:
                        print(record)
                else:
                    print("No data found in this collection.")
            table = input("\nSpecify the collection to query: ")
            user_query = input("Ask your question in natural language: ")
            process_natural_language_query(user_query, db, table)
            continue

        if choice == "4":
            file_name = input(f"\nEnter the file name to upload into {db_name} dataset: ")
            collection = db[file_name.split(".")[0]]

            with open(file_name,"r") as file:
                import json
                data = json.load(file)

            if isinstance(data, list):
                collection.insert_many(data)
            else:
                collection.insert_one(data)
            print("Data inserted successfully!")

        elif choice == "5":
            mongodb_workflow(client)  # Restart workflow for a new database
            break

        elif choice == "0":
            print("Exiting MongoDB exploration.")
            break

        else:
            print(f"\nFeature {choice} not implemented yet. Please try again.")

def main():
    db_choice = input("please select a database type:(MySQL or MongoDB): ").strip().lower()
    if db_choice == "mysql":
        # 提示用户选择一个 schema
        
        schemas = db_connection.get_available_schemas()
        if not schemas:
            print("unable to find any schema, please check database connection")
            return
        
        print("schemas available:")
        for i, schema in enumerate(schemas, 1):
            print(f"{i}. {schema}")
        # cursor = connection.cursor()
        # cursor.execute("SHOW DATABASES;")
        # schemas = [row[0] for row in cursor.fetchall()]
        # schemas = ["coffee_sales", "employee_info", "library_borrowing"]
        # for i, schema in enumerate(schemas, 1):
        #     print(f"{i}. {schema}")
        
        schema_choice = int(input("please choose a schema："))
        schema_name = schemas[schema_choice - 1]
        
        # 建立连接并传递 schema
        connection = db_connection.get_mysql_connection(schema_name)
        if connection:
            while True:
                choice = user_interface.main_menu()
                if choice == "1":
                    user_interface.explore_database(connection, schema_name)
                elif choice == "2":
                    user_interface.generate_sample_query_interface(connection, schema_name) # no schema_name
                elif choice == "3":
                    user_interface.generate_construct_specific_query_interface(connection, schema_name) #no schema name?
                elif choice == "4":
                    user_interface.process_nlp_query(connection, schema_name)
                elif choice == '5':
                    user_interface.upload_csv_interface(connection, schema_name)
                elif choice == "6":
                    main()  # Restart the main function to return to the main menu
                    break
                elif choice == "0":
                    print("exit the program")
                    break
                else:
                    print("invalid choice, please enter the number again")
            connection.close()
        else:
            print("connection error, please check the database set up")
    # ---------- mongodb ----------
    elif db_choice == "mongodb":
        try:
            client = MongoClient("mongodb://localhost:27017/")
            client.admin.command("ping")      # Test connection
            print("Welcome to ChatDB! MongoDB connected successfully!")
            mongodb_workflow(client)

        except Exception as err:
            print("Failed to connect to MongoDB, please check the database settings.")

    else:
        print("Unrecognized database type. Please choose either MySQL or MongoDB.")


if __name__ == "__main__":
    main()

