import re
import random
from pymongo import MongoClient

def random_mongo_query(db_name):

    client = MongoClient("mongodb://localhost:27017/")
    db = client[db_name]
    tables = db.list_collection_names()

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

    # random choose a table
    table = random.choice(tables)   
    operations = ["find", "aggregate", "distinct"]  
    operation = random.choice(operations)
    variable = random.choice(list(table_variable_types[table]))

    if operation == "find":
        
        if table_variable_types[table][variable] == 'object':
            operator = "$elemMatch"
            keys = list(random.choice(collections_dict[table][variable]).keys())
            values = list(random.choice(collections_dict[table][variable]).values())
            return table, operation, {f'{variable}.{keys[0]}': values[0], f'{variable}.{keys[1]}': values[1]}, f"Find data in table {table} where {variable} meets the condition."
            
        elif table_variable_types[table][variable] == 'int':
            operator = random.choice(["$gte", "$lte", "$gt", "$lt", "$eq", "$ne"])
            value = random.choice(collections_dict[table][variable])
            return table, operation, {variable: {operator: value, "$exists": True}}, f"Find data in table {table} where {variable} meets the condition."

        elif table_variable_types[table][variable] == 'string':
            number = random.randint(1,3)
            if number == 1:
                operator = "$regex"
                random_value = random.choice(collections_dict[table][variable])
                value = random.choice(re.findall(r'[\w\s]+', random_value))
                return table, operation, {variable: {operator: re.compile(value, re.IGNORECASE)}}, f"Find data in table {table} where {variable} meets the condition."
            elif number == 2:
                operator = random.choice(["$eq", "$ne"])
                value = random.choice(collections_dict[table][variable])
                return table, operation, {variable: {operator: value, "$exists": True}}, f"Find data in table {table} where {variable} meets the condition."
            else:
                operator = random.choice(["$or", "$and"])
                keys = random.sample(list(table_variable_types[table]), k = 2)
                value1 = random.choice(collections_dict[table][keys[0]])
                value2 = random.choice(collections_dict[table][keys[1]])            
                return table, operation, {operator: [{keys[0]: value1}, {keys[1]: value2}]}, f"Find data in table {table} where {variable} meets the condition."
                
        elif table_variable_types[table][variable] == 'array':
            number = random.randint(1,2)
            if number == 1:
                operator = "$elemMatch"
                value = random.choice(collections_dict[table][variable])
                return table, operation, {variable: {operator: value}}, f"Find data in table {table} where {variable} meets the condition."
            else:
                operator = random.choice(["$in", "$nin", "all"])
                value = random.sample(collections_dict[table][variable], k = 2)
                return table, operation, {variable: {operator: value}}

    elif operation == "distinct":
        variable = random.choice(list(table_variable_types[table]))
        return table, operation, variable, f"Retrieve distinct values of {variable} from table {table}."

    elif operation == "aggregate":
        pipelines = []
        operator = random.choice(["sum", "avg", "min", "max"])
        string_field = [key for key, value in table_variable_types[table].items() if value == 'string']
        numeric_field = [key for key, value in table_variable_types[table].items() if value == 'int']
        if not string_field or not numeric_field:
            return random_mongo_query(db_name)
        group_field = random.choice(string_field)
        numeric_field = random.choice(numeric_field)

        pipelines.append({"$group": {"_id": f"${group_field}", f"{operator}": {f"${operator}": f"${numeric_field}"}}})
        pipelines.append({"$project": {"_id": 1, f"{operator}": 1}})
        pipelines.append({"$sort": {f"{operator}": 1}})
        pipelines.append({"$limit": 3})

        return table, operation, pipelines, f"Aggregate data in table {table} by {variable} with transformations."

if __name__ == "__main__":
    for _ in range(5):        # Generate 5 sample queries
        query = random_mongo_query()
        print(query)