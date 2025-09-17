from dotenv import load_dotenv, find_dotenv
import os
import pprint
from pymongo import MongoClient

load_dotenv(find_dotenv())

connectionString = os.environ.get("MONGO_URI")

client = MongoClient(connectionString)

dbs = client.list_database_names()

test_db = client.test
collections = test_db.list_collection_names()

production = client.production
data_collection = production.data_collection

printer = pprint.PrettyPrinter()

def project_columns():
    columns = {"_id": 0, "value": 1, "description": 1}
    datas = data_collection.find({}, columns)
    for data in datas:
        printer.pprint(data)

# project_columns()



##############################
# CREATE
def insert_test_doc():
    collection = test_db.datamodels
    test_document = {
        "value": 1,
        "description": "this is a description"
    }
    inserted_id = collection.insert_one(test_document).inserted_id
    print(inserted_id)

def creat_documents():
    values = [3,4,5,6,7,8,9]
    descriptions = ["description of exemple 3",
                    "description of exemple 4",
                    "description of exemple 5",
                    "description of exemple 6",
                    "description of exemple 7",
                    "description of exemple 8",
                    "description of exemple 9",]
    
    docs = []
    for value, description in zip(values, descriptions):
        doc = {"value": value, "description": description}
        docs.append(doc)

    data_collection.insert_many(docs)

##############################

##############################
# READ

def find_all_data():
    datas = data_collection.find()

    for data in datas:
        printer.pprint(data)

# find_all_data()

def find_data_by_value(value: int):

    # SELECT * FROM datas WHERE value = {id}

    looked_value = data_collection.find_one({"value":value})
    printer.pprint(looked_value)

# find_data_by_id(4)

def count_all_data():
    count = data_collection.count_documents(filter={})
    print(f"Number of datas: {count}")

# count_all_data()

def get_data_by_id(id: str):
    from bson.objectid import ObjectId
    
    _id = ObjectId(id)
    data = data_collection.find_one({"_id": _id})
    printer.pprint(data)

# get_data_by_id("68caf7be7e758608567e88a6")

def get_value_range(min_value, max_value):
    """
    SELECT * FROM data WHERE value >= min_value AND max_value <= max_value
    """
    query = {"$and":
            [
                {"value": {"$gte": min_value}},
                {"value": {"$lte": max_value}}
            ]
            }
    
    datas = data_collection.find(query).sort("value")
    for data in datas:
        printer.pprint(data)

# get_value_range(4,8)
##############################

##############################
# UPDATE

def update_data_by_id(id: str):
    from bson.objectid import ObjectId

    _id = ObjectId(id)

    # all_updates = {
        # "$set": {
        #     "owner": True
        # },
        # "$inc": {
        #     "value": -4
        # },
        # "$rename": {
        #     "validation": "updated"
        # }
        # "$rename": {
        #     "owner": "validation"
        # }
    # }
    # data_collection.update_one({"_id": _id}, all_updates)

# update_data_by_id("68caf7be7e758608567e88a3")
##############################

##############################
# DELETE
def delete_doc_by_id(id: str):
    from bson.objectid import ObjectId
    _id = ObjectId(id)

    data_collection.delete_one({"_id": _id})

# delete_doc_by_id("68caf7be7e758608567e88a9")

def delete_all():
    data_collection.delete_many({})
##############################

###############################
######### RELATIONS ###########
###############################


