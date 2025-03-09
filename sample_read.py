from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from constants import atlas_pswd
import json

def read(mongo_client, which_city):
    # uri = f"mongodb+srv://varun:{atlas_pswd}@cluster0.j5gm2.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

    # client = MongoClient(uri, server_api=ServerApi('1'))
    # client = MongoClient('localhost', 27017)
    database = mongo_client["newsdata"]
    COLELCTION_NAME = "articles"
    collection = database[COLELCTION_NAME]
    items = collection.find(filter={"city": which_city})

    def convert_to_json_serializable(obj):
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        elif hasattr(obj, '__str__'):
            return str(obj)
        return obj

    result_list = list(items)
    json_data = json.dumps(result_list, default=convert_to_json_serializable, indent=4)

    return json_data