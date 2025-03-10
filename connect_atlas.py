from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from constants import atlas_pswd

uri = f"mongodb+srv://varun:{atlas_pswd}@cluster0.j5gm2.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

client = MongoClient(uri, server_api=ServerApi('1'))

try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)