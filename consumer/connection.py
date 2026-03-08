import os
import pymongo
from gridfs import GridFS
from elasticsearch import Elasticsearch

MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017')
client = pymongo.MongoClient(MONGO_URI)
db = client['podcasts']
fs = GridFS(db)

ES_HOST = os.getenv('ES_HOST', 'http://localhost:9200')
es = Elasticsearch(ES_HOST)
