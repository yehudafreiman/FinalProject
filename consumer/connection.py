import pymongo
from elasticsearch import Elasticsearch

client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client.test
collection = db['test']

es = Elasticsearch("http://localhost:9200")
