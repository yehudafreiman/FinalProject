import pymongo
from gridfs import GridFS
from elasticsearch import Elasticsearch

client = pymongo.MongoClient('mongodb://mongodb:27017/')
db = client['podcasts']
fs = GridFS(db)

es = Elasticsearch("http://elasticsearch:9200")
