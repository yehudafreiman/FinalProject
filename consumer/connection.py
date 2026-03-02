import pymongo
import gridfs
from elasticsearch import Elasticsearch

client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client['test']
fs = gridfs.GridFS(db)

es = Elasticsearch("http://localhost:9200")
