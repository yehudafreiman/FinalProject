import os
from elasticsearch import Elasticsearch

class Queries:
    def __init__(self):
        self.es = Elasticsearch(os.getenv('ES_HOST', 'http://localhost:9200'))

    def all_content(self):
        result = self.es.search(
            index='podcasts',
            query={'match_all': {}},
            size=50
        )
        return result['hits']['hits']



    