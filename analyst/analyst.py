import base64
import os
from elasticsearch import Elasticsearch
from logger import Logger

logger = Logger.get_logger()

def decode_list(base64_string):
    base64_bytes = base64_string.encode("ascii")
    sample_string_bytes = base64.b64decode(base64_bytes)
    sample_string = sample_string_bytes.decode("ascii")
    result = sample_string.split(',')
    return result

class Analyst:
    def __init__(self):
        ES_HOST = os.getenv('ES_HOST', 'http://localhost:9200')
        self.es = Elasticsearch(ES_HOST)
        ENCODE_HOSTILE_LIST = os.getenv('ENCODE_HOSTILE_LIST')
        self.hostile_list = [w.lower() for w in decode_list(ENCODE_HOSTILE_LIST)]
        ENCODE_LESS_HOSTILE_LIST = os.getenv('ENCODE_LESS_HOSTILE_LIST')
        self.less_hostile_list = [w.lower() for w in decode_list(ENCODE_LESS_HOSTILE_LIST)]

    def get_from_elasticsearch(self):
        result = self.es.search(
            index='podcasts',
            query={'match_all': {}},
            size=50
        )
        return result['hits']['hits']

    def set_danger_level(self, podcasts):
        for podcast in podcasts:
            words = podcast['_source']['content'].lower().split()
            danger_level = 0
            for word in words:
                if word in self.hostile_list:
                    danger_level = 2
                    break
                elif word in self.less_hostile_list:
                    danger_level  = 1
            podcast['_source']["danger_level"] = danger_level
            logger.info(f"{podcast['_id']}: updated danger level")

    def calculate_hostility_percentage(self, podcasts):
        for podcast in podcasts:
            count = 0
            words = podcast['_source']['content'].lower().split()
            for word in words:
                if word in self.hostile_list or word in self.less_hostile_list:
                    count += 1
            if len(words) == 0:
                podcast['_source']['bds_percent'] = 0
            else:
                percent = (count / len(words)) * 100
                podcast['_source']['bds_percent'] = percent
            logger.info(f"{podcast['_id']}: updated bds percent")

    def determine_criminalization_threshold(self, podcasts):
        for podcast in podcasts:
            if podcast['_source']['bds_percent'] > 50:
                podcast['_source']["is_bds"] = True
            else:
                podcast['_source']["is_bds"] = False
            logger.info(f"{podcast['_id']}: updated is bds")

    def determine_threat_level(self, podcasts):
        for podcast in podcasts:
            if podcast['_source']['bds_percent'] <= 25:
                podcast['_source']["bds_threat_level"] = "None"
            elif 25 < podcast['_source']['bds_percent'] <= 75:
                podcast['_source']["bds_threat_level"] = "Medium"
            else:
                podcast['_source']["bds_threat_level"] = "High"
            logger.info(f"{podcast['_id']}: updated bds threat level")

    def update_hostility_fields_elasticsearch(self, podcasts):
        for podcast in podcasts:
            try:
                self.es.update(
                    index='podcasts',
                    id=podcast['_id'],
                    doc={
                        'danger_level': podcast['_source']["danger_level"],
                        'bds_percent': podcast['_source']["bds_percent"],
                        'is_bds': podcast['_source']["is_bds"],
                        'bds_threat_level': podcast['_source']["bds_threat_level"]
                    })
                logger.info(f"{podcast['_id']}: updated content")
            except Exception as e:
                logger.error(f"ES update failed: {e}")

if __name__ == '__main__':
    analyst = Analyst()
    all_data = analyst.get_from_elasticsearch()
    analyst.set_danger_level(all_data)
    analyst.calculate_hostility_percentage(all_data)
    analyst.determine_criminalization_threshold(all_data)
    analyst.determine_threat_level(all_data)
    analyst.update_hostility_fields_elasticsearch(all_data)
