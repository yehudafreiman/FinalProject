import json
import os
import uuid
from confluent_kafka import Consumer
from connection import fs, es
from logger import Logger

logger = Logger.get_logger()

class Tracker:
    def __init__(self):
        KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
        self.consumer = Consumer({
            'bootstrap.servers': KAFKA_BROKER,
            "group.id": "podcasts-tracker",
            "auto.offset.reset": "earliest"
            })
        self.consumer.subscribe(["podcasts"])

    def listen_to_kafka(self):
        all_podcasts = []
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    if len(all_podcasts) > 0:
                        break
                    continue
                if msg.error():
                    print("Error:", msg.error())
                    logger.error(msg.error())
                    continue
                value = msg.value().decode("utf-8")
                podcast = json.loads(value)
                podcast["unique_id"] = str(uuid.uuid4())
                podcast["content"] = ""
                logger.info(f"{podcast['unique_id']}: listen kafka and create unique id successfully")
                all_podcasts.append(podcast)
        except KeyboardInterrupt:
            print("Stopping consumer")
        except Exception as e:
            logger.error(f"Kafka log failed:{e}")
        finally:
            self.consumer.close()
        return all_podcasts

    def send_metadata_elasticsearch(self, podcasts):
        if not es.indices.exists(index='podcasts'):
            es.indices.create(index='podcasts')
        for podcast in podcasts:
            try:
                es.index(
                    index='podcasts',
                    id=podcast['unique_id'],
                    document={'path': podcast['path'],
                              'name': podcast['name'],
                              'size': podcast['size'],
                              'created time': podcast['created time'],
                              'last modified': podcast['last modified'],
                              'content': podcast['content']
                              })
                logger.info(f"{podcast['unique_id']}: send metadata to elasticsearch")
            except Exception as e:
                logger.error(f"ES log failed: {e}")

    def send_file_mongodb(self, podcasts):
        for podcast in podcasts:
            try:
                with open(podcast['path'], 'rb') as file_data:
                    fs.put(file_data, filename=podcast['unique_id'])
                logger.info(f"{podcast['unique_id']}: send to mongodb")
            except Exception as e:
                logger.error(f"FS log failed: {e}")

if __name__ == '__main__':
    consumer = Tracker()
    all_data = consumer.listen_to_kafka()
    consumer.send_metadata_elasticsearch(all_data)
    consumer.send_file_mongodb(all_data)
