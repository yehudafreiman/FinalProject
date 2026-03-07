import json
import uuid
from confluent_kafka import Consumer
from connection import fs, es
from logger import Logger
import speech_recognition as sr

logger = Logger.get_logger()

class Tracker:
    def __init__(self):
        self.consumer = Consumer({
            "bootstrap.servers": "localhost:9092",
            "group.id": "podcasts-tracker",
            "auto.offset.reset": "earliest"
            })
        self.consumer.subscribe(["podcasts"])
        self.mongo_connection = fs
        self.elasticsearch_connection = es
        self.recognizer = sr.Recognizer()

    def listen_to_kafka(self):
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print("Error:", msg.error())
                    continue
                value = msg.value().decode("utf-8")
                podcast = json.loads(value)
                logger.info("listen kafka successfully")
                return podcast
        except KeyboardInterrupt:
            print("Stopping consumer")
        except Exception as e:
            logger.error(e)
        finally:
            self.consumer.close()

    def create_unique_id(self):
        data = Tracker.listen_to_kafka(self)
        data["unique_id"] = uuid.uuid4()
        logger.info("create unique id")

    def speach_to_text(self):
        data = self.listen_to_kafka()
        with sr.AudioFile(data['file path']) as source:
            audio_data = self.recognizer.record(source)
        context = self.recognizer.listen(audio_data)
        logger.info("create speach to text")
        return context

    def send_metadata_to_elasticsearch(self):
        data = self.listen_to_kafka()
        context = self.speach_to_text()
        self.elasticsearch_connection.index(
            index='test',
            document={
                'context': context,
                'character': data
            })
        logger.info("send metadata to elasticsearch")

    def send_file_to_mongodb(self):
        data = self.listen_to_kafka()
        self.mongo_connection.put(data['file path'])
        logger.info("send file to mongodb")

if __name__ == '__main__':
    consumer = Tracker()
    consumer.listen_to_kafka()
    # consumer.create_unique_id()
    # consumer.speach_to_text()
    # consumer.send_metadata_to_elasticsearch()
    # consumer.send_file_to_mongodb()
