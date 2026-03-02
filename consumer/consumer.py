import uuid
from kafka import KafkaConsumer
from json import loads
from connection import fs, es
from logger import Logger
import speech_recognition as sr

logger = Logger.get_logger()

class Consumer:
    def __init__(self):
        self.consumer = KafkaConsumer('test',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        group_id='my-group',
        value_deserializer=lambda x: loads(x.decode('utf-8')))
        self.mongo_connection = fs
        self.elasticsearch_connection = es
        self.recognizer = sr.Recognizer()

    def listen_to_kafka(self):
        for message in self.consumer:
            logger.info("listen to kafka")
            return message.value
        return None

    def create_unique_id(self):
        data = Consumer.listen_to_kafka(self)
        data["unique_id"] = uuid.uuid4()
        logger.info("create unique id")

    def speach_to_text(self):
        data = self.listen_to_kafka()
        with sr.AudioFile(data['file path']) as source:
            audio_data = self.recognizer.record(source)
        context = self.recognizer.listen(audio_data)
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