from kafka import KafkaProducer
from json import dumps
from pathlib import Path
from logger import Logger

logger = Logger.get_logger()

class Producer:
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: dumps(x).encode('utf-8'))
        self.file_path = Path("/Users/yehudafreiman/PycharmProjects/FinalProject/podcasts")

    def create_metadata(self):
        name = self.file_path.name
        size_bytes = self.file_path.stat().st_size
        last_modified_time = self.file_path.stat().st_mtime
        logger.info("create metadata")
        return {"metadata:", name, size_bytes, last_modified_time}

    def send_to_kafka(self):
        file_path = Path("/Users/yehudafreiman/PycharmProjects/FinalProject/podcasts")
        metadata_file = self.create_metadata()
        for e in range(1000):
            data = {'file path': file_path, 'metadata': metadata_file}
            self.producer.send('test', value=data)
        self.producer.flush()
        logger.info("send to kafka")

if __name__ == '__main__':
    producer = Producer()
    producer.create_metadata()
    producer.send_to_kafka()




