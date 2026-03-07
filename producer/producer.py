from confluent_kafka import Producer
import json
import os
import time
from logger import Logger

logger = Logger.get_logger()

def delivery_report(err, msg):
        if err:
            print(f"delivery failed: {err}")
        else:
            print(f"delivered {msg.value().decode("utf-8")}")
            print(f"delivered to {msg.topic()} : partition {msg.partition()} : at offset {msg.offset()}")

class Publisher:
    def __init__(self):
        self.producer = Producer({"bootstrap.servers": "localhost:9092"})
        self.folder_path = os.getenv("FOLDER_PATH", "/Users/yehudafreiman/PycharmProjects/FinalProject/podcasts")

    def create_metadata(self):
        all_metadata = []
        try:
            for f in os.scandir(self.folder_path):
                c_ti = time.ctime(os.path.getctime(f))
                m_ti = time.ctime(os.path.getmtime(f))
                all_metadata.append({"path": f.path,
                        "name": f.name,
                        "size": f.stat().st_size,
                        "created time": c_ti,
                        "last modified": m_ti})
            logger.info("create metadata successfully")
            return all_metadata
        except Exception as e:
            logger.error("The error is: ", e)

    def send_to_kafka(self):
        try:
            for metadata in self.create_metadata():
                value = json.dumps(metadata).encode("utf-8")
                self.producer.produce(
                    topic="podcasts",
                    value=value,
                    callback=delivery_report
                )
                self.producer.flush()
            logger.info("create metadata end send to kafka")
        except Exception as e:
            logger.error("The error is: ", e)


if __name__ == '__main__':
    producer = Publisher()
    producer.create_metadata()
    producer.send_to_kafka()




