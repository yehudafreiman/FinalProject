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
            'bootstrap.servers': 'kafka:9092',
            "group.id": "podcasts-tracker",
            "auto.offset.reset": "earliest"
            })
        self.consumer.subscribe(["podcasts"])

    def listen_to_kafka(self):
        all_data = []
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    if len(all_data) > 0:
                        break
                    continue
                if msg.error():
                    print("Error:", msg.error())
                    logger.error(msg.error())
                    continue
                value = msg.value().decode("utf-8")
                podcast = json.loads(value)
                podcast["unique_id"] = str(uuid.uuid4())
                logger.info("listen kafka and create unique id successfully")
                all_data.append(podcast)

                try:
                    r = sr.Recognizer()
                    with sr.AudioFile(podcast['path']) as source:
                        audio_data = r.record(source)
                    podcast['content'] = r.recognize_google(audio_data)
                    logger.info("create speach to text")
                except sr.RequestError as e:
                    logger.error(f"SR log failed: {e}")

                try:
                    es.index(
                        index='podcasts',
                        document={'path': podcast['path'],
                                  'name': podcast['name'],
                                  'size': podcast['size'],
                                  'created time': podcast['created time'],
                                  'last modified': podcast['last modified'],
                                  'unique_id': podcast['unique_id'],
                                  'content': podcast['content']
                                  })
                    logger.info("send metadata to elasticsearch")
                except Exception as e:
                    logger.error(f"ES log failed: {e}")

                try:
                    with open(podcast['path'], 'rb') as file_data:
                        file_id = fs.put(file_data, filename=podcast['unique_id'])
                    logger.info(f"send {file_id} to mongodb")
                except Exception as e:
                    logger.error(f"FS log failed: {e}")

                return all_data
        except KeyboardInterrupt:
            print("Stopping consumer")
        except Exception as e:
            logger.error(f"Kafka log failed:{e}")
        finally:
            self.consumer.close()

if __name__ == '__main__':
    consumer = Tracker()
    consumer.listen_to_kafka()
