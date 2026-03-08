from logger import Logger
from elasticsearch import Elasticsearch
import speech_recognition as sr

logger = Logger.get_logger()

class SpeechProcessor:
    def __init__(self):
        self.es = Elasticsearch("http://elasticsearch:9200")

    def get_from_elasticsearch(self):
        result = self.es.search(
            index='podcasts',
            query={'match_all': {}},
            size=50
        )
        return result['hits']['hits']

    def speech_to_text(self, podcasts):
        for podcast in podcasts:
            try:
                r = sr.Recognizer()
                with sr.AudioFile(podcast['_source']['path']) as source:
                    audio_data = r.record(source)
                podcast['_source']['content'] = r.recognize_google(audio_data)
                logger.info(f"{podcast['_id']}: create speach to text")
            except sr.UnknownValueError:
                podcast['_source']['content'] = ""
                logger.error("Speech not recognized")
            except sr.RequestError as e:
                podcast['_source']['content'] = ""
                logger.error(f"SR log failed: {e}")

    def update_content_elasticsearch(self, podcasts):
        for podcast in podcasts:
            try:
                self.es.update(
                    index='podcasts',
                    id=podcast['_id'],
                    doc={'content': podcast['_source']['content']}
                )
                logger.info(f"{podcast['_id']}: updated content")
            except Exception as e:
                logger.error(f"ES update failed: {e}")

if __name__ == '__main__':
    speech_processor = SpeechProcessor()
    all_data = speech_processor.get_from_elasticsearch()
    speech_processor.speech_to_text(all_data)
    speech_processor.update_content_elasticsearch(all_data)