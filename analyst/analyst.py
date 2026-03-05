import base64
from logger import Logger
from connection import es

logger = Logger.get_logger()

def decode_list(base64_string):
    base64_bytes = base64_string.encode("ascii")
    sample_string_bytes = base64.b64decode(base64_bytes)
    sample_string = sample_string_bytes.decode("ascii")
    result = sample_string.split(',')
    return result

class Analyst:
    def __init__(self, context):
        self.elasticsearch_connection = es
        self.context = context
        self.hostile_list = decode_list("R2Vub2NpZGUsV2FyIENyaW1lcyxBcGFydGhlaWQsTWFzc2FjcmUsTmFrYmEsRGlzcGxhY2VtZW50LEh1bWFuaXRhcmlhbiBDcmlzaXMsQmxvY2thZGUsT2NjdXBhdGlvbixSZWZ1Z2VlcyxJQ0MsQkRT")
        self.less_hostile_list = decode_list("RnJlZWRvbSBGbG90aWxsYSxSZXNpc3RhbmNlLExpYmVyYXRpb24sRnJlZSBQYWxlc3RpbmUsR2F6YSxDZWFzZWZpcmUsUHJvdGVzdCxVTlJXQQ==")

    def get_event_from_elasticsearch(self):
        return self.elasticsearch_connection.update()

    def content_classification(self):
        doc = self.get_event_from_elasticsearch()
        for i in range(len(self.context)):
            if self.context[i] in self.hostile_list:
                doc["is_hostile"] = 2
                break
            elif self.context[i] in self.less_hostile_list:
                doc["is_hostile"]  = 1
                break

    def calculate_hostility_percentage(self):
        doc = self.get_event_from_elasticsearch()
        count = 0
        for i in range(len(self.context)):
            if self.context[i] in (self.hostile_list or self.less_hostile_list):
                count += 1
        percent = (count / len(self.context)) * 100
        doc["bds_percent"] = percent
        return percent

    def determine_criminalization_threshold(self):
        doc = self.get_event_from_elasticsearch()
        if self.calculate_hostility_percentage() > 50:
            doc["is_bds"] = True
        else:
            doc["is_bds"] = False

    def determine_threat_level(self):
        doc = self.get_event_from_elasticsearch()
        if self.calculate_hostility_percentage() <= 25:
            doc["bds_threat_level"] = "None"
        elif 25 < self.calculate_hostility_percentage() <= 75:
            doc["bds_threat_level"] = "Medium"
        else:
            doc["bds_threat_level"] = "High"

if __name__ == '__main__':
    analyst = Analyst(Analyst.get_event_from_elasticsearch)
    analyst.content_classification()
    analyst.calculate_hostility_percentage()
    analyst.determine_criminalization_threshold()
    analyst.determine_threat_level()
