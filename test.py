import base64
import os
import speech_recognition as sr
from elasticsearch import Elasticsearch

es = Elasticsearch('http://localhost:9200')
print(f"Connected: {es.ping()}")

# # Initialize the recognizer
# r = sr.Recognizer()
#
# # Specify the audio file to transcribe
# AUDIO_FILE = "/Users/yehudafreiman/PycharmProjects/FinalProject/podcasts/download.wav"
#
# # Use the AudioFile class as the audio source
# with sr.AudioFile(AUDIO_FILE) as source:
#     print("Reading audio file...")
#     # Read the entire audio file data into memory
#     audio_data = r.record(source)
#     print("Audio data loaded successfully.")
#
# # Perform the speech recognition
# try:
#     print("Transcribing audio using Google Web Speech API...")
#     # Use Google's API to recognize the speech
#     text = r.recognize_google(audio_data)
#     print("\nRecognized Text:")
#     print(text)
#
# except sr.UnknownValueError:
#     print("Sorry, could not understand the audio within the file.")
# except sr.RequestError as e:
#     print("Could not request results from Google Speech Recognition service; {0}".format(e))
#
# from dotenv import load_dotenv
# load_dotenv()
#
# def decode_list(base64_string):
#     base64_bytes = base64_string.encode("ascii")
#     sample_string_bytes = base64.b64decode(base64_bytes)
#     sample_string = sample_string_bytes.decode("ascii")
#     result = sample_string.split(',')
#     return result
#
# ENCODE_HOSTILE_LIST = os.getenv('ENCODE_HOSTILE_LIST')
# hostile_list = decode_list(ENCODE_HOSTILE_LIST)
# ENCODE_LESS_HOSTILE_LIST = os.getenv('ENCODE_LESS_HOSTILE_LIST')
# less_hostile_list = decode_list(ENCODE_LESS_HOSTILE_LIST)
#
# print(f"hostile_list: {hostile_list}\n"
#       f"less_hostile_list: {less_hostile_list}")