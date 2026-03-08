import speech_recognition as sr

# Initialize the recognizer
r = sr.Recognizer()

# Specify the audio file to transcribe
AUDIO_FILE = "/Users/yehudafreiman/PycharmProjects/FinalProject/podcasts/download.wav"

# Use the AudioFile class as the audio source
with sr.AudioFile(AUDIO_FILE) as source:
    print("Reading audio file...")
    # Read the entire audio file data into memory
    audio_data = r.record(source)
    print("Audio data loaded successfully.")

# Perform the speech recognition
try:
    print("Transcribing audio using Google Web Speech API...")
    # Use Google's API to recognize the speech
    text = r.recognize_google(audio_data)
    print("\nRecognized Text:")
    print(text)

except sr.UnknownValueError:
    print("Sorry, could not understand the audio within the file.")
except sr.RequestError as e:
    print("Could not request results from Google Speech Recognition service; {0}".format(e))
