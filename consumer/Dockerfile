FROM python:3.10
RUN apt-get update && apt-get install -y gcc flac && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["python", "consumer.py"]