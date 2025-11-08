#01-kafka/producers/Dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY price_simulator.py .

RUN pip install kafka-python

ENTRYPOINT ["python", "price_simulator.py"]
