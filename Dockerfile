FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt ./
RUN pip install -r requirements.txt
COPY airplane_tracker ./airplane_tracker
CMD ["python", "airplane_tracker/kafka/producer.py"]