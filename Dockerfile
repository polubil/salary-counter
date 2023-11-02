FROM python:3.11-alpine

WORKDIR .

COPY . ./project

RUN ["pip", "install", "-r", "project/requirements.txt"]

ENV TOKEN=6928546580:AAFQeb84SbjvZ0_m_1S1B5KNIqFQnQbKO3k
ENV MONGO_USERNAME=ilsha-256
ENV MONGO_PASSWORD=veT07xuTghvIbZYQ
ENV MONGO_CLUSTER=cluster0.pbmhqqh.mongodb.net
ENV DATABASE_NAME=Salary
ENV COLLECTION_NAME=Salary
ENV PATH_TO_BSON=project/dump_for_test_task/sample_collection.bson
ENV UPDATE_DB=0
ENV LOG_LEVEL=WARNING
ENV LOG_PATH=logs.log

CMD ["python", "project/main.py"]