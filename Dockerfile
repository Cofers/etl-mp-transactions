
FROM python:3.11


ENV PYTHONUNBUFFERED True
RUN apt-get update && apt-get install -y redis-server && apt-get clean

COPY requirements.txt ./
COPY gunicorn.conf.py ./

RUN pip install -r requirements.txt

ENV APP_HOME /app
WORKDIR $APP_HOME
COPY . ./


CMD service redis-server start && exec gunicorn -c gunicorn.conf.py main:app