
FROM python:3.11


ENV PYTHONUNBUFFERED True


COPY requirements.txt ./
COPY gunicorn.conf.py ./

RUN pip install -r requirements.txt

ENV APP_HOME /app
WORKDIR $APP_HOME
COPY . ./


CMD exec gunicorn  -c gunicorn.conf.py main:app