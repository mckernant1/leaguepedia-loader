FROM 653528873951.dkr.ecr.us-west-2.amazonaws.com/docker-hub/library/python:3.11-slim-bookworm

COPY . /app
WORKDIR /app
RUN pip install pipenv

RUN pipenv sync
