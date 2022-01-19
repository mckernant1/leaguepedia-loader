FROM 653528873951.dkr.ecr.us-west-2.amazonaws.com/python-39-slim-bullseye

COPY . /app
WORKDIR /app
RUN pip install pipenv

RUN pipenv sync
