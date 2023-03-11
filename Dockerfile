FROM python:3.9-slim

RUN apt-get update -y &&  \
    apt-get install -y gcc g++

ENV PYTHONUNBUFFERED True

WORKDIR /app

ADD . /app
RUN pip install -e .
RUN bigloader install source-pypi

CMD bigloader run source-pypi