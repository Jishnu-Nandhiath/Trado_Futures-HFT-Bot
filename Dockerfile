FROM python:3.12

WORKDIR /app

ENV PYTHONUNBUFFERED 1

COPY requirements.txt requirements.txt

RUN python -m pip install --upgrade pip

RUN pip install -r requirements.txt

COPY . .