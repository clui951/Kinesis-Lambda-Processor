FROM python:3.6.5

RUN mkdir -p /testing
WORKDIR /testing

COPY requirements.txt /testing/
RUN pip install -r requirements.txt

COPY . /testing