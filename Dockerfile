FROM python:3.6

WORKDIR /app

ADD requirements.txt /app/
RUN pip install -r requirements.txt

ADD . /app

ENTRYPOINT /app/daemon.py