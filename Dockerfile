FROM python:3.6

ADD . /app
RUN pip install -r /app/requirements.txt
ENTRYPOINT /app/daemon.py