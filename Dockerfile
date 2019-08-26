FROM python:3.6-alpine

ADD . /app
WORKDIR /app
RUN pip install -r requirements.txt
RUN python setup.py install
ENV TZ UTC
CMD ["k8s-snapshots"]
