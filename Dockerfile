FROM python:3.6

ADD . /app
WORKDIR /app
RUN pip install -r requirements.txt
RUN python setup.py install
CMD ["k8s-snapshots"]
