FROM python:3.6

ADD . /app
ENV PYTHONPATH=/app
RUN pip install -r /app/requirements.txt
RUN python setup.py install
CMD ["k8s-snapshots"]
