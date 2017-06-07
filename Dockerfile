FROM python:3.6

ADD . /app
ENV PYTHONPATH=/app
RUN pip install -r /app/requirements.txt
CMD ["python", "-m", "k8s_snapshots"]
