FROM python:3.9-slim

ADD . /app
WORKDIR /app
RUN pip3 install --no-cache-dir poetry \
    && poetry config virtualenvs.create false \
    && poetry install --only main

ENV TZ UTC

CMD ["python", "-m", "k8s_snapshots"]
