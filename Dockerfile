FROM python:3.9-alpine

ADD . /app
WORKDIR /app
RUN apk add --no-cache --virtual .build_deps gcc musl-dev libffi-dev
RUN pip3 install poetry
RUN poetry config virtualenvs.create false
RUN poetry install --no-dev
RUN apk del .build_deps gcc musl-dev libffi-dev

ENV TZ UTC

CMD ["python", "-m", "k8s_snapshots"]
