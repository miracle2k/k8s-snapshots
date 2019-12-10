FROM python:3.6-alpine

ADD . /app
WORKDIR /app
RUN apk add --no-cache --virtual .build_deps gcc musl-dev \
    && pip install -r requirements.txt \
    && python setup.py install \
    && apk del .build_deps gcc musl-dev
ENV TZ UTC

CMD ["k8s-snapshots"]
