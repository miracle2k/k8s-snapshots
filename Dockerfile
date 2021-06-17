FROM docker-upgrade.artifactory.build.upgrade.com/python-base:2.0.20210421.0-222.3.7-232

USER root
ADD . /app
WORKDIR /app
RUN pip3 install -r requirements.txt \
    && python3 setup.py install

USER upgrade
ENV TZ UTC

CMD ["k8s-snapshots"]
