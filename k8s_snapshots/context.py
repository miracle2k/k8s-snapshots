import json
import os

import pykube
import structlog
from googleapiclient import discovery
from oauth2client.service_account import ServiceAccountCredentials

from k8s_snapshots.config import DEFAULT_CONFIG

_logger = structlog.get_logger()


class Context:

    def __init__(self, config=None):
        self.config = config
        self.kube = self.make_kubeclient()
        self.gcloud = self.make_gclient()

    def make_kubeclient(self):
        cfg = None

        kube_config_file = self.config.get('kube_config_file')

        if kube_config_file:
            _logger.info('kube-config.from-file', file=kube_config_file)
            cfg = pykube.KubeConfig.from_file(kube_config_file)

        if not cfg:
            # See where we can get it from.
            default_file = os.path.expanduser('~/.kube/config')
            if os.path.exists(default_file):
                _logger.info(
                    'kube-config.from-file.default',
                    file=default_file)
                cfg = pykube.KubeConfig.from_file(default_file)

        # Maybe we are running inside Kubernetes.
        if not cfg:
            _logger.info('kube-config.from-service-account')
            cfg = pykube.KubeConfig.from_service_account()

        return pykube.HTTPClient(cfg)

    def make_gclient(self):
        SCOPES = 'https://www.googleapis.com/auth/compute'
        credentials = None

        if self.config.get('gcloud_json_keyfile_name'):
            credentials = ServiceAccountCredentials.from_json_keyfile_name(
                self.config.get('gcloud_json_keyfile_name'),
                scopes=SCOPES)

        if self.config.get('gcloud_json_keyfile_string'):
            keyfile = json.loads(self.config.get('gcloud_json_keyfile_string'))
            credentials = ServiceAccountCredentials.from_json_keyfile_dict(
                keyfile, scopes=SCOPES)

        if not credentials:
            raise RuntimeError("Auth for Google Cloud was not configured")

        compute = discovery.build(
            'compute',
            'v1',
            credentials=credentials,
        )
        return compute
