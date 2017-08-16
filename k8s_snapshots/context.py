import json
import os
import pykube



class Context:
    def __init__(self, config=None):
        self.config = config
        self._kube_config = None

    @property
    def kube_config(self):
        if self._kube_config is None:
            self._kube_config = self.load_kube_config()

        return self._kube_config

    def load_kube_config(self):
        cfg = None

        kube_config_file = self.config.get('kube_config_file')

        if kube_config_file:
            cfg = pykube.KubeConfig.from_file(kube_config_file)

        if not cfg:
            # See where we can get it from.
            default_file = os.path.expanduser('~/.kube/config')
            if os.path.exists(default_file):
                cfg = pykube.KubeConfig.from_file(default_file)

        if not cfg:
            cfg = pykube.KubeConfig.from_service_account()

        return cfg

    def kube_client(self):
        return pykube.HTTPClient(self.kube_config)
