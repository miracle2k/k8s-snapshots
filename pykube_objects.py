from pykube.objects import APIObject

# AWAIT TODO: There's a PR out to add this to pykube.
# https://github.com/kelproject/pykube/pull/112
class StorageClass(APIObject):
    version = "storage.k8s.io/v1beta1"
    endpoint = "storageclasses"
    kind = "StorageClass"
