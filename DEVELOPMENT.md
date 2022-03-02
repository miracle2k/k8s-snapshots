Development
===========

For local development, you can still connect to an existing Google
Cloud Project and Kubernetes cluster using the config options
available. If you are lucky, your local workstation is already setup
the way you need it. If we can find credentials for Google Cloud
or Kubernetes, they will be used automatically.

However, depending on the backend, you need to provide some options that
otherwise would be read from the instance metadata:


For AWS:

    $ AWS_REGION=eu-west-1 python -m k8s_snapshots


For Google Cloud:

    $ GCLOUD_PROJECT=revolving-randy python -m k8s_snapshots


## Releasing a new version

- Update CHANGES.
- Tag with a v-prefix, which will cause a tag on Docker hub.

