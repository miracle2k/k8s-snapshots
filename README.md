Automatic Volume Snapshots on Kubernetes
========================================

**How is it useful?** Simply add an annotation to your `PersistentVolume` or
`PersistentVolumeClaim` resources, and let this tool create and expire snapshots
according to your specifications.


**Supported Environments**:

- Google Compute Engine disks.
- AWS EBS disks.


Want to help adding support for other backends? It's pretty straightforward.
Have a look at the
[API that backends need to implement](https://github.com/miracle2k/k8s-snapshots/blob/master/k8s_snapshots/backends/abstract.py).



Quickstart
----------

Let's run *k8s-snapshots* in your cluster:


```bash
cat <<EOF | kubectl create -f -
apiVersion: extensions/v1beta1
kind: Deployment
metadata:
  name: k8s-snapshots
  namespace: kube-system
spec:
  replicas: 1
  template:
    metadata:
      labels:
        app: k8s-snapshots
    spec:
      containers:
      - name: k8s-snapshots
        image: elsdoerfer/k8s-snapshots:v0.3
EOF
```

Add a backup annotation to one of your persistent volumes:

```bash
kubectl patch pv pvc-01f74065-8fe9-11e6-abdd-42010af00148 -p \
  '{"metadata": {"annotations": {"backup.kubernetes.io/deltas": "PT1H P30D P360D"}}}'
```

*k8s-snapshots* will now run in your cluster, and per the deltas
given, it will create a daily snapshot of the volume. It will keep
30 daily snapshots, and then for one year it will keep a monthly
snapshot. If the daemon is not running for a while, it will still
try to approximate your desired snapshot scheme as closely as possible.


How do the deltas work
----------------------

The expiry logic of [tarsnapper](https://github.com/miracle2k/tarsnapper)
is used.

The generations are defined by a list of deltas formatted as [ISO 8601
durations](https://en.wikipedia.org/wiki/ISO_8601#Durations) (this differs from
tarsnapper). ``PT60S`` or ``PT1M`` means a minute, ``PT12H`` or ``P0.5D`` is
half a day, ``P1W`` or ``P7D`` is a week. The number of backups in each
generation is implied by it's and the parent generation's delta.

For example, given the deltas ``PT1H P1D P7D``, the first generation will
consist of 24 backups each one hour older than the previous
(or the closest approximation possible given the available backups),
the second generation of 7 backups each one day older than the previous,
and backups older than 7 days will be discarded for good.

The most recent backup is always kept.

The first delta is the backup interval.


Running the daemon
------------------

Use the example deploymet file given above to start off. If you run
the daemon on a Google Container Engine cluster, it should already
have access to all the resources it needs.

However, depending on your configuration, you may need to assign the
correct RBAC rules, or give it access to a Google Cloud identity that
has permissions to create snapshots. See below more more on the
available configuration options.


How do enable backups
---------------------

To backup a volume, you should add an annotation with the name
``backup.kubernetes.io/deltas`` to either your `PersistentVolume` or
`PersistentVolumeClaim` resources.

Since ``PersistentVolumes`` are often created automatically for you
by Kubernetes, it you may want to annotate the volume claim in your
resource definition file. Alteratively, you can ``kubectl edit pv``
a ``PersistentVolume`` created by Kubernetes and add the annotation.

The value of the annotation are a set of deltas that define how often
a snapshot is created, and how many snapshots should be kept. See
the section above for more information on how deltas work.

In the end, you annotation may look like this:

```
backup.kubernetes.io/deltas: PT1H P2D P30D P180D
```

There is also the option of manually specifying the volume names
to be backed up as options to the *k8s-snapshots* daemon. See below
for more information.


Configuration
-------------

### Configure access permissions to Google Cloud.

If there are no default credentials to Kubernetes and the Cloud
snapshot API, or the default credentials do not hae the required
access scope, you may need to configure these.

<table>
  <tr>
    <td>GCLOUD_PROJECT</td>
    <td>
      Name of the Google Cloud project. This is required to use the Google
      Cloud API, but if it's not given, we try to read the value from
      the [instance metadata service](https://cloud.google.com/compute/docs/storing-retrieving-metadata)
      which will usually work.
     </td>
  </tr>
  <tr>
    <td>GCLOUD_JSON_KEYFILE_NAME</td>
    <td>
      Filename to the JSON keyfile that is used to authenticate.
      You'll want to mount it into the container.
    </td>
  </tr>
  <tr>
    <td>GCLOUD_JSON_KEYFILE_STRING</td>
    <td>
      The contents of the JSON keyfile that is used to authenticate.
    </td>
  </tr>
  <tr>
    <td>KUBE_CONFIG_FILE</td>
    <td>
      Authentification with the Kubernetes API. By default, the
      pod service account is used.
    </td>
  </tr>
</table>


### Configure access permissions on AWS.

Currently, we will try to connect with the default credentials.

The region is usually detected via the meta data service. If that is not
the case, you can set `AWS_REGION`.


### Pinging a third party service

<table>
  <tr>
    <td>PING_URL</td>
    <td>
      We'll send a GET request to this url whenever a backup completes.
      This is useful for integrating with monitoring services like
      Cronitor or Dead Man's Snitch.
    </td>
  </tr>
</table>


### Make snapshot names more readable

If your persistent volumes are auto-provisioned by Kubernetes, then
you'll end up with snapshot names such as
``pv-pvc-01f74065-8fe9-11e6-abdd-42010af00148``. If you want that
prettier, set the enviroment variable ``USE_CLAIM_NAME=true``. Instead
of the auto-generated name of the persistent volume, *k8s-snapshots*
will instead use the name that you give to your
``PersistentVolumeClaim``.


### Manual backups

<table>
  <tr>
    <td>VOLUMES</td>
    <td>
      Comma-separated list of volumes to backup. This allows you to
      manually specify volumes you want to create snapshots for; useful
      for volumes you are using without a PersistentVolume.
    </td>
  </tr>
  <tr>
    <td>VOLUME_{NAME}_DELTAS</td>
    <td>
      The deltas for this volume.
    </td>
  </tr>
  <tr>
    <td>VOLUME_{NAME}_ZONE</td>
    <td>
      The zone for this volume.
    </td>
  </tr>
</table>


### Other environment variables

<table>
  <tr>
    <td>LOG_LEVEL</td>
    <td>**Default: INFO**. Possible values: DEBUG, INFO, WARNING, ERROR</td>
  </tr>
  <tr>
    <td>JSON_LOG</td>
    <td>**Default: False**. Output the log messages as JSON objects for
        easier processing.</td>
  </tr>
</table>


FAQ
----

**What if I manually create snapshots for the same volumes that
*k8s-snapshots* manages?**

Starting with v0.3, when *k8s-snapshots* decides when to create the
next snapshot, and which snapshots it deletes, it no longer considers
snapshots that are not correctly labeled by it.


Development
-----------

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
