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
        image: elsdoerfer/k8s-snapshots:dev
EOF
```

Add a backup annotation to one of your persistent volumes:

```bash
kubectl patch pv pvc-01f74065-8fe9-11e6-abdd-42010af00148 -p \
  '{"metadata": {"annotations": {"backup.kubernetes.io/deltas": "P1D P30D P360D"}}}'
```

*k8s-snapshots* will now run in your cluster, and per the deltas
given, it will create a daily snapshot of the volume. It will keep
30 daily snapshots, and then for one year it will keep a monthly
snapshot. If the daemon is not running for a while, it will still
try to approximate your desired snapshot scheme as closely as possible.

### A tip for kops users

*k8s-snapshots* need EBS and S3 permissions to take and save snapshots. Under
the [kops](https://github.com/kubernetes/kops/) IAM Role scheme, only Masters
have these permissions. The easiest solution is to run k8s-snapshots on
Masters.

To run on a Master, we need to:
   * [Overcome a Taint](https://kubernetes.io/docs/concepts/configuration/taint-and-toleration/)
   * [Specify that we require a Master](https://kubernetes.io/docs/concepts/configuration/assign-pod-node/)

To do this, add the following to the above manifest for the k8s-snapshots
Deployment:

```
spec:
  ...
  template:
  ...
    spec:
      ...
      tolerations:
      - key: "node-role.kubernetes.io/master"
        operator: "Equal"
        value: ""
        effect: "NoSchedule"
      nodeSelector:
        kubernetes.io/role: master
```

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

Use the example deployment file given above to start off. If you run
the daemon on a Google Container Engine cluster, it should already
have access to all the resources it needs.

However, depending on your configuration, you may need to assign the
correct RBAC rules, or give it access to a Google Cloud identity that
has permissions to create snapshots. See below for more on the
available configuration options.


How to enable backups
---------------------

To backup a volume, you should add an annotation with the name
``backup.kubernetes.io/deltas`` to either your `PersistentVolume` or
`PersistentVolumeClaim` resources.

Since ``PersistentVolumes`` are often created automatically for you
by Kubernetes, you may want to annotate the volume claim in your
resource definition file. Alteratively, you can ``kubectl edit pv``
a ``PersistentVolume`` created by Kubernetes and add the annotation.

The value of the annotation are a set of deltas that define how often
a snapshot is created, and how many snapshots should be kept. See
the section above for more information on how deltas work.

In the end, your annotation may look like this:

```
backup.kubernetes.io/deltas: PT1H P2D P30D P180D
```

There is also the option of manually specifying the volume names
to be backed up as options to the *k8s-snapshots* daemon. See below
for more information.


Configuration
-------------

### Configure access permissions to Google Cloud

If there are no default credentials to Kubernetes and the Cloud
snapshot API, or the default credentials do not have the required
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

When using a service account with a custom role to access the Google Cloud API, the following permissions are required:
```
compute.disks.createSnapshot
compute.snapshots.create
compute.snapshots.delete
compute.snapshots.get
compute.snapshots.list
compute.snapshots.setLabels
compute.zoneOperations.get
```

### Configure access permissions on AWS

If there are no default credentials to the Cloud API, or the default
credentials do not have the required access scope, you may need to
configure these environment variables.

<table>
  <tr>
    <td>AWS_ACCESS_KEY_ID</td>
    <td>
      AWS IAM Access Key ID that is used to authenticate.
     </td>
  </tr>
  <tr>
    <td>AWS_SECRET_ACCESS_KEY</td>
    <td>
      AWS IAM Secret Access Key that is used to authenticate.
    </td>
  </tr>
  <tr>
    <td>AWS_REGION</td>
    <td>
      The region is usually detected via the meta data service. You can override the value.
    </td>
  </tr>
</table>


### For Role-based Access Control (RBAC) enabled clusters

In kubernetes clusters with RBAC, the required permissions need to be provided to the `k8s-snapshots` pods to watch and list `persistentvolume` or `persistentvolumeclaims`.

  ```
    kubectl apply -f rbac.yaml
  ```

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


### Manual snapshot rules

It's possible to ask *k8s-snapshots* to create snapshots of volumes
for which no `PersistentVolume` object exists within the Kubernetes
cluster. For example, you might have a volume at your Cloud provider
that you use within Kubernetes by referencing it directly.

To do this, we use a custom Kubernetes resource, `SnapshotRule`.

First, you need to create this custom resource.

On Kubernetes 1.7 and higher:

```
cat <<EOF | kubectl create -f -
apiVersion: apiextensions.k8s.io/v1beta1
kind: CustomResourceDefinition
metadata:
  name: snapshotrules.k8s-snapshots.elsdoerfer.com
spec:
  group: k8s-snapshots.elsdoerfer.com
  version: v1
  scope: Namespaced
  names:
    plural: snapshotrules
    singular: snapshotrule
    kind: SnapshotRule
    shortNames:
    - sr
EOF
```

Or on Kubernetes 1.6 and lower:

```
cat <<EOF | kubectl create -f -
apiVersion: extensions/v1beta1
kind: ThirdPartyResource
metadata:
  name: snapshot-rule.k8s-snapshots.elsdoerfer.com
description: "Defines snapshot management rules for a disk."
versions:
- name: v1
EOF
```

You can then create `SnapshotRule` resources:

```
cat <<EOF | kubectl apply -f -
apiVersion: "k8s-snapshots.elsdoerfer.com/v1"
kind: SnapshotRule
metadata:
  name: mysql
spec:
  deltas: P1D P30D
  backend: aws
  disk:
     region: eu-west-1
     volumeId: vol-0aa6f44aad0daf9f2
EOF
```

This is an example for backing up an EBS disk on the Amazon cloud. The
`disk` option requires different keys, depending on the backend. See
the [examples folder](https://github.com/miracle2k/k8s-snapshots/tree/master/examples).

You may also point `SnapshotRule` resources to PersistentVolumes (or
PersistentVolumeClaims). This is intended as an alternative to adding
an annotation; it may be desirable for some to separate the snapshot
functionality from the resource.

```
cat <<EOF | kubectl apply -f -
apiVersion: "k8s-snapshots.elsdoerfer.com/v1"
kind: SnapshotRule
metadata:
  name: mysql
spec:
  deltas: P1D P30D
  persistentVolumeClaim: datadir-mysql
EOF
```


### Backing up the etcd volumes of a kops cluster

After setting up the custom resource definitions (see previous section), use
snapshot rules as defined in the `examples/backup-kops-etcd.yml` file. Reference
the volume ids of your etcd volumes.


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
