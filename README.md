Interval-based Volume Snapshots and Expiry on Kubernetes
========================================================

**What you do:** Create a custom `SnapshotRule` resource which defines your desired snapshot intervals.
**What I do:** Create snapshots of your volumes, and expire old ones using a Grandfather-father-son backup scheme.

**Supported Environments**:

- Google Compute Engine disks.
- AWS EBS disks.
- Digital Ocean.

Want to help adding support for other backends? It's pretty straightforward.
Have a look at the [API that backends need to implement](https://github.com/miracle2k/k8s-snapshots/blob/master/k8s_snapshots/backends/abstract.py).


Quickstart
----------

A persistent volume claim:

```
cat <<EOF | kubectl apply -f -
apiVersion: "k8s-snapshots.elsdoerfer.com/v1"
kind: SnapshotRule
metadata:
  name: postgres
spec:
  deltas: P1D P30D
  persistentVolumeClaim: postgres-data
EOF
```

A specific AWS EC2 volume:

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


You can also use an annotation instead of the CRDs:


```bash
kubectl patch pv pvc-01f74065-8fe9-11e6-abdd-42010af00148 -p \
  '{"metadata": {"annotations": {"backup.kubernetes.io/deltas": "P1D P30D P360D"}}}'
``` 


Usage
-----

### How to enable backups

To backup a volume, you can create a `SnapshotRule` custom resource.
See more on this in the section further doiwn below.

Alternatively, you can add an annotation with the name
``backup.kubernetes.io/deltas`` to either your `PersistentVolume` or
`PersistentVolumeClaim` resources.

Since ``PersistentVolumes`` are often created automatically for you
by Kubernetes, you may want to annotate the volume claim in your
resource definition file. Alternatively, you can ``kubectl edit pv``
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


### How the deltas work

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

If the daemon is not running for a while, it will still  try to approximate your desired 
snapshot scheme as closely as possible.

The most recent backup is always kept.

The first delta is the backup interval.


Setup
-----

`k8s-snapshots` needs access to your Kubernetes cluster resources 
(to read the desired snapshot configuration) and access to your cloud infrastructure
(to make snapshots).

Depending on your environment, it may be able to configure itself. Or, you might need to 
provide some configuration options.

Use the example deployment file given below to start off.

```bash
cat <<EOF | kubectl create -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: k8s-snapshots
  namespace: kube-system
spec:
  replicas: 1
  selector:
    matchLabels:
      app: k8s-snapshots
  template:
    metadata:
      labels:
        app: k8s-snapshots
    spec:
      containers:
      - name: k8s-snapshots
        image: elsdoerfer/k8s-snapshots:latest
EOF
```

### 1. Based on your cluster.

See the [docs/](docs/) folder for platform-specific instructions.


### 2. For Role-based Access Control (RBAC) enabled clusters

In Kubernetes clusters with RBAC, the required permissions need to be provided to the `k8s-snapshots` pods to watch and list `persistentvolume` or `persistentvolumeclaims`. We provide a manifest to setup a `ServiceAccount` with a minimal set of permissions in [rbac.yaml](manifests/rbac.yaml).

```
kubectl apply -f manifests/rbac.yaml
```

Furthermore, under GKE, "Because of the way Container Engine checks permissions when you create a Role or ClusterRole, you must first create a RoleBinding that grants you all of the permissions included in the role you want to create."

If the above kubectl apply command produces an error about "attempt to grant extra privileges", the following will grant _your_ user the necessary privileges *first*, so that you can then bind them to the service account:

```
  kubectl create clusterrolebinding your-user-cluster-admin-binding --clusterrole=cluster-admin --user=your.google.cloud.email@example.org
```

Finally, adjust the deployment by adding ```serviceAccountName: k8s-snapshots``` to the spec (else you'll end up using the "default" service account), as follows:

```
<snip>
    spec:
     serviceAccountName: k8s-snapshots
     containers:
      - name: k8s-snapshots
        image: elsdoerfer/k8s-snapshots:v2.0
</snip>
```

Further Configuration Options
-----------------------------


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


### SnapshotRule resources

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
apiVersion: apps/v1
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
  <tr>
    <td>TZ</td>
    <td>**Default: UTC**. Used to change the timezone. ie. TZ=America/Montreal</td>
  </tr>
</table>


FAQ
----

**What if I manually create snapshots for the same volumes that
*k8s-snapshots* manages?**

Starting with v0.3, when *k8s-snapshots* decides when to create the
next snapshot, and which snapshots it deletes, it no longer considers
snapshots that are not correctly labeled by it.
