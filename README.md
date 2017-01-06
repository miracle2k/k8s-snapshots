Automatic Volume Snapshots on Kubernetes / Google Container Engine
==================================================================

The purpose of this tool is to backup persistent volumes in Kubernetes.
It does so, currently, only for Google Compute Engine Disks, by
creating regular snapshots, and expiring old snapshots using a GFS
scheme.

It works like this:

1. Run it as a service, preferable inside your Kubernetes cluster.
2. It will watch PersistentVolume resources and check those for an
   annotation named `backup.kubernetes.io/deltas`, which would have
   a value such as `1d 7d 30d`.
3. For every `PersistentVolume` that defines this annotation, and is
   a Google Compute disk, it will create new snapshots, and delete
   existing snapshots, according to the deltas defined.

  **WARNING**: The tool *will* consider snapshots not created by it.
  It will consider, and potentially delete, every snapshot that is
  associated with the disk in question.


How do the deltas work
----------------------

The expiry logic of [tarsnapper](https://github.com/miracle2k/tarsnapper)
is used.

The generations are defined by a list of deltas. ``60s`` means a minute,
``12h`` is half a day, ``7d`` is a week. The number of backups in each
generation is implied by it's and the parent generation's delta.

For example, given the deltas ``1h 1d 7d``, the first generation will
consist of 24 backups each one hour older than the previous
(or the closest approximation possible given the available backups),
the second generation of 7 backups each one day older than the previous,
and backups older than 7 days will be discarded for good.

The most recent backup is always kept.

The first delta is the backup interval.


Usage
-----

Run it with docker:

    docker run -e ... elsdoerfer/k8s-snapshots


Add annotations to your PersistentVolumes. If those volumes are auto
generated by a provisioner based on a PersistentVolumeClaim, you cannot
currently (it seems to me) define inside your claim which annotations
the volume should have. To enable backups for a volume, add the
deltas annotation manually:

    $ kubectl edit pv pvc-afee65c7-d014-084a-b158-42010af000bd

Add an annotation such as:

    backup.kubernetes.io/deltas: 1h 2d 30d 180d


Configuration
-------------

Provide environment variables to configure these.

<table>
  <tr>
    <th>Variable name</th>
    <th>Required</th>
    <th>Default</th>
    <th>Description</th>
  </tr>
  <tr>
    <td>GCLOUD_PROJECT</td>
    <td>Yes</td>
    <td></td>
    <td>Name of the Google Cloud project</td>
  </tr>
  <tr>
    <td>GCLOUD_JSON_KEYFILE_NAME</td>
    <td>One GCloud auth method is required</td>
    <td></td>
    <td>
      Filename to the JSON keyfile that is used to authenticate. You'll want
      to mount it into the container.
    </td>
  </tr>
  <tr>
    <td>GCLOUD_JSON_KEYFILE_STRING</td>
    <td>One GCloud auth method is required</td>
    <td></td>
    <td>
      The contents of the JSON keyfile that is used to authenticate.
    </td>
  </tr>
  <tr>
    <td>KUBE_CONFIG_FILE</td>
    <td>No</td>
    <td>Automatically uses the service account associated with the pod.</td>
    <td>
      Authentification with the Kubernetes API.
    </td>
  </tr>
  <tr>
    <td>USE_CLAIM_NAME</td>
    <td>No</td>
    <td>False</td>
    <td>If set, and the name of the volume is known to be autogenerated
        by the provisioner, and the volume is bound to a claim,
        then use the namespace/name of the claim as the name for the
        snapshots.
    </td>
  </tr>
  <tr>
    <td>LOG_LEVEL</td>
    <td>No</td>
    <td>INFO</td>
    <td>DEBUG, INFO, WARNING, ERROR</td>
  </tr>
</table>