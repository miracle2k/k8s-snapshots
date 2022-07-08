### Configure access permissions to Google Cloud

If there are no default credentials to Kubernetes and the Cloud
snapshot API, or the default credentials do not have the required
access scope, you may need to configure these.

<table>
  <tr>
    <td>CLOUD_PROVIDER</td>
    <td>
      Set to 'google' to use gcloud exclusively.
      Can be detected based on volume spec gcePersistentDisk.
     </td>
  </tr>
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
    <td>GCLOUD_CREDENTIALS_FILE</td>
    <td>
      Filename to the JSON gcloud credentials file used to authenticate.
      You'll want to mount it into the container.
      By default set to here for for PyKube:
      ~/.config/gcloud/application_default_credentials.json
      PyKube doesn't use env to locate the config but
      GOOGLE_APPLICATION_CREDENTIALS takes precedence.
    </td>
  </tr>
  <tr>
    <td>GOOGLE_APPLICATION_CREDENTIALS</td>
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
compute.regionOperations.get
```