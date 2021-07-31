### Configure access permissions on AWS

To be able to create snapshots, on AWS our pod will need the following permissions: 

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ec2:DescribeAvailabilityZones",
        "ec2:CreateTags",
        "ec2:DescribeTags",
        "ec2:DescribeVolumeAttribute",
        "ec2:DescribeVolumeStatus",
        "ec2:DescribeVolumes",
        "ec2:CreateSnapshot",
        "ec2:DeleteSnapshot",
        "ec2:DescribeSnapshots"
      ],
      "Resource": "*"
    }
  ]
}
```

If there are no default credentials injected into your nodes, or the default
credentials do not have the required access scope, you may need to
configure these environment variables:

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


### A tip for kops users

On older versions of kops, master nodes did have the permissions required. A solution there
is to just run `k8s-snapshots` on a master node.

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