from datetime import timedelta
from typing import Dict, Any, Optional, List

import attr
import pykube
from tarsnapper.config import parse_deltas, ConfigError

from k8s_snapshots.core import _logger
from k8s_snapshots.errors import UnsupportedVolume, AnnotationNotFound, \
    AnnotationError


@attr.s(slots=True)
class Rule:
    """
    A rule describes how and when to make backups.
    """

    name = attr.ib()
    namespace = attr.ib()

    deltas = attr.ib()
    gce_disk = attr.ib()
    gce_disk_zone = attr.ib()

    claim_name = attr.ib()

    @property
    def pretty_name(self):
        return self.claim_name or self.name

    def to_dict(self) -> Dict[str, Any]:
        """ Helper, returns attr.asdict(self) """
        return attr.asdict(self)

    def __str__ (self):
        return self.name


def rule_from_pv(
        volume: pykube.objects.PersistentVolume,
        api: pykube.HTTPClient,
        deltas_annotation_key: str,
        use_claim_name: bool=False
) -> Rule:
    """Given a persistent volume object, create a backup role
    object. Can return None if this volume is not configured for
    backups, or is not suitable.

    Parameters

    `use_claim_name` - if the persistent volume is bound, and it's
    name is auto-generated, then prefer to use the name of the claim
    for the snapshot.
    """
    _log = _logger.new(
        volume=volume.obj,
        annotation_key=deltas_annotation_key,
    )

    # Verify the provider

    provisioner = volume.annotations.get('pv.kubernetes.io/provisioned-by')
    _log = _log.bind(provider=provisioner)
    if provisioner != 'kubernetes.io/gce-pd':
        raise UnsupportedVolume(
            'Unsupported provisioner',
            provisioner=provisioner
        )

    def get_deltas(annotations: Dict) -> Optional[List[timedelta]]:
        """
        Helper annotation-deltas-getter

        Parameters
        ----------
        annotations

        Returns
        -------

        """
        try:
            deltas_str = annotations[deltas_annotation_key]
        except KeyError as exc:
            raise AnnotationNotFound(
                'No such annotation key',
                key=deltas_annotation_key
            ) from exc

        if not deltas_str:
            raise AnnotationError('Invalid delta string', deltas_str=deltas_str)

        try:
            deltas = parse_deltas(deltas_str)
        except ConfigError as exc:
            raise AnnotationError(
                'Invalid delta string',
                deltas_str=deltas_str
            ) from exc

        if deltas is None or not deltas:
            raise AnnotationError(
                'parse_deltas returned invalid deltas',
                deltas_str=deltas_str,
                deltas=deltas,
            )

        return deltas

    gce_disk = volume.obj['spec']['gcePersistentDisk']['pdName']

    # How can we know the zone? In theory, the storage class can
    # specify a zone; but if not specified there, K8s can choose a
    # random zone within the master region. So we really can't trust
    # that value anyway.
    # There is a label that gives a failure region, but labels aren't
    # really a trustworthy source for this.
    # Apparently, this is a thing in the Kubernetes source too, see:
    # getDiskByNameUnknownZone in pkg/cloudprovider/providers/gce/gce.go,
    # e.g. https://github.com/jsafrane/kubernetes/blob/2e26019629b5974b9a311a9f07b7eac8c1396875/pkg/cloudprovider/providers/gce/gce.go#L2455
    gce_disk_zone = volume.labels.get('failure-domain.beta.kubernetes.io/zone')

    rule_kwargs = dict(
        name=volume.name,
        namespace=volume.namespace,
        gce_disk=gce_disk,
        gce_disk_zone=gce_disk_zone,
    )

    claim_ref = volume.obj['spec'].get('claimRef')
    _log = _log.bind(claim_ref=claim_ref)

    volume_claim = (
        pykube.objects.PersistentVolumeClaim.objects(api)
        .filter(namespace=claim_ref['namespace'])
        .get_or_none(name=claim_ref['name'])
    )  # type: Optional[pykube.objects.PersistentVolumeClaim]

    try:
        deltas = get_deltas(volume.annotations)
        return Rule(
            deltas=deltas,
            claim_name=None,
            **rule_kwargs,
        )
    except AnnotationNotFound as exc:
        if claim_ref is None:
            raise AnnotationNotFound(
                'No volume claim found'
            ) from exc

    if volume_claim is None:
        raise AnnotationError(
            'Could not find the PersistentVolumeClaim from claim_ref',
            claim_ref=claim_ref,
        )

    try:
        deltas = get_deltas(volume_claim.annotations)
    except AnnotationNotFound as exc:
        raise AnnotationNotFound(
            'No deltas found via volume claim'
        ) from exc

    # If volume is not annotated, attempt ot read deltas from
    # PersistentVolumeClaim referenced in volume.claimRef

    claim_name = None
    if use_claim_name:
        if volume.annotations.get('kubernetes.io/createdby') == 'gce-pd-dynamic-provisioner':
            claim_name = f"{claim_ref['namespace']}--{claim_ref['name']}"

    return Rule(
        deltas=deltas,
        claim_name=claim_name,
        **rule_kwargs,
    )
