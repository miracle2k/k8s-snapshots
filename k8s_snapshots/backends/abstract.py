import enum
import pendulum
from typing import Dict, List, NamedTuple, Any
from ..context import Context


@enum.unique
class SnapshotStatus(enum.Enum):
    PENDING = 'snapshot.pending'
    COMPLETE = 'snapshot.complete'


class DiskIdentifier(NamedTuple):
    """
    Given an existing snapshot in a Cloud Provider Backend, we need to be
    able to figure out which Kubernetes volume resource that snapshot
    belongs to. The cloud provider may not know about Kubernetes, but
    Kubernetes should know about the Cloud provider disk.

    We ask the Cloud Provider to return some information about the disk
    in the form of this tuple. We can then search in Kubernetes for the
    right PersistentVolume matching this information.
    """
    disk_name: str
    zone_name: str


class Snapshot(NamedTuple):
    """
    Identifies an existing snapshot.
    """
    name: str
    created_at: pendulum.Pendulum
    # A disk id that is known to Kubernetes.
    disk: DiskIdentifier


# Snapshot creation is a multi-step process. This is an arbitrary value that
# a Cloud provider backend can return to refer to the snapshot within the
# cloud as it's being created. This is distinct from :class:`Snapshot`, which
# represents a completed snapshot.
NewSnapshotIdentifier = Any


def load_snapshots(ctx: Context, label_filters: Dict[str, str]) -> List[Snapshot]:
    """
    Return the existing snapshots. Important!! This function must filter
    the list of returned snapshots by ``label_filters``. This is because
    usually cloud providers make filtering part of their API.
    """
    raise NotImplementedError()


def create_snapshot(
    ctx: Context,
    disk_name: str,
    disk_zone: str,
    snapshot_name: str,
    snapshot_description: str
) -> NewSnapshotIdentifier:
    """
    Create a snapshot for the given disk.

    This operation is expected to be asynchronous, so the value you return
    will identify the snapshot for the next call.
    """
    raise NotImplementedError()


def set_snapshot_labels(
    ctx: Context,
    snapshot_identifier: NewSnapshotIdentifier,
    labels: Dict
):
    """
    Set labels on the snapshot.
    """
    raise NotImplementedError()


def delete_snapshot(
    ctx: Context,
    snapshot: Snapshot
):
    """
    Delete the snapshot given.
    """
    raise NotImplementedError()
