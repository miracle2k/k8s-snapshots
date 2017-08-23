import enum
import pendulum
from typing import Dict, List, NamedTuple, Any
from ..context import Context


@enum.unique
class SnapshotStatus(enum.Enum):
    PENDING = 'snapshot.pending'
    COMPLETE = 'snapshot.complete'


# It's up to a backend to decide how a disk should be identified.
# However, it does need to be something that is hashable, ideally
# a tuple.
DiskIdentifier = Any


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


def validate_disk_identifier(disk_id: Dict) -> DiskIdentifier:
    """Should take the user-specified dictionary, and convert it to
    it's own, local `DiskIdentifier`. If the disk_id is not valid,
    it should raise a `ValueError` with a suitable error message.
    """
    raise NotImplementedError()


def load_snapshots(ctx: Context, label_filters: Dict[str, str]) -> List[Snapshot]:
    """
    Return the existing snapshots. Important!! This function must filter
    the list of returned snapshots by ``label_filters``. This is because
    usually cloud providers make filtering part of their API.
    """
    raise NotImplementedError()


def create_snapshot(
    ctx: Context,
    disk: DiskIdentifier,
    snapshot_name: str,
    snapshot_description: str
) -> NewSnapshotIdentifier:
    """
    Create a snapshot for the given disk.

    This operation is expected to be asynchronous, so the value you return
    will identify the snapshot for the next call.
    """
    raise NotImplementedError()


def get_snapshot_status(
    ctx: Context,
    snapshot_identifier: NewSnapshotIdentifier
) -> SnapshotStatus:
    """
    Should return the current status of the snapshot.
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
