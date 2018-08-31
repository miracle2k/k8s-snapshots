"""
Here is a collection of logging ``event```values that are expected to be kept
more stable.

These events are provided as a reference for external logging metric tools.
"""
import enum


class EventEnum(enum.Enum):
    """ Base class for Event Enums """
    pass


@enum.unique
class Annotation(EventEnum):
    """
    Events related to 'deltas' annotations.
    """
    FOUND = 'annotation.found'
    NOT_FOUND = 'annotation.not-found'
    ERROR = 'annotation.error'
    INVALID = 'annotation.invalid'


@enum.unique
class VolumeEvent(EventEnum):
    """
    Events related to Kubernetes PersistentVolume and PersistentVolumeClaim
    resource events.
    """
    RECEIVED = 'volume-event.received'


@enum.unique
class Volume(EventEnum):
    """
    Events related to Kubernetes PersistentVolumes
    """
    UNSUPPORTED = 'volume.unsupported'
    NOT_FOUND = 'volume.not-found'


@enum.unique
class Snapshot(EventEnum):
    """
    Events related to snapshots.
    """
    SCHEDULED = 'snapshot.scheduled'
    START = 'snapshot.start'
    ERROR = 'snapshot.error'
    CREATED = 'snapshot.created'
    EXPIRED = 'snapshot.expired'


@enum.unique
class Rule(EventEnum):
    """
    Events related to snapshot Rule()s.
    """
    PENDING = 'rule.pending'
    ADDED_FROM_CONFIG = 'rule.from-config'
    ADDED = 'rule.added'
    UPDATED = 'rule.updated'
    REMOVED = 'rule.removed'
    HEARTBEAT = 'rule.heartbeat'


@enum.unique
class Expiration(EventEnum):
    """
    Events related to snapshot expiration.
    """
    STARTED = 'expire.started'
    KEPT = 'expire.kept'
    DELETE = 'expire.delete'
    COMPLETE = 'expire.complete'


@enum.unique
class Ping(EventEnum):
    """
    Events related to sending pings.
    """
    SENT = 'ping.sent'
