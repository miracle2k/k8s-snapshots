from typing import Dict, Any

import attr


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
