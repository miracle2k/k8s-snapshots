from datetime import timedelta
from pendulum import Pendulum
from unittest import TestCase

from k8s_snapshots.backends.abstract import Snapshot
from k8s_snapshots.context import Context
from k8s_snapshots.rule import Rule
from k8s_snapshots.snapshot import snapshots_for_rule_are_outdated


class TestSnapshotsAreUpToDate(TestCase):
    TEST_DISK = 'test-disk'

    def setUp(self):
        self.mock_context = Context({})
        self.rule = Rule(
            name='test_rule',
            deltas=[timedelta(hours=1), timedelta(days=30)],
            backend='test_backend',
            disk=self.TEST_DISK)

    def test_snapshot_is_required_without_existing_snapshots(self):
        assert snapshots_for_rule_are_outdated(self.rule, [])

    def test_snapshot_not_required_with_recent_snapshots(self):
        assert not snapshots_for_rule_are_outdated(self.rule, [
            Snapshot(
                created_at=Pendulum.now('utc') - timedelta(minutes=59),
                name='snapshot-1',
                disk=self.TEST_DISK)
        ])

    def test_snapshot_required_with_outdated_snapshot(self):
        assert snapshots_for_rule_are_outdated(self.rule, [
            Snapshot(
                created_at=Pendulum.now('utc') - timedelta(hours=1, minutes=1),
                name='snapshot-1',
                disk=self.TEST_DISK)
        ])

    def test_snapshot_required_with_snapshot_for_different_disk(self):
        assert snapshots_for_rule_are_outdated(self.rule, [
            Snapshot(
                created_at=Pendulum.now('utc') - timedelta(minutes=5),
                name='snapshot-1',
                disk='some-other-disk')
        ])
