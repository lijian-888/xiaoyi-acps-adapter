import time
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace

from xiaoyi_adapter.repair import validate_recovery


class FormalRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.settings = SimpleNamespace(aic="partner")
        self.store = SimpleNamespace(pending=lambda gid: [], all=lambda table: [])
        self.group = dict(id="group-1", leader="leader", state="leaving",
                          exit_confirmed=True, disband_id="disband-1",
                          exit_response_id="exit-1", deadline_at=time.time() - 1)
        self.proof = SimpleNamespace(
            evidenceComplete=False, connection="unknown", channel="unknown",
            connectionIdentityObserved=False, groupConsumer="absent",
            partnerQueue="absent", leaderQueue="absent", exchange="absent",
            groupAcl="absent", memberAcl="absent", inbox="present", inboxConsumers=1)
        self.attestation = dict(
            leaderAic="leader", groupId="group-1", partnerAic="partner",
            source="215-mq-auth-management-readback", partnerConnections=1,
            inboxConsumers=1, inboxConnectionMatches=True, leaderState="dissolved",
            observedAt=datetime.now(timezone.utc).isoformat())

    def test_precise_complete_evidence_permits_formal_recovery(self):
        validate_recovery(self.settings, self.store, self.group, self.proof, self.attestation)

    def test_unknown_platform_resource_is_not_absence(self):
        for field, value in (("partnerQueue", "unknown"),
                             ("inboxConsumers", 0), ("connection", "present")):
            with self.subTest(field=field):
                original = getattr(self.proof, field)
                setattr(self.proof, field, value)
                with self.assertRaises(ValueError):
                    validate_recovery(self.settings, self.store, self.group,
                                      self.proof, self.attestation)
                setattr(self.proof, field, original)

    def test_inbox_connection_attestation_must_be_exact_and_fresh(self):
        for field, value in (("partnerConnections", 2), ("inboxConnectionMatches", False),
                             ("partnerAic", "other"), ("leaderState", "active"),
                             ("observedAt", "2020-01-01T00:00:00+00:00")):
            with self.subTest(field=field):
                original = self.attestation[field]
                self.attestation[field] = value
                with self.assertRaises(ValueError):
                    validate_recovery(self.settings, self.store, self.group,
                                      self.proof, self.attestation)
                self.attestation[field] = original

    def test_unsettled_business_or_early_deadline_blocks_recovery(self):
        self.group["deadline_at"] = time.time() + 60
        with self.assertRaisesRegex(ValueError, "DISBAND_WINDOW_NOT_EXPIRED"):
            validate_recovery(self.settings, self.store, self.group,
                              self.proof, self.attestation)
        self.group["deadline_at"] = time.time() - 1
        self.store.all = lambda table: [dict(group="group-1", state="working", unsettled=True)]
        with self.assertRaisesRegex(ValueError, "BUSINESS_WORK_NOT_SETTLED"):
            validate_recovery(self.settings, self.store, self.group,
                              self.proof, self.attestation)


if __name__ == "__main__":
    unittest.main()
