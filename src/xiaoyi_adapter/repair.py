"""Audited offline recovery for an unobserved dedicated connection.

This is an exceptional operator action. It never claims automatic DISBAND
success and may only run while the runtime for this state directory is stopped.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import time
from pathlib import Path

from .config import Settings
from .evidence import EvidenceClient
from .models import TERMINAL, new_id, now, timestamp
from .store import Store


def validate_recovery(settings, store, group, proof, attestation):
    if not group or group.get("state") != "leaving" or not group.get("exit_confirmed"):
        raise ValueError("GROUP_NOT_READY_FOR_FORMAL_RECOVERY")
    if not group.get("disband_id") or not group.get("exit_response_id"):
        raise ValueError("DISBAND_AUDIT_INCOMPLETE")
    if time.time() < group.get("deadline_at", float("inf")):
        raise ValueError("DISBAND_WINDOW_NOT_EXPIRED")
    if store.pending(group["id"]):
        raise ValueError("UNCONFIRMED_OUTBOX")
    if any(t["group"] == group["id"] and (t["state"] not in TERMINAL or t.get("unsettled"))
           for t in store.all("tasks")):
        raise ValueError("BUSINESS_WORK_NOT_SETTLED")
    absent = (proof.groupConsumer, proof.partnerQueue, proof.leaderQueue,
              proof.exchange, proof.groupAcl, proof.memberAcl)
    # The platform marks the whole proof incomplete when a short-lived
    # connection was never observed. Only those two fields may be unknown;
    # the independent 215 management readback must account for the sole
    # remaining Partner connection as the long-lived Inbox connection.
    if (proof.connection != "unknown" or proof.channel != "unknown"
            or proof.connectionIdentityObserved or any(value != "absent" for value in absent)
            or proof.inbox != "present" or proof.inboxConsumers != 1):
        raise ValueError("PLATFORM_RESOURCES_NOT_EXACTLY_RECONCILED")
    expected = (group["leader"], group["id"], settings.aic)
    if tuple(attestation.get(key) for key in ("leaderAic", "groupId", "partnerAic")) != expected:
        raise ValueError("ATTESTATION_IDENTITY_MISMATCH")
    if (attestation.get("source") != "215-mq-auth-management-readback"
            or attestation.get("partnerConnections") != 1
            or attestation.get("inboxConsumers") != 1
            or attestation.get("inboxConnectionMatches") is not True
            or attestation.get("leaderState") != "dissolved"):
        raise ValueError("INBOX_ONLY_CONNECTION_NOT_PROVEN")
    age = time.time() - timestamp(attestation["observedAt"])
    if age < -5 or age > 60:
        raise ValueError("ATTESTATION_NOT_FRESH")


async def run(settings, store, group, attestation):
    client = EvidenceClient(settings.evidence_url, settings.aic, tls=settings.tls(),
                            max_age=settings.evidence_max_age_seconds)
    try:
        proof = await client.read(group)
    finally:
        await client.client.aclose()
    validate_recovery(settings, store, group, proof, attestation)
    return proof


def main():
    import fcntl  # Linux deployment runtime; validation remains portable for tests.
    parser = argparse.ArgumentParser(description="Audited recovery after fast DISBAND lost connection observation")
    parser.add_argument("--config", default="/config/runtime.json")
    parser.add_argument("--group-id", required=True)
    parser.add_argument("--attestation", required=True)
    parser.add_argument("--actor", required=True)
    parser.add_argument("--reason", required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if not args.actor.strip() or not args.reason.strip():
        parser.error("An actual operator and reason are required")
    settings = Settings.load(args.config)
    settings.check_acs()
    state_dir = Path(settings.state_file).parent
    with (state_dir / "instance.lock").open("a") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        store = Store(settings.state_file)
        try:
            store.bind_identity(settings.aic)
            group = store.get("groups", args.group_id)
            if not group:
                raise ValueError("GROUP_NOT_FOUND")
            attestation_bytes = Path(args.attestation).read_bytes()
            attestation = json.loads(attestation_bytes)
            proof = asyncio.run(run(settings, store, group, attestation))
            if args.dry_run:
                print(json.dumps({"eligible": True, "groupId": args.group_id,
                                  "platformEvidenceId": proof.evidenceId,
                                  "mode": "formal-unobserved-connection-recovery"}))
                return
            latest = store.get("groups", args.group_id)
            if latest != group:
                raise ValueError("GROUP_CHANGED_DURING_RECOVERY")
            operation_id = new_id("manual-recovery")
            group.update(state="closed", closed_at=now(), evidence_id=proof.evidenceId,
                         operation_id=operation_id, recovery_mode="formal-unobserved-connection",
                         attestation_sha256=hashlib.sha256(attestation_bytes).hexdigest(),
                         error=None)
            with store.transaction():
                store.put("groups", args.group_id, group)
                store.audit("formal_recovery_closed", args.group_id,
                            {"operation_id": operation_id, "actor": args.actor,
                             "reason": args.reason, "evidence_id": proof.evidenceId,
                             "attestation_sha256": group["attestation_sha256"]})
            print(json.dumps({"closed": True, "groupId": args.group_id,
                              "operationId": operation_id, "evidenceId": proof.evidenceId,
                              "mode": group["recovery_mode"]}))
        finally:
            store.db.close()


if __name__ == "__main__":
    main()
