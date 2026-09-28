"""Host-admin policy editor. OS permissions are the authentication boundary.

Keep the parent policy directory private to authorized administrators and mount it
read-only in the adapter. The adapter cannot issue or extend its own grants.
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from acps_sdk.aip.aip_group_runtime import ensure_valid_aic

from .models import now
from .policy import Policy


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["approve", "revoke"])
    parser.add_argument("--file", required=True)
    parser.add_argument("--partner", required=True)
    parser.add_argument("--leader", required=True)
    parser.add_argument("--capability", action="append", default=[])
    parser.add_argument("--actor", required=True)
    parser.add_argument("--reason", required=True)
    parser.add_argument("--purpose", choices=["test", "production"], default="test")
    parser.add_argument("--hours", type=int, default=24)
    args = parser.parse_args()
    ensure_valid_aic(args.partner)
    ensure_valid_aic(args.leader)
    if not 1 <= args.hours <= 24 * 30:
        parser.error("Grant lifetime must be between 1 hour and 30 days")
    if args.action == "approve" and not args.capability:
        parser.error("At least one exact capability is required")
    target = Path(args.file).resolve()
    target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    import fcntl
    with (target.parent / "policy.lock").open("a") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        old = json.loads(target.read_text()) if target.exists() else {"version": 0, "grants": [], "partner_aic": args.partner}
        if old["partner_aic"] != args.partner:
            raise ValueError("POLICY_IDENTITY_MISMATCH")
        grants = [g for g in old["grants"] if g["leader_aic"] != args.leader]
        if args.action == "approve":
            grants.append({"leader_aic": args.leader, "capabilities": args.capability, "purpose": args.purpose,
                           "approved_by": args.actor, "approved_at": now(), "reason": args.reason})
        expiry = (datetime.now(timezone.utc) + timedelta(hours=args.hours)).isoformat()
        # Revocation must never extend another Leader's existing approval lifetime.
        if args.action == "revoke" and old.get("expires_at"):
            expiry = old["expires_at"]
        elif grants and len(grants) > 1 and old.get("expires_at"):
            expiry = min(expiry, old["expires_at"])
        value = Policy(version=old["version"] + 1, partner_aic=args.partner, issued_at=now(), expires_at=expiry, grants=grants)
        audit = target.parent / "approval-audit.jsonl"
        with audit.open("a", encoding="utf-8") as log:
            log.write(json.dumps({"at": now(), "action": args.action, "actor": args.actor, "reason": args.reason,
                                  "policy": value.model_dump()}, ensure_ascii=False) + "\n")
            log.flush()
            os.fsync(log.fileno())
        pending = target.with_suffix(".pending")
        with pending.open("w", encoding="utf-8") as file:
            file.write(value.model_dump_json(indent=2))
            file.flush()
            os.fsync(file.fileno())
        os.chmod(pending, 0o640)
        os.replace(pending, target)
        print(json.dumps({"version": value.version, "action": args.action, "partner": args.partner, "leader": args.leader}))


if __name__ == "__main__":
    main()
