from __future__ import annotations

import asyncio
import hashlib
import logging
import time

from acps_sdk.aip import TaskCommand
from acps_sdk.aip.aip_group_model import GroupMemberStatus, GroupMgmtCommand, GroupMgmtResult, InboxGroupInvitation
from acps_sdk.aip.aip_group_runtime import build_group_exchange_name, invitation_is_expired
from pydantic import ValidationError

from .models import digest, new_id, now, timestamp

logger = logging.getLogger("xiaoyi_adapter")


class Runtime:
    def __init__(self, settings, store, engine, transport, policy, evidence):
        self.s, self.store, self.engine, self.transport = settings, store, engine, transport
        self.policy, self.evidence = policy, evidence
        self.lock = asyncio.Lock()
        self.stop = asyncio.Event()
        self.worker = None
        self.cleanup_jobs = {}
        self.last_policy_refresh = 0
        self.last_tick = 0
        self.observation_times = {}

    def status_message(self, group, connected, session=None, message_id=None):
        return GroupMgmtResult(id=message_id or new_id("mgmt-result"), sentAt=now(), senderRole="partner", senderId=self.s.aic,
                              mentions=[group["leader"]], groupId=group["id"], sessionId=session,
                              status=GroupMemberStatus(connected=connected, muted=group.get("muted", False))).model_dump(mode="json", exclude_none=True)

    async def invitation(self, raw):
        if raw.get("type") != "group-invitation":
            return
        try:
            invitation = InboxGroupInvitation.model_validate(raw)
            leader, gid = invitation.group.leader.aic, invitation.group.groupId
            if not self.policy.permits(leader):
                raise ValueError("LEADER_NOT_AUTHORIZED")
            if invitation.protocol != "rabbitmq:4.2" or invitation_is_expired(invitation.expiresAt) or not invitation.invitationToken:
                raise ValueError("INVALID_INVITATION")
            if self.s.aic not in {p.aic for p in invitation.group.partners}:
                raise ValueError("PARTNER_NOT_INCLUDED")
            if invitation.amqp.exchange != build_group_exchange_name(leader, gid) or invitation.amqp.exchangeType != "fanout" or invitation.amqp.routingKey:
                raise ValueError("GROUP_EXCHANGE_MISMATCH")
        except (ValidationError, ValueError) as exc:
            self.store.audit("invitation_rejected", raw.get("group", {}).get("groupId", "invalid"), {"error": type(exc).__name__})
            return
        token_hash = hashlib.sha256(invitation.invitationToken.encode()).hexdigest()
        async with self.lock:
            existing = self.store.get("groups", gid)
            if existing:
                self.store.audit("invitation_duplicate", gid)
                return
            if len([g for g in self.store.all("groups") if g["state"] != "closed"]) >= self.s.max_groups:
                self.store.audit("invitation_capacity_rejected", gid)
                return
            with self.store.transaction():
                if self.store.db.execute("SELECT 1 FROM invitations WHERE digest=?", (token_hash,)).fetchone():
                    self.store.audit("invitation_token_replay", gid)
                    return
                self.store.db.execute("INSERT INTO invitations VALUES (?,?)", (token_hash, gid))
                group = {"id": gid, "leader": leader, "state": "joining", "created_at": now(), "invitation_path": "inbox", "muted": False}
                self.store.put("groups", gid, group)
                self.store.audit("invitation_accepted", gid, {"leader": leader, "partner": self.s.aic, "path": "inbox"})
            try:
                await self.transport.join(group, lambda body: self.message(gid, body))
                # A Group can be dissolved seconds after connected=true.
                # Wait until 9007 has observed the exact dedicated connection;
                # otherwise its later absence remains unknown forever.
                self.store.audit("group_transport_joined", gid)
            except Exception as exc:
                group.update(error=type(exc).__name__)
                self.store.put("groups", gid, group)
                self.store.audit("join_pending", gid, {"error": type(exc).__name__})

    async def message(self, group_id, raw):
        if raw.get("type") == "task-command":
            try:
                command = TaskCommand.model_validate(raw)
            except ValidationError:
                self.store.audit("invalid_command", group_id)
                return
            if command.groupId == group_id:
                await self.engine.handle(command)
            return
        if raw.get("type") != "group-mgmt-command":
            return
        try:
            command = GroupMgmtCommand.model_validate(raw)
        except ValidationError:
            self.store.audit("invalid_management", group_id)
            return
        async with self.lock:
            group = self.store.get("groups", group_id)
            if not group or command.groupId != group_id or command.senderRole != "leader" or command.senderId != group["leader"] or not command.is_mentioned(self.s.aic):
                self.store.audit("management_identity_rejected", group_id)
                return
            with self.store.transaction():
                claim = self.store.receipt(command.id, digest(raw))
                if claim != "new":
                    return
                verb = command.command.value
                if verb == "get-status":
                    self.store.enqueue(group_id, self.status_message(group, group["state"] != "closed" and not group.get("exit_confirmed"), command.sessionId))
                elif verb in {"mute", "unmute"}:
                    group["muted"] = verb == "mute"
                    self.store.put("groups", group_id, group)
                    self.store.enqueue(group_id, self.status_message(group, group["state"] != "closed" and not group.get("exit_confirmed"), command.sessionId))
                elif verb in {"disband", "leave-group"} and group["state"] != "closed":
                    if "received_at" not in group:
                        group.update(state="leaving", received_at=now(), deadline_at=time.time() + self.s.disband_seconds,
                                     disband_id=command.id, exit_response_id=new_id("mgmt-result"), session=command.sessionId,
                                     exit_confirmed=False, exit_reason=verb)
                        self.store.put("groups", group_id, group)
                        self.store.audit("disband_received", group_id, {"message_id": command.id, "deadline_at": group["deadline_at"]})

    async def flush(self):
        blocked = set()
        for message_id, group_id, body in self.store.pending():
            if group_id in blocked or group_id not in self.transport.groups:
                continue
            try:
                await self.transport.publish(group_id, body)
                with self.store.transaction():
                    self.store.confirm(message_id)
                    group = self.store.get("groups", group_id)
                    if group and message_id == group.get("exit_response_id"):
                        group.update(exit_confirmed=True, exit_confirmed_at=now())
                        self.store.put("groups", group_id, group)
                        self.store.audit("exit_response_publisher_ack", group_id, {"message_id": message_id})
            except Exception as exc:
                blocked.add(group_id)
                logger.warning("publish_pending group_id=%s error_type=%s", group_id, type(exc).__name__)

    async def finalize(self, gid):
        group = self.store.get("groups", gid)
        if group["state"] != "leaving" or time.time() >= group["deadline_at"]:
            return
        if self.engine.group_has_work(gid):
            return
        if not group.get("exit_confirmed"):
            if not self.store.pending(gid):
                self.store.enqueue(gid, self.status_message(group, False, group.get("session"), group["exit_response_id"]))
            return
        if self.store.pending(gid):
            return
        local_closed = await self.transport.close_group(gid)
        proof = await self.evidence.read(group)
        if not local_closed and not self.transport.reconcile_closed(gid, proof):
            return
        if not proof.cleanup_complete():
            return
        # Re-read after IO; concurrent callbacks cannot reset or extend the deadline.
        current = self.store.get("groups", gid)
        if current["state"] != "leaving" or time.time() >= current["deadline_at"] or self.engine.group_has_work(gid):
            return
        with self.store.transaction():
            current.update(state="closed", closed_at=now(), evidence_id=proof.evidenceId, error=None,
                           operation_id=new_id("automatic-disband"))
            self.store.put("groups", gid, current)
            self.store.audit("automatic-disband", gid, {"operation_id": current["operation_id"], "evidence": proof.model_dump()})
        logger.info("group_closed group_id=%s evidence_id=%s", gid, proof.evidenceId)

    async def cleanup_once(self, gid):
        try:
            await self.finalize(gid)
        except Exception as exc:
            group = self.store.get("groups", gid)
            group["error"] = type(exc).__name__
            self.store.put("groups", gid, group)
            self.store.audit("cleanup_pending", gid, {"error": type(exc).__name__})

    async def restore_group(self, group):
        """Restore only an existing queue/exchange, never recreate a vanished Group."""
        proof = await self.evidence.read(group)
        if proof.partnerQueue != "present" or proof.exchange != "present":
            return
        if not self.policy.permits(group["leader"]):
            return
        await self.transport.join(group, lambda body: self.message(group["id"], body), recovery=True)
        group = self.store.get("groups", group["id"])
        if group["state"] == "joined":
            with self.store.transaction():
                self.store.enqueue(group["id"], self.status_message(group, True))

    async def confirm_join(self, gid, proof):
        required = (proof.connection, proof.channel, proof.groupConsumer,
                    proof.partnerQueue, proof.leaderQueue, proof.exchange,
                    proof.groupAcl, proof.memberAcl, proof.inbox)
        if not (proof.evidenceComplete and proof.connectionIdentityObserved
                and all(value == "present" for value in required)
                and proof.inboxConsumers == 1):
            return False
        async with self.lock:
            group = self.store.get("groups", gid)
            if not group or group["state"] != "joining" or gid not in self.transport.groups:
                return False
            group.update(state="joined", joined_at=now(),
                         connection_identity_observed=True, evidence_id=proof.evidenceId)
            with self.store.transaction():
                self.store.put("groups", gid, group)
                self.store.enqueue(gid, self.status_message(group, True))
                self.store.audit("partner_joined", gid, {"evidence_id": proof.evidenceId})
            return True

    async def tick(self):
        if time.time() - self.last_policy_refresh >= self.s.policy_refresh_seconds:
            await self.policy.refresh()
            self.last_policy_refresh = time.time()
        if not self.transport.inbox_ready:
            await self.transport.start_inbox(self.invitation)
        await self.flush()
        for group in self.store.all("groups"):
            gid = group["id"]
            if group["state"] in {"joining", "joined"} and time.time() - self.observation_times.get(gid, 0) >= (2 if group["state"] == "joining" else 5):
                self.observation_times[gid] = time.time()
                try:
                    proof = await self.evidence.read(group)
                    if group["state"] == "joining":
                        await self.confirm_join(gid, proof)
                    elif proof.connectionIdentityObserved and proof.connection == "present":
                        group = self.store.get("groups", gid)
                        group.update(connection_identity_observed=True, evidence_id=proof.evidenceId)
                        self.store.put("groups", gid, group)
                except Exception as exc:
                    self.store.audit("joined_evidence_pending", gid, {"error": type(exc).__name__})
            if group["state"] == "leaving":
                if time.time() >= group["deadline_at"]:
                    if group.get("finalize_status") != "finalize_pending":
                        group.update(finalize_status="finalize_pending")
                        self.store.put("groups", gid, group)
                        self.store.audit("finalize_pending", gid)
                    continue
                job = self.cleanup_jobs.get(gid)
                if job is None or job.done():
                    self.cleanup_jobs[gid] = asyncio.create_task(self.cleanup_once(gid))
            if group["state"] in {"joining", "joined"} or (group["state"] == "leaving" and not group.get("exit_confirmed") and time.time() < group["deadline_at"]):
                entry = self.transport.groups.get(gid)
                if entry and entry["connection"].is_closed:
                    await self.transport.close_group(gid)
                    entry = None
                if entry is None:
                    try:
                        await self.restore_group(group)
                    except Exception as exc:
                        logger.warning("group_restore_pending group_id=%s error_type=%s", gid, type(exc).__name__)
                if gid in self.transport.groups:
                    self.engine.recover(gid)
        self.last_tick = time.time()

    async def run(self):
        await self.policy.refresh()
        while not self.stop.is_set():
            try:
                await self.tick()
            except Exception as exc:
                logger.warning("runtime_retry error_type=%s", type(exc).__name__)
            try:
                await asyncio.wait_for(self.stop.wait(), 1)
            except TimeoutError:
                pass

    def health(self):
        groups = self.store.all("groups")
        ready = self.transport.inbox_ready and self.policy.valid and time.time() - self.last_tick < 60
        return {"ready": ready, "aic": self.s.aic, "inboxConnected": self.transport.inbox_ready,
                "authorizationValid": self.policy.valid, "activeGroups": sum(g["state"] != "closed" for g in groups),
                "pendingOutbox": len(self.store.pending()), "version": "0.1.0"}

    async def close(self):
        self.stop.set()
        if self.worker:
            self.worker.cancel()
            await asyncio.gather(self.worker, return_exceptions=True)
        for task in self.cleanup_jobs.values():
            task.cancel()
        await asyncio.gather(*self.cleanup_jobs.values(), return_exceptions=True)
        await self.engine.close()
        await self.transport.close()
        await self.policy.client.aclose()
        await self.evidence.client.aclose()
