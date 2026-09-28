import asyncio
import hashlib
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from acps_sdk.aip.aip_group_runtime import build_group_exchange_name

from xiaoyi_adapter.runtime import Runtime
from xiaoyi_adapter.store import Store
from test_runtime import LEADER, PARTNER, date


class InvitationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.tmp.name) / "state.db")
        self.transport = SimpleNamespace(join=AsyncMock())
        self.policy = SimpleNamespace(permits=lambda aic: aic == LEADER)
        self.runtime = Runtime(SimpleNamespace(aic=PARTNER, max_groups=2), self.store, None, self.transport, self.policy, None)

    async def asyncTearDown(self):
        self.store.db.close()
        self.tmp.cleanup()

    def invitation(self, gid="group-unit-1", token="random-test-token"):
        return {"type": "group-invitation", "protocol": "rabbitmq:4.2", "expiresAt": date(time.time() + 60),
            "invitationToken": token, "group": {"groupId": gid, "leader": {"aic": LEADER}, "partners": [{"aic": PARTNER}]},
            "amqp": {"exchange": build_group_exchange_name(LEADER, gid), "exchangeType": "fanout", "routingKey": ""}}

    async def test_join_uses_exact_identity_and_inbox_evidence(self):
        await self.runtime.invitation(self.invitation())
        self.transport.join.assert_awaited_once()
        self.assertEqual(self.store.get("groups", "group-unit-1")["state"], "joined")
        result = self.store.pending()[0][2]
        self.assertEqual(result["senderId"], PARTNER)
        self.assertTrue(result["status"]["connected"])

    async def test_replayed_token_cannot_join_other_group(self):
        await self.runtime.invitation(self.invitation())
        await self.runtime.invitation(self.invitation("group-unit-2"))
        self.assertIsNone(self.store.get("groups", "group-unit-2"))
        row = self.store.db.execute("SELECT digest FROM invitations").fetchone()
        self.assertEqual(row[0], hashlib.sha256(b"random-test-token").hexdigest())

    async def test_duplicate_invitation_does_not_create_second_connection(self):
        message = self.invitation()
        await asyncio.gather(self.runtime.invitation(message), self.runtime.invitation(message))
        self.transport.join.assert_awaited_once()

    async def test_expired_invitation_rejected(self):
        message = self.invitation()
        message["expiresAt"] = date(time.time() - 90)
        await self.runtime.invitation(message)
        self.transport.join.assert_not_awaited()

    async def test_exchange_mismatch_rejected(self):
        message = self.invitation()
        message["amqp"]["exchange"] += "-other"
        await self.runtime.invitation(message)
        self.transport.join.assert_not_awaited()

    async def test_not_invited_partner_rejected(self):
        message = self.invitation()
        message["group"]["partners"] = [{"aic": LEADER}]
        await self.runtime.invitation(message)
        self.transport.join.assert_not_awaited()

    async def test_group_capacity_includes_leaving(self):
        self.store.put("groups", "old-1", {"id": "old-1", "state": "leaving"})
        self.store.put("groups", "old-2", {"id": "old-2", "state": "leaving"})
        await self.runtime.invitation(self.invitation())
        self.transport.join.assert_not_awaited()

    async def test_failed_join_does_not_claim_closed(self):
        self.transport.join.side_effect = TimeoutError()
        await self.runtime.invitation(self.invitation())
        self.assertEqual(self.store.get("groups", "group-unit-1")["state"], "joining")
        self.assertEqual(self.store.pending(), [])
