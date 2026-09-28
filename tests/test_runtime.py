from __future__ import annotations

import asyncio
import json
import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import httpx
from acps_sdk.aip import StructuredDataItem, TaskCommand

from xiaoyi_adapter.connectors import EchoConnector, JsonHttpConnector, StandardHttpConnector
from xiaoyi_adapter.engine import Engine
from xiaoyi_adapter.evidence import Evidence, EvidenceClient
from xiaoyi_adapter.models import BusinessResult, Capability, ExecutionContext, new_id, now
from xiaoyi_adapter.policy import Authorization
from xiaoyi_adapter.runtime import Runtime
from xiaoyi_adapter.store import Store

PARTNER = "1.2.156.3088.1.0001.00001.80NSQB.2JZQPQ.0C61"
LEADER = "1.2.156.3088.1.0001.00001.ZJM2XM.50E81A.0XHW"


def date(seconds):
    return datetime.fromtimestamp(seconds, timezone.utc).isoformat()


def grant(version=1, capabilities=None):
    return {"version": version, "partner_aic": PARTNER, "issued_at": date(time.time() - 1),
            "expires_at": date(time.time() + 600), "grants": [{"leader_aic": LEADER,
            "capabilities": ["test.echo"] if capabilities is None else capabilities,
            "purpose": "test", "approved_by": "unit-test-reviewer", "approved_at": date(time.time() - 2), "reason": "unit test"}]}


def proof(**changes):
    body = {"apiVersion": "1", "leaderAic": LEADER, "partnerAic": PARTNER, "groupId": "group-unit-1",
            "evidenceId": "proof-1", "observedAt": now(), "connection": "absent", "channel": "absent",
            "groupConsumer": "absent", "partnerQueue": "absent", "leaderQueue": "absent", "exchange": "absent",
            "groupAcl": "absent", "memberAcl": "absent", "inbox": "present", "inboxConsumers": 1,
            "evidenceComplete": True, "connectionIdentityObserved": True}
    body.update(changes)
    return body


class EngineTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        asyncio.get_running_loop().slow_callback_duration = 5
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.tmp.name) / "state.db")
        self.store.bind_identity(PARTNER)
        self.policy = Authorization(PARTNER, Path(self.tmp.name) / "policy.json")
        self.policy.apply(grant())
        self.cap = Capability(id="test.echo", connector="echo", allow_continue=True, allow_cancel=True,
            poll_seconds=0.1, timeout_seconds=0.3,
            input_schema={"type": "object", "required": ["text"], "properties": {"text": {"type": "string"}}})
        self.settings = SimpleNamespace(aic=PARTNER, capabilities=[self.cap], max_tasks=2, io_timeout=0.2,
            max_groups=2, disband_seconds=300, policy_refresh_seconds=30)
        self.engine = Engine(self.settings, self.store, self.policy, {"echo": EchoConnector()})
        self.group = {"id": "group-unit-1", "leader": LEADER, "state": "joined"}
        self.store.put("groups", self.group["id"], self.group)

    async def asyncTearDown(self):
        await self.engine.close()
        await self.policy.client.aclose()
        self.store.db.close()
        self.tmp.cleanup()

    def command(self, verb="start", task="task-1", inputs=None, **changes):
        data = {"id": new_id("cmd"), "sentAt": now(), "senderRole": "leader", "senderId": LEADER,
                "groupId": self.group["id"], "sessionId": "session-1", "taskId": task, "command": verb,
                "mentions": [PARTNER], "dataItems": [StructuredDataItem(data={"skillId": "test.echo", "input": {"text": "真实回显"} if inputs is None else inputs})]}
        data.update(changes)
        return TaskCommand(**data)

    async def drain(self):
        await asyncio.gather(*list(self.engine.jobs.values()))

    async def start(self):
        await self.engine.handle(self.command())
        await self.drain()

    def last(self):
        return self.store.pending()[-1][2]

    async def test_echo_complete_roundtrip(self):
        await self.start()
        self.assertEqual(self.last()["status"]["state"], "awaiting-completion")
        self.assertEqual(self.last()["products"][0]["dataItems"][0]["text"], "真实回显")
        product_id = self.last()["products"][0]["id"]
        await self.engine.handle(self.command("complete"))
        self.assertEqual(self.last()["status"]["state"], "completed")
        self.assertEqual(product_id, self.last()["products"][0]["id"])

    async def test_missing_input_continue(self):
        await self.engine.handle(self.command(inputs={}))
        self.assertEqual(self.last()["status"]["state"], "awaiting-input")
        await self.engine.handle(self.command("continue", inputs={"text": "补充"}))
        await self.drain()
        self.assertEqual(self.last()["products"][0]["dataItems"][0]["text"], "补充")

    async def test_continue_updates_product(self):
        await self.start()
        await self.engine.handle(self.command("continue", inputs={"text": "修改"}))
        await self.drain()
        self.assertEqual(self.last()["products"][0]["dataItems"][0]["text"], "修改")

    async def test_complete_requires_awaiting_completion(self):
        await self.engine.handle(self.command(inputs={}))
        await self.engine.handle(self.command("complete"))
        self.assertEqual(self.store.get("tasks", "task-1")["state"], "awaiting-input")

    async def test_get_nonexistent_does_not_create_task(self):
        await self.engine.handle(self.command("get"))
        self.assertIsNone(self.store.get("tasks", "task-1"))
        self.assertEqual(self.last()["status"]["state"], "rejected")

    async def test_terminal_immutable(self):
        await self.start()
        await self.engine.handle(self.command("complete"))
        before = self.store.get("tasks", "task-1")
        await self.engine.handle(self.command("continue", inputs={"text": "不准改写"}))
        self.assertEqual(before, self.store.get("tasks", "task-1"))

    async def test_task_id_conflict(self):
        await self.start()
        await self.engine.handle(self.command(inputs={"text": "不同内容"}))
        self.assertEqual(self.last()["status"]["dataItems"][0]["data"]["errorCode"], "TASK_ID_CONFLICT")
        self.assertEqual(self.store.get("tasks", "task-1")["result"]["text"], "真实回显")

    async def test_message_redelivery_no_second_dispatch(self):
        command = self.command()
        await self.engine.handle(command)
        await self.drain()
        count = len(self.store.pending())
        await self.engine.handle(command)
        self.assertEqual(len(self.store.pending()), count)

    async def test_scope_isolation(self):
        await self.start()
        await self.engine.handle(self.command("get", sessionId="other-session"))
        self.assertNotIn("products", self.last())
        self.assertEqual(self.last()["status"]["state"], "rejected")

    async def test_wrong_sender_ignored(self):
        await self.engine.handle(self.command(senderId=PARTNER))
        self.assertEqual(self.store.pending(), [])

    async def test_unmentioned_ignored(self):
        await self.engine.handle(self.command(mentions=[]))
        self.assertEqual(self.store.pending(), [])

    async def test_revocation_blocks_new_but_allows_complete(self):
        await self.start()
        self.policy.apply(grant(2, []))
        await self.engine.handle(self.command(task="task-2"))
        self.assertEqual(self.last()["status"]["state"], "rejected")
        await self.engine.handle(self.command("complete"))
        self.assertEqual(self.last()["status"]["state"], "completed")

    async def test_disband_blocks_new_task(self):
        self.store.put("groups", self.group["id"], {**self.group, "state": "leaving"})
        await self.engine.handle(self.command())
        self.assertEqual(self.last()["status"]["state"], "rejected")

    async def test_cancel_only_when_confirmed(self):
        await self.start()
        await self.engine.handle(self.command("cancel"))
        await self.drain()
        self.assertEqual(self.store.get("tasks", "task-1")["state"], "canceled")

    async def test_unsupported_cancel_does_not_fake_cancellation(self):
        self.cap.allow_cancel = False
        await self.start()
        await self.engine.handle(self.command("cancel"))
        self.assertEqual(self.store.get("tasks", "task-1")["state"], "awaiting-completion")

    async def test_ambiguous_post_is_not_retried(self):
        class Ambiguous(EchoConnector):
            calls = 0
            async def execute(self, ctx, inputs):
                self.calls += 1
                raise TimeoutError()
            async def poll(self, ctx, job_id):
                return BusinessResult(state="succeeded", text="recovered")
        connector = Ambiguous()
        self.engine.connectors["echo"] = connector
        self.cap.side_effects = True
        await self.start()
        self.assertEqual(connector.calls, 1)
        self.assertEqual(self.last()["status"]["state"], "awaiting-completion")

    async def test_restart_reconciles_instead_of_posting_again(self):
        await self.start()
        task = self.store.get("tasks", "task-1")
        task.update(state="working", phase="dispatching", deadline=time.time() + 1)
        self.store.put("tasks", "task-1", task)
        class Recover(EchoConnector):
            async def execute(self, ctx, inputs):
                raise AssertionError("must not dispatch again")
            async def poll(self, ctx, job_id):
                return BusinessResult(state="succeeded", text="reconciled")
        self.engine.connectors["echo"] = Recover()
        self.engine.recover()
        await self.drain()
        self.assertEqual(self.store.get("tasks", "task-1")["result"]["text"], "reconciled")

    async def test_uncertain_side_effect_reserves_slot(self):
        class Uncertain(EchoConnector):
            async def execute(self, ctx, inputs):
                raise TimeoutError()
            async def poll(self, ctx, job_id):
                raise TimeoutError()
        self.engine.connectors["echo"] = Uncertain()
        self.cap.side_effects = True
        await self.start()
        self.assertTrue(self.store.get("tasks", "task-1")["unsettled"])
        self.assertTrue(self.engine.group_has_work(self.group["id"]))

    async def test_identity_bound_to_database(self):
        with self.assertRaises(ValueError):
            self.store.bind_identity(LEADER)


class EvidenceTests(unittest.IsolatedAsyncioTestCase):
    async def check_response(self, code, body):
        http = httpx.AsyncClient(transport=httpx.MockTransport(lambda req: httpx.Response(code, json=body)))
        client = EvidenceClient("https://example.invalid:9007/evidence/v1/groups", PARTNER, client=http)
        try:
            return await client.read({"id": "group-unit-1", "leader": LEADER})
        finally:
            await http.aclose()

    async def test_complete_proof(self):
        self.assertTrue((await self.check_response(200, proof())).cleanup_complete())

    async def test_http_404_is_not_absence(self):
        with self.assertRaises(httpx.HTTPStatusError):
            await self.check_response(404, {})

    async def test_204_is_not_complete_proof(self):
        with self.assertRaises(ValueError):
            await self.check_response(204, {})

    async def test_wrong_identity(self):
        with self.assertRaises(ValueError):
            await self.check_response(200, proof(partnerAic=LEADER))

    async def test_expired_proof(self):
        with self.assertRaises(ValueError):
            await self.check_response(200, proof(observedAt=date(time.time() - 90)))

    async def test_unknown_blocks_closure(self):
        self.assertFalse((await self.check_response(200, proof(connection="unknown"))).cleanup_complete())

    async def test_inbox_must_still_be_consuming(self):
        self.assertFalse((await self.check_response(200, proof(inboxConsumers=0))).cleanup_complete())


class PolicyTests(unittest.IsolatedAsyncioTestCase):
    async def test_expiry_revocation_and_no_rollback(self):
        with tempfile.TemporaryDirectory() as tmp:
            auth = Authorization(PARTNER, Path(tmp) / "grants.json")
            try:
                first = grant()
                auth.apply(first)
                self.assertTrue(auth.permits(LEADER, "test.echo"))
                auth.apply(grant(2, []))
                self.assertFalse(auth.permits(LEADER))
                with self.assertRaises(ValueError):
                    auth.apply(first)
                auth.clock = lambda: time.time() + 1000
                self.assertFalse(auth.valid)
            finally:
                await auth.client.aclose()

    async def test_missing_policy_file_revokes(self):
        with tempfile.TemporaryDirectory() as tmp:
            auth = Authorization(PARTNER, Path(tmp) / "missing.json")
            auth.apply(grant())
            await auth.refresh()
            self.assertFalse(auth.valid)
            await auth.client.aclose()


class ConnectorTests(unittest.IsolatedAsyncioTestCase):
    def context(self):
        return ExecutionContext(request_id="durable-id", task_id="t", session_id="s", group_id="g", leader_aic=LEADER, partner_aic=PARTNER, capability_id="knowledge.ask")

    async def test_existing_api_mapping(self):
        def server(req):
            self.assertEqual(req.url.path, "/api/knowledge/ask")
            self.assertEqual(json.loads(req.content), {"question": "hello"})
            self.assertEqual(req.headers["Idempotency-Key"], "durable-id")
            return httpx.Response(200, json={"code": 0, "data": {"answer": "actual-answer", "private": "excluded"}})
        async with httpx.AsyncClient(base_url="https://example.invalid/api/", transport=httpx.MockTransport(server)) as http:
            plugin = JsonHttpConnector({"base_url": "https://example.invalid/api/", "path": "knowledge/ask",
                "request_fields": {"question": "text"}, "response_fields": {"text": "data.answer"}, "success": {"field": "code", "equals": 0}}, client=http)
            result = await plugin.execute(self.context(), {"text": "hello"})
            self.assertEqual(result.output, {"text": "actual-answer"})

    async def test_standard_async_interface(self):
        def server(req):
            if req.method == "POST":
                self.assertEqual(json.loads(req.content)["requestId"], "durable-id")
                return httpx.Response(202, json={"state": "working", "job_id": "job-1"})
            self.assertEqual(req.url.path, "/v1/tasks/durable-id")
            return httpx.Response(200, json={"state": "succeeded", "output": {"text": "done"}})
        async with httpx.AsyncClient(base_url="https://example.invalid/", transport=httpx.MockTransport(server)) as http:
            connector = StandardHttpConnector({"base_url": "https://example.invalid/"}, client=http)
            self.assertEqual((await connector.execute(self.context(), {})).state, "working")
            self.assertEqual((await connector.poll(self.context(), "job-1")).output["text"], "done")


class FakeTransport:
    groups = {}
    inbox_ready = True
    async def close_group(self, group_id):
        return True
    async def publish(self, group_id, body):
        pass


class FinalizeTests(unittest.IsolatedAsyncioTestCase):
    asyncTearDown = EngineTests.asyncTearDown
    command = EngineTests.command
    drain = EngineTests.drain
    start = EngineTests.start
    last = EngineTests.last

    async def asyncSetUp(self):
        await EngineTests.asyncSetUp(self)
        class EvidenceProvider:
            async def read(inner, group):
                return Evidence.model_validate(proof())
        self.runtime = Runtime(self.settings, self.store, self.engine, FakeTransport(), self.policy, EvidenceProvider())

    async def disband(self, message_id="disband-1"):
        body = {"id": message_id, "type": "group-mgmt-command", "sentAt": now(), "senderRole": "leader", "senderId": LEADER,
                "groupId": self.group["id"], "sessionId": "session-1", "mentions": [PARTNER], "command": "disband"}
        await self.runtime.message(self.group["id"], body)

    async def test_disband_deadline_fixed(self):
        await self.disband()
        first = self.store.get("groups", self.group["id"])
        await self.disband("disband-2")
        self.assertEqual(first["deadline_at"], self.store.get("groups", self.group["id"])["deadline_at"])

    async def test_closed_requires_publisher_ack(self):
        await self.disband()
        await self.runtime.finalize(self.group["id"])
        self.assertEqual(self.store.get("groups", self.group["id"])["state"], "leaving")

    async def test_auto_closed_full_proof(self):
        await self.disband()
        group = self.store.get("groups", self.group["id"])
        group["exit_confirmed"] = True
        self.store.put("groups", group["id"], group)
        await self.runtime.finalize(group["id"])
        self.assertEqual(self.store.get("groups", group["id"])["state"], "closed")
        before = self.store.get("groups", group["id"])
        await self.runtime.finalize(group["id"])
        self.assertEqual(before, self.store.get("groups", group["id"]))

    async def test_active_business_blocks_exit(self):
        await self.engine.handle(self.command(inputs={}))
        await self.disband()
        group = self.store.get("groups", self.group["id"])
        group["exit_confirmed"] = True
        self.store.put("groups", group["id"], group)
        await self.runtime.finalize(group["id"])
        self.assertEqual(self.store.get("groups", group["id"])["state"], "leaving")

    async def test_expired_window_cannot_close(self):
        await self.disband()
        group = self.store.get("groups", self.group["id"])
        group.update(exit_confirmed=True, deadline_at=time.time() - 1)
        self.store.put("groups", group["id"], group)
        await self.runtime.finalize(group["id"])
        self.assertEqual(self.store.get("groups", group["id"])["state"], "leaving")


if __name__ == "__main__":
    unittest.main()
