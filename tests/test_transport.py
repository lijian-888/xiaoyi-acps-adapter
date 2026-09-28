import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock

from pamqp.commands import Basic

from xiaoyi_adapter.transport import Transport


class TransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_inbox_consumer_can_be_exclusive(self):
        transport = Transport(SimpleNamespace(max_message_bytes=262144), None)
        queue = SimpleNamespace(consume=AsyncMock(return_value="tag"))
        self.assertEqual(await transport.consume(queue, AsyncMock(), exclusive=True), "tag")
        self.assertTrue(queue.consume.call_args.kwargs["exclusive"])

    def fixture(self, consumers=0, messages=0):
        settings = SimpleNamespace(io_timeout=1, max_message_bytes=262144)
        transport = Transport(settings, None)
        queue = SimpleNamespace(declaration_result=SimpleNamespace(consumer_count=consumers, message_count=messages), delete=AsyncMock(), cancel=AsyncMock())
        channel = SimpleNamespace(declare_queue=AsyncMock(return_value=queue), close=AsyncMock(), is_closed=False)
        conn = SimpleNamespace(is_closed=False, channel=AsyncMock(return_value=channel), transport=object())
        async def close():
            conn.is_closed = True
        conn.close = AsyncMock(side_effect=close)
        transport.groups["group-1"] = {"connection": conn, "tag": "consumer-1", "queue": queue, "queue_name": "exact-partner-queue"}
        return transport, queue, channel, conn

    async def test_delete_requires_both_broker_conditions(self):
        transport, queue, channel, conn = self.fixture()
        self.assertTrue(await transport.close_group("group-1"))
        channel.declare_queue.assert_awaited_once_with("exact-partner-queue", passive=True)
        queue.delete.assert_awaited_once_with(if_unused=True, if_empty=True, timeout=1)
        self.assertNotIn("group-1", transport.groups)

    async def test_busy_consumer_not_deleted(self):
        transport, queue, _, _ = self.fixture(consumers=1)
        await transport.close_group("group-1")
        queue.delete.assert_not_awaited()

    async def test_queued_message_not_deleted(self):
        transport, queue, _, _ = self.fixture(messages=1)
        await transport.close_group("group-1")
        queue.delete.assert_not_awaited()

    async def test_delete_race_does_not_force_delete(self):
        transport, queue, _, _ = self.fixture()
        queue.delete.side_effect = RuntimeError("AMQP 406 resource became nonempty")
        await transport.close_group("group-1")
        self.assertEqual(queue.delete.await_count, 1)
        self.assertTrue(queue.delete.call_args.kwargs["if_empty"])

    async def test_close_timeout_retains_reference(self):
        transport, _, _, conn = self.fixture()
        conn.close = AsyncMock(side_effect=TimeoutError())
        self.assertFalse(await transport.close_group("group-1"))
        self.assertIn("group-1", transport.groups)

    async def test_reconcile_needs_local_and_remote_evidence(self):
        transport, _, _, conn = self.fixture()
        proof = SimpleNamespace(connectionIdentityObserved=True, evidenceComplete=True, connection="absent", channel="absent", groupConsumer="absent")
        self.assertFalse(transport.reconcile_closed("group-1", proof))
        conn.transport = None
        proof.connection = "unknown"
        self.assertFalse(transport.reconcile_closed("group-1", proof))
        proof.connection = "absent"
        self.assertTrue(transport.reconcile_closed("group-1", proof))

    async def test_inflight_ack_blocks_connection_close(self):
        transport, _, _, conn = self.fixture()
        transport.groups["group-1"]["callbacks"] = 1
        self.assertFalse(await transport.close_group("group-1"))
        conn.close.assert_not_awaited()

    async def test_publisher_nack_is_not_success(self):
        transport, _, _, _ = self.fixture()
        transport.groups["group-1"]["exchange"] = SimpleNamespace(publish=AsyncMock(return_value=Basic.Nack()))
        with self.assertRaises(RuntimeError):
            await transport.publish("group-1", {"id": "result-1"})

    async def test_publisher_ack_is_awaited(self):
        transport, _, _, _ = self.fixture()
        publish = AsyncMock(return_value=Basic.Ack())
        transport.groups["group-1"]["exchange"] = SimpleNamespace(publish=publish)
        await transport.publish("group-1", {"id": "result-1"})
        self.assertEqual(publish.await_count, 1)
