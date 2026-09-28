"""AMQPS/EXTERNAL transport using official SDK names and protocol models.

Own connection per Group; no stock SDK unconditional deletion/close callbacks.
"""
from __future__ import annotations

import asyncio
import json

import aio_pika
from acps_sdk.aip.aip_group_runtime import (
    INBOX_EXCHANGE_NAME, INBOX_MESSAGE_TTL_MS, INBOX_QUEUE_EXPIRES_MS,
    build_external_connection_url, build_group_exchange_name, build_group_queue_name, build_inbox_queue_name,
)
from pamqp.commands import Basic

from .models import dumps


class Transport:
    def __init__(self, settings, tls):
        self.s, self.tls = settings, tls
        self.inbox = None
        self.inbox_queue = None
        self.inbox_tag = None
        self.groups = {}

    async def connection(self, group_id=None):
        url = build_external_connection_url(host=self.s.mq_host, port=self.s.mq_port,
              vhost=self.s.mq_vhost, connection_name=f"agent-{self.s.aic}")
        return await aio_pika.connect(url, ssl_context=self.tls, timeout=self.s.io_timeout,
                                      heartbeat=30, client_properties={"xiaoyi_group_id": group_id or "inbox"})

    async def consume(self, queue, handler, entry=None, *, exclusive=False):
        async def receive(message):
            try:
                if len(message.body) > self.s.max_message_bytes:
                    await message.reject(requeue=False)
                    return
                body = json.loads(message.body)
                if not isinstance(body, dict):
                    raise ValueError("Message must be an object")
            except (ValueError, UnicodeError):
                await message.reject(requeue=False)
                return
            # Handler persists receipts/state/outbox before ACK. A crash after durable
            # processing produces a safe duplicate on redelivery.
            if entry is not None:
                entry["callbacks"] = entry.get("callbacks", 0) + 1
            try:
                try:
                    await handler(body)
                except Exception:
                    if not message.channel.is_closed:
                        await message.nack(requeue=True)
                    return
                if not message.channel.is_closed:
                    await message.ack()
            finally:
                if entry is not None:
                    entry["callbacks"] -= 1
        return await queue.consume(receive, exclusive=exclusive)

    async def start_inbox(self, handler):
        if self.inbox is not None:
            await self.inbox.close()
        self.inbox = await self.connection()
        channel = await self.inbox.channel(publisher_confirms=True, on_return_raises=True)
        await channel.set_qos(prefetch_count=1)
        self.inbox_queue = await channel.declare_queue(build_inbox_queue_name(self.s.aic), durable=True, auto_delete=False,
                 arguments={"x-expires": INBOX_QUEUE_EXPIRES_MS, "x-message-ttl": INBOX_MESSAGE_TTL_MS})
        exchange = await channel.declare_exchange(INBOX_EXCHANGE_NAME, aio_pika.ExchangeType.TOPIC, durable=True)
        await self.inbox_queue.bind(exchange, routing_key=build_inbox_queue_name(self.s.aic))
        self.inbox_tag = await self.consume(self.inbox_queue, handler, exclusive=True)

    async def join(self, group, handler, *, recovery=False):
        if group["id"] in self.groups:
            raise ValueError("GROUP_TRANSPORT_ALREADY_EXISTS")
        conn = await self.connection(group["id"])
        entry = {"connection": conn, "tag": None, "queue": None}
        self.groups[group["id"]] = entry  # Retain exact instance even when setup fails.
        channel = await conn.channel(publisher_confirms=True, on_return_raises=True)
        await channel.set_qos(prefetch_count=8)
        exchange = await channel.declare_exchange(build_group_exchange_name(group["leader"], group["id"]),
                         aio_pika.ExchangeType.FANOUT, passive=True)
        queue_name = build_group_queue_name(group["leader"], group["id"], self.s.aic)
        queue = await channel.declare_queue(queue_name, durable=True, auto_delete=True, passive=recovery)
        if not recovery:
            await queue.bind(exchange)
        entry.update(channel=channel, exchange=exchange, queue=queue, queue_name=queue_name)
        entry["tag"] = await self.consume(queue, handler, entry)

    async def publish(self, group_id, body):
        entry = self.groups[group_id]
        encoded = dumps(body).encode()
        if len(encoded) > self.s.max_message_bytes:
            raise ValueError("PRODUCT_TOO_LARGE")
        confirmation = await entry["exchange"].publish(aio_pika.Message(
            body=encoded, content_type="application/json", message_id=body["id"],
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT), routing_key="", mandatory=True, timeout=self.s.io_timeout)
        if not isinstance(confirmation, Basic.Ack):
            raise RuntimeError("PUBLISH_NOT_CONFIRMED")

    async def close_group(self, group_id):
        entry = self.groups.get(group_id)
        if entry is None:
            return True
        if entry.get("callbacks", 0):
            return False  # Wait for incoming ACK before canceling its consumer/connection.
        conn = entry["connection"]
        if not conn.is_closed:
            if entry.get("tag") and entry.get("queue"):
                try:
                    await entry["queue"].cancel(entry["tag"], timeout=self.s.io_timeout)
                    entry["tag"] = None
                except Exception:
                    pass
            # Only exact Partner queue, separate passive channel, conditionally deleted.
            channel = None
            try:
                channel = await conn.channel()
                if entry.get("queue_name"):
                    queue = await channel.declare_queue(entry["queue_name"], passive=True)
                    if queue.declaration_result.consumer_count == 0 and queue.declaration_result.message_count == 0:
                        await queue.delete(if_unused=True, if_empty=True, timeout=self.s.io_timeout)
            except Exception:
                # Not proof of absence. Full platform evidence remains mandatory.
                pass
            finally:
                if channel and not channel.is_closed:
                    try:
                        await asyncio.wait_for(channel.close(), self.s.io_timeout)
                    except Exception:
                        pass
            try:
                await asyncio.wait_for(conn.close(), self.s.io_timeout)
            except Exception:
                return False
        if conn.is_closed and self.groups.get(group_id) is entry:
            del self.groups[group_id]
            return True
        return False

    def reconcile_closed(self, group_id, proof):
        """Handle a close timeout only with exact platform AND local transport proof."""
        entry = self.groups.get(group_id)
        if entry is None:
            return True
        conn = entry["connection"]
        local_closed = conn.is_closed or getattr(conn, "transport", object()) is None
        remote_closed = (proof.connectionIdentityObserved and proof.evidenceComplete and
                         proof.connection == proof.channel == proof.groupConsumer == "absent")
        if local_closed and remote_closed and self.groups.get(group_id) is entry:
            del self.groups[group_id]
            return True
        return False

    @property
    def inbox_ready(self):
        return self.inbox is not None and not self.inbox.is_closed and self.inbox_tag is not None

    async def close(self):
        for gid in list(self.groups):
            await self.close_group(gid)
        if self.inbox and not self.inbox.is_closed:
            await self.inbox.close()  # Never delete the long-term Inbox.
