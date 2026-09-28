"""Business connectors. HTTP here is an internal business hop, never ACPs Direct RPC."""
from __future__ import annotations

from pathlib import Path
from typing import Protocol
from urllib.parse import quote, urlsplit

import httpx

from .models import BusinessResult, ExecutionContext


class UnsupportedOperation(Exception):
    pass


class Connector(Protocol):
    async def execute(self, ctx: ExecutionContext, inputs: dict) -> BusinessResult: ...
    async def poll(self, ctx: ExecutionContext, job_id: str | None) -> BusinessResult: ...
    async def resume(self, ctx: ExecutionContext, job_id: str | None, inputs: dict) -> BusinessResult: ...
    async def cancel(self, ctx: ExecutionContext, job_id: str | None) -> BusinessResult: ...


class EchoConnector:
    async def execute(self, ctx, inputs):
        text = inputs.get("text", "")
        if not text:
            return BusinessResult(state="awaiting-input", text="请输入需要回显的文本。")
        return BusinessResult(state="succeeded", text=text, output={"text": text})

    async def poll(self, ctx, job_id):
        raise UnsupportedOperation("ECHO_HAS_NO_REMOTE_JOB")

    async def resume(self, ctx, job_id, inputs):
        return await self.execute(ctx, inputs)

    async def cancel(self, ctx, job_id):
        return BusinessResult(state="canceled")


class StandardHttpConnector:
    def __init__(self, config: dict, *, client=None):
        url = config["base_url"]
        parsed = urlsplit(url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.query or parsed.fragment:
            raise ValueError("Invalid business base URL")
        if parsed.scheme == "http" and config.get("token_file") and not config.get("allow_private_http", False):
            raise ValueError("Credentials require HTTPS or explicit trusted-private-network configuration")
        self.config = config
        self.client = client or httpx.AsyncClient(base_url=url.rstrip("/") + "/", timeout=config.get("timeout_seconds", 10), follow_redirects=False, trust_env=False)

    async def _call(self, method, path, ctx, payload=None):
        headers = {"Idempotency-Key": ctx.request_id}
        if token_file := self.config.get("token_file"):
            headers["Authorization"] = "Bearer " + Path(token_file).read_text().strip()
        async with self.client.stream(method, path, json=payload, headers=headers) as response:
            response.raise_for_status()
            body = bytearray()
            async for chunk in response.aiter_bytes():
                body.extend(chunk)
                if len(body) > self.config.get("max_response_bytes", 262144):
                    raise ValueError("BUSINESS_RESPONSE_TOO_LARGE")
            import json
            return json.loads(body)

    async def execute(self, ctx, inputs):
        body = await self._call("POST", f"v1/capabilities/{quote(ctx.capability_id, safe='')}/execute", ctx,
                                {"requestId": ctx.request_id, "input": inputs})
        return BusinessResult.model_validate(body)

    async def poll(self, ctx, job_id):
        # The business API MUST support lookup by the durable client request ID,
        # including after a lost POST response. A 404 never proves nonexecution.
        return BusinessResult.model_validate(await self._call("GET", f"v1/tasks/{quote(ctx.request_id, safe='')}", ctx))

    async def resume(self, ctx, job_id, inputs):
        return BusinessResult.model_validate(await self._call("POST", f"v1/tasks/{quote(job_id or ctx.request_id, safe='')}/continue", ctx,
                                               {"requestId": ctx.request_id, "input": inputs}))

    async def cancel(self, ctx, job_id):
        return BusinessResult.model_validate(await self._call("POST", f"v1/tasks/{quote(job_id or ctx.request_id, safe='')}/cancel", ctx))


class JsonHttpConnector(StandardHttpConnector):
    """Reviewed config mapping for existing synchronous JSON APIs. No eval/import."""
    async def execute(self, ctx, inputs):
        fields = self.config.get("request_fields", {})
        payload = {target: inputs[source] for target, source in fields.items() if source in inputs}
        path = self.config["path"]
        if path.startswith("/") or ".." in path or ":" in path or "?" in path or "#" in path:
            raise ValueError("Mapping path must be a fixed relative path")
        body = await self._call("POST", path, ctx, payload)
        # A transport HTTP 200 does not mean business success.
        if rule := self.config.get("success"):
            if self._field(body, rule["field"]) != rule["equals"]:
                return BusinessResult(state="failed", error_code="BUSINESS_REJECTED")
        output = {target: self._field(body, source) for target, source in self.config["response_fields"].items()}
        return BusinessResult(state="succeeded", output=output, text=str(output.get("text", "")))

    @staticmethod
    def _field(body, dotted):
        current = body
        for key in dotted.split("."):
            current = current[key]
        return current

    async def poll(self, ctx, job_id):
        raise UnsupportedOperation("LEGACY_API_RECONCILIATION_REQUIRES_REVIEWED_PLUGIN")

    async def resume(self, ctx, job_id, inputs):
        raise UnsupportedOperation("CONTINUE_UNSUPPORTED")

    async def cancel(self, ctx, job_id):
        raise UnsupportedOperation("CANCEL_UNSUPPORTED")


def build_connectors(configs):
    factories = {"echo": EchoConnector, "standard-http": StandardHttpConnector, "json-http": JsonHttpConnector}
    result = {}
    for name, config in configs.items():
        kind = config["type"]
        if kind not in factories:
            raise ValueError("Connector plugin must be reviewed and registered in this build")
        result[name] = EchoConnector() if kind == "echo" else factories[kind](config)
    return result
