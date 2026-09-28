"""Expiring, administrator-issued grants, reloaded without restarting the runtime."""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Literal

import httpx
from pydantic import BaseModel, ConfigDict, Field

from .models import timestamp


class Grant(BaseModel):
    model_config = ConfigDict(extra="forbid")
    leader_aic: str
    capabilities: list[str]
    purpose: Literal["test", "production"]
    approved_by: str = Field(min_length=1)
    approved_at: str
    reason: str = Field(min_length=1)


class Policy(BaseModel):
    model_config = ConfigDict(extra="forbid")
    version: int = Field(ge=1)
    partner_aic: str
    issued_at: str
    expires_at: str
    grants: list[Grant]


class Authorization:
    def __init__(self, aic, file, url=None, *, tls=True, clock=time.time):
        self.aic, self.file, self.url, self.clock = aic, Path(file), url, clock
        if url and not url.startswith("https://"):
            raise ValueError("Remote authorization service requires HTTPS")
        self.client = httpx.AsyncClient(verify=tls, timeout=10, follow_redirects=False, trust_env=False)
        self.current: Policy | None = None
        self.last_error: str | None = None

    def apply(self, raw):
        value = Policy.model_validate(raw)
        current = self.clock()
        if value.partner_aic != self.aic:
            raise ValueError("POLICY_IDENTITY_MISMATCH")
        if not timestamp(value.issued_at) <= current < timestamp(value.expires_at):
            raise ValueError("POLICY_NOT_CURRENT")
        for grant in value.grants:
            if timestamp(grant.approved_at) > current:
                raise ValueError("APPROVAL_IN_FUTURE")
        if self.current:
            if value.version < self.current.version:
                raise ValueError("POLICY_ROLLBACK")
            if value.version == self.current.version and value != self.current:
                raise ValueError("POLICY_VERSION_CONFLICT")
        self.current, self.last_error = value, None

    async def refresh(self):
        try:
            if self.url:
                response = await self.client.get(self.url, params={"partnerAic": self.aic})
                if response.status_code in {401, 403, 404, 410}:
                    self.current = None  # Explicit denial is not a temporary network outage.
                response.raise_for_status()
                self.apply(response.json())
            else:
                if not self.file.exists():
                    self.current = None
                self.apply(json.loads(self.file.read_text(encoding="utf-8")))
        except Exception as exc:
            self.last_error = type(exc).__name__  # No endpoints, tokens or raw payloads in logs.

    @property
    def valid(self):
        return self.current is not None and timestamp(self.current.expires_at) > self.clock()

    def permits(self, leader, capability=None):
        if not self.valid:
            return False
        return any(g.leader_aic == leader and g.capabilities and
                   (capability is None or capability in g.capabilities) for g in self.current.grants)
