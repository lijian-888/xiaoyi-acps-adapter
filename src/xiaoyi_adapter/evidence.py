"""Read-only platform proof. HTTP errors, including HTTP 404, are never absence."""
from __future__ import annotations

import time
from typing import Literal
from urllib.parse import urlsplit

import httpx
from pydantic import BaseModel, StrictBool, StrictInt

from .models import timestamp

ResourceState = Literal["present", "absent", "unknown"]


class Evidence(BaseModel):
    leaderAic: str
    groupId: str
    partnerAic: str
    evidenceId: str
    apiVersion: Literal["1"]
    observedAt: str
    connection: ResourceState
    channel: ResourceState
    groupConsumer: ResourceState
    partnerQueue: ResourceState
    leaderQueue: ResourceState
    exchange: ResourceState
    groupAcl: ResourceState
    memberAcl: ResourceState
    inbox: ResourceState
    inboxConsumers: StrictInt
    evidenceComplete: StrictBool
    connectionIdentityObserved: StrictBool

    def cleanup_complete(self):
        fields = (self.connection, self.channel, self.groupConsumer, self.partnerQueue,
                  self.leaderQueue, self.exchange, self.groupAcl, self.memberAcl)
        return (self.evidenceComplete and self.connectionIdentityObserved and
                all(v == "absent" for v in fields) and self.inbox == "present" and self.inboxConsumers == 1)


class EvidenceClient:
    def __init__(self, url, aic, *, tls=True, max_age=60, client=None):
        parsed = urlsplit(url)
        if parsed.scheme != "https" or parsed.port != 9007 or parsed.path != "/evidence/v1/groups":
            raise ValueError("Evidence requires the approved 9007 mTLS endpoint")
        self.url, self.aic, self.max_age = url, aic, max_age
        self.client = client or httpx.AsyncClient(verify=tls, timeout=10, follow_redirects=False, trust_env=False)

    async def read(self, group):
        response = await self.client.get(self.url, params={"leaderAic": group["leader"], "groupId": group["id"], "partnerAic": self.aic})
        response.raise_for_status()
        if response.status_code != 200:
            raise ValueError("EVIDENCE_RESPONSE_NOT_200")
        proof = Evidence.model_validate(response.json())
        if (proof.leaderAic, proof.groupId, proof.partnerAic) != (group["leader"], group["id"], self.aic):
            raise ValueError("EVIDENCE_IDENTITY_MISMATCH")
        age = time.time() - timestamp(proof.observedAt)
        if age < -5 or age > self.max_age:
            raise ValueError("EVIDENCE_NOT_FRESH")
        if group.get("received_at") and timestamp(proof.observedAt) < timestamp(group["received_at"]):
            raise ValueError("EVIDENCE_PREDATES_DISBAND")
        return proof
