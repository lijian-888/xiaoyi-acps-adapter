from __future__ import annotations

import json
import ssl
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

from acps_sdk.aip.aip_group_runtime import ensure_valid_aic
from cryptography import x509
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
from jsonschema.validators import validator_for
from pydantic import BaseModel, ConfigDict, Field, model_validator

from .models import Capability


class Settings(BaseModel):
    model_config = ConfigDict(extra="forbid")
    aic: str
    acs_file: str
    state_file: str
    cert_file: str
    key_file: str
    ca_file: str
    mq_host: str
    mq_port: int = 5671
    mq_vhost: str = "acps"
    policy_file: str
    policy_url: str | None = None
    policy_refresh_seconds: float = Field(default=30, ge=1, le=60)
    evidence_url: str
    evidence_max_age_seconds: float = Field(default=60, ge=1, le=300)
    max_groups: int = Field(default=2, ge=1, le=100)
    max_tasks: int = Field(default=2, ge=1, le=100)
    disband_seconds: int = Field(default=300, ge=1, le=300)
    io_timeout: float = Field(default=10, gt=0, le=60)
    max_message_bytes: int = Field(default=262144, ge=1024, le=1048576)
    capabilities: list[Capability]
    connectors: dict[str, dict]

    @model_validator(mode="after")
    def validate_manifest(self):
        ensure_valid_aic(self.aic)
        ids = [c.id for c in self.capabilities]
        if len(ids) != len(set(ids)) or not ids:
            raise ValueError("Capabilities must have unique nonempty IDs")
        for c in self.capabilities:
            if c.connector not in self.connectors:
                raise ValueError("Missing connector")
            for schema in (c.input_schema, c.output_schema):
                validator_for(schema).check_schema(schema)
                if '"$ref"' in json.dumps(schema):
                    raise ValueError("External/reference schemas are not supported; provide inline schema")
        return self

    @classmethod
    def load(cls, file: str) -> "Settings":
        return cls.model_validate_json(Path(file).read_text(encoding="utf-8"))

    def tls(self) -> ssl.SSLContext:
        cert = x509.load_pem_x509_certificate(Path(self.cert_file).read_bytes())
        cn = cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
        if len(cn) != 1 or cn[0].value != self.aic:
            raise ValueError("Certificate identity differs from configured Partner AIC")
        current = datetime.now(timezone.utc)
        if not cert.not_valid_before_utc <= current < cert.not_valid_after_utc:
            raise ValueError("Certificate is not currently valid")
        if ExtendedKeyUsageOID.CLIENT_AUTH not in cert.extensions.get_extension_for_class(x509.ExtendedKeyUsage).value:
            raise ValueError("Certificate lacks clientAuth")
        ctx = ssl.create_default_context(cafile=self.ca_file)
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        ctx.load_cert_chain(self.cert_file, self.key_file)
        return ctx

    def check_acs(self):
        from acps_sdk.acs import AgentCapabilitySpec
        acs = json.loads(Path(self.acs_file).read_text(encoding="utf-8"))
        sdk_view = deepcopy(acs)
        queue_protocols = sdk_view.get("capabilities", {}).get("messageQueue")
        if isinstance(queue_protocols, list):
            # Registry 2.1.0 approves rabbitmq:>=4.2, while SDK 2.1.0's
            # enum stops at rabbitmq:3.11. Adapt only this known enum value
            # for structural validation; never rewrite the approved ACS.
            sdk_view["capabilities"]["messageQueue"] = [
                "rabbitmq:3.11" if value == "rabbitmq:>=4.2" else value
                for value in queue_protocols
            ]
        AgentCapabilitySpec.model_validate(sdk_view)
        if acs.get("aic") != self.aic:
            raise ValueError("ACS identity mismatch")
        skills = {s["id"] for s in acs.get("skills", [])}
        if not {c.id for c in self.capabilities} <= skills:
            raise ValueError("Runtime capability missing from approved ACS")
        from acps_sdk.aip.aip_group_runtime import parse_amqp_endpoint_url
        expected = (self.mq_host, self.mq_port, self.mq_vhost, f"inbox_{self.aic}")
        for endpoint in acs.get("endPoints", []):
            if endpoint.get("transport", "").upper() == "AMQP":
                actual = parse_amqp_endpoint_url(endpoint["url"], aic=self.aic)
                if (actual.host, actual.port, actual.vhost, actual.inbox) == expected:
                    return
        raise ValueError("ACS lacks the exact configured AMQP Inbox endpoint")
