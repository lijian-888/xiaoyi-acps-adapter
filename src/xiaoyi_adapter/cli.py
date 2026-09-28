from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from .config import Settings
from .connectors import build_connectors
from .engine import Engine
from .evidence import EvidenceClient
from .policy import Authorization
from .runtime import Runtime
from .store import Store


def application(settings):
    @asynccontextmanager
    async def lifespan(app):
        # Serialize instances sharing this state mount; independent AICs get their own mount.
        import fcntl
        state_dir = Path(settings.state_file).parent
        state_dir.mkdir(parents=True, exist_ok=True)
        lock = (state_dir / "instance.lock").open("a")
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        settings.check_acs()
        tls = settings.tls()
        store = Store(settings.state_file)
        store.bind_identity(settings.aic)
        policy = Authorization(settings.aic, settings.policy_file, settings.policy_url, tls=tls)
        from .transport import Transport
        runtime = Runtime(settings, store, Engine(settings, store, policy, build_connectors(settings.connectors)),
                          Transport(settings, tls), policy, EvidenceClient(settings.evidence_url, settings.aic, tls=tls, max_age=settings.evidence_max_age_seconds))
        app.state.runtime = runtime
        runtime.worker = asyncio.create_task(runtime.run())
        try:
            yield
        finally:
            await runtime.close()
            store.db.close()
            lock.close()

    app = FastAPI(title="Xiaoyi ACPs Group Partner", version="0.1.0", lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)

    @app.get("/health/live")
    async def live():
        return {"live": True}

    @app.get("/health/ready")
    async def ready():
        data = app.state.runtime.health()
        return JSONResponse(data, status_code=200 if data["ready"] else 503)

    return app


def main():
    parser = argparse.ArgumentParser(description="Independent Group-only ACPs Partner runtime")
    parser.add_argument("command", choices=["serve", "check", "inspect"])
    parser.add_argument("--config", default=os.getenv("ADAPTER_CONFIG", "/config/runtime.json"))
    args = parser.parse_args()
    settings = Settings.load(args.config)
    if args.command == "check":
        settings.check_acs()
        settings.tls()
        print(json.dumps({"configurationValid": True, "aic": settings.aic, "capabilities": [c.id for c in settings.capabilities]}))
    elif args.command == "inspect":
        import sqlite3
        db = sqlite3.connect(f"file:{Path(settings.state_file).as_posix()}?mode=ro", uri=True)
        groups = [json.loads(row[0]) for row in db.execute("SELECT body FROM groups")]
        print(json.dumps({"aic": settings.aic, "groups": groups}, ensure_ascii=False, indent=2))
        db.close()
    else:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
        uvicorn.run(application(settings), host="0.0.0.0", port=8080, access_log=False)


if __name__ == "__main__":
    main()
