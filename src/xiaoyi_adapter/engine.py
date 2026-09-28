"""Durable AIP command processing; business execution runs outside MQ callbacks."""
from __future__ import annotations

import asyncio
import time

from acps_sdk.aip import Product, StructuredDataItem, TaskCommand, TaskResult, TaskStatus, TextDataItem
from jsonschema import ValidationError, validate

from .connectors import UnsupportedOperation
from .models import BUSY, TERMINAL, BusinessResult, ExecutionContext, digest, dumps, new_id, now


class Engine:
    def __init__(self, settings, store, authorization, connectors):
        self.settings, self.store, self.authorization = settings, store, authorization
        self.capabilities = {c.id: c for c in settings.capabilities}
        self.connectors = connectors
        self.lock = asyncio.Lock()
        self.jobs: dict[str, asyncio.Task] = {}
        self.closed = False

    @staticmethod
    def inputs(command):
        skill, values = "", {}
        text = "".join(item.text for item in (command.dataItems or []) if isinstance(item, TextDataItem))
        for item in command.dataItems or []:
            if isinstance(item, StructuredDataItem) and isinstance(item.data, dict):
                skill = item.data.get("skillId", skill)
                if isinstance(item.data.get("input"), dict):
                    values = item.data["input"]
        if "text" not in values and text:
            values = {**values, "text": text}
        return skill, values

    def emit(self, command, task=None, error=None):
        state = task["state"] if task else "rejected"
        products = []
        if task and task.get("result") and state in {"awaiting-completion", "completed"}:
            result = task["result"]
            products = [Product(id=task["product_id"], name=task["skill"], dataItems=[
                TextDataItem(text=result.get("text", "")), StructuredDataItem(data=result.get("output", {}))])]
        details = []
        code = error or (task or {}).get("error")
        if code:
            details.append(StructuredDataItem(data={"errorCode": code}))
        if task and state == "awaiting-input":
            details.append(TextDataItem(text=task.get("prompt", "请补充必要参数。")))
        result = TaskResult(id=new_id("result"), sentAt=now(), senderRole="partner", senderId=self.settings.aic,
                            mentions=[command.senderId], groupId=command.groupId, sessionId=command.sessionId,
                            taskId=command.taskId, status=TaskStatus(state=state, stateChangedAt=(task or {}).get("changed_at", now()), dataItems=details),
                            products=products or None)
        self.store.enqueue(command.groupId, result.model_dump(mode="json", exclude_none=True))

    async def handle(self, command: TaskCommand):
        if not command.taskId or not command.sessionId or not command.groupId:
            return
        group = self.store.get("groups", command.groupId)
        if not group or command.senderRole != "leader" or command.senderId != group["leader"]:
            self.store.audit("command_identity_rejected", command.id)
            return
        if command.mentions != "all" and self.settings.aic not in (command.mentions or []):
            return
        launch = None
        async with self.lock:
            with self.store.transaction():
                claim = self.store.receipt(command.id, digest(command.model_dump(mode="json")))
                task = self.store.get("tasks", command.taskId)
                if claim == "duplicate":
                    return  # Existing durable outbox/terminal result is retained.
                if claim == "conflict":
                    self.store.audit("message_id_conflict", command.id)
                    return
                if task and (task["group"], task["session"], task["leader"]) != (command.groupId, command.sessionId, command.senderId):
                    self.emit(command, error="TASK_SCOPE_MISMATCH")
                    return
                verb = command.command.value
                if verb == "get":
                    self.emit(command, task, None if task else "TASK_NOT_FOUND")
                    return
                if verb == "complete":
                    if task and task["state"] == "awaiting-completion" and not task.get("unsettled"):
                        task.update(state="completed", changed_at=now())
                        self.store.put("tasks", task["id"], task)
                        self.emit(command, task)
                    else:
                        self.emit(command, task, None if task and task["state"] == "completed" else "INVALID_COMPLETE_STATE")
                    return
                if task and task["state"] in TERMINAL:
                    skill, inputs = self.inputs(command)
                    conflict = verb == "start" and digest({"skill": skill, "input": inputs}) != task["start_digest"]
                    self.emit(command, task, "TASK_ID_CONFLICT" if conflict else (None if verb == "start" else "TASK_TERMINAL"))
                    return
                if verb == "cancel":
                    if not task:
                        self.emit(command, error="TASK_NOT_FOUND")
                    elif not self.capabilities[task["skill"]].allow_cancel:
                        self.emit(command, task, "CANCEL_UNSUPPORTED")
                    else:
                        task["cancel_requested"] = True
                        self.store.put("tasks", task["id"], task)
                        launch = task["id"]
                        self.emit(command, task)
                elif verb in {"start", "continue"}:
                    skill, inputs = self.inputs(command)
                    if verb == "continue" and task:
                        skill = skill or task["skill"]
                    cap = self.capabilities.get(skill)
                    if not cap or (task and skill != task["skill"]):
                        self.emit(command, task, "CAPABILITY_UNSUPPORTED")
                        return
                    fingerprint = digest({"skill": skill, "input": inputs})
                    if verb == "start" and task:
                        self.emit(command, task, None if fingerprint == task["start_digest"] else "TASK_ID_CONFLICT")
                        return
                    if group["state"] != "joined" or group.get("muted"):
                        self.emit(command, task, "GROUP_NOT_ACCEPTING_TASKS")
                        return
                    if not self.authorization.permits(command.senderId, skill):
                        self.emit(command, task, "CALL_NOT_AUTHORIZED")
                        return
                    if verb == "continue" and (not task or not cap.allow_continue or task["state"] not in {"awaiting-input", "awaiting-completion"} or task.get("unsettled")):
                        self.emit(command, task, "CONTINUE_NOT_ALLOWED")
                        return
                    busy = sum(t["state"] in BUSY or t.get("unsettled", False) for t in self.store.all("tasks"))
                    if busy >= self.settings.max_tasks:
                        self.emit(command, task, "CAPACITY_EXCEEDED")
                        return
                    state, error = "accepted", None
                    try:
                        validate(inputs, cap.input_schema)
                    except ValidationError as exc:
                        state = "awaiting-input" if exc.validator == "required" else "rejected"
                        error = "INPUT_SCHEMA_INVALID"
                    request_id = digest({"aic": self.settings.aic, "group": command.groupId, "task": command.taskId, "action": command.id})
                    old_state = (task or {}).get("state")
                    prior_job = (task or {}).get("job_id")
                    if task and task.get("result") and not prior_job:
                        prior_job = task["request_id"]
                    task = {**(task or {}), "id": command.taskId, "group": command.groupId, "session": command.sessionId,
                            "leader": command.senderId, "skill": skill, "inputs": inputs, "state": state,
                            "start_digest": (task or {}).get("start_digest", fingerprint), "changed_at": now(), "error": error,
                            "command": command.model_dump(mode="json"), "request_id": request_id,
                            "phase": "pending", "verb": "continue" if verb == "continue" and old_state != "awaiting-input" else "start",
                            "product_id": new_id("product"), "result": None, "unsettled": False,
                            "deadline": time.time() + cap.timeout_seconds, "cancel_requested": False}
                    task["job_id"] = prior_job
                    # For actual business awaiting-input, continue must reach that same job.
                    if verb == "continue" and task.get("job_id"):
                        task["verb"] = "continue"
                    self.store.put("tasks", task["id"], task)
                    self.emit(command, task)
                    if state == "accepted":
                        launch = task["id"]
                else:
                    self.emit(command, task, "COMMAND_UNSUPPORTED")
        if launch:
            self.launch(launch)

    def launch(self, task_id):
        if not self.closed and (task_id not in self.jobs or self.jobs[task_id].done()):
            self.jobs[task_id] = asyncio.create_task(self.run(task_id), name=f"business-{task_id}")

    def context(self, task):
        return ExecutionContext(request_id=task["request_id"], task_id=task["id"], session_id=task["session"],
                                group_id=task["group"], leader_aic=task["leader"], partner_aic=self.settings.aic, capability_id=task["skill"])

    def save_result(self, task, value: BusinessResult):
        cap = self.capabilities[task["skill"]]
        if len(dumps(value.model_dump()).encode()) > getattr(self.settings, "max_message_bytes", 262144) - 8192:
            value = BusinessResult(state="failed", error_code="BUSINESS_OUTPUT_TOO_LARGE", settled=value.settled)
        if value.state == "succeeded":
            validate(value.output, cap.output_schema)
        states = {"succeeded": "awaiting-completion", "working": "working", "awaiting-input": "awaiting-input", "failed": "failed", "canceled": "canceled"}
        task.update(state=states[value.state], phase="poll", changed_at=now(), result=value.model_dump(),
                    error=value.error_code, unsettled=not value.settled, prompt=value.text,
                    job_id=value.job_id or task.get("job_id"))
        if not value.settled:
            task["state"] = "working"
        with self.store.transaction():
            self.store.put("tasks", task["id"], task)
            self.emit(TaskCommand.model_validate(task["command"]), task)

    async def run(self, task_id):
        while not self.closed:
            task = self.store.get("tasks", task_id)
            cap = self.capabilities[task["skill"]]
            connector = self.connectors[cap.connector]
            ctx = self.context(task)
            try:
                if task.get("cancel_requested"):
                    value = await asyncio.wait_for(connector.cancel(ctx, task.get("job_id")), self.settings.io_timeout)
                    task["cancel_requested"] = False
                    if value.state != "canceled" or not value.settled:
                        raise UnsupportedOperation("BUSINESS_CANCEL_NOT_CONFIRMED")
                elif task["state"] not in BUSY:
                    return
                elif time.time() >= task["deadline"]:
                    if cap.side_effects or task.get("unsettled"):
                        task.update(error="BUSINESS_OUTCOME_UNCONFIRMED", unsettled=True, phase="reconcile", changed_at=now())
                        with self.store.transaction():
                            self.store.put("tasks", task_id, task)
                            self.emit(TaskCommand.model_validate(task["command"]), task)
                        return
                    self.save_result(task, BusinessResult(state="failed", error_code="BUSINESS_DEADLINE_EXCEEDED"))
                    return
                else:
                    operation = task["phase"]
                    if operation == "pending":
                        task.update(phase="dispatching", state="working", changed_at=now())
                        with self.store.transaction():
                            self.store.put("tasks", task_id, task)
                            self.emit(TaskCommand.model_validate(task["command"]), task)
                        call = connector.resume(ctx, task.get("job_id"), task["inputs"]) if task["verb"] == "continue" else connector.execute(ctx, task["inputs"])
                    else:
                        call = connector.poll(ctx, task.get("job_id"))
                    value = await asyncio.wait_for(call, min(self.settings.io_timeout, max(0.01, task["deadline"] - time.time())))
                # Reload cancel intent which may have arrived while awaiting business IO.
                task["cancel_requested"] = self.store.get("tasks", task_id).get("cancel_requested", False)
                if value.state == "canceled":
                    task["cancel_requested"] = False
                self.save_result(task, value)
                if value.state != "working" and not task.get("cancel_requested"):
                    return
            except asyncio.CancelledError:
                raise  # Shutdown is not business cancellation; durable state is retained.
            except UnsupportedOperation as exc:
                was_cancel = task.get("cancel_requested", False)
                task.update(cancel_requested=False, error=str(exc), changed_at=now())
                if not was_cancel:
                    task.update(phase="reconcile", unsettled=True)
                with self.store.transaction():
                    self.store.put("tasks", task_id, task)
                    self.emit(TaskCommand.model_validate(task["command"]), task)
                return
            except Exception as exc:
                # Never retry a possibly accepted POST. Poll by stable request ID instead.
                task.update(phase="reconcile", error=type(exc).__name__, unsettled=cap.side_effects, changed_at=now())
                with self.store.transaction():
                    self.store.put("tasks", task_id, task)
                    self.emit(TaskCommand.model_validate(task["command"]), task)
            await asyncio.sleep(cap.poll_seconds)

    def recover(self, group_id=None):
        for task in self.store.all("tasks"):
            if group_id and task["group"] != group_id:
                continue
            if (task["state"] in BUSY or task.get("cancel_requested")) and task.get("error") not in {"BUSINESS_OUTCOME_UNCONFIRMED", "LEGACY_API_RECONCILIATION_REQUIRES_REVIEWED_PLUGIN", "ECHO_HAS_NO_REMOTE_JOB"}:
                self.launch(task["id"])

    def group_has_work(self, group_id):
        return any(t["group"] == group_id and (t["state"] not in TERMINAL or t.get("unsettled")) for t in self.store.all("tasks"))

    async def close(self):
        self.closed = True
        for job in self.jobs.values():
            job.cancel()
        await asyncio.gather(*self.jobs.values(), return_exceptions=True)
        for connector in self.connectors.values():
            if client := getattr(connector, "client", None):
                await client.aclose()
