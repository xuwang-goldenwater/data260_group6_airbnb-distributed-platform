"""Persistence for the AI service: tasks, debate state, events and results, all keyed by trace_id.

MongoStore is used in the running system. MemoryStore has the same interface and is used by the tests.
The Chair writes here BEFORE its Kafka offset is committed, so a crash never loses a processed message
(at-least-once), and the saved `seen` idempotency keys stop a redelivered message from counting twice.
"""
from __future__ import annotations

import copy
from typing import Any, Protocol


class Store(Protocol):
    def create_task(self, task: dict[str, Any]) -> None: ...
    def get_task(self, task_id: str) -> dict[str, Any] | None: ...
    def save_debate(self, debate: dict[str, Any]) -> None: ...
    def get_debate(self, trace_id: str) -> dict[str, Any] | None: ...
    def open_debates(self) -> list[dict[str, Any]]: ...
    def append_event(self, trace_id: str, event: dict[str, Any]) -> None: ...
    def events(self, trace_id: str) -> list[dict[str, Any]]: ...
    def save_result(self, result: dict[str, Any]) -> None: ...
    def get_result(self, trace_id: str) -> dict[str, Any] | None: ...
    def insert_approval(self, approval: dict[str, Any]) -> bool: ...
    def get_approval(self, trace_id: str) -> dict[str, Any] | None: ...
    def mark_approval_published(self, trace_id: str) -> None: ...
    def set_result_decision(self, trace_id: str, status: str, decision: dict[str, Any]) -> None: ...


class MemoryStore:
    def __init__(self) -> None:
        self.tasks: dict[str, dict] = {}
        self.debates: dict[str, dict] = {}
        self._events: dict[str, list] = {}
        self.results: dict[str, dict] = {}
        self.approvals: dict[str, dict] = {}

    def create_task(self, task):
        self.tasks[task["task_id"]] = copy.deepcopy(task)

    def get_task(self, task_id):
        return copy.deepcopy(self.tasks.get(task_id))

    def save_debate(self, debate):
        self.debates[debate["trace_id"]] = copy.deepcopy(debate)

    def get_debate(self, trace_id):
        return copy.deepcopy(self.debates.get(trace_id))

    def open_debates(self):
        return [copy.deepcopy(d) for d in self.debates.values() if d["phase"] != "done" or d.get("outbox")]

    def append_event(self, trace_id, event):
        self._events.setdefault(trace_id, []).append(copy.deepcopy(event))

    def events(self, trace_id):
        return copy.deepcopy(self._events.get(trace_id, []))

    def save_result(self, result):
        self.results[result["trace_id"]] = copy.deepcopy(result)

    def get_result(self, trace_id):
        return copy.deepcopy(self.results.get(trace_id))

    def insert_approval(self, approval):
        """Insert-once: False if this trace already has a decision (the API turns that into 409)."""
        if approval["trace_id"] in self.approvals:
            return False
        self.approvals[approval["trace_id"]] = copy.deepcopy(approval)
        return True

    def get_approval(self, trace_id):
        return copy.deepcopy(self.approvals.get(trace_id))

    def mark_approval_published(self, trace_id):
        self.approvals[trace_id]["published"] = True

    def set_result_decision(self, trace_id, status, decision):
        self.results[trace_id]["status"] = status
        self.results[trace_id]["human_decision"] = copy.deepcopy(decision)


class MongoStore:
    """Collections: ai_tasks, ai_debates, ai_events, ai_results, ai_approvals (database from MONGO_DB)."""

    def __init__(self, uri: str, db_name: str) -> None:
        from pymongo import ASCENDING, MongoClient
        self._db = MongoClient(uri, serverSelectionTimeoutMS=3000)[db_name]
        self._db.ai_tasks.create_index("task_id", unique=True)
        self._db.ai_debates.create_index("trace_id", unique=True)
        self._db.ai_debates.create_index("phase")
        self._db.ai_events.create_index([("trace_id", ASCENDING), ("timestamp", ASCENDING)])
        self._db.ai_results.create_index("trace_id", unique=True)
        # the unique index is what makes "one decision per result" hold even with two API replicas
        self._db.ai_approvals.create_index("trace_id", unique=True)

    @staticmethod
    def _clean(doc):
        if doc is not None:
            doc.pop("_id", None)
        return doc

    def create_task(self, task):
        self._db.ai_tasks.insert_one(dict(task))

    def get_task(self, task_id):
        return self._clean(self._db.ai_tasks.find_one({"task_id": task_id}))

    def save_debate(self, debate):
        self._db.ai_debates.replace_one({"trace_id": debate["trace_id"]}, dict(debate), upsert=True)

    def get_debate(self, trace_id):
        return self._clean(self._db.ai_debates.find_one({"trace_id": trace_id}))

    def open_debates(self):
        query = {"$or": [{"phase": {"$ne": "done"}}, {"outbox.0": {"$exists": True}}]}
        return [self._clean(d) for d in self._db.ai_debates.find(query)]

    def append_event(self, trace_id, event):
        self._db.ai_events.insert_one({"trace_id": trace_id, "timestamp": event.get("timestamp"), "event": event})

    def events(self, trace_id):
        return [d["event"] for d in self._db.ai_events.find({"trace_id": trace_id}).sort("timestamp", 1)]

    def save_result(self, result):
        self._db.ai_results.replace_one({"trace_id": result["trace_id"]}, dict(result), upsert=True)

    def get_result(self, trace_id):
        return self._clean(self._db.ai_results.find_one({"trace_id": trace_id}))

    def insert_approval(self, approval):
        from pymongo.errors import DuplicateKeyError
        try:
            self._db.ai_approvals.insert_one(dict(approval))
            return True
        except DuplicateKeyError:
            return False

    def get_approval(self, trace_id):
        return self._clean(self._db.ai_approvals.find_one({"trace_id": trace_id}))

    def mark_approval_published(self, trace_id):
        self._db.ai_approvals.update_one({"trace_id": trace_id}, {"$set": {"published": True}})

    def set_result_decision(self, trace_id, status, decision):
        self._db.ai_results.update_one({"trace_id": trace_id},
                                       {"$set": {"status": status, "human_decision": decision}})
