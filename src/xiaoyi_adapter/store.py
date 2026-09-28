"""One SQLite database per identity. State, receipts and outbox share transactions."""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path

from .models import dumps, now


class Store:
    def __init__(self, file):
        Path(file).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(file, timeout=10, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
          CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY,value TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS groups (id TEXT PRIMARY KEY,body TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS tasks (id TEXT PRIMARY KEY,body TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS receipts (id TEXT PRIMARY KEY,digest TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS invitations (digest TEXT PRIMARY KEY,group_id TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS outbox (
            seq INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT UNIQUE NOT NULL,
            group_id TEXT NOT NULL,body TEXT NOT NULL,confirmed INTEGER NOT NULL DEFAULT 0);
          CREATE TABLE IF NOT EXISTS audit (
            seq INTEGER PRIMARY KEY AUTOINCREMENT,at TEXT NOT NULL,
            event TEXT NOT NULL,target TEXT NOT NULL,body TEXT NOT NULL);
        """)
        version = self.db.execute("SELECT value FROM metadata WHERE key='schema_version'").fetchone()
        if version and version[0] != "1":
            raise ValueError("Unsupported database schema; migration required")
        self.db.execute("INSERT OR IGNORE INTO metadata VALUES ('schema_version','1')")

    def bind_identity(self, aic):
        row = self.db.execute("SELECT value FROM metadata WHERE key='aic'").fetchone()
        if row and row[0] != aic:
            raise ValueError("STATE_DATABASE_IDENTITY_MISMATCH")
        self.db.execute("INSERT OR IGNORE INTO metadata VALUES ('aic',?)", (aic,))

    @contextmanager
    def transaction(self):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
        except BaseException:
            self.db.execute("ROLLBACK")
            raise
        else:
            self.db.execute("COMMIT")

    def get(self, table, key):
        assert table in {"tasks", "groups"}
        row = self.db.execute(f"SELECT body FROM {table} WHERE id=?", (key,)).fetchone()
        return json.loads(row[0]) if row else None

    def put(self, table, key, body):
        assert table in {"tasks", "groups"}
        self.db.execute(f"INSERT INTO {table} VALUES (?,?) ON CONFLICT(id) DO UPDATE SET body=excluded.body", (key, dumps(body)))

    def all(self, table):
        assert table in {"tasks", "groups"}
        return [json.loads(row[0]) for row in self.db.execute(f"SELECT body FROM {table}")]

    def receipt(self, message_id, fingerprint):
        row = self.db.execute("SELECT digest FROM receipts WHERE id=?", (message_id,)).fetchone()
        if row:
            return "duplicate" if row[0] == fingerprint else "conflict"
        self.db.execute("INSERT INTO receipts VALUES (?,?)", (message_id, fingerprint))
        return "new"

    def enqueue(self, group_id, body):
        self.db.execute("INSERT OR IGNORE INTO outbox(id,group_id,body) VALUES (?,?,?)", (body["id"], group_id, dumps(body)))

    def pending(self, group_id=None):
        sql = "SELECT id,group_id,body FROM outbox WHERE confirmed=0"
        args = ()
        if group_id:
            sql += " AND group_id=?"
            args = (group_id,)
        return [(r[0], r[1], json.loads(r[2])) for r in self.db.execute(sql + " ORDER BY seq", args)]

    def confirm(self, message_id):
        self.db.execute("UPDATE outbox SET confirmed=1 WHERE id=?", (message_id,))

    def audit(self, event, target, data=None):
        self.db.execute("INSERT INTO audit(at,event,target,body) VALUES (?,?,?,?)", (now(), event, target, dumps(data or {})))
