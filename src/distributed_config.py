#!/usr/bin/env python3
"""
BlackRoad Distributed Config - etcd-inspired KV store
Implements MVCC versioning, leases, watches, transactions, and cluster info.
"""

import sqlite3
import threading
import time
import json
import argparse
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, List, Any, Callable, Tuple
from enum import Enum


@dataclass
class KVEntry:
    """Key-value entry with versioning"""
    key: str
    value: str
    version: int
    create_revision: int
    mod_revision: int
    lease_id: Optional[str]
    created_at: datetime
    modified_at: datetime


@dataclass
class Lease:
    """Lease with TTL"""
    id: str
    ttl_s: int
    granted_at: datetime
    expires_at: datetime
    keys: List[str] = field(default_factory=list)


@dataclass
class Watch:
    """Watch key prefix for changes"""
    id: str
    key_prefix: str
    last_revision: int
    callback_fn_name: str
    created_at: datetime


class EventType(str, Enum):
    """Watch event types"""
    PUT = "put"
    DELETE = "delete"


class DistributedConfig:
    """
    Distributed KV config store with MVCC, leases, watches, transactions.
    """

    def __init__(self, db_path: Optional[str] = None):
        """Initialize distributed config store"""
        config_dir = Path(db_path or "~/.blackroad").expanduser()
        config_dir.mkdir(parents=True, exist_ok=True)

        self.db_path = config_dir / "distributed_config.db"
        self.lock = threading.RLock()
        self.watches: Dict[str, Watch] = {}
        self.revision = 0
        self.compacted_rev = 0
        self.member_id = "local-node-1"

        self._init_db()
        self._start_lease_sweeper()

    def _init_db(self):
        """Initialize database schema"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS kv_entries (
                    key TEXT NOT NULL,
                    value TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    create_revision INTEGER NOT NULL,
                    mod_revision INTEGER NOT NULL,
                    lease_id TEXT,
                    created_at TEXT NOT NULL,
                    modified_at TEXT NOT NULL,
                    deleted INTEGER DEFAULT 0,
                    PRIMARY KEY (key, mod_revision)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS leases (
                    id TEXT PRIMARY KEY,
                    ttl_s INTEGER NOT NULL,
                    granted_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    keys TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS revisions (
                    revision INTEGER PRIMARY KEY,
                    timestamp TEXT NOT NULL,
                    operation TEXT NOT NULL,
                    key TEXT,
                    details TEXT
                )
            """)
            conn.commit()

    # ========================================================================
    # Core KV Operations
    # ========================================================================

    def put(self, key: str, value: str, lease_id: Optional[str] = None) -> int:
        """Put key-value and return new revision"""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                # Get current version
                row = conn.execute(
                    "SELECT version FROM kv_entries WHERE key = ? AND deleted = 0 ORDER BY mod_revision DESC LIMIT 1",
                    (key,)
                ).fetchone()
                version = (row[0] if row else 0) + 1

                # Increment revision
                self.revision += 1
                now = datetime.utcnow()

                # Insert new entry
                conn.execute(
                    """INSERT INTO kv_entries 
                       (key, value, version, create_revision, mod_revision, lease_id, created_at, modified_at, deleted)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)""",
                    (key, value, version, self.revision, self.revision, lease_id,
                     now.isoformat(), now.isoformat())
                )

                # Add to revision log
                conn.execute(
                    "INSERT INTO revisions (revision, timestamp, operation, key, details) VALUES (?, ?, ?, ?, ?)",
                    (self.revision, now.isoformat(), "put", key, json.dumps({"version": version}))
                )

                # Update lease if present
                if lease_id:
                    self._add_key_to_lease(lease_id, key)

                conn.commit()
                return self.revision

    def get(self, key: str, revision: Optional[int] = None) -> Optional[KVEntry]:
        """Get key at specific revision or latest"""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                if revision is None:
                    query = """
                        SELECT key, value, version, create_revision, mod_revision, lease_id, created_at, modified_at
                        FROM kv_entries
                        WHERE key = ? AND deleted = 0
                        ORDER BY mod_revision DESC LIMIT 1
                    """
                    row = conn.execute(query, (key,)).fetchone()
                else:
                    query = """
                        SELECT key, value, version, create_revision, mod_revision, lease_id, created_at, modified_at
                        FROM kv_entries
                        WHERE key = ? AND mod_revision <= ? AND deleted = 0
                        ORDER BY mod_revision DESC LIMIT 1
                    """
                    row = conn.execute(query, (key, revision)).fetchone()

            if not row:
                return None

            return KVEntry(
                key=row[0], value=row[1], version=row[2],
                create_revision=row[3], mod_revision=row[4], lease_id=row[5],
                created_at=datetime.fromisoformat(row[6]),
                modified_at=datetime.fromisoformat(row[7])
            )

    def delete(self, key: str, range_end: Optional[str] = None) -> int:
        """Delete key or range, return revision"""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                self.revision += 1
                now = datetime.utcnow()

                if range_end:
                    # Range delete
                    rows = conn.execute(
                        "SELECT key FROM kv_entries WHERE key >= ? AND key < ? AND deleted = 0",
                        (key, range_end)
                    ).fetchall()
                else:
                    rows = [(key,)]

                for row in rows:
                    k = row[0]
                    conn.execute(
                        "UPDATE kv_entries SET deleted = 1 WHERE key = ?",
                        (k,)
                    )
                    conn.execute(
                        "INSERT INTO revisions (revision, timestamp, operation, key) VALUES (?, ?, ?, ?)",
                        (self.revision, now.isoformat(), "delete", k)
                    )

                conn.commit()
                return self.revision

    def get_prefix(self, prefix: str) -> List[KVEntry]:
        """Get all keys with prefix"""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                rows = conn.execute(
                    """SELECT key, value, version, create_revision, mod_revision, lease_id, created_at, modified_at
                       FROM kv_entries
                       WHERE key LIKE ? AND deleted = 0
                       ORDER BY key""",
                    (f"{prefix}%",)
                ).fetchall()

            return [
                KVEntry(
                    key=r[0], value=r[1], version=r[2],
                    create_revision=r[3], mod_revision=r[4], lease_id=r[5],
                    created_at=datetime.fromisoformat(r[6]),
                    modified_at=datetime.fromisoformat(r[7])
                )
                for r in rows
            ]

    # ========================================================================
    # Watch Operations
    # ========================================================================

    def watch(self, key_prefix: str, callback: Callable[[str, str, str], None]) -> str:
        """Watch key prefix for changes"""
        import uuid
        watch_id = str(uuid.uuid4())[:8]

        watch = Watch(
            id=watch_id,
            key_prefix=key_prefix,
            last_revision=self.revision,
            callback_fn_name=callback.__name__,
            created_at=datetime.utcnow()
        )
        self.watches[watch_id] = watch

        # Start polling thread
        def poll_changes():
            while watch_id in self.watches:
                time.sleep(1)  # Poll every second
                with self.lock:
                    with sqlite3.connect(self.db_path) as conn:
                        rows = conn.execute(
                            """SELECT key, value, operation FROM revisions r
                               WHERE r.key LIKE ? AND r.revision > ?
                               ORDER BY r.revision""",
                            (f"{key_prefix}%", watch.last_revision)
                        ).fetchall()

                for row in rows:
                    event_type = row[2]  # "put" or "delete"
                    callback(event_type, row[0], row[1] if row[1] else "")

                if rows:
                    watch.last_revision = rows[-1][2] if rows else self.revision

        thread = threading.Thread(target=poll_changes, daemon=True)
        thread.start()
        return watch_id

    def unwatch(self, watch_id: str):
        """Stop watching"""
        self.watches.pop(watch_id, None)

    # ========================================================================
    # Lease Operations
    # ========================================================================

    def grant_lease(self, ttl_s: int) -> str:
        """Grant lease, return lease ID"""
        import uuid
        lease_id = f"lease-{uuid.uuid4().hex[:8]}"

        now = datetime.utcnow()
        expires_at = now + timedelta(seconds=ttl_s)

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO leases (id, ttl_s, granted_at, expires_at, keys) VALUES (?, ?, ?, ?, ?)",
                (lease_id, ttl_s, now.isoformat(), expires_at.isoformat(), json.dumps([]))
            )
            conn.commit()

        return lease_id

    def revoke_lease(self, lease_id: str):
        """Revoke lease and delete all attached keys"""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                row = conn.execute(
                    "SELECT keys FROM leases WHERE id = ?", (lease_id,)
                ).fetchone()

                if row:
                    keys = json.loads(row[0])
                    for key in keys:
                        self.delete(key)

                conn.execute("DELETE FROM leases WHERE id = ?", (lease_id,))
                conn.commit()

    def keepalive(self, lease_id: str) -> bool:
        """Extend lease TTL"""
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT ttl_s FROM leases WHERE id = ?", (lease_id,)
            ).fetchone()

        if not row:
            return False

        ttl_s = row[0]
        expires_at = (datetime.utcnow() + timedelta(seconds=ttl_s)).isoformat()

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "UPDATE leases SET expires_at = ? WHERE id = ?",
                (expires_at, lease_id)
            )
            conn.commit()

        return True

    def _add_key_to_lease(self, lease_id: str, key: str):
        """Add key to lease's key list"""
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT keys FROM leases WHERE id = ?", (lease_id,)
            ).fetchone()

            if row:
                keys = json.loads(row[0])
                if key not in keys:
                    keys.append(key)
                    conn.execute(
                        "UPDATE leases SET keys = ? WHERE id = ?",
                        (json.dumps(keys), lease_id)
                    )
                    conn.commit()

    # ========================================================================
    # Transaction Operations
    # ========================================================================

    def txn(self, compare: List[Tuple[str, str, str]], success_ops: List[Tuple[str, str, str]],
            failure_ops: Optional[List[Tuple[str, str, str]]] = None) -> Dict[str, Any]:
        """Compare-and-swap transaction"""
        with self.lock:
            # Evaluate compare conditions
            passed = True
            for key, op, value in compare:
                entry = self.get(key)
                if op == "==" and (entry is None or entry.value != value):
                    passed = False
                elif op == "!=" and entry is not None and entry.value == value:
                    passed = False

            # Execute appropriate operations
            ops = success_ops if passed else (failure_ops or [])
            results = []

            for op_type, key, value in ops:
                if op_type == "put":
                    rev = self.put(key, value)
                    results.append({"key": key, "revision": rev})
                elif op_type == "delete":
                    rev = self.delete(key)
                    results.append({"key": key, "revision": rev})

            return {"succeeded": passed, "results": results}

    # ========================================================================
    # Compaction & History
    # ========================================================================

    def compact(self, revision: int):
        """Remove all old revisions"""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                # Keep only latest version of each key
                conn.execute(
                    "DELETE FROM revisions WHERE revision <= ?",
                    (revision,)
                )
                conn.commit()
                self.compacted_rev = max(self.compacted_rev, revision)

    def list_revision_history(self, key: str, limit: int = 10) -> List[KVEntry]:
        """Get key change history"""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                rows = conn.execute(
                    """SELECT key, value, version, create_revision, mod_revision, lease_id, created_at, modified_at
                       FROM kv_entries
                       WHERE key = ?
                       ORDER BY mod_revision DESC
                       LIMIT ?""",
                    (key, limit)
                ).fetchall()

            return [
                KVEntry(
                    key=r[0], value=r[1], version=r[2],
                    create_revision=r[3], mod_revision=r[4], lease_id=r[5],
                    created_at=datetime.fromisoformat(r[6]),
                    modified_at=datetime.fromisoformat(r[7])
                )
                for r in rows
            ]

    # ========================================================================
    # Cluster Info
    # ========================================================================

    def get_cluster_info(self) -> Dict[str, Any]:
        """Get cluster/node info"""
        return {
            "member_id": self.member_id,
            "revision": self.revision,
            "compacted_revision": self.compacted_rev,
            "lease_count": self._count_leases(),
            "key_count": self._count_keys()
        }

    def _count_leases(self) -> int:
        """Count active leases"""
        now = datetime.utcnow().isoformat()
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM leases WHERE expires_at > ?", (now,)
            ).fetchone()
        return row[0] if row else 0

    def _count_keys(self) -> int:
        """Count live keys"""
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT COUNT(DISTINCT key) FROM kv_entries WHERE deleted = 0"
            ).fetchone()
        return row[0] if row else 0

    # ========================================================================
    # Background Operations
    # ========================================================================

    def _start_lease_sweeper(self):
        """Start background lease expiry sweeper"""
        def sweep_expired_leases():
            while True:
                time.sleep(5)  # Check every 5 seconds
                now = datetime.utcnow().isoformat()
                with sqlite3.connect(self.db_path) as conn:
                    rows = conn.execute(
                        "SELECT id FROM leases WHERE expires_at < ?", (now,)
                    ).fetchall()

                for row in rows:
                    self.revoke_lease(row[0])

        thread = threading.Thread(target=sweep_expired_leases, daemon=True)
        thread.start()


# ============================================================================
# CLI
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="BlackRoad Distributed Config CLI")
    subparsers = parser.add_subparsers(dest="command", help="Command")

    # Put command
    put_parser = subparsers.add_parser("put", help="Put key-value")
    put_parser.add_argument("key", help="Key")
    put_parser.add_argument("value", help="Value")
    put_parser.add_argument("--lease", help="Lease ID")

    # Get command
    get_parser = subparsers.add_parser("get", help="Get value")
    get_parser.add_argument("key", help="Key")
    get_parser.add_argument("--revision", type=int, help="Specific revision")

    # Delete command
    delete_parser = subparsers.add_parser("delete", help="Delete key")
    delete_parser.add_argument("key", help="Key")

    # Watch command
    watch_parser = subparsers.add_parser("watch", help="Watch key prefix")
    watch_parser.add_argument("prefix", help="Key prefix")
    watch_parser.add_argument("--timeout", type=int, default=30, help="Timeout in seconds")

    # Lease command
    lease_parser = subparsers.add_parser("lease", help="Manage leases")
    lease_parser.add_argument("action", choices=["grant", "revoke", "keepalive"])
    lease_parser.add_argument("--ttl", type=int, help="TTL for grant")
    lease_parser.add_argument("--id", help="Lease ID")

    # Info command
    info_parser = subparsers.add_parser("info", help="Get cluster info")

    args = parser.parse_args()

    config = DistributedConfig()

    if args.command == "put":
        rev = config.put(args.key, args.value, args.lease)
        print(f"Put: key={args.key}, revision={rev}")

    elif args.command == "get":
        entry = config.get(args.key, args.revision)
        if entry:
            print(f"Value: {entry.value}")
        else:
            print("Key not found")

    elif args.command == "delete":
        rev = config.delete(args.key)
        print(f"Deleted: {args.key}, revision={rev}")

    elif args.command == "watch":
        def callback(event_type, key, value):
            print(f"[{event_type}] {key} = {value}")

        watch_id = config.watch(args.prefix, callback)
        print(f"Watching {args.prefix} (id={watch_id})")
        time.sleep(args.timeout)
        config.unwatch(watch_id)

    elif args.command == "lease":
        if args.action == "grant":
            lease_id = config.grant_lease(args.ttl)
            print(f"Granted: {lease_id}")
        elif args.action == "keepalive":
            ok = config.keepalive(args.id)
            print(f"Keepalive: {ok}")
        elif args.action == "revoke":
            config.revoke_lease(args.id)
            print(f"Revoked: {args.id}")

    elif args.command == "info":
        info = config.get_cluster_info()
        print(json.dumps(info, indent=2))


if __name__ == "__main__":
    main()
