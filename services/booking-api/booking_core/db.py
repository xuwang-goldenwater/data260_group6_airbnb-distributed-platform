"""Minimal thread-safe PyMySQL connection pool with explicit transactions."""
from __future__ import annotations

import contextlib
import queue
import threading
from typing import Iterator

import pymysql
import pymysql.cursors

from .config import Settings
from .errors import DependencyUnavailable

# Deadlock, lock-wait timeout, can't connect, server gone away, lost connection, lost during query
TRANSIENT_MYSQL_CODES = {1205, 1213, 2003, 2006, 2013, 2055}
DUPLICATE_KEY = 1062


def is_transient(exc: BaseException) -> bool:
    if isinstance(exc, DependencyUnavailable):
        return True
    if isinstance(exc, pymysql.err.InterfaceError):
        return True
    if isinstance(exc, pymysql.err.OperationalError):
        return bool(exc.args) and exc.args[0] in TRANSIENT_MYSQL_CODES
    return False


def retry_transaction(fn, *, attempts_backoff=(0.05, 0.2, 1.0), sleep=None):
    """Re-run a whole transaction on deadlock / lock-wait timeout (InnoDB rolled it back). 3 retries."""
    import time as _time
    sleep = sleep or _time.sleep
    for i in range(len(attempts_backoff) + 1):
        try:
            return fn()
        except pymysql.err.OperationalError as exc:
            if exc.args and exc.args[0] in (1205, 1213) and i < len(attempts_backoff):
                sleep(attempts_backoff[i])
                continue
            raise


def is_duplicate_key(exc: BaseException) -> bool:
    return isinstance(exc, pymysql.err.IntegrityError) and bool(exc.args) and exc.args[0] == DUPLICATE_KEY


class Database:
    def __init__(self, settings: Settings, *, database: str | None = None, pool_size: int | None = None):
        self._settings = settings
        self._database = database or settings.mysql_database
        self._max = pool_size or settings.mysql_pool_size
        self._idle: queue.LifoQueue = queue.LifoQueue()
        self._created = 0
        self._lock = threading.Lock()

    def _connect(self) -> pymysql.connections.Connection:
        s = self._settings
        try:
            return pymysql.connect(
                host=s.mysql_host, port=s.mysql_port, user=s.mysql_user, password=s.mysql_password,
                database=self._database, autocommit=False, charset="utf8mb4",
                cursorclass=pymysql.cursors.DictCursor, connect_timeout=5, read_timeout=60,
                write_timeout=60, init_command="SET time_zone = '+00:00'",
            )
        except pymysql.err.MySQLError as exc:
            raise DependencyUnavailable("mysql", exc) from exc

    def _acquire(self) -> pymysql.connections.Connection:
        try:
            conn = self._idle.get_nowait()
        except queue.Empty:
            with self._lock:
                can_create = self._created < self._max
                if can_create:
                    self._created += 1
            if can_create:
                try:
                    return self._connect()
                except Exception:
                    with self._lock:
                        self._created -= 1
                    raise
            try:
                conn = self._idle.get(timeout=10)
            except queue.Empty as exc:
                raise DependencyUnavailable("mysql", RuntimeError("connection pool exhausted")) from exc
        try:
            conn.ping(reconnect=False)
        except pymysql.err.MySQLError:
            self._discard(conn)              # stale pooled connection: replace it with a fresh one
            with self._lock:
                self._created += 1
            try:
                return self._connect()
            except Exception:
                with self._lock:
                    self._created -= 1
                raise
        return conn

    def _discard(self, conn) -> None:
        with contextlib.suppress(Exception):
            conn.close()
        with self._lock:
            self._created -= 1

    @contextlib.contextmanager
    def transaction(self) -> Iterator[pymysql.connections.Connection]:
        """BEGIN … COMMIT; rolls back on any exception. Reads also run inside a (short) transaction."""
        conn = self._acquire()
        healthy = True
        try:
            conn.begin()
            yield conn
            conn.commit()
        except BaseException:
            try:
                conn.rollback()
            except Exception:
                healthy = False
            raise
        finally:
            if healthy and conn.open:
                self._idle.put(conn)
            else:
                self._discard(conn)

    def ping(self) -> bool:
        with self.transaction() as conn, conn.cursor() as cur:
            cur.execute("SELECT 1 AS ok")
            return cur.fetchone()["ok"] == 1

    def close(self) -> None:
        while True:
            try:
                conn = self._idle.get_nowait()
            except queue.Empty:
                return
            self._discard(conn)
