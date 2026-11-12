"""SQLite connection management and schema.

The reader thread (collector) and the Flask request threads open separate
connections. WAL mode lets the writer commit while readers see the last
fully committed snapshot -- progress and rows become visible together.
"""
from __future__ import annotations

import os
import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS ingest_progress (
    dev            INTEGER NOT NULL,
    ino            INTEGER NOT NULL,
    path           TEXT    NOT NULL,
    committed_off  INTEGER NOT NULL DEFAULT 0,
    line_no        INTEGER NOT NULL DEFAULT 0,
    pending_bytes  INTEGER NOT NULL DEFAULT 0,
    skipping       INTEGER NOT NULL DEFAULT 0,
    status         TEXT    NOT NULL DEFAULT 'active',
    first_seen     TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at     TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (dev, ino)
);

CREATE TABLE IF NOT EXISTS readings (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    waybill     TEXT    NOT NULL,
    ts_utc      TEXT    NOT NULL,
    temperature REAL    NOT NULL,
    source_dev  INTEGER NOT NULL,
    source_ino  INTEGER NOT NULL,
    source_path TEXT    NOT NULL,
    line_no     INTEGER NOT NULL,
    raw_line    TEXT
);

CREATE INDEX IF NOT EXISTS idx_readings_waybill
    ON readings (waybill, ts_utc, id);

CREATE TABLE IF NOT EXISTS ingest_errors (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source_dev  INTEGER NOT NULL,
    source_ino  INTEGER NOT NULL,
    source_path TEXT    NOT NULL,
    line_no     INTEGER NOT NULL,
    byte_offset INTEGER NOT NULL,
    reason      TEXT    NOT NULL,
    raw_line    TEXT,
    created_at  TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_errors_file
    ON ingest_errors (source_dev, source_ino, line_no);

-- A sample point is a distinct (waybill, ts_utc) pair: several raw rows that
-- share the same UTC timestamp count as one point while every raw reading is
-- still kept separately above. The view makes that definition explicit without
-- any extra write path to keep consistent.
CREATE VIEW IF NOT EXISTS samples AS
    SELECT DISTINCT waybill, ts_utc FROM readings;

-- Adjacent sample points (per waybill) more than GAP_THRESHOLD_SECONDS apart.
-- Identity is the full (waybill, start_ts, end_ts) tuple, so a split gap gets
-- brand-new rows while unchanged gaps keep both their row and their mark.
CREATE TABLE IF NOT EXISTS gaps (
    waybill    TEXT    NOT NULL,
    start_ts   TEXT    NOT NULL,
    end_ts     TEXT    NOT NULL,
    duration_s REAL    NOT NULL,
    PRIMARY KEY (waybill, start_ts, end_ts)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_gaps_waybill
    ON gaps (waybill, start_ts);

-- Review marks. Rows are deliberately retained even when their gap is later
-- split or eliminated (audit trail); they simply stop joining to `gaps`,
-- which is exactly the "old mark becomes invalid" behaviour.
CREATE TABLE IF NOT EXISTS gap_marks (
    waybill   TEXT NOT NULL,
    start_ts  TEXT NOT NULL,
    end_ts    TEXT NOT NULL,
    note      TEXT,
    marked_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (waybill, start_ts, end_ts)
) WITHOUT ROWID;

-- Single-row state machine for the one-off historical backfill:
-- pending -> running -> ready. Survives restarts; a crash mid-run simply makes
-- the next process resume (rebuilds are idempotent).
CREATE TABLE IF NOT EXISTS gap_jobs (
    id              INTEGER PRIMARY KEY CHECK (id = 1),
    status          TEXT    NOT NULL DEFAULT 'pending',
    total_waybills  INTEGER NOT NULL DEFAULT 0,
    done_waybills   INTEGER NOT NULL DEFAULT 0,
    current_waybill TEXT,
    started_at      TEXT,
    finished_at     TEXT,
    updated_at      TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
"""


def connect(db_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def init_db(db_path: str) -> None:
    conn = connect(db_path)
    try:
        conn.executescript(SCHEMA)
        conn.commit()
    finally:
        conn.close()
