"""Missing-sample ("gap") detection, incremental maintenance and backfill.

Vocabulary
----------
* **sample point**: a distinct ``(waybill, ts_utc)``. Several raw ``readings``
  rows arriving for the same waybill at the same UTC instant count as one
  point; the raw rows themselves are never merged or deleted.
* **gap**: two chronologically adjacent sample points of one waybill whose
  distance is strictly greater than the configured threshold (5 minutes).

Correctness model
-----------------
All gap mutations run inside the caller's SQLite transaction, so a tick's
ingested readings, its gap adjustments and any mark changes become visible
atomically together.

* :func:`rebuild_waybill_gaps` computes the full gap set of one waybill from
  the ``samples`` view, deleting rows first. Marks live in their own table keyed
  by the exact gap endpoints: surviving gaps keep their marks, split/removed
  gaps leave stale marks that no longer join to any gap ("invalidated").
* :func:`refresh_gaps_for_waybill` is the incremental path used after each
  ingest. It only touches gaps that overlap the inserted time window and
  recomputes that window from scratch, so the amount of work is proportional
  to the affected neighbourhood rather than the waybill history.
* :class:`GapBackfill` performs the first historical build in a background
  thread while the collector keeps ingesting. Gap queries are refused until the
  job reaches ``ready``, so the UI never serves a partial result set.
"""
from __future__ import annotations

import logging
import threading
from datetime import datetime
from typing import Any

from .db import connect

log = logging.getLogger("cold-chain.gaps")

JOB_PENDING = "pending"
JOB_RUNNING = "running"
JOB_READY = "ready"


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _insert_gaps_from_points(
    conn, waybill: str, points: list[str], threshold_s: float
) -> int:
    """Insert gaps between consecutive distinct sample timestamps."""
    rows = []
    prev = None
    for ts in points:  # points are already DISTINCT + ORDER BY ts_utc
        if prev is not None:
            duration = (_parse_ts(ts) - _parse_ts(prev)).total_seconds()
            if duration > threshold_s:
                rows.append((waybill, prev, ts, duration))
        prev = ts
    if rows:
        # OR IGNORE: a framing gap that did not overlap the window (e.g. an
        # insert that adds no new sample point because the timestamp already
        # exists) is still present with the same primary key; ignoring it is
        # correct, since identical endpoints mean an identical gap.
        conn.executemany(
            "INSERT OR IGNORE INTO gaps (waybill, start_ts, end_ts, duration_s) "
            "VALUES (?, ?, ?, ?)",
            rows,
        )
    return len(rows)


def rebuild_waybill_gaps(conn, waybill: str, threshold_s: float) -> int:
    """Recompute every gap of one waybill from the sample points.

    Marks are intentionally not touched.
    """
    conn.execute("DELETE FROM gaps WHERE waybill = ?", (waybill,))
    points = [
        r[0]
        for r in conn.execute(
            "SELECT DISTINCT ts_utc FROM readings WHERE waybill = ? "
            "ORDER BY ts_utc",
            (waybill,),
        )
    ]
    return _insert_gaps_from_points(conn, waybill, points, threshold_s)


def refresh_gaps_for_waybill(
    conn, waybill: str, min_ts: str, max_ts: str, threshold_s: float
) -> None:
    """Incrementally adjust gaps after new points were inserted in one waybill.

    Only gaps whose open interval ``(start_ts, end_ts)`` contains an inserted
    timestamp, plus the gap that ends at or starts on the window borders, are
    affected. The affected neighbourhood is then rebuilt from the points
    between its boundary points.
    """
    # A gap is affected when its open interval overlaps the inserted window
    # [min_ts, max_ts]: start_ts < max_ts AND end_ts > min_ts. Gaps that only
    # share an endpoint cannot change (an inserted point equal to an existing
    # endpoint is a same-instant duplicate, not a new sample point).
    conn.execute(
        "DELETE FROM gaps WHERE waybill = ? AND start_ts < ? AND end_ts > ?",
        (waybill, max_ts, min_ts),
    )

    # Recompute the neighbourhood between the sample points immediately
    # outside the inserted window. The boundary points themselves frame the
    # window (strict inequalities), so adjacency across it is re-established
    # even when no point exists on one side.
    lower = conn.execute(
        "SELECT MAX(ts_utc) FROM (SELECT DISTINCT ts_utc FROM readings "
        "WHERE waybill = ? AND ts_utc < ?)",
        (waybill, min_ts),
    ).fetchone()[0]
    upper = conn.execute(
        "SELECT MIN(ts_utc) FROM (SELECT DISTINCT ts_utc FROM readings "
        "WHERE waybill = ? AND ts_utc > ?)",
        (waybill, max_ts),
    ).fetchone()[0]
    # Fall back to the window edges themselves when no outer point exists
    # (insertion before the first point or after the last one).
    lo = lower if lower is not None else min_ts
    hi = upper if upper is not None else max_ts
    if lo == hi:
        return

    points = [
        r[0]
        for r in conn.execute(
            "SELECT DISTINCT ts_utc FROM readings WHERE waybill = ? "
            "AND ts_utc BETWEEN ? AND ? ORDER BY ts_utc",
            (waybill, lo, hi),
        )
    ]
    _insert_gaps_from_points(conn, waybill, points, threshold_s)


# --------------------------------------------------------------------- job state
def ensure_job_row(db_path: str) -> None:
    """Create the single backfill job row on first boot.

    The row always starts ``pending`` so gap queries stay blocked until a
    build has actually run. Whether a worker starts immediately is decided by
    :class:`GapBackfill` callers.
    """
    conn = connect(db_path)
    try:
        conn.execute(
            "INSERT OR IGNORE INTO gap_jobs (id, status) VALUES (1, ?)",
            (JOB_PENDING,),
        )
        conn.commit()
    finally:
        conn.close()


def get_job_status(db_path: str) -> dict[str, Any]:
    conn = connect(db_path)
    try:
        row = conn.execute("SELECT * FROM gap_jobs WHERE id = 1").fetchone()
        if row is None:  # defensive: row is normally created at boot
            return {
                "status": JOB_PENDING,
                "total_waybills": 0,
                "done_waybills": 0,
                "current_waybill": None,
            }
        return dict(row)
    finally:
        conn.close()


def mark_job_ready(db_path: str) -> None:
    """Mark the job ready without a build (used by tests / empty histories)."""
    conn = connect(db_path)
    try:
        total = conn.execute(
            "SELECT COUNT(DISTINCT waybill) FROM readings"
        ).fetchone()[0]
        conn.execute(
            "UPDATE gap_jobs SET status = ?, total_waybills = ?, "
            "done_waybills = ?, current_waybill = NULL, "
            "finished_at = COALESCE(finished_at, "
            "strftime('%Y-%m-%dT%H:%M:%fZ', 'now')), updated_at = "
            "strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = 1",
            (JOB_READY, total, total),
        )
        conn.commit()
    finally:
        conn.close()


class GapBackfill:
    """Background thread that builds the historical gap set once.

    The collector thread keeps ingesting while this runs. Before flipping the
    job to ``ready`` the worker re-rebuilds every waybill that received rows
    after its first pass (tracked via reading-id watermarks), so rows ingested
    during the build are guaranteed to be included in the switch.
    """

    def __init__(self, config) -> None:
        self.config = config
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self.running = False
        self.last_error: str | None = None

    # -------------------------------------------------------------- lifecycle
    def start(self) -> None:
        with self._lock:
            if self.running:
                return
            status = get_job_status(self.config.db_path)["status"]
            if status == JOB_READY:
                return  # nothing to do (e.g. previous process already finished)
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._run, name="gap-backfill", daemon=True
            )
            self.running = True
            self._thread.start()

    def stop(self, timeout: float = 10.0) -> None:
        with self._lock:
            if not self.running:
                self._thread = None
                return
            self._stop.set()
            thread = self._thread
        if thread is not None:
            thread.join(timeout=timeout)
        with self._lock:
            self.running = False
            self._thread = None

    def run_once(self) -> None:
        """Synchronous entry point used by tests."""
        self._run()

    # ------------------------------------------------------------------ work
    def _claim_job(self, conn) -> bool:
        cur = conn.execute(
            "UPDATE gap_jobs SET status = ?, started_at = COALESCE(started_at, "
            "strftime('%Y-%m-%dT%H:%M:%fZ', 'now')), updated_at = "
            "strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = 1 AND status != ?",
            (JOB_RUNNING, JOB_READY),
        )
        return cur.rowcount > 0

    def _run(self) -> None:
        cfg = self.config
        conn = connect(cfg.db_path)
        try:
            if not self._claim_job(conn):
                return
            conn.commit()

            waybills = [
                r[0]
                for r in conn.execute(
                    "SELECT DISTINCT waybill FROM readings ORDER BY waybill"
                )
            ]
            # Watermark: reading ids at the start of the build. Waybills whose
            # max id later exceeds their pass-time max need a final rebuild.
            start_max_id = conn.execute(
                "SELECT COALESCE(MAX(id), 0) FROM readings"
            ).fetchone()[0]
            conn.execute(
                "UPDATE gap_jobs SET total_waybills = ?, done_waybills = 0, "
                "current_waybill = NULL, updated_at = "
                "strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = 1",
                (len(waybills),),
            )
            conn.commit()

            done = 0
            pass_max_ids: dict[str, int] = {}
            for waybill in waybills:
                if self._stop.is_set():
                    self._reset_to_pending(conn)
                    log.info("gap backfill stopped before completion")
                    return
                rebuild_waybill_gaps(conn, waybill, cfg.gap_threshold_seconds)
                pass_max_ids[waybill] = conn.execute(
                    "SELECT COALESCE(MAX(id), 0) FROM readings WHERE waybill = ?",
                    (waybill,),
                ).fetchone()[0]
                done += 1
                conn.execute(
                    "UPDATE gap_jobs SET done_waybills = ?, current_waybill = ?, "
                    "updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') "
                    "WHERE id = 1",
                    (done, waybill),
                )
                conn.commit()

            # Catch-up phase: close the window in which the collector may have
            # ingested concurrently. Loop until the watermark stops moving so a
            # waybill that is still receiving data cannot be missed.
            for _ in range(10_000):
                if self._stop.is_set():
                    self._reset_to_pending(conn)
                    return
                laggards = [
                    wb
                    for wb, max_id in pass_max_ids.items()
                    if conn.execute(
                        "SELECT COALESCE(MAX(id), 0) FROM readings WHERE waybill = ?",
                        (wb,),
                    ).fetchone()[0]
                        > max_id
                ]
                new_waybills = [
                    r[0]
                    for r in conn.execute(
                        "SELECT DISTINCT waybill FROM readings WHERE id > ? "
                        "ORDER BY waybill",
                        (start_max_id,),
                    )
                    if r[0] not in pass_max_ids
                ]
                if not laggards and not new_waybills:
                    break
                for waybill in laggards + new_waybills:
                    rebuild_waybill_gaps(conn, waybill, cfg.gap_threshold_seconds)
                    pass_max_ids[waybill] = conn.execute(
                        "SELECT COALESCE(MAX(id), 0) FROM readings WHERE waybill = ?",
                        (waybill,),
                    ).fetchone()[0]
                    conn.commit()

            self._finalize_ready(conn, pass_max_ids, start_max_id, cfg)
        except Exception as exc:  # keep the process alive; surface via API
            self.last_error = f"{type(exc).__name__}: {exc}"
            log.exception("gap backfill failed")
            try:
                conn.rollback()
                self._reset_to_pending(conn)
            except Exception:
                pass
        finally:
            conn.close()
            with self._lock:
                self.running = False
                self._thread = None

    def _reset_to_pending(self, conn) -> None:
        """Return the job to pending so a later start can resume cleanly."""
        conn.execute(
            "UPDATE gap_jobs SET status = ?, current_waybill = NULL, "
            "updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = 1",
            (JOB_PENDING,),
        )
        conn.commit()

    def _finalize_ready(self, conn, pass_max_ids, start_max_id, cfg) -> None:
        """Flip the job to ready with a write lock that blocks concurrent ticks.

        ``BEGIN IMMEDIATE`` acquires SQLite's reserved write lock before the
        check, so no collector transaction can commit between the watermark
        comparison and the status flip. Any rows already committed (including
        those landing while we wait for the lock) are reconciled here, which is
        what makes the switch include every ingested reading.
        """
        conn.commit()  # release any open read transaction first
        conn.execute("BEGIN IMMEDIATE")
        try:
            laggards = [
                wb
                for wb, max_id in pass_max_ids.items()
                if conn.execute(
                    "SELECT COALESCE(MAX(id), 0) FROM readings WHERE waybill = ?",
                    (wb,),
                ).fetchone()[0]
                    > max_id
            ]
            new_waybills = [
                r[0]
                for r in conn.execute(
                    "SELECT DISTINCT waybill FROM readings WHERE id > ?",
                    (start_max_id,),
                )
                if r[0] not in pass_max_ids
            ]
            for waybill in laggards + new_waybills:
                rebuild_waybill_gaps(conn, waybill, cfg.gap_threshold_seconds)
            total = conn.execute(
                "SELECT COUNT(DISTINCT waybill) FROM readings"
            ).fetchone()[0]
            conn.execute(
                "UPDATE gap_jobs SET status = ?, total_waybills = ?, "
                "done_waybills = ?, current_waybill = NULL, finished_at = "
                "strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), updated_at = "
                "strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = 1",
                (JOB_READY, total, total),
            )
            conn.commit()
            log.info(
                "gap backfill ready: %d waybills, %d gaps",
                total,
                conn.execute("SELECT COUNT(*) FROM gaps").fetchone()[0],
            )
        except Exception:
            conn.rollback()
            raise
