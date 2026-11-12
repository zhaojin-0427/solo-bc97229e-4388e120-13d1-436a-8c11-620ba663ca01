"""Tests for gap detection, incremental maintenance, marks and backfill.

Timestamps are fixed (not "now") so the 5-minute threshold math is exact.
Run with: python3 -m unittest discover -s tests -v
"""
from __future__ import annotations

import json
import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone

from app.collector import Collector
from app.db import connect, init_db
from app.gaps import (
    JOB_PENDING,
    JOB_READY,
    JOB_RUNNING,
    GapBackfill,
    ensure_job_row,
    get_job_status,
    mark_job_ready,
    rebuild_waybill_gaps,
)
from app.web import create_app

from tests.test_collector import make_config

BASE = datetime(2026, 11, 12, 10, 0, 0, tzinfo=timezone.utc)


def ts(minutes: float = 0, seconds: float = 0) -> str:
    dt = BASE + timedelta(minutes=minutes, seconds=seconds)
    return dt.isoformat().replace("+00:00", "Z")


def line(waybill: str, when: str, temp: float = 2.0) -> str:
    return json.dumps(
        {"waybill": waybill, "ts_utc": when, "temperature": temp},
        ensure_ascii=False,
    )


def list_gaps(cfg, waybill: str):
    conn = connect(cfg.db_path)
    try:
        return [
            (r["start_ts"], r["end_ts"], r["duration_s"])
            for r in conn.execute(
                "SELECT * FROM gaps WHERE waybill = ? ORDER BY start_ts",
                (waybill,),
            )
        ]
    finally:
        conn.close()


def list_marks(cfg, waybill: str):
    conn = connect(cfg.db_path)
    try:
        return [
            (r["start_ts"], r["end_ts"])
            for r in conn.execute(
                "SELECT * FROM gap_marks WHERE waybill = ? ORDER BY start_ts",
                (waybill,),
            )
        ]
    finally:
        conn.close()


class GapDetectionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        # GAP_BACKFILL_AUTOSTART defaults off so the job row is created 'ready'
        # and the collector maintains gaps incrementally right away.
        self.cfg = make_config(self.tmp.name)
        init_db(self.cfg.db_path)
        ensure_job_row(self.cfg.db_path)
        mark_job_ready(self.cfg.db_path)
        self.collector = Collector(self.cfg)
        self.active = self.cfg.active_path

    def tearDown(self):
        self.collector.stop()
        self.tmp.cleanup()

    def ingest(self, *items):
        """items: (waybill, ts) or (waybill, ts, temp) tuples, one tick."""
        with open(self.active, "a", encoding="utf-8") as fh:
            for item in items:
                waybill, when = item[0], item[1]
                temp = item[2] if len(item) > 2 else 2.0
                fh.write(line(waybill, when, temp) + "\n")
        self.collector.tick()

    def test_gap_found_over_threshold_and_boundary_exclusive(self):
        # exactly 5 minutes -> no gap; 5 min + 1 s -> gap
        self.ingest(
            ("WB-1", ts(0)), ("WB-1", ts(5)), ("WB-1", ts(10, 1)),
        )
        gaps = list_gaps(self.cfg, "WB-1")
        self.assertEqual(len(gaps), 1)
        self.assertEqual(gaps[0][0], ts(5))
        self.assertEqual(gaps[0][1], ts(10, 1))
        self.assertAlmostEqual(gaps[0][2], 301.0)

    def test_same_utc_instant_counts_as_one_sample_point(self):
        # three raw rows at the same instant, then a point 12 minutes later
        when = ts(0)
        with open(self.active, "a", encoding="utf-8") as fh:
            for _ in range(3):
                fh.write(line("WB-2", when, 1.0) + "\n")
            fh.write(line("WB-2", ts(12), 2.0) + "\n")
        self.collector.tick()
        conn = connect(self.cfg.db_path)
        try:
            raw = conn.execute(
                "SELECT COUNT(*) FROM readings WHERE waybill = 'WB-2'"
            ).fetchone()[0]
            points = conn.execute(
                "SELECT COUNT(*) FROM samples WHERE waybill = 'WB-2'"
            ).fetchone()[0]
        finally:
            conn.close()
        self.assertEqual(raw, 4)       # raw rows kept separately
        self.assertEqual(points, 2)    # collapsed into two sample points
        self.assertEqual(len(list_gaps(self.cfg, "WB-2")), 1)

    def test_gaps_per_waybill_are_independent(self):
        self.ingest(
            ("WB-A", ts(0)), ("WB-B", ts(0)),
            ("WB-A", ts(20)), ("WB-B", ts(3)),
        )
        self.assertEqual(len(list_gaps(self.cfg, "WB-A")), 1)
        self.assertEqual(list_gaps(self.cfg, "WB-B"), [])

    def test_incremental_refresh_only_changes_affected_neighbourhood(self):
        # two separate gaps; the second one must survive late arrivals in the
        # first one's window
        self.ingest(
            ("WB-3", ts(0)), ("WB-3", ts(20)), ("WB-3", ts(22)),
            ("WB-3", ts(40)),
        )
        gaps = list_gaps(self.cfg, "WB-3")
        self.assertEqual([(g[0], g[1]) for g in gaps],
                         [(ts(0), ts(20)), (ts(22), ts(40))])

        # late log inside the first gap splits it into two smaller gaps
        self.ingest(("WB-3", ts(10)))
        gaps = list_gaps(self.cfg, "WB-3")
        self.assertEqual([(g[0], g[1]) for g in gaps],
                         [(ts(0), ts(10)), (ts(10), ts(20)),
                          (ts(22), ts(40))])

    def test_late_log_eliminates_gap_completely(self):
        self.ingest(("WB-4", ts(0)), ("WB-4", ts(20)))
        self.assertEqual(len(list_gaps(self.cfg, "WB-4")), 1)
        # four late points keep every adjacency within the 5-minute window
        self.ingest(
            ("WB-4", ts(4)), ("WB-4", ts(8)),
            ("WB-4", ts(12)), ("WB-4", ts(16)),
        )
        self.assertEqual(list_gaps(self.cfg, "WB-4"), [])

    def test_one_flush_with_many_points_recomputes_whole_overlap(self):
        # Regression: a single ingest whose time window spans several existing
        # gaps must delete/rebuild every gap overlapping that window.
        self.ingest(
            ("WB-S", ts(0)), ("WB-S", ts(20)),
            ("WB-S", ts(22)), ("WB-S", ts(40)),
        )
        self.assertEqual(len(list_gaps(self.cfg, "WB-S")), 2)
        # dense points land across both gaps in one tick; every resulting
        # adjacency is at most 5 minutes, so no gap may survive
        self.ingest(
            ("WB-S", ts(5)), ("WB-S", ts(8)), ("WB-S", ts(11)),
            ("WB-S", ts(14)), ("WB-S", ts(17)),
            ("WB-S", ts(27)), ("WB-S", ts(30)), ("WB-S", ts(34)),
            ("WB-S", ts(37)),
        )
        self.assertEqual(list_gaps(self.cfg, "WB-S"), [])

    def test_duplicate_instant_insert_keeps_endpoint_gap(self):
        # inserting another raw row at an existing sample instant must not
        # delete the adjacent gap (no new sample point is created)
        self.ingest(("WB-D", ts(0)), ("WB-D", ts(20)))
        self.ingest(("WB-D", ts(0), 9.9))  # same UTC instant, new raw row
        self.assertEqual(len(list_gaps(self.cfg, "WB-D")), 1)

    def test_incremental_ingest_skipped_while_backfill_not_ready(self):
        conn = connect(self.cfg.db_path)
        try:
            conn.execute(
                "UPDATE gap_jobs SET status = ? WHERE id = 1", (JOB_PENDING,)
            )
            conn.commit()
        finally:
            conn.close()
        self.ingest(("WB-5", ts(0)), ("WB-5", ts(30)))
        # the backfill owns the gap tables until ready
        self.assertEqual(list_gaps(self.cfg, "WB-5"), [])


class GapMarkTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.cfg = make_config(self.tmp.name)
        init_db(self.cfg.db_path)
        ensure_job_row(self.cfg.db_path)
        mark_job_ready(self.cfg.db_path)
        self.collector = Collector(self.cfg)
        self.active = self.cfg.active_path

    def tearDown(self):
        self.collector.stop()
        self.tmp.cleanup()

    def ingest(self, *items):
        with open(self.active, "a", encoding="utf-8") as fh:
            for waybill, when in items:
                fh.write(line(waybill, when) + "\n")
        self.collector.tick()

    def test_mark_survives_unchanged_gap_but_invalidates_on_split(self):
        self.ingest(
            ("WB-6", ts(0)), ("WB-6", ts(20)), ("WB-6", ts(22)),
            ("WB-6", ts(40)),
        )
        conn = connect(self.cfg.db_path)
        try:
            conn.execute(
                "INSERT INTO gap_marks (waybill, start_ts, end_ts, note) "
                "VALUES (?, ?, ?, ?), (?, ?, ?, ?)",
                ("WB-6", ts(0), ts(20), "old mark",
                 "WB-6", ts(22), ts(40), "keep me"),
            )
            conn.commit()
        finally:
            conn.close()
        self.assertEqual(len(list_marks(self.cfg, "WB-6")), 2)

        # late arrival splits the first gap; its mark must become invalid,
        # the untouched second gap keeps its mark
        self.ingest(("WB-6", ts(10)))
        gaps = list_gaps(self.cfg, "WB-6")
        self.assertEqual(len(gaps), 3)
        marks = list_marks(self.cfg, "WB-6")
        # stale mark retained for audit but no longer joins any gap
        self.assertIn((ts(0), ts(20)), marks)
        self.assertIn((ts(22), ts(40)), marks)
        conn = connect(self.cfg.db_path)
        try:
            joined = conn.execute(
                "SELECT COUNT(*) FROM gaps g JOIN gap_marks m "
                "ON g.waybill = m.waybill AND g.start_ts = m.start_ts "
                "AND g.end_ts = m.end_ts WHERE g.waybill = 'WB-6'"
            ).fetchone()[0]
        finally:
            conn.close()
        self.assertEqual(joined, 1)  # only the unchanged gap is still marked

    def test_mark_removed_when_gap_eliminated(self):
        self.ingest(("WB-7", ts(0)), ("WB-7", ts(20)))
        conn = connect(self.cfg.db_path)
        try:
            conn.execute(
                "INSERT INTO gap_marks (waybill, start_ts, end_ts) "
                "VALUES (?, ?, ?)", ("WB-7", ts(0), ts(20))
            )
            conn.commit()
        finally:
            conn.close()
        self.ingest(
            ("WB-7", ts(4)), ("WB-7", ts(8)),
            ("WB-7", ts(12)), ("WB-7", ts(16)),
        )
        self.assertEqual(list_gaps(self.cfg, "WB-7"), [])
        # stale mark row remains but nothing joins it
        self.assertEqual(list_marks(self.cfg, "WB-7"), [(ts(0), ts(20))])


class BackfillTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.cfg = make_config(self.tmp.name)
        init_db(self.cfg.db_path)
        self.active = self.cfg.active_path
        self.collector = Collector(self.cfg)

    def tearDown(self):
        self.collector.stop()
        self.tmp.cleanup()

    def _seed_history(self):
        with open(self.active, "w", encoding="utf-8") as fh:
            for wb in ("WB-H1", "WB-H2"):
                fh.write(line(wb, ts(0)) + "\n")
                fh.write(line(wb, ts(20)) + "\n")  # one gap each
        self.collector.tick()  # backfill works on ingested readings

    def test_backfill_builds_all_waybills_then_switch_to_ready(self):
        ensure_job_row(self.cfg.db_path)
        self._seed_history()
        self.assertEqual(get_job_status(self.cfg.db_path)["status"],
                         JOB_PENDING)
        GapBackfill(self.cfg).run_once()
        job = get_job_status(self.cfg.db_path)
        self.assertEqual(job["status"], JOB_READY)
        self.assertEqual(job["total_waybills"], 2)
        self.assertEqual(len(list_gaps(self.cfg, "WB-H1")), 1)
        self.assertEqual(len(list_gaps(self.cfg, "WB-H2")), 1)

    def test_backfill_includes_rows_ingested_during_build(self):
        ensure_job_row(self.cfg.db_path)
        self._seed_history()
        collector = Collector(self.cfg)
        hook_ran = threading.Event()     # H1 first pass executed the hook
        pass_committed = threading.Event()  # H1 first pass is durably committed

        # Hook the rebuild: H1's *first* pass finishes against the original two
        # points, then a late arrival lands (as it would from a live gateway),
        # so the catch-up phase must rebuild H1 before flipping to ready.
        import app.gaps as gaps_mod
        original = gaps_mod.rebuild_waybill_gaps

        def patched(conn, waybill, threshold):
            result = original(conn, waybill, threshold)
            if waybill == "WB-H1" and not hook_ran.is_set():
                hook_ran.set()
                with open(self.active, "a", encoding="utf-8") as fh:
                    fh.write(line("WB-H1", ts(10)) + "\n")
                    fh.write(line("WB-LATE", ts(0)) + "\n")
                    fh.write(line("WB-LATE", ts(30)) + "\n")
            return result

        gaps_mod.rebuild_waybill_gaps = patched
        try:
            worker = GapBackfill(self.cfg)
            thread = threading.Thread(target=worker.run_once)
            thread.start()
            self.assertTrue(hook_ran.wait(timeout=5))
            # Poll until the worker's H1 pass has committed, so the collector
            # tick necessarily starts a fresh transaction above that snapshot
            # and the catch-up watermark is guaranteed to see H1's new ids.
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                job = get_job_status(self.cfg.db_path)
                if job["done_waybills"] >= 1:
                    pass_committed.set()
                    break
                time.sleep(0.01)
            self.assertTrue(pass_committed.is_set(),
                            "H1 first-pass commit was never observed")
            collector.tick()
            thread.join(timeout=10)
            self.assertFalse(thread.is_alive())
        finally:
            gaps_mod.rebuild_waybill_gaps = original
        collector.stop()

        job = get_job_status(self.cfg.db_path)
        self.assertEqual(job["status"], JOB_READY)
        # late point split the H1 gap (00->20 became 00->10, 10->20)
        self.assertEqual(
            [(g[0], g[1]) for g in list_gaps(self.cfg, "WB-H1")],
            [(ts(0), ts(10)), (ts(10), ts(20))],
        )
        # brand-new waybill that only appeared during the build is included
        self.assertEqual(len(list_gaps(self.cfg, "WB-LATE")), 1)

    def test_backfill_restarts_after_crash_and_is_idempotent(self):
        ensure_job_row(self.cfg.db_path)
        self._seed_history()
        # simulate a crash mid-run: job stuck in 'running'
        conn = connect(self.cfg.db_path)
        try:
            conn.execute(
                "UPDATE gap_jobs SET status = ?, total_waybills = 9, "
                "done_waybills = 1 WHERE id = 1", (JOB_RUNNING,)
            )
            conn.commit()
        finally:
            conn.close()
        GapBackfill(self.cfg).run_once()
        job = get_job_status(self.cfg.db_path)
        self.assertEqual(job["status"], JOB_READY)
        self.assertEqual(job["total_waybills"], 2)  # counters corrected
        # a second run is a no-op once ready
        GapBackfill(self.cfg).run_once()
        self.assertEqual(len(list_gaps(self.cfg, "WB-H1")), 1)

    def test_backfill_with_no_readings(self):
        ensure_job_row(self.cfg.db_path)
        GapBackfill(self.cfg).run_once()
        self.assertEqual(get_job_status(self.cfg.db_path)["status"],
                         JOB_READY)

    def test_backfill_stop_mid_run_returns_to_pending(self):
        ensure_job_row(self.cfg.db_path)
        self._seed_history()
        import app.gaps as gaps_mod
        original = gaps_mod.rebuild_waybill_gaps

        worker = GapBackfill(self.cfg)

        def stop_on_first(conn, waybill, threshold):
            result = original(conn, waybill, threshold)
            worker._stop.set()  # ask for shutdown during the first waybill
            return result

        gaps_mod.rebuild_waybill_gaps = stop_on_first
        try:
            worker.run_once()
        finally:
            gaps_mod.rebuild_waybill_gaps = original
        self.assertEqual(get_job_status(self.cfg.db_path)["status"],
                         JOB_PENDING)
        # a later, uninterrupted run completes the build
        GapBackfill(self.cfg).run_once()
        self.assertEqual(get_job_status(self.cfg.db_path)["status"],
                         JOB_READY)

    def test_full_rebuild_is_idempotent(self):
        ensure_job_row(self.cfg.db_path)
        mark_job_ready(self.cfg.db_path)
        with open(self.active, "w", encoding="utf-8") as fh:
            fh.write(line("WB-R", ts(0)) + "\n")
            fh.write(line("WB-R", ts(20)) + "\n")
        self.collector.tick()
        conn = connect(self.cfg.db_path)
        try:
            n1 = rebuild_waybill_gaps(conn, "WB-R", 300)
            conn.commit()
            n2 = rebuild_waybill_gaps(conn, "WB-R", 300)
            conn.commit()
        finally:
            conn.close()
        self.assertEqual((n1, n2), (1, 1))


class GapApiTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.cfg = make_config(self.tmp.name)
        self.app, self.collector = create_app(self.cfg)
        self.backfill = self.app.config["GAP_BACKFILL"]
        self.client = self.app.test_client()
        self.active = self.cfg.active_path
        # backfill autostart is disabled in tests; flip the job ready so the
        # API tests exercise the post-build serving path by default
        mark_job_ready(self.cfg.db_path)

    def tearDown(self):
        self.collector.stop()
        self.backfill.stop()
        self.tmp.cleanup()

    def ingest(self, *items):
        with open(self.active, "a", encoding="utf-8") as fh:
            for waybill, when in items:
                fh.write(line(waybill, when) + "\n")
        self.collector.tick()

    def test_gap_pages_render(self):
        self.assertEqual(self.client.get("/gaps").status_code, 200)
        body = self.client.get("/gaps").get_data(as_text=True)
        self.assertIn("缺测核查", body)
        # original page and waybill query still exist
        self.assertEqual(self.client.get("/").status_code, 200)
        self.assertEqual(
            self.client.get("/api/waybills").status_code, 200
        )

    def test_manual_backfill_endpoint_builds_then_serves(self):
        # reset to a never-built state with some history present
        conn = connect(self.cfg.db_path)
        try:
            conn.execute(
                "UPDATE gap_jobs SET status = ? WHERE id = 1", (JOB_PENDING,)
            )
            conn.commit()
        finally:
            conn.close()
        self.ingest(("WB-M", ts(0)), ("WB-M", ts(20)))
        # the endpoint kicks off the background worker
        r = self.client.post("/api/gaps/backfill")
        self.assertEqual(r.status_code, 200)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            st = self.client.get("/api/gaps/status").get_json()
            if st["gap_job"]["status"] == JOB_READY:
                break
            time.sleep(0.05)
        self.assertEqual(st["gap_job"]["status"], JOB_READY)
        body = self.client.get(
            "/api/gaps/waybills/WB-M"
        ).get_json()
        self.assertEqual(body["count"], 1)
        # a second trigger is a no-op once ready
        self.client.post("/api/gaps/backfill")
        self.assertEqual(
            self.client.get("/api/gaps/status").get_json()["gap_job"][
                "status"],
            JOB_READY,
        )

    def test_gap_api_503_until_ready_then_serves_results(self):
        # flip the job back to pending to simulate first-boot backfill
        conn = connect(self.cfg.db_path)
        try:
            conn.execute(
                "UPDATE gap_jobs SET status = ? WHERE id = 1", (JOB_PENDING,)
            )
            conn.commit()
        finally:
            conn.close()
        r = self.client.get("/api/gaps/waybills")
        self.assertEqual(r.status_code, 503)
        self.assertEqual(r.get_json()["error"],
                         "gap_backfill_not_ready")
        r2 = self.client.get("/api/gaps/waybills/WB-X")
        self.assertEqual(r2.status_code, 503)
        # status endpoint always answers and carries progress
        st = self.client.get("/api/gaps/status").get_json()
        self.assertEqual(st["gap_job"]["status"], JOB_PENDING)

        # mark attempts while not ready are refused too
        r3 = self.client.post(
            "/api/gaps/waybills/WB-X/marks",
            json={"start_ts": ts(0), "end_ts": ts(20)},
        )
        self.assertEqual(r3.status_code, 503)

        conn = connect(self.cfg.db_path)
        try:
            conn.execute(
                "UPDATE gap_jobs SET status = ? WHERE id = 1", (JOB_READY,)
            )
            conn.commit()
        finally:
            conn.close()
        self.ingest(("WB-X", ts(0)), ("WB-X", ts(20)))
        body = self.client.get("/api/gaps/waybills").get_json()
        self.assertEqual(body["count"], 1)
        self.assertEqual(body["waybills"][0]["gap_count"], 1)

    def test_mark_lifecycle_and_stale_gap_rejected(self):
        self.ingest(
            ("WB-8", ts(0)), ("WB-8", ts(20)), ("WB-8", ts(22)),
            ("WB-8", ts(40)),
        )
        # mark the first gap
        r = self.client.post(
            "/api/gaps/waybills/WB-8/marks",
            json={"start_ts": ts(0), "end_ts": ts(20), "note": "补取日志A"},
        )
        self.assertEqual(r.status_code, 200)

        detail = self.client.get("/api/gaps/waybills/WB-8").get_json()
        marked = [g for g in detail["gaps"] if g["marked"]]
        self.assertEqual(len(marked), 1)
        self.assertEqual(marked[0]["mark_note"], "补取日志A")

        # filter parameter
        unmarked = self.client.get(
            "/api/gaps/waybills/WB-8?marked=unmarked"
        ).get_json()
        self.assertEqual(unmarked["count"], 1)
        only_marked = self.client.get(
            "/api/gaps/waybills/WB-8?marked=marked"
        ).get_json()
        self.assertEqual(only_marked["count"], 1)
        bad_filter = self.client.get(
            "/api/gaps/waybills/WB-8?marked=nope"
        )
        self.assertEqual(bad_filter.status_code, 400)

        # late log splits the first gap
        self.ingest(("WB-8", ts(10)))
        # old page holding the original endpoints must not be able to mark it
        stale = self.client.post(
            "/api/gaps/waybills/WB-8/marks",
            json={"start_ts": ts(0), "end_ts": ts(20)},
        )
        self.assertEqual(stale.status_code, 404)
        self.assertEqual(stale.get_json()["error"],
                         "gap_not_found")

        detail = self.client.get("/api/gaps/waybills/WB-8").get_json()
        self.assertEqual(detail["count"], 3)
        self.assertEqual(len(detail["invalidated_marks"]), 1)
        # the other gap was untouched and stays unmarked
        self.assertTrue(
            all(not g["marked"] for g in detail["gaps"])
        )

        # unmark the remaining (second) gap after marking it
        self.client.post(
            "/api/gaps/waybills/WB-8/marks",
            json={"start_ts": ts(22), "end_ts": ts(40)},
        )
        removed = self.client.delete(
            "/api/gaps/waybills/WB-8/marks",
            json={"start_ts": ts(22), "end_ts": ts(40)},
        ).get_json()
        self.assertEqual(removed["removed"], 1)

    def test_marks_and_gaps_persist_across_restart(self):
        self.ingest(("WB-9", ts(0)), ("WB-9", ts(20)))
        self.client.post(
            "/api/gaps/waybills/WB-9/marks",
            json={"start_ts": ts(0), "end_ts": ts(20), "note": "persist"},
        )
        # brand-new processes over the same DB files
        app2, collector2 = create_app(self.cfg)
        backfill2 = app2.config["GAP_BACKFILL"]
        try:
            client2 = app2.test_client()
            detail = client2.get(
                "/api/gaps/waybills/WB-9"
            ).get_json()
            self.assertEqual(detail["count"], 1)
            self.assertTrue(detail["gaps"][0]["marked"])
            self.assertEqual(detail["gaps"][0]["mark_note"], "persist")
            status = client2.get("/api/gaps/status").get_json()
            self.assertEqual(status["gap_job"]["status"], JOB_READY)
        finally:
            collector2.stop()
            backfill2.stop()


if __name__ == "__main__":
    unittest.main(verbosity=2)
