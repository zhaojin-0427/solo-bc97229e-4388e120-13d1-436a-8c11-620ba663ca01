"""Flask application: control panel, progress/error views, waybill and gap APIs."""
from __future__ import annotations

from flask import Flask, abort, jsonify, render_template, request
from werkzeug.exceptions import HTTPException

from .collector import Collector
from .config import Config
from .db import connect, init_db
from .gaps import (
    JOB_READY,
    GapBackfill,
    ensure_job_row,
    get_job_status,
)


def create_app(config: Config | None = None) -> tuple[Flask, Collector]:
    cfg = config or Config()
    init_db(cfg.db_path)
    ensure_job_row(cfg.db_path)
    collector = Collector(cfg)
    backfill = GapBackfill(cfg)

    app = Flask(__name__)
    app.config["COLLECTOR"] = collector
    app.config["GAP_BACKFILL"] = backfill

    @app.errorhandler(HTTPException)
    def _json_error(exc: HTTPException):
        # Keep machine-readable payloads (e.g. gap-not-found hints) as JSON
        # instead of Flask's default HTML error page.
        payload = {"error": exc.name.lower().replace(" ", "_")}
        if isinstance(exc.description, dict):
            payload = exc.description
        elif exc.description:
            payload["message"] = exc.description
        return jsonify(payload), exc.code

    if cfg.autostart:
        collector.start()
    if cfg.backfill_autostart:
        backfill.start()

    # ----------------------------------------------------------------- pages
    @app.get("/")
    def index():
        return render_template("index.html", poll_interval=cfg.poll_interval)

    @app.get("/gaps")
    def gaps_page():
        return render_template("gaps.html",
                               threshold_seconds=cfg.gap_threshold_seconds)

    # ----------------------------------------------------------------- status
    @app.get("/api/status")
    def api_status():
        conn = connect(cfg.db_path)
        try:
            progress = [
                dict(r)
                for r in conn.execute(
                    "SELECT * FROM ingest_progress ORDER BY status, path"
                )
            ]
            totals = conn.execute(
                "SELECT "
                "(SELECT COUNT(*) FROM readings) AS readings, "
                "(SELECT COUNT(*) FROM ingest_errors) AS errors, "
                "(SELECT COUNT(DISTINCT waybill) FROM readings) AS waybills"
            ).fetchone()
        finally:
            conn.close()
        return jsonify(
            {
                "collector": {
                    "running": collector.running,
                    "tick_count": collector.tick_count,
                    "last_tick_at": collector.last_tick_at,
                    "last_error": collector.last_error,
                    "poll_interval_seconds": cfg.poll_interval,
                    "watch_dir": cfg.watch_dir,
                    "active_file": cfg.active_name,
                },
                "gap_job": get_job_status(cfg.db_path),
                "gap_backfill": {"running": backfill.running,
                                 "last_error": backfill.last_error},
                "totals": dict(totals),
                "progress": progress,
            }
        )

    # ----------------------------------------------------------------- control
    @app.post("/api/start")
    def api_start():
        collector.start()
        return jsonify({"running": collector.running})

    @app.post("/api/stop")
    def api_stop():
        collector.stop()
        return jsonify({"running": collector.running})

    # ------------------------------------------------------------------ errors
    @app.get("/api/errors")
    def api_errors():
        limit = min(int(request.args.get("limit", 100)), 1000)
        conn = connect(cfg.db_path)
        try:
            rows = [
                dict(r)
                for r in conn.execute(
                    "SELECT * FROM ingest_errors ORDER BY id DESC LIMIT ?",
                    (limit,),
                )
            ]
        finally:
            conn.close()
        return jsonify({"errors": rows, "count": len(rows)})

    # ------------------------------------------------------------- waybill API
    @app.get("/api/waybills/<waybill>")
    def api_waybill(waybill):
        if not waybill:
            abort(400, "waybill is required")
        limit = min(int(request.args.get("limit", 500)), 10_000)
        conn = connect(cfg.db_path)
        try:
            rows = [
                dict(r)
                for r in conn.execute(
                    "SELECT id, waybill, ts_utc, temperature, source_path, "
                    "line_no, raw_line FROM readings WHERE waybill = ? "
                    "ORDER BY ts_utc ASC, id ASC LIMIT ?",
                    (waybill, limit),
                )
            ]
            stats = conn.execute(
                "SELECT COUNT(*) AS n, MIN(ts_utc) AS first_ts, "
                "MAX(ts_utc) AS last_ts, MIN(temperature) AS min_temperature, "
                "MAX(temperature) AS max_temperature, "
                "AVG(temperature) AS avg_temperature "
                "FROM readings WHERE waybill = ?",
                (waybill,),
            ).fetchone()
        finally:
            conn.close()
        return jsonify(
            {
                "waybill": waybill,
                "count": len(rows),
                "stats": dict(stats),
                "readings": rows,
            }
        )

    @app.get("/api/waybills")
    def api_waybills():
        conn = connect(cfg.db_path)
        try:
            rows = [
                dict(r)
                for r in conn.execute(
                    "SELECT waybill, COUNT(*) AS readings, MIN(ts_utc) AS first_ts, "
                    "MAX(ts_utc) AS last_ts, MIN(temperature) AS min_temperature, "
                    "MAX(temperature) AS max_temperature "
                    "FROM readings GROUP BY waybill ORDER BY waybill"
                )
            ]
        finally:
            conn.close()
        return jsonify({"waybills": rows, "count": len(rows)})

    @app.get("/healthz")
    def healthz():
        return {"status": "ok"}

    # ------------------------------------------------------------- gap API
    def gap_ready_or_abort() -> dict:
        """Never serve gap rows while the historical build is incomplete."""
        job = get_job_status(cfg.db_path)
        if job["status"] != JOB_READY:
            abort(
                503,
                description={
                    "error": "gap_backfill_not_ready",
                    "gap_job": job,
                },
            )
        return job

    @app.get("/api/gaps/status")
    def api_gap_status():
        return jsonify(
            {
                "gap_job": get_job_status(cfg.db_path),
                "backfill": {
                    "running": backfill.running,
                    "last_error": backfill.last_error,
                },
                "threshold_seconds": cfg.gap_threshold_seconds,
            }
        )

    @app.post("/api/gaps/backfill")
    def api_gap_backfill_start():
        # Manual (re)start: useful when autostart is off or a previous run
        # failed. No-op once the job is already ready.
        job = get_job_status(cfg.db_path)
        if job["status"] != JOB_READY:
            backfill.start()
        return jsonify(
            {
                "gap_job": get_job_status(cfg.db_path),
                "backfill": {"running": backfill.running,
                             "last_error": backfill.last_error},
            }
        )

    @app.get("/api/gaps/waybills")
    def api_gap_waybills():
        job = gap_ready_or_abort()
        conn = connect(cfg.db_path)
        try:
            rows = [
                dict(r)
                for r in conn.execute(
                    "SELECT g.waybill, "
                    "COUNT(*) AS gap_count, "
                    "SUM(CASE WHEN m.waybill IS NULL THEN 0 ELSE 1 END) "
                    "  AS marked_count, "
                    "MIN(g.start_ts) AS first_gap_start, "
                    "MAX(g.end_ts) AS last_gap_end, "
                    "MAX(g.duration_s) AS max_duration_s "
                    "FROM gaps g LEFT JOIN gap_marks m "
                    "ON m.waybill = g.waybill AND m.start_ts = g.start_ts "
                    "AND m.end_ts = g.end_ts "
                    "GROUP BY g.waybill ORDER BY g.waybill"
                )
            ]
        finally:
            conn.close()
        return jsonify({"ready": True, "gap_job": job, "waybills": rows,
                        "count": len(rows)})

    @app.get("/api/gaps/waybills/<waybill>")
    def api_gap_waybill(waybill):
        if not waybill:
            abort(400, "waybill is required")
        job = gap_ready_or_abort()
        include = request.args.get("marked", "all")
        if include not in ("all", "marked", "unmarked"):
            abort(400, "marked must be one of all, marked, unmarked")
        conn = connect(cfg.db_path)
        try:
            where = ""
            if include == "marked":
                where = "AND m.waybill IS NOT NULL"
            elif include == "unmarked":
                where = "AND m.waybill IS NULL"
            rows = [
                dict(r)
                for r in conn.execute(
                    "SELECT g.waybill, g.start_ts, g.end_ts, g.duration_s, "
                    "CASE WHEN m.waybill IS NULL THEN 0 ELSE 1 END AS marked, "
                    "m.note AS mark_note, m.marked_at AS marked_at "
                    "FROM gaps g LEFT JOIN gap_marks m "
                    "ON m.waybill = g.waybill AND m.start_ts = g.start_ts "
                    "AND m.end_ts = g.end_ts "
                    "WHERE g.waybill = ? " + where +
                    " ORDER BY g.start_ts",
                    (waybill,),
                )
            ]
            invalid = [
                dict(r)
                for r in conn.execute(
                    "SELECT m.waybill, m.start_ts, m.end_ts, m.note, m.marked_at "
                    "FROM gap_marks m WHERE m.waybill = ? AND NOT EXISTS ("
                    "SELECT 1 FROM gaps g WHERE g.waybill = m.waybill "
                    "AND g.start_ts = m.start_ts AND g.end_ts = m.end_ts) "
                    "ORDER BY m.marked_at DESC",
                    (waybill,),
                )
            ]
        finally:
            conn.close()
        return jsonify(
            {
                "ready": True,
                "gap_job": job,
                "waybill": waybill,
                "gaps": rows,
                "count": len(rows),
                "invalidated_marks": invalid,
            }
        )

    @app.post("/api/gaps/waybills/<waybill>/marks")
    def api_gap_mark(waybill):
        gap_ready_or_abort()
        payload = request.get_json(silent=True) or {}
        start_ts = (payload.get("start_ts") or "").strip()
        end_ts = (payload.get("end_ts") or "").strip()
        note = payload.get("note")
        if not start_ts or not end_ts:
            abort(400, "start_ts and end_ts are required")
        conn = connect(cfg.db_path)
        try:
            # A mark is only accepted when the exact original gap still exists:
            # split/eliminated intervals cannot be marked through stale UI.
            exists = conn.execute(
                "SELECT 1 FROM gaps WHERE waybill = ? AND start_ts = ? "
                "AND end_ts = ?",
                (waybill, start_ts, end_ts),
            ).fetchone()
            if exists is None:
                abort(
                    404,
                    description={
                        "error": "gap_not_found",
                        "waybill": waybill,
                        "start_ts": start_ts,
                        "end_ts": end_ts,
                        "hint": "该缺口已被迟到日志拆分或消除，无法再标记",
                    },
                )
            conn.execute(
                "INSERT INTO gap_marks (waybill, start_ts, end_ts, note) "
                "VALUES (?, ?, ?, ?) ON CONFLICT(waybill, start_ts, end_ts) "
                "DO UPDATE SET note = excluded.note",
                (waybill, start_ts, end_ts, note),
            )
            conn.commit()
        finally:
            conn.close()
        return jsonify({"marked": True, "waybill": waybill,
                        "start_ts": start_ts, "end_ts": end_ts})

    @app.delete("/api/gaps/waybills/<waybill>/marks")
    def api_gap_unmark(waybill):
        gap_ready_or_abort()
        payload = request.get_json(silent=True) or {}
        start_ts = (payload.get("start_ts") or "").strip()
        end_ts = (payload.get("end_ts") or "").strip()
        if not start_ts or not end_ts:
            abort(400, "start_ts and end_ts are required")
        conn = connect(cfg.db_path)
        try:
            cur = conn.execute(
                "DELETE FROM gap_marks WHERE waybill = ? AND start_ts = ? "
                "AND end_ts = ?",
                (waybill, start_ts, end_ts),
            )
            conn.commit()
        finally:
            conn.close()
        return jsonify({"removed": cur.rowcount})

    return app, collector
