#!/usr/bin/env python3
"""Correctness tests for the failure-analysis batch claim/confirm API.

Runs entirely on the local filesystem + SQLite (no Docker needed):

* batches are capped at 100 rows and follow warehouse (ingest) order
* every row carries request time/path/status plus source-file identity
  (source_code, dev, inode) and byte_offset
* re-claiming inside the lease returns the same batch with the SAME token
* after lease expiry the same id range is reclaimed with a NEW token, and the
  old token can no longer confirm (409)
* confirming with the valid token advances the durable cursor; repeating the
  same confirmation is idempotent and returns the original result
* concurrent claims (two+ instances) never split into different batches
* a simulated service restart leaves an unconfirmed batch reclaimable
* rows committed after a claim stay out of that batch; an uncommitted row can
  never appear in any response
* empty warehouse -> empty claim without creating a lease
* HTTP semantics through the real FastAPI app (200/404/409, /docs schema)
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "collector"))

from app import delivery, storage  # noqa: E402

PASS, FAIL = 0, 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {detail}")


def fresh_env():
    base = tempfile.mkdtemp(prefix="delivery-test-")
    db_path = os.path.join(base, "data", "warehouse.db")
    os.makedirs(os.path.dirname(db_path))
    conn = storage.connect(db_path)
    storage.init_db(conn)
    return base, db_path, conn


def insert_events(conn, n, *, prefix="/p", status=200, source="access.log",
                  start_offset=0, offset_base=0):
    """Commit n rows mimicking ingested nginx events."""
    with storage.transaction(conn):
        for i in range(n):
            conn.execute(
                """INSERT INTO log_events
                       (ts, path, status, method, source_code,
                        dev, inode, byte_offset, raw_line)
                   VALUES (?, ?, ?, 'GET', ?, 7, 11, ?, ?)""",
                (f"2026-11-15T10:00:{i % 60:02d}Z", f"{prefix}/{i}",
                 status if i % 5 else 500, source,
                 offset_base + start_offset + i * 40,
                 json.dumps({"path": f"{prefix}/{i}"}),),
            )


class FakeClock:
    """Deterministic UTC clock so lease expiry needs no real sleeping."""

    def __init__(self, start: datetime):
        self.now = start

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float):
        self.now += timedelta(seconds=seconds)


def test_empty_claim_creates_no_lease():
    print("\n[1] 空仓领取：空批次、不发令牌、不建租约")
    base, db_path, conn = fresh_env()
    try:
        resp = delivery.claim_batch(db_path, lease_seconds=30)
        check("empty warehouse -> row_count 0", resp["row_count"] == 0)
        check("empty warehouse -> token is None", resp["token"] is None)
        check("empty warehouse -> batch_no is None", resp["batch_no"] is None)
        check("empty warehouse -> empty items", resp["items"] == [])
        state = delivery.delivery_state(db_path)
        check("no outstanding lease after empty claim",
              state["outstanding_claim"] is None)
        check("cursor still at 1", state["next_event_id"] == 1)
        n = conn.execute("SELECT COUNT(*) c FROM batch_claims").fetchone()["c"]
        check("no batch_claims row written", n == 0, f"rows={n}")
    finally:
        conn.close()
        shutil.rmtree(base)


def test_batch_cap_order_and_fields():
    print("\n[2] 每批最多 100 行、按入仓顺序、字段含来源身份与字节位置")
    base, db_path, conn = fresh_env()
    try:
        insert_events(conn, 250)
        b1 = delivery.claim_batch(db_path, lease_seconds=30)
        check("first batch has exactly 100 rows", b1["row_count"] == 100
              and len(b1["items"]) == 100)
        check("first batch covers ids 1..100",
              b1["first_event_id"] == 1 and b1["last_event_id"] == 100)
        ids = [it["event_id"] for it in b1["items"]]
        check("items follow warehouse order 1..100", ids == list(range(1, 101)))
        item = b1["items"][0]
        for field in ("ts", "path", "status", "source_code", "dev",
                      "inode", "byte_offset"):
            check(f"row exposes field {field}", field in item)
        check("source identity present",
              item["source_code"] == "access.log" and item["dev"] == 7
              and item["inode"] == 11)
        check("byte_offset present and increasing",
              all(b1["items"][i]["byte_offset"]
                  < b1["items"][i + 1]["byte_offset"] for i in range(99)))
        check("request time/path/status present",
              item["ts"] is not None and item["path"] == "/p/0"
              and item["status"] == 500)

        # Confirm, then the next batch must continue at id 101.
        c1 = delivery.confirm_batch(db_path, b1["token"])
        check("confirm advances cursor to 101", c1["next_event_id"] == 101)
        b2 = delivery.claim_batch(db_path, lease_seconds=30)
        check("second batch continues at 101..200",
              b2["first_event_id"] == 101 and b2["last_event_id"] == 200
              and b2["row_count"] == 100)
        check("brand-new batch after confirm is not flagged reclaimed",
              b2["reclaimed"] is False)
        delivery.confirm_batch(db_path, b2["token"])
        b3 = delivery.claim_batch(db_path, lease_seconds=30)
        check("final partial batch has remaining 50 rows",
              b3["first_event_id"] == 201 and b3["last_event_id"] == 250
              and b3["row_count"] == 50 and len(b3["items"]) == 50)
        delivery.confirm_batch(db_path, b3["token"])
        b4 = delivery.claim_batch(db_path, lease_seconds=30)
        check("nothing left after all batches confirmed",
              b4["row_count"] == 0 and b4["token"] is None)
    finally:
        conn.close()
        shutil.rmtree(base)


def test_repeat_claim_same_batch_same_token():
    print("\n[3] 租约内重复领取：同一批、同一令牌")
    base, db_path, conn = fresh_env()
    try:
        insert_events(conn, 150)
        b1 = delivery.claim_batch(db_path, lease_seconds=30)
        for k in range(3):
            again = delivery.claim_batch(db_path, lease_seconds=30)
            check(f"repeat claim {k} keeps token", again["token"] == b1["token"])
            check(f"repeat claim {k} keeps batch_no",
                  again["batch_no"] == b1["batch_no"])
            check(f"repeat claim {k} keeps same id range",
                  again["first_event_id"] == 1 and again["last_event_id"] == 100)
            check(f"repeat claim {k} is not flagged reclaimed",
                  again["reclaimed"] is False)
            check(f"repeat claim {k} returns identical items",
                  [i["event_id"] for i in again["items"]]
                  == [i["event_id"] for i in b1["items"]])
        n = conn.execute("SELECT COUNT(*) c FROM batch_claims").fetchone()["c"]
        check("only one lease row exists", n == 1, f"rows={n}")
    finally:
        conn.close()
        shutil.rmtree(base)


def test_lease_expiry_new_token_old_token_rejected():
    print("\n[4] 租约到期：同批换新令牌重领，旧令牌不能确认")
    base, db_path, conn = fresh_env()
    clock = FakeClock(datetime(2026, 11, 15, tzinfo=timezone.utc))
    try:
        insert_events(conn, 100)
        b1 = delivery.claim_batch(db_path, lease_seconds=30, now=clock)
        old_token = b1["token"]

        # Still inside the lease: identical token.
        clock.advance(29)
        mid = delivery.claim_batch(db_path, lease_seconds=30, now=clock)
        check("29s in: same token still served", mid["token"] == old_token)

        # Expired: same id range, new token, flagged reclaimed.
        clock.advance(2)  # 31s total
        b2 = delivery.claim_batch(db_path, lease_seconds=30, now=clock)
        check("after expiry: token rotates", b2["token"] != old_token)
        check("after expiry: same id range reclaimed",
              b2["first_event_id"] == 1 and b2["last_event_id"] == 100
              and b2["row_count"] == 100)
        check("after expiry: reclaimed flag set", b2["reclaimed"] is True)
        check("after expiry: new lease window computed",
              b2["lease_expires_at"] != b1["lease_expires_at"])

        try:
            delivery.confirm_batch(db_path, old_token, now=clock)
        except delivery.StaleToken as exc:
            check("old token confirm rejected with 409", exc.status_code == 409
                  and exc.code == "superseded_token")
        else:
            check("old token confirm rejected with 409", False, "no exception")

        # Cursor must not have moved on the failed confirmation.
        state = delivery.delivery_state(db_path)
        check("cursor unchanged after rejected confirm",
              state["next_event_id"] == 1)

        c = delivery.confirm_batch(db_path, b2["token"], now=clock)
        check("new token confirms and advances cursor",
              c["status"] == "confirmed" and c["next_event_id"] == 101)
        rows = conn.execute(
            "SELECT status FROM batch_claims ORDER BY batch_no").fetchall()
        check("old lease marked expired, new one confirmed",
              [r["status"] for r in rows] == ["expired", "confirmed"],
              str([r["status"] for r in rows]))
    finally:
        conn.close()
        shutil.rmtree(base)


def test_confirm_idempotent_original_result():
    print("\n[5] 有效令牌确认推进游标；同令牌重复确认返回原结果")
    base, db_path, conn = fresh_env()
    clock = FakeClock(datetime(2026, 11, 15, tzinfo=timezone.utc))
    try:
        insert_events(conn, 100)
        b1 = delivery.claim_batch(db_path, lease_seconds=30, now=clock)
        c1 = delivery.confirm_batch(db_path, b1["token"], now=clock)
        first_confirmed_at = c1["confirmed_at"]

        clock.advance(10)
        c2 = delivery.confirm_batch(db_path, b1["token"], now=clock)
        check("repeat confirm returns same result object", c2 == c1)
        check("repeat confirm keeps original confirmed_at",
              c2["confirmed_at"] == first_confirmed_at)
        check("cursor still 101 (no double advance)",
              c2["next_event_id"] == 101)
        state = delivery.delivery_state(db_path)
        check("durable cursor reads 101", state["next_event_id"] == 101)
        n = conn.execute(
            "SELECT COUNT(*) c FROM batch_claims WHERE status='confirmed'"
        ).fetchone()["c"]
        check("exactly one confirmed lease row", n == 1)
    finally:
        conn.close()
        shutil.rmtree(base)


def test_unknown_and_expired_token_http_mapping():
    print("\n[6] 未知令牌 404；未重领但已过期的令牌 409")
    base, db_path, conn = fresh_env()
    clock = FakeClock(datetime(2026, 11, 15, tzinfo=timezone.utc))
    try:
        insert_events(conn, 10)
        b1 = delivery.claim_batch(db_path, lease_seconds=30, now=clock)
        try:
            delivery.confirm_batch(db_path, "never-issued-token", now=clock)
        except delivery.InvalidToken as exc:
            check("unknown token -> 404", exc.status_code == 404
                  and exc.code == "unknown_token")
        else:
            check("unknown token -> 404", False, "no exception")

        clock.advance(31)
        try:
            delivery.confirm_batch(db_path, b1["token"], now=clock)
        except delivery.StaleToken as exc:
            check("expired (not yet reclaimed) token -> 409",
                  exc.status_code == 409 and exc.code == "lease_expired")
        else:
            check("expired token -> 409", False, "no exception")
        # The expired lease is now marked, and a fresh claim re-issues a token.
        b2 = delivery.claim_batch(db_path, lease_seconds=30, now=clock)
        check("claim after expiry issues a new token",
              b2["token"] != b1["token"] and b2["reclaimed"] is True)
    finally:
        conn.close()
        shutil.rmtree(base)


def test_concurrent_claims_never_split():
    print("\n[7] 并发领取（模拟两个实例）：不得分到不同批")
    base, db_path, conn = fresh_env()
    try:
        insert_events(conn, 250)
        results: list[dict] = []
        errors: list[Exception] = []
        barrier = threading.Barrier(8)

        def worker():
            try:
                barrier.wait()
                # Each call opens its own SQLite connection, exactly like
                # separate service processes/instances would.
                results.append(delivery.claim_batch(db_path, lease_seconds=30))
            except Exception as exc:  # pragma: no cover - failure path
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
        check("all 8 concurrent claims succeeded", not errors, str(errors))
        check("8 claims returned", len(results) == 8)
        tokens = {r["token"] for r in results}
        check("all claimants got the SAME token", len(tokens) == 1, str(tokens))
        check("all claimants got the same id range",
              {(r["first_event_id"], r["last_event_id"]) for r in results}
              == {(1, 100)})
        check("all claimants got 100 identical rows",
              {len(r["items"]) for r in results} == {100})
        n = conn.execute("SELECT COUNT(*) c FROM batch_claims").fetchone()["c"]
        check("only one lease row despite 8 concurrent claims", n == 1,
              f"rows={n}")
    finally:
        conn.close()
        shutil.rmtree(base)


def test_restart_unconfirmed_batch_reclaimable():
    print("\n[8] 服务重启后未确认批仍可重领，租约按墙钟时间继续到期")
    base, db_path, conn = fresh_env()
    clock = FakeClock(datetime(2026, 11, 15, tzinfo=timezone.utc))
    try:
        insert_events(conn, 120)
        b1 = delivery.claim_batch(db_path, lease_seconds=30, now=clock)

        # Simulate a full process restart: drop every connection, then reopen
        # the DB from scratch exactly like the service would on boot.
        conn.close()
        conn = storage.connect(db_path)
        storage.init_db(conn)

        state = delivery.delivery_state(db_path)
        check("after restart: outstanding lease survived",
              state["outstanding_claim"] is not None
              and state["outstanding_claim"]["token"] == b1["token"])
        check("after restart: cursor still at 1", state["next_event_id"] == 1)

        # Within lease, restart changes nothing: same batch/token.
        same = delivery.claim_batch(db_path, lease_seconds=30, now=clock)
        check("after restart: same batch and token within lease",
              same["token"] == b1["token"]
              and same["first_event_id"] == 1 and same["last_event_id"] == 100)

        # Lease expiry is wall-clock based, so downtime counts against it.
        clock.advance(45)
        reclaimed = delivery.claim_batch(db_path, lease_seconds=30, now=clock)
        check("after restart+expiry: same batch, new token",
              reclaimed["reclaimed"] is True
              and reclaimed["token"] != b1["token"]
              and (reclaimed["first_event_id"], reclaimed["last_event_id"])
              == (1, 100))
        c = delivery.confirm_batch(db_path, reclaimed["token"], now=clock)
        check("reclaimed batch confirms after restart",
              c["next_event_id"] == 101)
        # Remaining 20 rows form the next batch after restart.
        nxt = delivery.claim_batch(db_path, lease_seconds=30, now=clock)
        check("remaining rows delivered in a later batch",
              nxt["first_event_id"] == 101 and nxt["row_count"] == 20)
    finally:
        conn.close()
        shutil.rmtree(base)


def test_uncommitted_and_late_rows_never_leak():
    print("\n[9] 未提交行不进响应；领取后新提交的行留到后续批次")
    base, db_path, conn = fresh_env()
    try:
        insert_events(conn, 100)
        # A separate writer holds an UNCOMMITTED insert while the claim tries
        # to start. The claim's BEGIN IMMEDIATE blocks until the writer ends;
        # the writer rolls back, so the row must never appear. This also proves
        # the claim survives write-lock contention instead of failing.
        uncommitted_visible = threading.Event()
        claimer_started = threading.Event()
        claim_result: dict = {}
        claim_error: list[Exception] = []

        def hold_uncommitted_then_rollback():
            w = storage.connect(db_path)
            try:
                w.execute("BEGIN IMMEDIATE")
                w.execute(
                    """INSERT INTO log_events
                           (ts, path, status, method, source_code,
                            dev, inode, byte_offset, raw_line)
                       VALUES ('x', '/uncommitted', 500, 'GET', 'access.log',
                               7, 11, 9999, '{}')""")
                uncommitted_visible.set()
                # Only roll back once the claimer has demonstrably started and
                # is queued on this write lock.
                claimer_started.wait(timeout=10)
                time.sleep(0.3)
                w.execute("ROLLBACK")
            finally:
                w.close()

        def do_claim():
            uncommitted_visible.wait(timeout=10)
            claimer_started.set()
            try:
                claim_result["resp"] = delivery.claim_batch(
                    db_path, lease_seconds=30)
            except Exception as exc:  # pragma: no cover - failure path
                claim_error.append(exc)

        wt = threading.Thread(target=hold_uncommitted_then_rollback)
        ct = threading.Thread(target=do_claim)
        wt.start()
        ct.start()
        wt.join(timeout=15)
        ct.join(timeout=15)
        check("claim succeeded despite lock contention", not claim_error,
              str(claim_error))
        b1 = claim_result["resp"]
        check("uncommitted row absent from claim",
              all(it["path"] != "/uncommitted" for it in b1["items"])
              and b1["last_event_id"] == 100
              and b1["row_count"] == 100,
              f"last={b1['last_event_id']} rows={b1['row_count']}")

        # Commit 30 more rows while batch 1 is still leased. They must not
        # change the outstanding batch; they land in batch 2.
        insert_events(conn, 30, prefix="/late", offset_base=1_000_000)
        again = delivery.claim_batch(db_path, lease_seconds=30)
        check("late commits do not alter the outstanding batch",
              again["last_event_id"] == 100 and again["row_count"] == 100
              and all(not it["path"].startswith("/late") for it in again["items"]))
        delivery.confirm_batch(db_path, again["token"])
        b2 = delivery.claim_batch(db_path, lease_seconds=30)
        check("newly committed rows surface in the next batch",
              b2["row_count"] == 30 and b2["first_event_id"] == 101
              and all(it["path"].startswith("/late") for it in b2["items"]))
    finally:
        conn.close()
        shutil.rmtree(base)


def test_http_api_end_to_end():
    print("\n[10] HTTP 层：/batches/claim 与 /batches/{token}/confirm 全流程")
    base = tempfile.mkdtemp(prefix="delivery-api-")
    log_dir = os.path.join(base, "logs")
    db_path = os.path.join(base, "data", "warehouse.db")
    os.makedirs(log_dir)
    os.makedirs(os.path.dirname(db_path))
    os.environ["DB_PATH"] = db_path
    os.environ["LOG_DIR"] = log_dir
    os.environ["POLL_INTERVAL"] = "3600"
    os.environ["BATCH_LEASE_SECONDS"] = "2"

    import uvicorn
    from app.main import app

    config = uvicorn.Config(app, host="127.0.0.1", port=8771, log_level="warning")
    server = uvicorn.Server(config)
    t = threading.Thread(target=server.run, daemon=True)
    t.start()
    try:
        for _ in range(100):
            try:
                urllib.request.urlopen(
                    "http://127.0.0.1:8771/health", timeout=1)
                break
            except Exception:
                time.sleep(0.1)

        def post(path):
            req = urllib.request.Request(
                "http://127.0.0.1:8771" + path, method="POST")
            try:
                with urllib.request.urlopen(req, timeout=5) as r:
                    return r.status, json.loads(r.read())
            except urllib.error.HTTPError as e:
                return e.code, json.loads(e.read())

        def get(path):
            with urllib.request.urlopen(
                    "http://127.0.0.1:8771" + path, timeout=5) as r:
                return r.status, json.loads(r.read())

        # Seed rows straight through the warehouse DB.
        seed_conn = storage.connect(db_path)
        insert_events(seed_conn, 130)
        seed_conn.close()

        status, b1 = post("/batches/claim")
        check("claim 200 with 100 rows",
              status == 200 and b1["row_count"] == 100
              and b1["first_event_id"] == 1 and b1["last_event_id"] == 100)
        token = b1["token"]

        status, b1b = post("/batches/claim")
        check("repeat claim returns same token over HTTP",
              status == 200 and b1b["token"] == token)

        status, bad = post("/batches/does-not-exist/confirm")
        check("unknown token -> HTTP 404", status == 404
              and bad["detail"]["code"] == "unknown_token", str(bad))

        # Wait out the real 2s lease, then re-claim: token must rotate.
        time.sleep(2.2)
        status, b2 = post("/batches/claim")
        check("expired re-claim -> new token, same range",
              status == 200 and b2["reclaimed"] is True
              and b2["token"] != token
              and (b2["first_event_id"], b2["last_event_id"]) == (1, 100))

        status, stale = post(f"/batches/{token}/confirm")
        check("old token confirm -> HTTP 409", status == 409
              and stale["detail"]["code"] == "superseded_token", str(stale))

        status, c1 = post(f"/batches/{b2['token']}/confirm")
        check("valid confirm 200, cursor -> 101",
              status == 200 and c1["next_event_id"] == 101
              and c1["status"] == "confirmed")

        status, c1b = post(f"/batches/{b2['token']}/confirm")
        check("idempotent re-confirm returns original result",
              status == 200 and c1b == c1)

        status, b3 = post("/batches/claim")
        check("next batch has remaining 30 rows",
              status == 200 and b3["row_count"] == 30
              and b3["first_event_id"] == 101 and b3["last_event_id"] == 130)
        check("fresh batch after confirm is not reclaimed over HTTP",
              b3["reclaimed"] is False)
        status, c2 = post(f"/batches/{b3['token']}/confirm")
        check("second batch confirmed", status == 200
              and c2["next_event_id"] == 131)

        status, empty = post("/batches/claim")
        check("empty claim over HTTP",
              status == 200 and empty["row_count"] == 0
              and empty["token"] is None)

        status, st = get("/batches/state")
        check("state endpoint reports cursor and no outstanding lease",
              status == 200 and st["next_event_id"] == 131
              and st["unclaimed_events"] == 0
              and st["outstanding_claim"] is None)

        # /docs and OpenAPI must document the new endpoints (task requirement).
        with urllib.request.urlopen(
                "http://127.0.0.1:8771/openapi.json", timeout=5) as r:
            spec = json.loads(r.read())
        paths = set(spec["paths"])
        check("openapi documents claim/confirm/state",
              {"/batches/claim", "/batches/{token}/confirm",
               "/batches/state"}.issubset(paths), str(sorted(paths)))
        with urllib.request.urlopen(
                "http://127.0.0.1:8771/docs", timeout=5) as r:
            check("/docs page serves Swagger UI",
                  r.status == 200 and b"swagger" in r.read().lower())

        # Original collection/query endpoints are preserved.
        status, page = get("/events?limit=10&offset=0")
        check("original /events query still works",
              status == 200 and page["total"] == 130
              and len(page["items"]) == 10)
    finally:
        server.should_exit = True
        t.join(timeout=5)
        shutil.rmtree(base)


if __name__ == "__main__":
    test_empty_claim_creates_no_lease()
    test_batch_cap_order_and_fields()
    test_repeat_claim_same_batch_same_token()
    test_lease_expiry_new_token_old_token_rejected()
    test_confirm_idempotent_original_result()
    test_unknown_and_expired_token_http_mapping()
    test_concurrent_claims_never_split()
    test_restart_unconfirmed_batch_reclaimable()
    test_uncommitted_and_late_rows_never_leak()
    test_http_api_end_to_end()
    print(f"\n==== {PASS} passed, {FAIL} failed ====")
    sys.exit(1 if FAIL else 0)
