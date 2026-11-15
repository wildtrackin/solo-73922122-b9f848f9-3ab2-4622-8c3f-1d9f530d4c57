#!/usr/bin/env python3
"""Full local E2E simulation (no Docker):

1. starts the real uvicorn/FastAPI service against a temp log dir + DB
2. writes nginx-style JSON lines, including an unterminated tail
3. performs a rename rotation with late writes into the old inode
4. restarts the service and proves resume without gaps/duplicates
5. exercises every HTTP endpoint with urllib
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

BASE = tempfile.mkdtemp(prefix="e2e-")
LOG_DIR = os.path.join(BASE, "logs")
DB_PATH = os.path.join(BASE, "data", "warehouse.db")
os.makedirs(LOG_DIR)
os.makedirs(os.path.dirname(DB_PATH))

os.environ["DB_PATH"] = DB_PATH
os.environ["LOG_DIR"] = LOG_DIR
# Long interval guarantees no background poll lands inside the
# rename/reopen critical section; progress is advanced explicitly where
# needed via POST /ingest/poll-now.
os.environ["POLL_INTERVAL"] = "3600"
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "collector"))

import uvicorn  # noqa: E402
from app.main import app  # noqa: E402

PORT = 8799
BASE_URL = f"http://127.0.0.1:{PORT}"


def wait_ready(timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"{BASE_URL}/health", timeout=1) as r:
                if r.status == 200:
                    return
        except Exception:
            time.sleep(0.1)
    raise RuntimeError("service did not become ready")


def get(path):
    with urllib.request.urlopen(BASE_URL + path, timeout=5) as r:
        return r.status, json.loads(r.read())


def post(path):
    req = urllib.request.Request(BASE_URL + path, method="POST")
    with urllib.request.urlopen(req, timeout=5) as r:
        return r.status, json.loads(r.read())


def jline(path, status):
    return json.dumps({"time": "2026-11-13T10:00:00Z", "path": path,
                       "status": status, "method": "GET"}) + "\n"


def start_server():
    cfg = uvicorn.Config(app, host="127.0.0.1", port=PORT,
                         log_level="warning")
    srv = uvicorn.Server(cfg)
    t = threading.Thread(target=srv.run, daemon=True)
    t.start()
    wait_ready()
    return srv, t


def main():
    failures = []

    def check(name, cond, detail=""):
        print(("  PASS  " if cond else "  FAIL  ") + name + ("" if cond else f"  {detail}"))
        if not cond:
            failures.append(name)

    print("[A] 首次启动并写入日志（含未换行尾部）")
    srv, t = start_server()
    cur = os.path.join(LOG_DIR, "access.log")
    with open(cur, "w") as fh:
        for i in range(20):
            fh.write(jline("/api/orders", 200))
        for i in range(4):
            fh.write(jline("/api/orders", 500))
        fh.write('{"time":"t","path":"/dangling","status":502')  # no \n

    post("/ingest/poll-now")
    _, h = get("/health")
    check("24 complete lines ingested, dangling tail excluded",
          h["counts"]["events"] == 24, str(h["counts"]))
    _, prog = get("/ingest/progress")
    f0 = prog["files"][0]
    check("progress reports offset < size while tail uncommitted",
          f0["offset"] < f0["size"] and f0["complete"] is False, str(f0))

    print("[B] 补齐尾部")
    with open(cur, "a") as fh:
        fh.write("}\n")
    post("/ingest/poll-now")
    _, prog = get("/ingest/progress")
    check("file complete once tail gets its newline", prog["files"][0]["complete"])
    _, page = get("/events?path=/dangling")
    check("dangling line now present with status 502",
          page["total"] == 1 and page["items"][0]["status"] == 502)

    print("[C] rename 轮转 + 旧 inode 延迟写入 + 新文件")
    # Perform the whole rename/reopen sequence back-to-back so no poll can
    # observe the directory in the intermediate state. Holding the archive
    # fd mirrors nginx workers and keeps the old inode allocated.
    archive = os.path.join(LOG_DIR, "access.log.arc1.log")
    os.rename(cur, archive)
    arch_fh = open(archive, "a")  # nginx worker still holds it
    try:
        arch_fh.write(jline("/late/after-rotate", 503))
        arch_fh.flush()
        with open(cur, "w") as fh:
            for i in range(10):
                fh.write(jline("/api/payments", 200))
            fh.write(jline("/api/payments", 500))
    finally:
        pass  # arch_fh stays open until shutdown, like a draining worker

    post("/ingest/poll-now")
    _, h = get("/health")
    check("all 37 events present across rotation",
          h["counts"]["events"] == 37, str(h["counts"]))
    _, late = get("/events?path=/late/after-rotate")
    check("late line attributed to the archive source",
          late["total"] == 1 and
          late["items"][0]["source_code"] == "access.log.arc1.log",
          str(late))
    # The 25 pre-rotation rows were also ingested under "access.log", so
    # assert on the new file's distinctive /api/payments rows instead.
    _, bysrc = get("/events?path=/api/payments&limit=200")
    check("new-file rows carry access.log source code",
          bysrc["total"] == 11 and all(
              i["source_code"] == "access.log" for i in bysrc["items"]),
          str([(i["path"], i["source_code"]) for i in bysrc["items"]]))

    print("[D] 模拟服务重启：停止后再启动，验证自动续采不漏不重")
    srv.should_exit = True
    t.join(timeout=10)
    time.sleep(0.3)
    # write more lines while the service is down
    with open(cur, "a") as fh:
        for i in range(7):
            fh.write(jline("/while/down", 504))
    srv2, t2 = start_server()
    try:
        post("/ingest/poll-now")
        _, h = get("/health")
        check("lines written while down picked up after restart",
              h["counts"]["events"] == 44, str(h["counts"]))
        post("/ingest/poll-now")
        _, h = get("/health")
        check("forced poll adds no duplicates", h["counts"]["events"] == 44)
        _, down = get("/events?path=/while/down")
        check("all 7 downtime rows present once", down["total"] == 7)

        print("[E] 查询接口：故障请求、分页、单条、坏行")
        _, errs = get("/events?status_min=500&status_max=599&limit=100")
        expected_5xx = 4 + 1 + 1 + 1 + 7  # 500x4, 502, 503, new 500, 504x7
        check("5xx range query returns all failures",
              errs["total"] == expected_5xx and all(
                  i["status"] >= 500 for i in errs["items"]),
              f"{errs['total']} != {expected_5xx}")
        _, p1 = get("/events?limit=20&offset=0")
        _, p3 = get("/events?limit=20&offset=40")
        check("pagination chain",
              p1["total"] == 44 and p1["next_offset"] == 20
              and len(p3["items"]) == 4 and p3["next_offset"] is None,
              f"{p1['next_offset']}, {len(p3['items'])}")
        eid = p1["items"][0]["id"]
        _, one = get(f"/events/{eid}")
        check("single event raw line is parseable JSON",
              json.loads(one["raw_line"])["path"] == one["path"])
        _, mal = get("/malformed")
        check("malformed endpoint responds", "total" in mal)
        _, prog = get("/ingest/progress")
        check("progress lists both sources fully drained",
                  {f["source_code"] for f in prog["files"]} >=
                  {"access.log", "access.log.arc1.log"}
                  and all(f["complete"] for f in prog["files"]),
                  str(prog["files"]))
    finally:
        srv2.should_exit = True
        t2.join(timeout=10)
        arch_fh.close()

    print(f"\n==== E2E {'ALL PASS' if not failures else f'{len(failures)} FAIL'} ====")
    shutil.rmtree(BASE, ignore_errors=True)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
