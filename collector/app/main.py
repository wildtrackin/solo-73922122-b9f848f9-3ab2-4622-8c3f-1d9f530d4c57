"""FastAPI service: continuous ingestion + query API.

Endpoints
---------
GET  /health                 liveness + counters
GET  /ingest/progress        per-file collection progress (offset/size)
POST /ingest/poll-now        force an immediate collection pass
GET  /events                 paginated events, filter by path / status /
                             status range / source code
GET  /events/{event_id}      single event with the raw JSON line
GET  /malformed              quarantined lines that failed JSON parsing

Failure-analysis batch delivery (lease-based, restart-safe)
-----------------------------------------------------------
POST /batches/claim          claim/re-claim up to 100 committed rows
POST /batches/{token}/confirm  confirm a batch; advances the durable cursor
GET  /batches/state          delivery cursor + outstanding lease (read-only)
"""

from __future__ import annotations

import logging
import os
import threading
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from . import delivery, storage
from .ingest import CollectorLoop, poll_once

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("collector.api")

DB_PATH = os.getenv("DB_PATH", "/data/warehouse.db")
LOG_DIR = os.getenv("LOG_DIR", "/logs")
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "0.5"))
# Validity window of a claim token. On expiry the same batch may be reclaimed
# with a new token; the old token can no longer confirm it.
BATCH_LEASE_SECONDS = float(os.getenv("BATCH_LEASE_SECONDS", "30"))


class Health(BaseModel):
    status: str
    log_dir: str
    db_path: str
    poll_interval_seconds: float
    collector_running: bool
    total_polls: int
    total_ingested: int
    counts: dict


class FileProgress(BaseModel):
    source_code: str
    dev: int
    inode: int
    path: str
    offset: int
    size: Optional[int] = None
    complete: bool
    last_seen: str


class ProgressResponse(BaseModel):
    collector_running: bool
    total_polls: int
    total_ingested: int
    last_poll: Optional[dict] = None
    counts: dict
    files: list[FileProgress]


class Event(BaseModel):
    id: int
    ts: Optional[str]
    path: Optional[str]
    status: Optional[int]
    method: Optional[str]
    source_code: str
    dev: int
    inode: int
    byte_offset: int
    raw_line: str
    ingested_at: str


class EventPage(BaseModel):
    total: int
    limit: int
    offset: int
    next_offset: Optional[int]
    items: list[Event]


class BatchItem(BaseModel):
    event_id: int = Field(..., description="入仓行 id（log_events.id，全局入仓顺序）")
    ts: Optional[str] = Field(None, description="请求时间（日志中的 time 字段）")
    path: Optional[str] = Field(None, description="请求路径（去掉 query string）")
    status: Optional[int] = Field(None, description="HTTP 状态码")
    source_code: str = Field(..., description="来源文件身份：文件名/归档名")
    dev: int = Field(..., description="来源文件所在设备号（st_dev）")
    inode: int = Field(..., description="来源文件 inode（st_ino）")
    byte_offset: int = Field(..., description="该行在来源文件中的起始字节位置")


class BatchClaimResponse(BaseModel):
    batch_no: Optional[int] = Field(
        None, description="批次号；无新数据时为 null")
    token: Optional[str] = Field(
        None, description="领取令牌；确认时原样回传。无新数据时为 null")
    leased_at: str = Field(..., description="本次领取（发令牌）的 UTC 时间")
    lease_expires_at: Optional[str] = Field(
        None, description="租约到期 UTC 时间；到期后本批可被重新领取")
    lease_seconds: float = Field(..., description="租约时长（秒）")
    reclaimed: bool = Field(
        ..., description="是否为租约到期后的重新领取（同一批、新令牌）")
    row_count: int = Field(..., description="本批行数，最多 100")
    first_event_id: Optional[int] = Field(
        None, description="本批首行 event_id；无新数据时为 null")
    last_event_id: Optional[int] = Field(
        None, description="本批末行 event_id；无新数据时为 null")
    next_event_id: int = Field(
        ..., description="确认本批后持久游标将指向的 event_id")
    items: list[BatchItem] = Field(
        ..., description="按入仓顺序排列的请求行；仅含已提交事务中的行")


class BatchConfirmResponse(BaseModel):
    batch_no: int
    token: str
    status: str = Field(..., description="固定为 confirmed")
    confirmed_at: str
    row_count: int
    first_event_id: int
    last_event_id: int
    next_event_id: int = Field(
        ..., description="已推进后的持久游标（下一批首行 event_id）")


class OutstandingClaim(BaseModel):
    batch_no: int
    token: str
    first_event_id: int
    last_event_id: int
    row_count: int
    leased_at: str
    lease_expires_at: str


class BatchState(BaseModel):
    next_event_id: int = Field(..., description="持久游标：下一未确认行的 event_id")
    total_events: int = Field(..., description="入仓总行数")
    unclaimed_events: int = Field(..., description="游标之后尚未领取的行数")
    outstanding_claim: Optional[OutstandingClaim] = Field(
        None, description="当前未确认的在租批次；没有则为 null")


class BatchError(BaseModel):
    detail: dict


state: dict = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = storage.connect(DB_PATH)
    storage.init_db(conn)

    loop = CollectorLoop(conn, LOG_DIR, interval=POLL_INTERVAL)
    thread = threading.Thread(target=loop.run_forever, name="collector",
                              daemon=True)
    thread.start()
    state.update(conn=conn, loop=loop, thread=thread)
    log.info("API ready; watching %s, db %s", LOG_DIR, DB_PATH)
    try:
        yield
    finally:
        log.info("shutdown requested, stopping collector loop...")
        loop.request_stop()
        thread.join(timeout=10)
        # A checkpoint persists WAL frames into the main db file on exit.
        try:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except Exception:
            log.exception("checkpoint on shutdown failed")
        conn.close()
        log.info("shutdown complete")


app = FastAPI(
    title="Nginx 访问日志入仓 API",
    description=(
        "持续采集共享只读目录中的 JSON 行 Nginx 日志，按 (设备, inode) "
        "识别文件、行与续采偏移同事务提交，支持按路径与状态码分页查询故障请求。\n\n"
        "## 故障分析作业批次领取/确认\n"
        "两个作业实例通过 `/batches/claim` 按入仓顺序每次领取最多 100 行"
        "（仅已提交事务的行），拿到不透明令牌与租约；处理完成后用同一令牌调用 "
        "`/batches/{token}/confirm`，确认成功才推进持久游标。\n\n"
        "- 租约内重复领取：返回**同一批、同一令牌**，两个实例并发领取不会拆批。\n"
        "- 租约到期：同批可被重新领取但**更换令牌**，旧令牌确认会被拒绝（409）。\n"
        "- 同令牌重复确认：幂等，返回**首次确认的原结果**。\n"
        "- 所有租约/游标状态持久化在 SQLite 中，服务重启后未确认批仍可重领。\n"
        "- 新入仓的行只进入后续批次，响应绝不会包含未提交的行。"
    ),
    version="1.1.0",
    lifespan=lifespan,
    openapi_tags=[
        {"name": "meta", "description": "存活与运行状态"},
        {"name": "ingest", "description": "持续采集控制与进度"},
        {"name": "query", "description": "原始采集查询（保留原有分页/过滤接口）"},
        {"name": "batch-delivery",
         "description": "故障分析作业的批次领取与确认（租约 + 持久游标）"},
    ],
)


def _conn():
    return state["conn"]


@app.get("/health", response_model=Health, tags=["meta"])
def health():
    loop: CollectorLoop = state["loop"]
    return Health(
        status="ok",
        log_dir=LOG_DIR,
        db_path=DB_PATH,
        poll_interval_seconds=POLL_INTERVAL,
        collector_running=threading.main_thread().is_alive() and not loop._stop,
        total_polls=loop.total_polls,
        total_ingested=loop.total_ingested,
        counts=storage.counts(_conn()),
    )


@app.get("/ingest/progress", response_model=ProgressResponse, tags=["ingest"])
def progress():
    conn = _conn()
    loop: CollectorLoop = state["loop"]
    rows = storage.all_states(conn)
    # Current sizes from the live directory, read-only.
    current: dict[tuple[int, int], int] = {}
    try:
        for name in os.listdir(LOG_DIR):
            try:
                st = os.stat(os.path.join(LOG_DIR, name))
            except FileNotFoundError:
                continue
            current[(st.st_dev, st.st_ino)] = st.st_size
    except FileNotFoundError:
        pass

    files = []
    for row in rows:
        size = current.get((row["dev"], row["inode"]))
        files.append(FileProgress(
            source_code=row["source_code"],
            dev=row["dev"],
            inode=row["inode"],
            path=row["path"],
            offset=row["offset"],
            size=size,
            complete=(size is not None and row["offset"] >= size),
            last_seen=row["last_seen"],
        ))
    return ProgressResponse(
        collector_running=not loop._stop,
        total_polls=loop.total_polls,
        total_ingested=loop.total_ingested,
        last_poll=loop.last_result,
        counts=storage.counts(conn),
        files=files,
    )


@app.post("/ingest/poll-now", tags=["ingest"])
def poll_now():
    """Force an immediate scan/ingest pass (useful for tests/demos)."""
    loop: CollectorLoop = state["loop"]
    with loop.write_lock:
        return poll_once(_conn(), LOG_DIR)


@app.get("/events", response_model=EventPage, tags=["query"])
def list_events(
    path: Optional[str] = Query(None, description="精确路径，例如 /api/orders"),
    status: Optional[int] = Query(None, description="精确状态码，例如 500"),
    status_min: Optional[int] = Query(None, description="状态码下界，例如 400"),
    status_max: Optional[int] = Query(None, description="状态码上界，例如 599"),
    source_code: Optional[str] = Query(None, description="来源文件代号，例如 access.log.20261113T100000.log"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """按路径、状态码分页取数。查故障请求示例：
    ``/events?status_min=500&status_max=599&limit=50``。"""
    return storage.query_events(
        _conn(),
        path=path,
        status=status,
        status_min=status_min,
        status_max=status_max,
        source_code=source_code,
        limit=limit,
        offset=offset,
    )


@app.get("/events/{event_id}", response_model=Event, tags=["query"])
def get_event(event_id: int):
    row = _conn().execute(
        "SELECT * FROM log_events WHERE id = ?", (event_id,)
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="event not found")
    return dict(row)


@app.get("/malformed", tags=["query"])
def list_malformed(limit: int = Query(100, ge=1, le=1000),
                   offset: int = Query(0, ge=0)):
    conn = _conn()
    total = conn.execute(
        "SELECT COUNT(*) c FROM malformed_lines"
    ).fetchone()["c"]
    rows = conn.execute(
        "SELECT * FROM malformed_lines ORDER BY id LIMIT ? OFFSET ?",
        (limit, offset),
    ).fetchall()
    return {"total": total, "limit": limit, "offset": offset,
            "items": [dict(r) for r in rows]}


# ---------------------------------------------------------------------------
# Failure-analysis batch delivery
# ---------------------------------------------------------------------------

@app.post(
    "/batches/claim",
    response_model=BatchClaimResponse,
    tags=["batch-delivery"],
    summary="领取（或重复领取）下一个批次，最多 100 行",
    responses={
        409: {"description": "持久游标与在租批次不一致（不应发生的保护性错误）",
              "model": BatchError},
    },
)
def claim_batch():
    """按入仓顺序（`log_events.id` 升序）领取最多 100 行已提交请求。

    典型流程（两个作业实例都适用）：

    1. `POST /batches/claim` 取得 `token`、`lease_expires_at` 与 `items`。
    2. 在租约到期前处理这批故障请求。
    3. `POST /batches/{token}/confirm` 确认；确认成功后持久游标才推进。

    语义保证：

    * **租约未到期重复领取**：返回完全相同的批次与令牌（`reclaimed=false`），
      因此两个实例并发领取不会被分到不同批次。
    * **租约到期后领取**：同一 id 区间被重新领取，`reclaimed=true` 且令牌换新；
      旧令牌立即失效，不能再确认。
    * **暂无可领取行**：返回 `row_count=0`、`token=null`、空 `items`，
      不建立租约；新入仓并提交的行留待后续批次。
    * 每行只包含已提交事务的数据，并带来源文件身份
      （`source_code`/`dev`/`inode`）与字节位置 `byte_offset`。
    """
    try:
        return delivery.claim_batch(
            DB_PATH, lease_seconds=BATCH_LEASE_SECONDS
        )
    except delivery.DeliveryError as exc:
        raise HTTPException(
            status_code=exc.status_code,
            detail={"code": exc.code, "message": exc.message},
        )


@app.post(
    "/batches/{token}/confirm",
    response_model=BatchConfirmResponse,
    tags=["batch-delivery"],
    summary="用有效令牌确认批次，推进持久游标",
    responses={
        404: {"description": "令牌不存在（从未签发）", "model": BatchError},
        409: {"description": "令牌已失效：租约到期被新令牌取代，或已过期",
              "model": BatchError},
    },
)
def confirm_batch(token: str):
    """确认一个已领取的批次。

    * 令牌有效：游标在同一事务中推进到 `last_event_id + 1`，返回确认结果。
    * **同一令牌重复确认**：幂等，返回首次确认时的原始结果（含原
      `confirmed_at`），游标不会重复推进。
    * 旧令牌（租约到期后已重新领取）：返回 409，游标保持不动。
    * 未知令牌：返回 404。
    """
    try:
        return delivery.confirm_batch(DB_PATH, token)
    except delivery.DeliveryError as exc:
        raise HTTPException(
            status_code=exc.status_code,
            detail={"code": exc.code, "message": exc.message},
        )


@app.get(
    "/batches/state",
    response_model=BatchState,
    tags=["batch-delivery"],
    summary="持久游标与当前在租批次（只读观测）",
)
def batch_state():
    """查看交付进度：持久游标位置、尚未领取的行数、当前未确认的在租批次。"""
    return delivery.delivery_state(DB_PATH)
