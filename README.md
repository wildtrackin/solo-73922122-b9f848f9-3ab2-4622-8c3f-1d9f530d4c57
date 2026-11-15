# Nginx 访问日志持续入仓（FastAPI + SQLite）

把 Nginx 的 JSON 行访问日志从共享只读目录**持续、精确一次（exactly-once）**
地采集进 SQLite，并提供按路径、状态码分页检索故障请求的 HTTP API。

故障分析作业（两个实例、处理可能超时）不依赖 `/events` 的 offset 翻页，而是
通过**批次领取/确认 API** 按入仓顺序领取已提交的请求：租约内重复领取返回同一批、
租约到期换令牌重领、有效令牌确认后才推进持久游标，服务重启后未确认批仍可重领。

## 为什么不会漏行 / 重记 / 错用偏移

| 风险 | 设计对策 |
| --- | --- |
| 轮转后把旧偏移用到新文件 | 文件身份用 `(st_dev, st_ino)`，**不是文件名**。rename 后旧 inode 与新 inode 各自独立追踪 |
| 旧文件轮转后仍被短暂写入 | 旧 inode 继续按已提交偏移采集到 EOF；归档永不删除、不截断 |
| 读到写了一半的行 | 只入仓以 `\n` 结尾的完整行；未换行尾部保持原偏移，下轮重读 |
| 进程在写入中途/前后退出 | 行数据与“下一字节偏移”在**同一个 SQLite 事务**提交，重启从最后提交点续采 |
| 查询读到半成品 | WAL + 事务隔离，查询只能看到已完整提交的事务 |
| 内容相同的两行 | 唯一性只约束物理位置 `(dev,inode,source_code,byte_offset)`，不含内容，重复内容各自成行 |
| JSON 坏行 | 隔离进 `malformed_lines` 表，不阻断后续行 |

## 目录结构

```
.
├── docker-compose.yml      # nginx + collector + traffic 一键启动
├── collector/              # FastAPI + SQLite 采集/查询服务
│   ├── app/storage.py      # 表结构、事务、查询（含交付游标/租约表）
│   ├── app/ingest.py       # 目录扫描、inode 识别、完整行切分、续采
│   ├── app/delivery.py     # 批次领取/确认：租约、令牌、持久游标
│   └── app/main.py         # API（/docs 可直接试领取与确认）
├── nginx/                  # JSON 行日志格式 + 可控轮转脚本
├── traffic/                # 持续请求生成器（含 4xx/5xx）
└── tests/                  # 单元/集成断言 + 端到端模拟
```

## 快速开始

```bash
docker compose build
docker compose up -d
# 实时看日志
docker compose logs -f collector
```

| 服务 | 地址 | 说明 |
| --- | --- | --- |
| API 文档 Swagger | http://localhost:8000/docs | 直接在页面上试所有接口 |
| 示例站点 | http://localhost:8080/ | Nginx，访问即产生 JSON 日志 |
| OpenAPI JSON | http://localhost:8000/openapi.json | |

### 可控轮转（手动）

```bash
# 随时手动轮转：rename 成唯一归档名 + nginx 重开同名新日志
docker compose exec nginx /usr/local/bin/rotate-nginx-log.sh

# 自定义归档后缀
docker compose exec nginx ROTATE_NAME=incident-1 \
    /usr/local/bin/rotate-nginx-log.sh
```

### 自动周期轮转（演示轮转与采集交错）

```bash
# 每 15 秒自动轮转一次
AUTO_ROTATE_INTERVAL=15 docker compose up -d
# 恢复纯手动
docker compose up -d --force-recreate nginx
```

轮转后共享目录里同时存在 `access.log` 与所有
`access.log.<时间戳>.log` 归档，采集器分别追踪、全部保留。

## 常用查询

```bash
# 采集进度（每个文件的 offset / size、是否采完）
curl -s localhost:8000/ingest/progress | python3 -m json.tool

# 所有 5xx 故障请求，分页
curl -s "localhost:8000/events?status_min=500&status_max=599&limit=50"

# 某路径的 404
curl -s "localhost:8000/events?path=/missing/page&status=404"

# 只看某个归档来源
curl -s "localhost:8000/events?source_code=access.log.20261113T100000Z.log"

# 翻页
curl -s "localhost:8000/events?limit=100&offset=200"

# 立即触发一次采集（默认每 0.5s 自动轮询）
curl -s -X POST localhost:8000/ingest/poll-now

# 无法解析的坏行
curl -s localhost:8000/malformed
```

## 故障分析作业：批次领取与确认

两个作业实例（或实例内的工作线程）按下述循环消费，**不需要也不应该**用
`/events` 的 offset 自行翻页：

```bash
# 1) 领取：按入仓顺序最多 100 行，返回令牌与租约
curl -s -X POST localhost:8000/batches/claim | python3 -m json.tool

# 2) 处理 items 中的故障请求（可能超时）……

# 3) 确认：有效令牌确认后持久游标才推进
curl -s -X POST localhost:8000/batches/<token>/confirm | python3 -m json.tool

# 观测：持久游标位置、未领取行数、当前在租批次
curl -s localhost:8000/batches/state | python3 -m json.tool
```

### 交付语义（故障分析作业依赖的保证）

| 场景 | 行为 |
| --- | --- |
| 领取批次 | 按 `log_events.id`（入仓顺序）返回**最多 100 行**已提交请求，含请求时间 `ts`、路径 `path`、状态码 `status` 及来源文件身份 `source_code`/`dev`/`inode` 与字节位置 `byte_offset` |
| 租约内重复领取 | 返回**同一批、同一令牌**（`reclaimed=false`）；两个实例并发领取在 SQLite `BEGIN IMMEDIATE` 写锁上串行化，**绝不会拆成不同批** |
| 租约到期后领取 | 同一 id 区间重新领取，`reclaimed=true` 且**更换令牌**；旧令牌立即置为 `expired`，再确认返回 **409** |
| 有效令牌确认 | 游标在同一事务中推进到 `last_event_id + 1`；确认前任何崩溃都不会跳过行 |
| 同令牌重复确认 | 幂等，返回**首次确认的原结果**（含原 `confirmed_at`），游标不重复推进 |
| 未知令牌确认 | 返回 **404** |
| 服务重启 | 租约与游标全部持久化在 SQLite（`synchronous=FULL`），未确认批重启后仍可重领；租约按墙钟时间到期，停机时间也计入 |
| 领取后新入仓的行 | 只进入**后续批次**；批次是领取时固化的 id 区间，且仅含已提交事务，响应不可能出现未提交行 |
| 暂无可领取行 | 返回 `row_count=0`、`token=null`、空 `items`，不建立租约 |

### 响应示例

```jsonc
// POST /batches/claim
{
  "batch_no": 1,
  "token": "k3nP...urlsafe...",
  "leased_at": "2026-11-15T03:20:00.123Z",
  "lease_expires_at": "2026-11-15T03:20:30.123Z",
  "lease_seconds": 30.0,
  "reclaimed": false,
  "row_count": 100,
  "first_event_id": 1,
  "last_event_id": 100,
  "next_event_id": 101,
  "items": [
    {
      "event_id": 1,
      "ts": "2026-11-15T03:19:58Z",
      "path": "/api/orders",
      "status": 500,
      "source_code": "access.log",
      "dev": 2049,
      "inode": 1179652,
      "byte_offset": 0
    }
  ]
}
```

完整字段与错误码可在 Swagger（http://localhost:8000/docs，标签
`batch-delivery`）页面直接试跑。

## API 一览

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| GET | `/health` | 存活、计数、采集线程状态 |
| GET | `/ingest/progress` | 各来源文件 offset/size/是否完成 |
| POST | `/ingest/poll-now` | 立即执行一轮采集 |
| GET | `/events` | 分页取数，支持 `path`、`status`、`status_min/max`、`source_code`、`limit`、`offset`（保留原有采集查询） |
| GET | `/events/{id}` | 单条事件（含原始 JSON 行 `raw_line`） |
| GET | `/malformed` | 坏行隔离区 |
| POST | `/batches/claim` | 领取/重复领取下一批（最多 100 行，带令牌与租约） |
| POST | `/batches/{token}/confirm` | 确认批次并推进持久游标；同令牌幂等 |
| GET | `/batches/state` | 持久游标、未领取行数、当前在租批次 |

## 数据模型

```
log_events(id, ts, path, status, method, source_code,
           dev, inode, byte_offset, raw_line, ingested_at)
ingest_state(dev, inode, source_code, path, offset, last_seen)  -- 续采位置
malformed_lines(..., raw_line, error)                            -- 坏行隔离
delivery_cursor(id=1, next_event_id, updated_at)                 -- 交付持久游标
batch_claims(batch_no, token, first_event_id, last_event_id,
             row_count, status[leased/confirmed/expired],
             leased_at, lease_expires_at, confirmed_at, result_json)
```

- `source_code`：来源文件代号（当前文件名/归档名）。
- `byte_offset`：该行在其来源文件中的起始字节位置。
- 唯一索引 `(dev, inode, source_code, byte_offset)` 是防重的最后防线；
  内容不在键中，所以两行完全相同也分别保留。
- `delivery_cursor.next_event_id` 只在有效令牌确认时推进；`batch_claims`
  保留每次发放的租约历史，到期重领会把旧租约置 `expired` 并新发令牌，
  确认结果原样存入 `result_json` 以支持幂等重放。
- SQLite 运行在 `WAL + synchronous=FULL`：提交即落盘，读不阻塞写。

## 配置（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `LOG_DIR` | `/logs` | 共享日志目录（compose 中以 `:ro` 只读挂载） |
| `DB_PATH` | `/data/warehouse.db` | SQLite 数据库（独立可写卷） |
| `POLL_INTERVAL` | `0.5` | 轮询间隔秒 |
| `BATCH_LEASE_SECONDS` | `30` | 批次令牌租约秒数；到期后同批换新令牌重领、旧令牌失效 |
| `LOG_LEVEL` | `INFO` | 日志级别 |
| `AUTO_ROTATE_INTERVAL` | 空 | 仅 nginx 容器；非空时按秒自动轮转 |

## 本地测试（无需 Docker）

```bash
python3 -m venv --without-pip .venv && . .venv/bin/activate
# 若 venv 内无 pip：python get-pip.py
pip install -r collector/requirements.txt httpx pyyaml
python -I tests/test_collector.py   # 46 项：部分行/轮转/崩溃恢复/重复行/API
python -I tests/test_delivery.py    # 87 项：批次上限/租约/令牌失效/并发不拆批/重启重领/未提交隔离
python -I tests/e2e_sim.py          # 完整服务启停 + 轮转 + 重启续采 + 批次领取确认 E2E
```

覆盖的关键场景：未换行尾部等待补齐、rename 轮转双 inode、旧 inode 延迟写入、
20 轮随机“进程退出”后续采不漏不重、相同内容多行保留、坏行隔离、分页与过滤；
批次侧覆盖租约内同批同令牌、到期换令牌且旧令牌 409、确认幂等、8 路并发领取
不拆批、重启后未确认批可重领、未提交/新入仓行不串批。

## 故障排查

```bash
docker compose ps
docker compose logs --tail=100 collector
docker compose exec nginx ls -li /var/log/nginx/json   # 看 inode 与归档
docker compose exec collector sh -c 'ls -li /logs'      # 只读视角
# 入库数与文件总字节核对
curl -s localhost:8000/ingest/progress
```
