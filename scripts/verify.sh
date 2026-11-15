#!/usr/bin/env bash
# 校验入仓完整性：每个被追踪文件的已提交 offset 是否等于其当前大小，
# 并打印事件总数、坏行数、各来源计数。
set -euo pipefail
API="${API:-http://localhost:8000}"

echo "== health =="
curl -fsS "$API/health" | python3 -m json.tool

echo
echo "== per-file progress =="
curl -fsS "$API/ingest/progress" | python3 -c '
import json, sys
d = json.load(sys.stdin)
ok = True
for f in d["files"]:
    state = "complete" if f["complete"] else "PENDING"
    if not f["complete"] and f["size"] is not None:
        ok = False
    print(f"  [{state:7}] {f[\"source_code\"]:<40} "
          f"offset={f[\"offset\"]} size={f[\"size\"]} inode={f[\"inode\"]}")
print()
print("counts:", d["counts"])
sys.exit(0 if ok else 1)
'

echo
echo "== 5xx failures (first page) =="
curl -fsS "$API/events?status_min=500&status_max=599&limit=10" \
  | python3 -c '
import json, sys
d = json.load(sys.stdin)
print(f"total 5xx: {d[\"total\"]}")
for i in d["items"]:
    print(f"  {i[\"status\"]} {i[\"path\"]:<24} src={i[\"source_code\"]} @{i[\"byte_offset\"]}")
'
