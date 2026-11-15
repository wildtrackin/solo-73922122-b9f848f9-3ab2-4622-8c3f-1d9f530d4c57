"""Batch claim/confirm delivery for the failure-analysis job.

Why not offset pagination on ``/events``?
-----------------------------------------
The analysis job runs two instances and its processing may time out. Offset
pagination cannot express "hand this row to exactly one worker and retry it if
the worker dies". This module implements a durable, leased work queue on top
of the already-committed ``log_events`` table:

* **One outstanding batch at a time.** The persistent delivery cursor
  (``delivery_cursor.next_event_id``) points at the first row never handed out
  in a confirmed batch. A claim reads at most ``BATCH_SIZE`` committed rows in
  warehouse order (``log_events.id``).
* **Leases, not locks across processes.** A claim gets an opaque token valid
  until ``lease_expires_at``. Re-claiming before expiry returns the *same*
  batch with the *same* token. After expiry the same id range is reclaimed with
  a *new* token and the old lease is superseded, so an old token can never
  confirm a batch that was handed to someone else.
* **Confirmation advances the cursor, once.** Only the currently valid token
  may confirm; confirmation moves the cursor past the batch in the same
  transaction. Confirming the same token again is idempotent and returns the
  original result.
* **Crash-safe by construction.** All lease/cursor state lives in SQLite
  (``synchronous=FULL``). A service restart changes nothing: an unconfirmed
  batch remains reclaimable, its lease still expires on wall-clock time.
* **No uncommitted rows can leak.** Rows enter ``log_events`` only inside the
  ingestor's committed transaction (see :mod:`app.ingest`). A batch is a fixed
  id range ``[first_event_id, last_event_id]`` captured at claim time; rows
  ingested afterwards have higher ids and can only appear in later batches.
"""

from __future__ import annotations

import json
import logging
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional

from . import storage

log = logging.getLogger("collector.delivery")

# Hard requirement: at most 100 warehouse rows per batch, in ingest order.
BATCH_SIZE = 100


class DeliveryError(Exception):
    """Base class; ``status_code`` maps directly onto the HTTP response."""

    status_code = 409

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


class InvalidToken(DeliveryError):
    """Token is unknown / never issued by this service."""

    status_code = 404


class StaleToken(DeliveryError):
    """Token was superseded by a re-lease after expiry or already replaced."""

    status_code = 409


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    # Millisecond precision is enough for lease bookkeeping and keeps the API
    # output stable/readable; SQLite stores it as TEXT regardless.
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def _parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _new_token() -> str:
    # 256 bits of entropy; opaque, unguessable, carries no server state.
    return secrets.token_urlsafe(32)


@dataclass
class ClaimedRow:
    event_id: int
    ts: Optional[str]
    path: Optional[str]
    status: Optional[int]
    source_code: str
    dev: int
    inode: int
    byte_offset: int


def _load_rows(conn, first_id: int, last_id: int) -> list[ClaimedRow]:
    """Fetch the fixed id range. Only committed rows are visible (WAL)."""
    rows = conn.execute(
        """SELECT id, ts, path, status, source_code, dev, inode, byte_offset
             FROM log_events
            WHERE id BETWEEN ? AND ?
            ORDER BY id""",
        (first_id, last_id),
    ).fetchall()
    return [
        ClaimedRow(
            event_id=r["id"],
            ts=r["ts"],
            path=r["path"],
            status=r["status"],
            source_code=r["source_code"],
            dev=r["dev"],
            inode=r["inode"],
            byte_offset=r["byte_offset"],
        )
        for r in rows
    ]


def _row_payload(row: ClaimedRow) -> dict[str, Any]:
    # Exactly the fields the failure-analysis job needs, including the physical
    # source identity (file + byte position) so results can be traced back.
    return {
        "event_id": row.event_id,
        "ts": row.ts,
        "path": row.path,
        "status": row.status,
        "source_code": row.source_code,
        "dev": row.dev,
        "inode": row.inode,
        "byte_offset": row.byte_offset,
    }


def claim_batch(
    db_path: str,
    *,
    lease_seconds: float,
    now: Optional[Callable[[], datetime]] = None,
) -> dict[str, Any]:
    """Claim the next batch (or re-claim the outstanding one).

    Runs in a single ``BEGIN IMMEDIATE`` transaction, so concurrent callers —
    including the two analysis instances hitting the endpoint at the same
    instant — are serialised and always observe/return the same batch.
    """
    now_fn = now or _utc_now
    conn = storage.open_connection(db_path)
    try:
        with storage.transaction(conn):
            moment = now_fn()
            cursor_row = conn.execute(
                "SELECT next_event_id FROM delivery_cursor WHERE id = 1"
            ).fetchone()
            next_event_id = cursor_row["next_event_id"]

            latest = conn.execute(
                "SELECT * FROM batch_claims ORDER BY batch_no DESC LIMIT 1"
            ).fetchone()

            if latest is not None and latest["status"] == "leased":
                # Defence in depth: a live lease must always start exactly at
                # the cursor. If that ever stops holding, refuse to reuse it
                # rather than silently skipping or duplicating rows.
                if latest["first_event_id"] != next_event_id:
                    raise DeliveryError(
                        "cursor_lease_mismatch",
                        "outstanding lease does not match the delivery cursor",
                    )
                if _parse_iso(latest["lease_expires_at"]) > moment:
                    # Lease still valid: every claimant gets the identical
                    # batch and the identical token.
                    rows = _load_rows(
                        conn, latest["first_event_id"], latest["last_event_id"]
                    )
                    return _claim_response(latest, rows, reclaimed=False)

                # Lease expired: supersede the old token, keep the same id
                # range so no row is skipped, issue a brand-new token.
                conn.execute(
                    "UPDATE batch_claims SET status = 'expired' "
                    "WHERE batch_no = ?",
                    (latest["batch_no"],),
                )
                first_id = latest["first_event_id"]
                last_id = latest["last_event_id"]
                row_count = latest["row_count"]
                is_reclaimed = True
                log.info(
                    "batch %s lease expired; re-claiming same id range "
                    "[%s, %s] with a new token",
                    latest["batch_no"], first_id, last_id,
                )
            else:
                # No outstanding lease: either the very first claim or a brand
                # new range after the previous batch was confirmed/expired.
                first_id = next_event_id
                is_reclaimed = False
                # Snapshot the committed high-water mark. Rows committed after
                # this point get higher AUTOINCREMENT ids and stay out of this
                # batch even if ingestion commits during the transaction.
                bound = conn.execute(
                    "SELECT COALESCE(MAX(id), 0) AS max_id FROM log_events"
                ).fetchone()["max_id"]
                if first_id > bound:
                    # Nothing new to deliver. Do not create a lease: the next
                    # claim after more rows land simply starts at the cursor.
                    return {
                        "batch_no": None,
                        "token": None,
                        "leased_at": _iso(moment),
                        "lease_expires_at": None,
                        "lease_seconds": lease_seconds,
                        "reclaimed": False,
                        "row_count": 0,
                        "next_event_id": first_id,
                        "items": [],
                    }
                last_id = min(first_id + BATCH_SIZE - 1, bound)
                row_count = conn.execute(
                    "SELECT COUNT(*) AS c FROM log_events "
                    "WHERE id BETWEEN ? AND ?",
                    (first_id, last_id),
                ).fetchone()["c"]

            expires = moment + timedelta(seconds=lease_seconds)
            token = _new_token()
            cur = conn.execute(
                """INSERT INTO batch_claims
                       (token, first_event_id, last_event_id, row_count,
                        status, leased_at, lease_expires_at)
                   VALUES (?, ?, ?, ?, 'leased', ?, ?)""",
                (token, first_id, last_id, row_count,
                 _iso(moment), _iso(expires)),
            )
            batch_no = cur.lastrowid
            claim_row = conn.execute(
                "SELECT * FROM batch_claims WHERE batch_no = ?", (batch_no,)
            ).fetchone()
            rows = _load_rows(conn, first_id, last_id)
            response = _claim_response(
                claim_row, rows, reclaimed=is_reclaimed,
            )
            return response
    finally:
        conn.close()


def _claim_response(claim_row, rows: list[ClaimedRow], *, reclaimed: bool) -> dict:
    return {
        "batch_no": claim_row["batch_no"],
        "token": claim_row["token"],
        "leased_at": claim_row["leased_at"],
        "lease_expires_at": claim_row["lease_expires_at"],
        "lease_seconds": (
            _parse_iso(claim_row["lease_expires_at"])
            - _parse_iso(claim_row["leased_at"])
        ).total_seconds(),
        # True when this token replaces an expired lease on the same id range.
        "reclaimed": reclaimed,
        "row_count": claim_row["row_count"],
        "first_event_id": claim_row["first_event_id"],
        "last_event_id": claim_row["last_event_id"],
        "next_event_id": claim_row["last_event_id"] + 1,
        "items": [_row_payload(r) for r in rows],
    }


def confirm_batch(
    db_path: str,
    token: str,
    *,
    now: Optional[Callable[[], datetime]] = None,
) -> dict[str, Any]:
    """Confirm a claimed batch with a currently valid token.

    * valid current token  -> cursor advances past the batch, idempotent store
    * same token again     -> original confirmation result, unchanged
    * superseded/old token -> 409, cursor untouched
    * unknown token        -> 404
    """
    now_fn = now or _utc_now
    conn = storage.open_connection(db_path)
    try:
        with storage.transaction(conn):
            moment = now_fn()
            claim_row = conn.execute(
                "SELECT * FROM batch_claims WHERE token = ?", (token,)
            ).fetchone()
            if claim_row is None:
                raise InvalidToken(
                    "unknown_token",
                    "no batch claim matches this token; request a new batch",
                )

            if claim_row["status"] == "confirmed":
                # Idempotent replay: return the exact stored result.
                return json.loads(claim_row["result_json"])

            if claim_row["status"] == "expired":
                raise StaleToken(
                    "superseded_token",
                    "this token was invalidated when the batch was reclaimed "
                    "after its lease expired; use the current token",
                )

            # status == 'leased'
            if _parse_iso(claim_row["lease_expires_at"]) <= moment:
                # Wall-clock expiry wins even if the row has not been re-leased
                # yet: mark it expired so the next claim re-issues a token.
                conn.execute(
                    "UPDATE batch_claims SET status = 'expired' "
                    "WHERE batch_no = ?",
                    (claim_row["batch_no"],),
                )
                raise StaleToken(
                    "lease_expired",
                    "the lease for this token has expired; re-claim the batch",
                )

            cursor_row = conn.execute(
                "SELECT next_event_id FROM delivery_cursor WHERE id = 1"
            ).fetchone()
            if cursor_row["next_event_id"] != claim_row["first_event_id"]:
                # Should be impossible given the one-outstanding-batch design,
                # but never advance a cursor past a range it no longer points at.
                raise StaleToken(
                    "cursor_advanced",
                    "the delivery cursor no longer matches this batch",
                )

            new_cursor = claim_row["last_event_id"] + 1
            confirmed_at = _iso(moment)
            conn.execute(
                "UPDATE delivery_cursor SET next_event_id = ?, updated_at = ? "
                "WHERE id = 1",
                (new_cursor, confirmed_at),
            )
            result = {
                "batch_no": claim_row["batch_no"],
                "token": claim_row["token"],
                "status": "confirmed",
                "confirmed_at": confirmed_at,
                "row_count": claim_row["row_count"],
                "first_event_id": claim_row["first_event_id"],
                "last_event_id": claim_row["last_event_id"],
                "next_event_id": new_cursor,
            }
            conn.execute(
                "UPDATE batch_claims SET status = 'confirmed', "
                "confirmed_at = ?, result_json = ? WHERE batch_no = ?",
                (confirmed_at, json.dumps(result, ensure_ascii=False),
                 claim_row["batch_no"]),
            )
            log.info(
                "batch %s confirmed (%s rows); cursor -> %s",
                claim_row["batch_no"], claim_row["row_count"], new_cursor,
            )
            return result
    finally:
        conn.close()


def delivery_state(db_path: str) -> dict[str, Any]:
    """Read-only observability: cursor position and the outstanding lease."""
    conn = storage.open_connection(db_path)
    try:
        cursor_row = conn.execute(
            "SELECT * FROM delivery_cursor WHERE id = 1"
        ).fetchone()
        latest = conn.execute(
            "SELECT * FROM batch_claims ORDER BY batch_no DESC LIMIT 1"
        ).fetchone()
        total_events = conn.execute(
            "SELECT COUNT(*) AS c FROM log_events"
        ).fetchone()["c"]
        outstanding = None
        if latest is not None and latest["status"] == "leased":
            outstanding = {
                "batch_no": latest["batch_no"],
                "token": latest["token"],
                "first_event_id": latest["first_event_id"],
                "last_event_id": latest["last_event_id"],
                "row_count": latest["row_count"],
                "leased_at": latest["leased_at"],
                "lease_expires_at": latest["lease_expires_at"],
            }
        next_id = cursor_row["next_event_id"]
        # Rows with id >= cursor that exist in the warehouse. A leased but
        # unconfirmed batch is still counted as unclaimed until confirmed.
        unclaimed = conn.execute(
            "SELECT COUNT(*) AS c FROM log_events WHERE id >= ?", (next_id,)
        ).fetchone()["c"]
        return {
            "next_event_id": next_id,
            "total_events": total_events,
            "unclaimed_events": unclaimed,
            "outstanding_claim": outstanding,
        }
    finally:
        conn.close()
