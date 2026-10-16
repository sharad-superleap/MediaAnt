"""
Superleap Bulk Field Update
===========================
Reads record ids from a CSV and sets one or more fields on every record of an
object to fixed values, in batches, via bulk_upsert.

Configure OBJECT_SLUG and FIELD_UPDATES below:

    FIELD_UPDATES = {
        "usid": "",            # "" → field is cleared (sent as BLANK_VALUE)
        "status": "Inactive",  # any other value → field set to that value
    }

Each record sent:

    {"id": "<id from csv>", "usid": null, "status": "Inactive",
     "ignore_update": <run epoch ms>}

Usage
-----
    python3 updateUSIDToBlank.py ids.csv
    python3 updateUSIDToBlank.py ids.csv --dry-run --limit 10
    python3 updateUSIDToBlank.py ids.csv --id-column record_id --yes

Auth: export SUPERLEAP_API_TOKEN, or set API_TOKEN below.
Base URL: export SUPERLEAP_BASE_URL, set BASE_URL, or pass --base-url.

Output: <csv name>_results_<timestamp>.csv next to the input, one row per id
with status updated / failed / unknown and the error, if any. Re-run with that
file filtered to failed rows to retry them.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

import aiohttp

csv.field_size_limit(sys.maxsize)

# ─────────────────────────── CONFIG ───────────────────────────
BASE_URL = os.environ.get(
    "SUPERLEAP_BASE_URL",
    "https://aakash.superleap.com/api/v1/org/objects",
)
API_TOKEN = "t98xyD:"  # ← fill in here, or export SUPERLEAP_API_TOKEN before running

OBJECT_SLUG = "leads_object"  # ← object to update

# field slug → value. "" clears the field; anything else is sent as-is
# (use the type the field expects: str, int, bool, list for multi-select, …).
FIELD_UPDATES: Dict[str, Any] = {
    "AESL_LMS_USID__c": "",
}

# What "" is sent as. None → JSON null (clears the field). Set to "" if your
# tenant expects an empty string instead.
BLANK_VALUE: Any = None

ID_COLUMN = "id"  # CSV column holding the Superleap record id (case-insensitive)

BATCH_SIZE = 300
CONCURRENCY = 3
MAX_RETRIES = 3
RETRY_BASE_DELAY = 2
REQUEST_TIMEOUT = 180

TRANSACTION_ERROR_MARKERS = ("TRANSACTION_FAILED", "current transaction is aborted")


# ─────────────────────────── HELPERS ───────────────────────────

def get_api_token() -> str:
    token = API_TOKEN or os.environ.get("SUPERLEAP_API_TOKEN", "")
    if not token:
        raise RuntimeError("Set API_TOKEN in this script or export SUPERLEAP_API_TOKEN.")
    return token


def build_update_values() -> Dict[str, Any]:
    if not FIELD_UPDATES:
        raise RuntimeError("FIELD_UPDATES is empty — add at least one field slug.")
    reserved = {"id", "external_id", "ignore_update"} & set(FIELD_UPDATES)
    if reserved:
        raise RuntimeError(f"FIELD_UPDATES cannot contain {sorted(reserved)}.")
    return {
        slug: (BLANK_VALUE if isinstance(value, str) and value.strip() == "" else value)
        for slug, value in FIELD_UPDATES.items()
    }


def read_ids(path: str, id_column: str) -> List[str]:
    with open(path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.reader(fh)
        header = next(reader, None)
        if not header:
            raise RuntimeError(f"{path} is empty.")
        normalized = [h.strip().lower() for h in header]
        if id_column.lower() in normalized:
            idx = normalized.index(id_column.lower())
        elif len(header) == 1:
            idx = 0
            print(f"⚠ No {id_column!r} column; using the only column {header[0]!r}.")
        else:
            raise RuntimeError(f"Column {id_column!r} not found in {path}. Header: {header}")

        ids: List[str] = []
        seen: Set[str] = set()
        blanks = dupes = 0
        for row in reader:
            value = row[idx].strip() if idx < len(row) else ""
            if not value:
                blanks += 1
            elif value in seen:
                dupes += 1
            else:
                seen.add(value)
                ids.append(value)
    if blanks or dupes:
        print(f"Skipped {blanks} blank and {dupes} duplicate id(s).")
    return ids


def entry_id(entry: Any) -> str:
    if not isinstance(entry, dict):
        return ""
    for key in ("id", "record_id"):
        if entry.get(key):
            return str(entry[key])
    for nested_key in ("record", "data", "value", "payload"):
        nested = entry.get(nested_key)
        if isinstance(nested, dict) and nested.get("id"):
            return str(nested["id"])
    return ""


def entry_error(entry: Any) -> str:
    if not isinstance(entry, dict):
        return str(entry)[:1000]
    message = (
        entry.get("error")
        or entry.get("error_message")
        or entry.get("message")
        or entry.get("reason")
        or json.dumps(entry, ensure_ascii=False, default=str)
    )
    code = entry.get("error_code") or entry.get("code") or ""
    return f"[{code}] {message}"[:1000] if code else str(message)[:1000]


def parse_validation_errors(text: str) -> List[Dict[str, Any]]:
    try:
        data = json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return []
    node = data.get("data") if isinstance(data, dict) else None
    if not isinstance(node, dict):
        node = data if isinstance(data, dict) else {}
    errors = node.get("validation_errors") or []
    return [e for e in errors if isinstance(e, dict)] if isinstance(errors, list) else []


def record_index_to_offset(record_index: Any, batch_len: int) -> Optional[int]:
    try:
        idx = int(record_index)
    except (TypeError, ValueError):
        return None
    if idx == 0 and batch_len > 0:
        return 0
    if 1 <= idx <= batch_len:
        return idx - 1
    return None


# ─────────────────────────── API CALLER ───────────────────────────

@dataclass
class BatchResult:
    ok: List[str] = field(default_factory=list)          # ids updated
    failed: Dict[str, str] = field(default_factory=dict)  # id → error
    batch_error: str = ""
    batch_error_body: str = ""


async def send_batch(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    records: List[Dict[str, Any]],
    token: str,
    base_url: str,
    *,
    allow_split_retry: bool = True,
) -> BatchResult:
    url = f"{base_url}/{OBJECT_SLUG}/record/bulk_upsert"
    payload = {"values": records, "enable_partial_upsert": True}
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
    result = BatchResult()

    for attempt in range(MAX_RETRIES):
        try:
            async with semaphore:
                async with session.post(url, json=payload, headers=headers, timeout=timeout) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("success"):
                            body = data.get("data") or {}
                            for entry in body.get("failed_records") or []:
                                rid = entry_id(entry)
                                result.failed[rid or f"<unknown #{len(result.failed)}>"] = entry_error(entry)
                            for entry in (body.get("updated_records") or []) + (body.get("created_records") or []):
                                rid = entry_id(entry)
                                if rid:
                                    result.ok.append(rid)
                            return result
                        if attempt == MAX_RETRIES - 1:
                            result.batch_error = entry_error(data.get("data") or data)
                            result.batch_error_body = json.dumps(data, ensure_ascii=False)
                            return result
                    elif resp.status == 401:
                        result.batch_error = "Authentication failed — check API_TOKEN"
                        return result
                    elif resp.status == 429:
                        await asyncio.sleep(RETRY_BASE_DELAY * (2 ** attempt) * 2)
                        continue
                    else:
                        text = await resp.text()
                        if attempt < MAX_RETRIES - 1 and resp.status >= 500:
                            await asyncio.sleep(RETRY_BASE_DELAY * (2 ** attempt))
                            continue

                        # Per-record validation errors: fail those, resend the rest once.
                        bad: Set[int] = set()
                        for err in parse_validation_errors(text):
                            offset = record_index_to_offset(err.get("record_index"), len(records))
                            if offset is None:
                                continue
                            bad.add(offset)
                            msg = err.get("message") or "validation failed"
                            result.failed[str(records[offset]["id"])] = (
                                f"{err.get('field')}: {msg}" if err.get("field") else msg
                            )
                        if bad:
                            good = [r for i, r in enumerate(records) if i not in bad]
                            if allow_split_retry and good:
                                retry = await send_batch(
                                    session, semaphore, good, token, base_url,
                                    allow_split_retry=False,
                                )
                                result.ok.extend(retry.ok)
                                result.failed.update(retry.failed)
                                if retry.batch_error:
                                    for r in good:
                                        result.failed[str(r["id"])] = (
                                            f"batch rejected after removing invalid siblings: {retry.batch_error}"
                                        )
                            return result

                        result.batch_error = f"HTTP {resp.status}: {text[:1000]}"
                        result.batch_error_body = text
                        return result
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            if attempt == MAX_RETRIES - 1:
                result.batch_error = f"{type(exc).__name__}: {exc}"
                return result

        await asyncio.sleep(RETRY_BASE_DELAY * (2 ** attempt))

    result.batch_error = "Max retries exceeded"
    return result


async def send_batch_isolating(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    records: List[Dict[str, Any]],
    token: str,
    base_url: str,
) -> BatchResult:
    """send_batch, but when one record aborts the DB transaction, split the
    batch in halves and resend until the offending record(s) are isolated."""
    result = await send_batch(session, semaphore, records, token, base_url)
    blob = f"{result.batch_error} {result.batch_error_body}"
    if not result.batch_error or not any(m in blob for m in TRANSACTION_ERROR_MARKERS):
        if result.batch_error:
            for r in records:
                result.failed.setdefault(str(r["id"]), result.batch_error)
        return result
    if len(records) == 1:
        return BatchResult(failed={
            str(records[0]["id"]): f"isolated: this record aborts the transaction — {result.batch_error[:400]}"
        })
    mid = len(records) // 2
    halves = await asyncio.gather(
        send_batch_isolating(session, semaphore, records[:mid], token, base_url),
        send_batch_isolating(session, semaphore, records[mid:], token, base_url),
    )
    merged = BatchResult()
    for half in halves:
        merged.ok.extend(half.ok)
        merged.failed.update(half.failed)
    return merged


# ─────────────────────────── MAIN ───────────────────────────

async def run(args: argparse.Namespace) -> int:
    updates = build_update_values()
    ids = read_ids(args.csv, args.id_column)
    if args.limit:
        ids = ids[: args.limit]
    if not ids:
        print("No ids to update.")
        return 0

    stamp = int(time.time() * 1000)
    records = [{"id": rid, **updates, "ignore_update": stamp} for rid in ids]
    batches = [records[i: i + args.batch_size] for i in range(0, len(records), args.batch_size)]

    print(f"Object      : {OBJECT_SLUG}")
    print(f"Base URL    : {args.base_url}")
    print(f"Updates     : {json.dumps(updates, ensure_ascii=False)}")
    print(f"Records     : {len(records)} in {len(batches)} batch(es) of ≤{args.batch_size}")
    print(f"Sample      : {json.dumps(records[0], ensure_ascii=False)}")

    if args.dry_run:
        print("\nDry run — nothing sent.")
        return 0
    if not args.yes and input("\nProceed? [y/N] ").strip().lower() != "y":
        print("Aborted.")
        return 1

    token = get_api_token()
    semaphore = asyncio.Semaphore(CONCURRENCY)
    ok: Set[str] = set()
    failed: Dict[str, str] = {}
    started = time.time()

    async with aiohttp.ClientSession() as session:
        async def worker(n: int, batch: List[Dict[str, Any]]) -> None:
            res = await send_batch_isolating(session, semaphore, batch, token, args.base_url)
            ok.update(res.ok)
            failed.update(res.failed)
            print(
                f"  batch {n}/{len(batches)}: {len(res.ok)} updated, {len(res.failed)} failed"
                f"  (total {len(ok)} ✓ / {len(failed)} ✗, {time.time() - started:.0f}s)"
            )

        await asyncio.gather(*(worker(i + 1, b) for i, b in enumerate(batches)))

    base, _ = os.path.splitext(os.path.abspath(args.csv))
    out_path = f"{base}_results_{datetime.now():%Y%m%d_%H%M%S}.csv"
    with open(out_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["id", "status", "error"])
        for rid in ids:
            if rid in failed:
                writer.writerow([rid, "failed", failed[rid]])
            elif rid in ok:
                writer.writerow([rid, "updated", ""])
            else:
                writer.writerow([rid, "unknown", "not reported in API response"])
        id_set = set(ids)
        for rid, err in failed.items():
            if rid not in id_set:
                writer.writerow([rid, "failed", err])

    unknown = len(ids) - len(ok | set(failed))
    print(
        f"\nDone in {time.time() - started:.0f}s — {len(ok)} updated, {len(failed)} failed, "
        f"{max(unknown, 0)} unknown."
    )
    print(f"Results: {out_path}")
    return 0 if not failed else 2


def main() -> None:
    parser = argparse.ArgumentParser(description="Bulk-update fields on Superleap records by id.")
    parser.add_argument("csv", help="CSV with a column of Superleap record ids")
    parser.add_argument("--id-column", default=ID_COLUMN, help=f"id column name (default {ID_COLUMN!r})")
    parser.add_argument("--base-url", default=BASE_URL)
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    parser.add_argument("--limit", type=int, default=0, help="only process the first N ids")
    parser.add_argument("--dry-run", action="store_true", help="print the payload plan, send nothing")
    parser.add_argument("--yes", action="store_true", help="skip the confirmation prompt")
    args = parser.parse_args()
    sys.exit(asyncio.run(run(args)))


if __name__ == "__main__":
    main()
