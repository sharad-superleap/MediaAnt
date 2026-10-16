"""
Tag the matching contact on mails (associated_contact).

Input: a CSV with the contact id and the contact email (headers: id / contact_id and email).

Steps:
  1. fetch the mails on the `email` object that have a global_message_id AND where any address field
     (from_email, to_recipients, recipient_email, cc, bcc) contains one of the contact emails
  2. pick ONE contact per mail, by field priority:
        from_email → to_recipients → recipient_email → cc → bcc
     (within a field, the first address wins)
  3. set associated_contact = that contact id on the mail
     (mails that already have an associated_contact are skipped unless --overwrite)

The changes are sent with bulk_upsert (same call as scripts/updateTrials.py), in batches.

Usage:
  export SUPERLEAP_API_TOKEN=...
  python associate_contacts_on_email.py contacts.csv              # writes the to-update CSV, asks y/yes, then updates
  python associate_contacts_on_email.py contacts.csv --dry-run    # writes the to-update CSV, sends nothing
  python associate_contacts_on_email.py contacts.csv --yes        # no confirmation prompt

Each record sent: {"id": "<mail id>", "associated_contact": "<contact id>", "ignore_update": <run epoch ms>}

Addresses are matched exactly (a@x.com never matches aa@x.com or a@x.com.au), ignoring case.

Output, next to the input CSV:
  <csv name>_to_update_<timestamp>.csv  written BEFORE any update: email_record_id, associated_contact (+ why it matched)
  <csv name>_results_<timestamp>.csv    after the update: one row per mail, updated / failed / unknown
"""

import argparse
import csv
import json
import os
import re
import sys
import time
from datetime import datetime

import requests

BASE_URL = os.environ.get('SUPERLEAP_BASE_URL', 'https://app.superleap.com')
QUERY_URL = BASE_URL + '/api/v1/reports/execute'
BULK_UPSERT_URL = BASE_URL + '/api/v1/org/objects/{slug}/record/bulk_upsert'

EMAIL_OBJECT_SLUG = 'email'
F = {
    'message_id': 'global_message_id',
    'contact': 'associated_contact',
}
# address fields, highest priority first: the first field holding a contact's address decides the contact
ADDRESS_FIELDS = ['from_email', 'to_recipients', 'recipient_email', 'cc', 'bcc']

EMAILS_PER_QUERY = 25       # contact addresses OR-ed into one query (x 5 address fields)
PAGE_SIZE = 1000            # rows per query page
UPSERT_BATCH = 300          # records per bulk_upsert call

EMAIL_RE = re.compile(r"[^\s<>,;\"'{}\[\]]+@[^\s<>,;\"'{}\[\]]+")


# ─────────────────────────────────────────────
# helpers
# ─────────────────────────────────────────────
def extract_emails(v):
    """address fields may be a list, 'a@x.com, b@x.com', 'Name <a@x.com>' or '{a@x.com,b@x.com}'."""
    raw = ','.join(map(str, v)) if isinstance(v, list) else str(v or '')
    return [e.lower() for e in EMAIL_RE.findall(raw)]


def id_list(v):
    """a reference field may come back as an id, a list, '{id1,id2}', '["id1"]', an object or blank."""
    if v is None or v == '':
        return []
    if isinstance(v, list):
        return [i for x in v for i in id_list(x)]
    if isinstance(v, dict):
        return id_list(v.get('id') or v.get('value') or '')
    t = str(v).strip()
    if t[:1] in '[{':
        try:
            return id_list(json.loads(t))
        except ValueError:
            return [x.strip().strip('"') for x in t[1:-1].split(',') if x.strip().strip('"')]
    return [t] if t else []


def sql_str(v):
    return "'" + str(v).replace("'", "''") + "'"


# characters that can be part of an address: the match must not be next to one of these
ADDR_CHARS = 'A-Za-z0-9._%+-'


def exact_match(col, email):
    """the field holds exactly this address (as a whole entry of the list, not inside a longer one), ignoring case"""
    escaped = re.sub(r'([^A-Za-z0-9@])', r'\\\1', email)
    pattern = f'(^|[^{ADDR_CHARS}]){escaped}($|[^{ADDR_CHARS}])'
    return f"{col}::text ~* {sql_str(pattern)}"


def entry_id(entry):
    if not isinstance(entry, dict):
        return ''
    for key in ('id', 'record_id'):
        if entry.get(key):
            return str(entry[key])
    for key in ('record', 'data', 'value', 'payload'):
        nested = entry.get(key)
        if isinstance(nested, dict) and nested.get('id'):
            return str(nested['id'])
    return ''


def entry_error(entry):
    if not isinstance(entry, dict):
        return str(entry)[:1000]
    msg = entry.get('error') or entry.get('error_message') or entry.get('message') or entry.get('reason') \
        or json.dumps(entry, ensure_ascii=False, default=str)
    code = entry.get('error_code') or entry.get('code')
    return (f'[{code}] {msg}' if code else str(msg))[:1000]


def with_retry(fn, retries=3):
    for i in range(retries):
        try:
            return fn()
        except Exception:
            if i == retries - 1:
                raise
            time.sleep(1.5 * (i + 1))


# ─────────────────────────────────────────────
# API
# ─────────────────────────────────────────────
class Api:
    def __init__(self, token):
        self.s = requests.Session()
        self.s.headers.update({'Content-Type': 'application/json', 'Authorization': f'Bearer {token}'})

    def query(self, q, slug):
        def call():
            r = self.s.post(QUERY_URL, json={'query': q, 'slug': slug}, timeout=120)
            if not r.ok:
                raise RuntimeError(f'HTTP {r.status_code}: {r.text[:500]}')
            j = r.json()
            if not j.get('success'):
                raise RuntimeError((j.get('data') or {}).get('message') or 'API error')
            return (j.get('data') or {}).get('records') or []
        return with_retry(call)

    def bulk_upsert(self, slug, records):
        """returns (ok ids, {failed id: error})"""
        def call():
            r = self.s.post(BULK_UPSERT_URL.format(slug=slug),
                            json={'values': records, 'enable_partial_upsert': True}, timeout=180)
            if r.status_code == 401:
                raise SystemExit('Authentication failed: check SUPERLEAP_API_TOKEN')
            if not r.ok:
                raise RuntimeError(f'HTTP {r.status_code}: {r.text[:1000]}')
            j = r.json()
            if not j.get('success'):
                raise RuntimeError(entry_error(j.get('data') or j))
            return j.get('data') or {}
        body = with_retry(call)
        failed = {entry_id(e) or f'<unknown #{i}>': entry_error(e) for i, e in enumerate(body.get('failed_records') or [])}
        ok = [entry_id(e) for e in (body.get('updated_records') or []) + (body.get('created_records') or []) if entry_id(e)]
        return ok, failed


# ─────────────────────────────────────────────
# steps
# ─────────────────────────────────────────────
def read_contacts(path):
    """email -> contact id (when one address is on several contacts, the first CSV row wins)"""
    by_email, dupes = {}, 0
    with open(path, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        cols = {c.strip().lower(): c for c in reader.fieldnames or []}
        id_col = cols.get('id') or cols.get('contact_id')
        email_col = cols.get('email') or cols.get('contact_email')
        if not id_col or not email_col:
            sys.exit(f'CSV needs an id (or contact_id) and an email column, got: {reader.fieldnames}')
        for row in reader:
            cid = (row.get(id_col) or '').strip()
            if not cid:
                continue
            for e in extract_emails(row.get(email_col)):
                if e in by_email and by_email[e] != cid:
                    dupes += 1
                    continue
                by_email[e] = cid
    if dupes:
        print(f'⚠ {dupes} address(es) appear on more than one contact: the first contact in the CSV is used')
    return by_email


def find_mails(api, emails):
    """mails with a global_message_id where any address field contains one of these emails, paged"""
    match = ' OR '.join(exact_match(c, e) for e in emails for c in ADDRESS_FIELDS)
    where = f"{F['message_id']} IS NOT NULL AND {F['message_id']}::text <> '' AND ({match})"
    select = f"id, {F['contact']} as associated_contact, " + ', '.join(f'{c} as {c}' for c in ADDRESS_FIELDS)
    rows, offset = [], 0
    while True:
        q = f"SELECT {select} FROM {EMAIL_OBJECT_SLUG} WHERE {where} ORDER BY id LIMIT {PAGE_SIZE} OFFSET {offset}"
        page = api.query(q, EMAIL_OBJECT_SLUG)
        rows.extend(page)
        if len(page) < PAGE_SIZE:
            return rows
        offset += PAGE_SIZE


def pick_contact(mail, by_email):
    """(contact id, field, address) by field priority, or None"""
    for field in ADDRESS_FIELDS:
        for addr in extract_emails(mail.get(field)):
            if addr in by_email:
                return by_email[addr], field, addr
    return None


def main():
    ap = argparse.ArgumentParser(description='Tag the matching contact on mails (associated_contact).')
    ap.add_argument('csv', help='CSV with contact id and email columns')
    ap.add_argument('--token', default=os.environ.get('SUPERLEAP_API_TOKEN'), help='API token (or set SUPERLEAP_API_TOKEN)')
    ap.add_argument('--dry-run', action='store_true', help='write the to-update CSV, send nothing')
    ap.add_argument('--yes', action='store_true', help='skip the confirmation prompt')
    ap.add_argument('--overwrite', action='store_true', help='also replace an associated_contact that is already set')
    args = ap.parse_args()
    if not args.token:
        sys.exit('Missing token: pass --token or set SUPERLEAP_API_TOKEN')

    api = Api(args.token)
    by_email = read_contacts(args.csv)
    emails = sorted(by_email)
    print(f'{len(emails)} contact address(es) from {args.csv}')

    mails = {}   # a mail can come back for several address chunks: keep it once
    for i in range(0, len(emails), EMAILS_PER_QUERY):
        for r in find_mails(api, emails[i:i + EMAILS_PER_QUERY]):
            mails[str(r['id'])] = r
        print(f'  scanned {min(i + EMAILS_PER_QUERY, len(emails))}/{len(emails)} addresses, {len(mails)} mail(s) found')

    updates, skipped_set = [], 0
    for mail_id, m in mails.items():
        # several contacts can be on one mail: the field priority picks one
        hit = pick_contact(m, by_email)
        if not hit:
            continue
        cid, field, addr = hit
        existing = id_list(m.get('associated_contact'))
        if existing == [cid]:
            continue
        if existing and not args.overwrite:
            skipped_set += 1
            continue
        updates.append({'id': mail_id, F['contact']: cid, '_existing': ','.join(existing), '_field': field, '_addr': addr})
    if skipped_set:
        print(f'{skipped_set} mail(s) skipped: associated_contact already set (use --overwrite)')

    if not updates:
        print('Nothing to update.')
        return

    base, _ = os.path.splitext(os.path.abspath(args.csv))
    run_at = datetime.now()
    plan_path = f'{base}_to_update_{run_at:%Y%m%d_%H%M%S}.csv'
    with open(plan_path, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['email_record_id', 'associated_contact', 'existing_associated_contact', 'matched_field', 'matched_email'])
        for u in updates:
            w.writerow([u['id'], u[F['contact']], u['_existing'], u['_field'], u['_addr']])
    print(f'{len(updates)} mail(s) to update, written to {plan_path}')

    stamp = int(time.time() * 1000)   # same ignore_update stamp on every record of this run, as in updateTrials.py
    print(f"Sample      : {json.dumps({'id': updates[0]['id'], F['contact']: updates[0][F['contact']], 'ignore_update': stamp})}")
    if args.dry_run:
        print('Dry run: nothing sent.')
        return
    if not args.yes:
        answer = input(f'Check {os.path.basename(plan_path)}, then update {len(updates)} mail(s)? [y/yes to continue] ')
        if answer.strip().lower() not in ('y', 'yes'):
            print('Aborted: nothing sent.')
            return

    ok, failed = set(), {}
    batches = [updates[i:i + UPSERT_BATCH] for i in range(0, len(updates), UPSERT_BATCH)]
    for n, part in enumerate(batches, 1):
        batch = [{'id': u['id'], F['contact']: u[F['contact']], 'ignore_update': stamp} for u in part]
        try:
            b_ok, b_failed = api.bulk_upsert(EMAIL_OBJECT_SLUG, batch)
            ok.update(b_ok)
            failed.update(b_failed)
        except Exception as e:
            failed.update({b['id']: str(e)[:1000] for b in batch})
        print(f'  batch {n}/{len(batches)}: total {len(ok)} updated / {len(failed)} failed')

    out_path = f'{base}_results_{run_at:%Y%m%d_%H%M%S}.csv'
    with open(out_path, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['email_id', 'status', 'associated_contact', 'error'])
        for u in updates:
            rid = u['id']
            status = 'failed' if rid in failed else 'updated' if rid in ok else 'unknown'
            w.writerow([rid, status, u[F['contact']], failed.get(rid, '' if rid in ok else 'not reported in API response')])
    unknown = len(updates) - len(ok | set(failed))
    print(f'Done: {len(ok)} updated, {len(failed)} failed, {max(unknown, 0)} unknown. Results: {out_path}')
    if failed:
        sys.exit(2)


if __name__ == '__main__':
    main()
