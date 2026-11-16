#!/usr/bin/env python3
"""Provision Metabase from questions/*.sql: connection, cards, dashboards.

Idempotent -- matches objects by name and updates them. Standard library only.

    export MB_URL=http://localhost:3000
    export MB_ADMIN_EMAIL=admin@example.com MB_ADMIN_PASSWORD='...'
    export CLICKHOUSE_USER=admin CLICKHOUSE_PASSWORD='...'
    python3 metabase/provision.py
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

QUESTIONS_DIR = Path(__file__).parent / "questions"
DB_NAME = "ClickHouse (gold + monitoring)"

DASHBOARDS = {
    "Revenue & Discounts":
        "What we earned and what we gave away. Starts with the headline totals, "
        "then the trend, then where the money comes from. Measured at line grain "
        "from obt_sales -- obt_orders is a rollup of the same money.",
    "Customer Journey":
        "How a visitor becomes a buyer. Funnel entry and drop-off first, then how "
        "people engage, then whether stated intent (wishlist) turns into a sale.",
    "Retention & Returns":
        "What a customer is worth and what erodes it. Tier value first, then what "
        "comes back, why, and how long after the order. Return rates are by refund "
        "AMOUNT -- the source carries no return quantity.",
    "Real-Time Pipeline Monitoring":
        "Read straight from Kafka, independent of the batch layers. Freshness, "
        "funnel health, anomalies and dirty data.",
}

WIDTHS = {"full": 24, "half": 12, "third": 8}

FRONT_MATTER = re.compile(r"^--\s*(name|dashboard|display|width|height|description|viz):\s*(.*)$")


class MetabaseError(RuntimeError):
    pass


class Client:
    def __init__(self, base_url: str, dry_run: bool = False):
        self.base = base_url.rstrip("/")
        self.token: str | None = None
        self.dry_run = dry_run

    def _request(self, method: str, path: str, body=None, allow_fail=False):
        url = f"{self.base}{path}"
        if self.dry_run and method != "GET":
            print(f"  [dry-run] {method} {path} {json.dumps(body)[:160] if body else ''}")
            return {}
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Content-Type", "application/json")
        if self.token:
            req.add_header("X-Metabase-Session", self.token)
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                raw = resp.read().decode()
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode()[:600]
            if allow_fail:
                return {"_error": detail, "_status": exc.code}
            raise MetabaseError(f"{method} {path} -> HTTP {exc.code}: {detail}") from exc
        except urllib.error.URLError as exc:
            raise MetabaseError(f"{method} {path} -> {exc.reason}") from exc

    get = lambda self, p, **kw: self._request("GET", p, **kw)          # noqa: E731
    post = lambda self, p, b, **kw: self._request("POST", p, b, **kw)  # noqa: E731
    put = lambda self, p, b, **kw: self._request("PUT", p, b, **kw)    # noqa: E731

    @staticmethod
    def rows(payload):
        if isinstance(payload, dict):
            return payload.get("data", [])
        return payload or []

    def wait_until_up(self, timeout: int = 300) -> None:
        deadline = time.time() + timeout
        last = ""
        while time.time() < deadline:
            try:
                health = self.get("/api/health", allow_fail=True)
                if health.get("status") == "ok":
                    print(f"Metabase is up at {self.base}")
                    return
                last = str(health)
            except MetabaseError as exc:
                last = str(exc)
            time.sleep(5)
        raise MetabaseError(f"Metabase not healthy within {timeout}s. Last: {last}")

    def authenticate(self, email: str, password: str) -> None:
        props = self.get("/api/session/properties", allow_fail=True)
        setup_token = props.get("setup-token") if isinstance(props, dict) else None
        if setup_token:
            print("First run detected -- completing the setup wizard")
            result = self.post("/api/setup", {
                "token": setup_token,
                "user": {
                    "first_name": "Data", "last_name": "Platform",
                    "email": email, "password": password, "site_name": "Lakehouse",
                },
                "prefs": {"site_name": "Lakehouse", "site_locale": "en", "allow_tracking": False},
            })
            self.token = result.get("id")
            if self.token:
                return
        session = self.post("/api/session", {"username": email, "password": password})
        self.token = session.get("id")
        if not self.token:
            raise MetabaseError("Login succeeded but returned no session id")
        print(f"Authenticated as {email}")


def ensure_database(mb: Client, ch_host: str, ch_port: int, ch_user: str,
                    ch_password: str, ch_db: str) -> int:
    details = {
        "host": ch_host,
        "port": ch_port,
        "user": ch_user,
        "password": ch_password,
        "dbname": ch_db,
        "ssl": False,
        "scan-all-databases": True,
    }
    existing = next((d for d in Client.rows(mb.get("/api/database"))
                     if d.get("name") == DB_NAME), None)
    if existing:
        db_id = existing["id"]
        mb.put(f"/api/database/{db_id}", {"name": DB_NAME, "engine": "clickhouse", "details": details})
        print(f"Updated ClickHouse connection (id={db_id})")
    else:
        created = mb.post("/api/database", {"name": DB_NAME, "engine": "clickhouse", "details": details})
        db_id = created.get("id")
        if not db_id:
            raise MetabaseError(f"Database creation returned no id: {created}")
        print(f"Created ClickHouse connection (id={db_id})")
    mb.post(f"/api/database/{db_id}/sync_schema", {}, allow_fail=True)
    return db_id


def parse_question(path: Path, gold_db: str) -> dict:
    meta, sql_lines, in_body = {}, [], False
    for line in path.read_text().splitlines():
        match = FRONT_MATTER.match(line) if not in_body else None
        if match:
            meta[match.group(1)] = match.group(2).strip()
            continue
        if line.startswith("--") and not in_body:
            continue
        if line.strip():
            in_body = True
        if in_body:
            sql_lines.append(line)

    missing = {"name", "dashboard", "display"} - meta.keys()
    if missing:
        raise MetabaseError(f"{path.name} is missing front matter: {sorted(missing)}")
    if meta["dashboard"] not in DASHBOARDS:
        raise MetabaseError(f"{path.name} names unknown dashboard {meta['dashboard']!r}")

    width = meta.get("width", "full" if meta["display"] == "table" else "half")
    if width not in WIDTHS:
        raise MetabaseError(f"{path.name}: width must be one of {sorted(WIDTHS)}, got {width!r}")

    height = int(meta.get("height", 8))
    viz = json.loads(meta["viz"]) if meta.get("viz") else {}
    return {
        "file": path.name,
        "name": meta["name"],
        "dashboard": meta["dashboard"],
        "display": meta["display"],
        "width": width,
        "height": height,
        "description": meta.get("description", ""),
        "viz": viz,
        "sql": "\n".join(sql_lines).replace("{{GOLD_DB}}", gold_db).strip(),
    }


def ensure_card(mb: Client, question: dict, db_id: int, existing: dict) -> int:
    payload = {
        "name": question["name"],
        "description": question["description"] or None,
        "display": question["display"],
        "visualization_settings": question["viz"],
        "dataset_query": {
            "database": db_id,
            "type": "native",
            "native": {"query": question["sql"], "template-tags": {}},
        },
    }
    found = existing.get(question["name"])
    if found:
        mb.put(f"/api/card/{found}", payload)
        return found
    created = mb.post("/api/card", payload)
    card_id = created.get("id")
    if not card_id:
        raise MetabaseError(f"Card creation returned no id for {question['name']!r}: {created}")
    return card_id


def ensure_dashboard(mb: Client, name: str, description: str, existing: dict) -> int:
    found = existing.get(name)
    if found:
        mb.put(f"/api/dashboard/{found}", {"name": name, "description": description})
        return found
    created = mb.post("/api/dashboard", {"name": name, "description": description})
    dash_id = created.get("id")
    if not dash_id:
        raise MetabaseError(f"Dashboard creation returned no id for {name!r}: {created}")
    return dash_id


def layout(cards: list[tuple[int, str, int]]) -> list[dict]:
    """Pack cards left-to-right on Metabase's 24-column grid, in file order.

    File order is narrative order: headline first, then trend, then breakdown,
    then the detail tables. Rows advance only when the next card will not fit,
    so a dashboard reads top-to-bottom like a story rather than a grid of tiles.
    """
    dashcards, row, col, row_height = [], 0, 0, 0
    for index, (card_id, width_name, height) in enumerate(cards):
        width = WIDTHS[width_name]
        if col + width > 24:
            row, col, row_height = row + row_height, 0, 0
        dashcards.append({
            "id": -(index + 1),
            "card_id": card_id,
            "row": row, "col": col,
            "size_x": width, "size_y": height,
            "parameter_mappings": [], "visualization_settings": {},
        })
        col += width
        row_height = max(row_height, height)
        if col >= 24:
            row, col, row_height = row + row_height, 0, 0
    return dashcards


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                        help="parse and print, send no writes")
    parser.add_argument("--gold-db", default=os.environ.get("GOLD_DB", "gold"),
                        help="ClickHouse database holding the obt_* tables (default: gold)")
    parser.add_argument("--exclude-dashboard", action="append", default=[],
                        metavar="NAME",
                        help="skip every question belonging to this dashboard; repeatable")
    args = parser.parse_args()

    mb_url = os.environ.get("MB_URL", "http://localhost:3000")
    email = os.environ.get("MB_ADMIN_EMAIL")
    password = os.environ.get("MB_ADMIN_PASSWORD")
    ch_user = os.environ.get("CLICKHOUSE_USER")
    ch_password = os.environ.get("CLICKHOUSE_PASSWORD")
    ch_host = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
    ch_port = int(os.environ.get("CLICKHOUSE_PORT", "8123"))

    missing = [k for k, v in {
        "MB_ADMIN_EMAIL": email, "MB_ADMIN_PASSWORD": password,
        "CLICKHOUSE_USER": ch_user, "CLICKHOUSE_PASSWORD": ch_password,
    }.items() if not v]
    if missing and not args.dry_run:
        print(f"error: missing required environment variables: {', '.join(missing)}", file=sys.stderr)
        return 2

    files = sorted(QUESTIONS_DIR.glob("*.sql"))
    if not files:
        print(f"error: no .sql files in {QUESTIONS_DIR}", file=sys.stderr)
        return 2
    questions = [parse_question(f, args.gold_db) for f in files]
    if args.exclude_dashboard:
        skipped = [q for q in questions if q["dashboard"] in args.exclude_dashboard]
        questions = [q for q in questions if q["dashboard"] not in args.exclude_dashboard]
        for name in args.exclude_dashboard:
            print(f"Excluding dashboard {name!r} "
                  f"({sum(1 for q in skipped if q['dashboard'] == name)} questions)")
    print(f"Parsed {len(questions)} questions across {len({q['dashboard'] for q in questions})} dashboards")

    if args.dry_run:
        for q in questions:
            print(f"  {q['file']:<38} -> [{q['dashboard']}] {q['name']} ({q['display']})")
        print("\ndry-run: nothing was sent to Metabase")
        return 0

    mb = Client(mb_url)
    mb.wait_until_up()
    mb.authenticate(email, password)
    db_id = ensure_database(mb, ch_host, ch_port, ch_user, ch_password, args.gold_db)

    existing_cards = {c["name"]: c["id"] for c in Client.rows(mb.get("/api/card"))}
    existing_dashes = {d["name"]: d["id"] for d in Client.rows(mb.get("/api/dashboard"))}

    by_dashboard: dict[str, list[tuple[int, str]]] = {}
    for question in questions:
        card_id = ensure_card(mb, question, db_id, existing_cards)
        by_dashboard.setdefault(question["dashboard"], []).append(
            (card_id, question["width"], question["height"]))
        print(f"  card {card_id:>4}  [{question['width']:<5}] {question['name']}")

    for name in DASHBOARDS:
        cards = by_dashboard.get(name)
        if not cards:
            continue
        dash_id = ensure_dashboard(mb, name, DASHBOARDS[name], existing_dashes)
        mb.put(f"/api/dashboard/{dash_id}", {"dashcards": layout(cards)})
        print(f"  dashboard {dash_id:>3}  {name} ({len(cards)} cards)")

    print(f"\nDone. Open {mb_url}/browse and check the dashboards.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except MetabaseError as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(1)
