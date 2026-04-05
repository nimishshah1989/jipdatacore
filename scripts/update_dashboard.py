#!/usr/bin/env python3
"""
Forge Dashboard Updater — generates live HTML dashboard.
Called by Ralph after each chunk completes.

Usage:
    python3 scripts/update_dashboard.py --chunk 7 --status done
    python3 scripts/update_dashboard.py --phase post-build
    python3 scripts/update_dashboard.py --phase deploying
    python3 scripts/update_dashboard.py --phase migrating --detail "equity_ohlcv: 2.3M rows"
    python3 scripts/update_dashboard.py --phase ingesting --detail "BHAV pipeline running"
"""

import argparse
import json
import os
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
TASKS_FILE = PROJECT_ROOT / "tasks.json"
DASHBOARD_FILE = PROJECT_ROOT / "docs" / "forge-dashboard.html"
STATUS_FILE = PROJECT_ROOT / "docs" / ".dashboard-state.json"


def load_tasks() -> dict:
    with open(TASKS_FILE) as f:
        return json.load(f)


def load_state() -> dict:
    if STATUS_FILE.exists():
        with open(STATUS_FILE) as f:
            return json.load(f)
    return {
        "phase": "building",
        "detail": "",
        "events": [],
        "chunk_details": {},
    }


def save_state(state: dict) -> None:
    with open(STATUS_FILE, "w") as f:
        json.dump(state, f, indent=2, default=str)


def generate_html(tasks: dict, state: dict) -> str:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S IST")
    chunks = tasks.get("chunks", [])

    # Count statuses
    done = sum(1 for c in chunks if c["status"] == "done")
    failed = sum(1 for c in chunks if c["status"] == "failed")
    blocked = sum(1 for c in chunks if c["status"] == "blocked")
    building = sum(1 for c in chunks if c["status"] == "in_progress")
    pending = sum(1 for c in chunks if c["status"] == "pending")
    total = len(chunks)

    phase = state.get("phase", "building")
    detail = state.get("detail", "")
    events = state.get("events", [])[-30:]  # Last 30 events

    # Phase display
    phase_labels = {
        "building": "Building Chunks",
        "post-build": "Post-Build QA",
        "deploying": "Deploying to EC2",
        "migrating": "Data Migration",
        "ingesting": "Data Ingestion",
        "complete": "All Complete",
    }
    phase_colors = {
        "building": "#f59e0b",
        "post-build": "#3b82f6",
        "deploying": "#8b5cf6",
        "migrating": "#06b6d4",
        "ingesting": "#10b981",
        "complete": "#1D9E75",
    }

    # Build chunk rows
    chunk_rows = ""
    for c in chunks:
        cid = c["id"]
        status = c["status"]
        status_icon = {
            "done": "&#9989;",
            "failed": "&#10060;",
            "blocked": "&#9940;",
            "in_progress": "&#9203;",
            "pending": "&#9898;",
        }.get(status, "&#9898;")
        status_color = {
            "done": "#10b981",
            "failed": "#ef4444",
            "blocked": "#f59e0b",
            "in_progress": "#3b82f6",
            "pending": "#6b7280",
        }.get(status, "#6b7280")

        detail_info = state.get("chunk_details", {}).get(cid, {})
        tests = detail_info.get("tests", "-")
        files = detail_info.get("files", "-")
        loc = detail_info.get("loc", "-")

        chunk_rows += f"""
        <tr>
            <td style="font-weight:600">{cid}</td>
            <td>{c['name']}</td>
            <td>L{c['layer']}</td>
            <td style="color:{status_color};font-weight:600">{status_icon} {status.upper()}</td>
            <td>{tests}</td>
            <td>{files}</td>
            <td>{loc}</td>
        </tr>"""

    # Build event log
    event_rows = ""
    for ev in reversed(events):
        event_rows += f'<div class="event">{ev}</div>\n'

    progress_pct = int((done / total) * 100) if total > 0 else 0

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta http-equiv="refresh" content="15">
    <title>Forge Build Dashboard — JIP Data Engine</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0f172a; color: #e2e8f0; padding: 24px; }}
        .header {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 24px; }}
        .header h1 {{ font-size: 24px; color: #1D9E75; }}
        .header .time {{ color: #94a3b8; font-size: 14px; }}
        .phase-banner {{ background: {phase_colors.get(phase, '#6b7280')}22; border: 1px solid {phase_colors.get(phase, '#6b7280')}; border-radius: 8px; padding: 16px 24px; margin-bottom: 24px; display: flex; justify-content: space-between; align-items: center; }}
        .phase-banner .label {{ font-size: 20px; font-weight: 700; color: {phase_colors.get(phase, '#6b7280')}; }}
        .phase-banner .detail {{ color: #94a3b8; font-size: 14px; }}
        .stats {{ display: grid; grid-template-columns: repeat(5, 1fr); gap: 12px; margin-bottom: 24px; }}
        .stat {{ background: #1e293b; border-radius: 8px; padding: 16px; text-align: center; }}
        .stat .num {{ font-size: 32px; font-weight: 700; }}
        .stat .label {{ font-size: 12px; color: #94a3b8; text-transform: uppercase; margin-top: 4px; }}
        .progress {{ background: #1e293b; border-radius: 8px; height: 12px; margin-bottom: 24px; overflow: hidden; }}
        .progress .bar {{ height: 100%; background: linear-gradient(90deg, #1D9E75, #10b981); border-radius: 8px; transition: width 0.5s; }}
        .grid {{ display: grid; grid-template-columns: 2fr 1fr; gap: 24px; }}
        table {{ width: 100%; border-collapse: collapse; background: #1e293b; border-radius: 8px; overflow: hidden; }}
        th {{ background: #334155; padding: 12px 16px; text-align: left; font-size: 12px; text-transform: uppercase; color: #94a3b8; }}
        td {{ padding: 10px 16px; border-top: 1px solid #334155; font-size: 14px; }}
        .event-log {{ background: #1e293b; border-radius: 8px; padding: 16px; max-height: 500px; overflow-y: auto; }}
        .event-log h3 {{ color: #94a3b8; font-size: 12px; text-transform: uppercase; margin-bottom: 12px; }}
        .event {{ font-size: 13px; padding: 6px 0; border-bottom: 1px solid #334155; color: #cbd5e1; font-family: monospace; }}
        .done {{ color: #10b981; }} .failed {{ color: #ef4444; }} .building {{ color: #3b82f6; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Forge Build Dashboard</h1>
        <div class="time">Last updated: {now} &bull; Auto-refresh: 15s</div>
    </div>

    <div class="phase-banner">
        <div class="label">{phase_labels.get(phase, phase.upper())}</div>
        <div class="detail">{detail}</div>
    </div>

    <div class="stats">
        <div class="stat"><div class="num done">{done}</div><div class="label">Done</div></div>
        <div class="stat"><div class="num building">{building}</div><div class="label">Building</div></div>
        <div class="stat"><div class="num" style="color:#6b7280">{pending}</div><div class="label">Pending</div></div>
        <div class="stat"><div class="num failed">{failed}</div><div class="label">Failed</div></div>
        <div class="stat"><div class="num" style="color:#f59e0b">{blocked}</div><div class="label">Blocked</div></div>
    </div>

    <div class="progress"><div class="bar" style="width:{progress_pct}%"></div></div>

    <div class="grid">
        <table>
            <thead><tr><th>ID</th><th>Chunk</th><th>Layer</th><th>Status</th><th>Tests</th><th>Files</th><th>LOC</th></tr></thead>
            <tbody>{chunk_rows}</tbody>
        </table>

        <div class="event-log">
            <h3>Event Log</h3>
            {event_rows}
        </div>
    </div>
</body>
</html>"""
    return html


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--chunk", type=int, help="Chunk number")
    parser.add_argument("--status", type=str, help="Chunk status: building|done|failed|blocked")
    parser.add_argument("--phase", type=str, help="Build phase: building|post-build|deploying|migrating|ingesting|complete")
    parser.add_argument("--detail", type=str, default="", help="Detail text for phase banner")
    parser.add_argument("--tests", type=int, help="Number of tests for this chunk")
    parser.add_argument("--files", type=int, help="Number of files changed")
    parser.add_argument("--loc", type=int, help="Lines of code added")
    args = parser.parse_args()

    tasks = load_tasks()
    state = load_state()
    now = datetime.now().strftime("%H:%M:%S")

    if args.phase:
        state["phase"] = args.phase
        if args.detail:
            state["detail"] = args.detail
        state["events"].append(f"[{now}] Phase: {args.phase} {args.detail}")

    if args.chunk and args.status:
        cid = f"C{args.chunk}"
        state["events"].append(f"[{now}] {cid}: {args.status}")
        if args.tests or args.files or args.loc:
            state.setdefault("chunk_details", {})[cid] = {
                "tests": args.tests or "-",
                "files": args.files or "-",
                "loc": args.loc or "-",
            }

    save_state(state)
    html = generate_html(tasks, state)

    os.makedirs(DASHBOARD_FILE.parent, exist_ok=True)
    with open(DASHBOARD_FILE, "w") as f:
        f.write(html)

    print(f"Dashboard updated: {DASHBOARD_FILE}")


if __name__ == "__main__":
    main()
