"""
track_foreign_drops.py

Tracks per-company consecutive "all-jobs-dropped" runs in two categories:

  1. Foreign drops   – every job from a company is filtered out by the US
                       location check.  Confirmed after 6 consecutive runs.

  2. No-remote drops – every job from a company is non-remote (rare).
                       Confirmed after 15 consecutive runs.

State is persisted in  data/drop_tracker.json  and updated on every run.
A trend report is printed to stdout.

Usage
-----
  python scripts/track_foreign_drops.py
  python scripts/track_foreign_drops.py --dry-run          # report only, no save
  python scripts/track_foreign_drops.py --data-dir path/to/data
"""

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

# ── reuse helpers already defined in export_filtered ─────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent))
from export_filtered import (
    load_manifest_jobs,
    is_us_location,
    is_remote_job,
    job_location_value,
)

ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / 'data'
STATE_FILE = DATA_DIR / 'drop_tracker.json'

FOREIGN_THRESHOLD    = 6   # consecutive runs before a company is foreign-confirmed
NO_REMOTE_THRESHOLD  = 15   # consecutive runs before a company is no-remote-confirmed


# ── state helpers ─────────────────────────────────────────────────────────────

def load_state(state_file: Path) -> dict:
    if state_file.exists():
        with state_file.open('r', encoding='utf-8') as fh:
            return json.load(fh)
    return {
        'foreign_strikes':      {},
        'foreign_confirmed':    [],
        'no_remote_strikes':    {},
        'no_remote_confirmed':  [],
        'runs':                 0,
        'last_run':             None,
    }


def save_state(state: dict, state_file: Path):
    state_file.parent.mkdir(parents=True, exist_ok=True)
    with state_file.open('w', encoding='utf-8') as fh:
        json.dump(state, fh, indent=2)


# ── analysis ──────────────────────────────────────────────────────────────────

def group_by_company(jobs: list) -> dict:
    groups: dict = defaultdict(list)
    for job in jobs:
        company = job.get('company') or job.get('company_slug') or 'Unknown'
        groups[company].append(job)
    return dict(groups)


def analyze_companies(groups: dict) -> tuple:
    """Return (foreign_set, no_remote_set) for the current run."""
    foreign:   set = set()
    no_remote: set = set()
    for company, jobs in groups.items():
        if all(not is_us_location(job_location_value(j)) for j in jobs):
            foreign.add(company)
        if not any(is_remote_job(j) for j in jobs):
            no_remote.add(company)
    return foreign, no_remote


def update_strikes(
    state: dict,
    foreign_this_run: set,
    no_remote_this_run: set,
    all_companies: set,
) -> tuple:
    """Increment / reset strike counters; return newly-confirmed lists."""

    # ── foreign ───────────────────────────────────────────────────────────────
    newly_foreign: list = []
    f_strikes   = state['foreign_strikes']
    f_confirmed = set(state['foreign_confirmed'])

    for company in all_companies:
        if company in f_confirmed:
            continue
        if company in foreign_this_run:
            f_strikes[company] = f_strikes.get(company, 0) + 1
        else:
            f_strikes.pop(company, None)   # reset streak on any US job

    for company, count in list(f_strikes.items()):
        if count >= FOREIGN_THRESHOLD:
            newly_foreign.append(company)
            f_confirmed.add(company)
            del f_strikes[company]

    state['foreign_strikes']   = f_strikes
    state['foreign_confirmed'] = sorted(f_confirmed)

    # ── no-remote ─────────────────────────────────────────────────────────────
    newly_no_remote: list = []
    nr_strikes   = state['no_remote_strikes']
    nr_confirmed = set(state['no_remote_confirmed'])

    for company in all_companies:
        if company in nr_confirmed:
            continue
        if company in no_remote_this_run:
            nr_strikes[company] = nr_strikes.get(company, 0) + 1
        else:
            nr_strikes.pop(company, None)  # reset streak when remote job appears

    for company, count in list(nr_strikes.items()):
        if count >= NO_REMOTE_THRESHOLD:
            newly_no_remote.append(company)
            nr_confirmed.add(company)
            del nr_strikes[company]

    state['no_remote_strikes']   = nr_strikes
    state['no_remote_confirmed'] = sorted(nr_confirmed)

    return newly_foreign, newly_no_remote


# ── report ────────────────────────────────────────────────────────────────────

def _bar(n: int, threshold: int, width: int = 10) -> str:
    filled = round(n / threshold * width)
    return '▓' * filled + '░' * (width - filled)


def print_report(state: dict, newly_foreign: list, newly_no_remote: list):
    run_num  = state['runs']
    run_time = state['last_run']

    print(f'\n{"═" * 62}')
    print(f'  Drop Tracker — Run #{run_num}   {run_time}')
    print(f'{"═" * 62}')

    # ── foreign section ───────────────────────────────────────────────────────
    print(f'\n┌─ FOREIGN COMPANIES  (threshold: {FOREIGN_THRESHOLD} consecutive runs)')
    if newly_foreign:
        print(f'│  ★ Newly confirmed this run ({len(newly_foreign)}):')
        for c in sorted(newly_foreign):
            print(f'│      ✗  {c}')
    else:
        print('│  ★ No new foreign confirmations this run.')

    if state['foreign_strikes']:
        pending = sorted(state['foreign_strikes'].items(), key=lambda x: -x[1])
        print(f'│  ⏳ Accumulating strikes ({len(pending)} companies):')
        for c, n in pending:
            print(f'│      [{_bar(n, FOREIGN_THRESHOLD)}] {n}/{FOREIGN_THRESHOLD}  {c}')
    else:
        print('│  ⏳ No companies mid-streak.')

    total_f = len(state['foreign_confirmed'])
    print(f'│\n│  ✔ Confirmed foreign blacklist: {total_f} companies')
    for c in state['foreign_confirmed']:
        print(f'│      • {c}')
    print('└' + '─' * 60)

    # ── no-remote section ─────────────────────────────────────────────────────
    print(f'\n┌─ NO-REMOTE COMPANIES  (threshold: {NO_REMOTE_THRESHOLD} consecutive runs)')
    if newly_no_remote:
        print(f'│  ★ Newly confirmed this run ({len(newly_no_remote)}):')
        for c in sorted(newly_no_remote):
            print(f'│      ✗  {c}')
    else:
        print('│  ★ No new no-remote confirmations this run.')

    if state['no_remote_strikes']:
        pending_nr = sorted(state['no_remote_strikes'].items(), key=lambda x: -x[1])
        print(f'│  ⏳ Accumulating strikes ({len(pending_nr)} companies):')
        for c, n in pending_nr:
            print(f'│      [{_bar(n, NO_REMOTE_THRESHOLD)}] {n}/{NO_REMOTE_THRESHOLD}  {c}')
    else:
        print('│  ⏳ No companies mid-streak.')

    total_nr = len(state['no_remote_confirmed'])
    print(f'│\n│  ✔ Confirmed no-remote blacklist: {total_nr} companies')
    for c in state['no_remote_confirmed']:
        print(f'│      • {c}')
    print('└' + '─' * 60)
    print()


# ── entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Track and confirm foreign / no-remote companies across scraper runs.'
    )
    parser.add_argument('--data-dir',   default=str(DATA_DIR),   help='Path to job data directory')
    parser.add_argument('--state-file', default=str(STATE_FILE), help='Path to persistent state JSON')
    parser.add_argument('--dry-run', action='store_true',
                        help='Print report without writing state changes')
    args = parser.parse_args()

    data_dir   = Path(args.data_dir)
    state_file = Path(args.state_file)

    jobs = load_manifest_jobs(data_dir)
    print(f'Loaded {len(jobs):,} jobs.')

    state           = load_state(state_file)
    state['runs']   = state.get('runs', 0) + 1
    state['last_run'] = datetime.now(timezone.utc).isoformat()

    groups        = group_by_company(jobs)
    all_companies = set(groups.keys())
    print(f'Tracking {len(all_companies):,} companies across {len(jobs):,} jobs.')

    foreign_this_run, no_remote_this_run = analyze_companies(groups)
    newly_foreign, newly_no_remote = update_strikes(
        state, foreign_this_run, no_remote_this_run, all_companies
    )

    print_report(state, newly_foreign, newly_no_remote)

    if not args.dry_run:
        save_state(state, state_file)
        print(f'State saved → {state_file}')
    else:
        print('Dry-run: state NOT saved.')


if __name__ == '__main__':
    main()
