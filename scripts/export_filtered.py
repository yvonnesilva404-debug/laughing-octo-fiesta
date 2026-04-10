import argparse
import csv
import gzip
import json
import os
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / 'data'

US_LOCATION_ALIASES = {
    'us', 'usa', 'u.s.', 'u.s.a.', 'united states', 'united states of america',
}

REMOTE_TERMS = {'remote', 'anywhere', 'work from home', 'wfh', 'distributed', 'virtual', 'telecommute'}
NON_US_HINTS = {
    'canada', 'portugal', 'belgium', 'japan', 'china', 'hong kong', 'taiwan', 'south korea',
    'north korea', 'czech republic', 'czechia', 'czech', 'chile', 'uruguay', 'malaysia',
    'costa rica', 'south africa', 'austria', 'denmark', 'estonia', 'norway', 'finland',
    'poland', 'greece', 'switzerland', 'europe', 'emea', 'apac', 'latam', 'uk', 'united kingdom',
    'ireland', 'germany', 'france', 'spain', 'italy', 'netherlands', 'sweden', 'stockholm',
    'india', 'bengaluru', 'australia', 'new zealand', 'singapore', 'mexico', 'brazil',
    'argentina', 'philippines', 'vietnam', 'remote-europe', 'remote uk',
}

US_STATE_CODES = {
    'al', 'ak', 'az', 'ar', 'ca', 'co', 'ct', 'de', 'fl', 'ga', 'hi', 'id', 'il', 'in', 'ia', 'ks',
    'ky', 'la', 'me', 'md', 'ma', 'mi', 'mn', 'ms', 'mo', 'mt', 'ne', 'nv', 'nh', 'nj', 'nm',
    'ny', 'nc', 'nd', 'oh', 'ok', 'or', 'pa', 'ri', 'sc', 'sd', 'tn', 'tx', 'ut', 'vt', 'va',
    'wa', 'wv', 'wi', 'wy', 'dc',
}

US_STATE_NAMES = {
    'alabama', 'alaska', 'arizona', 'arkansas', 'california', 'colorado', 'connecticut', 'delaware',
    'florida', 'georgia', 'hawaii', 'idaho', 'illinois', 'indiana', 'iowa', 'kansas', 'kentucky',
    'louisiana', 'maine', 'maryland', 'massachusetts', 'michigan', 'minnesota', 'mississippi',
    'missouri', 'montana', 'nebraska', 'nevada', 'new hampshire', 'new jersey', 'new mexico',
    'new york', 'north carolina', 'north dakota', 'ohio', 'oklahoma', 'oregon', 'pennsylvania',
    'rhode island', 'south carolina', 'south dakota', 'tennessee', 'texas', 'utah', 'vermont',
    'virginia', 'washington', 'west virginia', 'wisconsin', 'wyoming', 'district of columbia',
}

US_CITY_HINTS = {
    'miami', 'washington', 'washington dc', 'washington, dc', 'new york city', 'san francisco',
    'los angeles', 'chicago', 'seattle', 'austin', 'boston', 'atlanta', 'dallas', 'denver',
    'philadelphia', 'phoenix', 'houston', 'san diego', 'portland', 'nashville', 'charlotte',
    'detroit', 'minneapolis',
}

UNKNOWN_LOCATION_STRINGS = {'not specified', 'n/a', 'not available', 'unknown', 'unspecified', 'none', '-'}
RECRUITER_TERMS = {
    'recruit', 'recruiting', 'recruiter', 'staffing', 'talent', 'talenthub', 'talentgroup',
    'solutions', 'consulting', 'placement', 'search', 'resources', 'agency',
}

POSTED_FIELDS = [
    'date_posted', 'posted_at', 'posted_on', 'postedDate', 'postedOn', 'datePosted',
    'publish_date', 'published_at', 'startDate', 'scraped_at',
]


def load_manifest_jobs(data_dir: Path):
    manifest_path = data_dir / 'jobs_manifest.json'
    if not manifest_path.exists():
        raise FileNotFoundError(f'Manifest not found: {manifest_path}')

    with manifest_path.open('r', encoding='utf-8') as manifest_file:
        manifest = json.load(manifest_file)

    jobs = []
    for chunk_name in manifest.get('chunks', []):
        chunk_path = data_dir / chunk_name
        if not chunk_path.exists():
            continue
        with gzip.open(chunk_path, 'rt', encoding='utf-8') as chunk_file:
            jobs.extend(json.load(chunk_file))
    return jobs


def normalize_location(location):
    if not location:
        return ''
    return str(location).strip().lower()


def is_remote_location(raw_location):
    location = normalize_location(raw_location)
    return any(term in location for term in REMOTE_TERMS)


def is_us_location(raw_location):
    if raw_location is None:
        return True
    location = normalize_location(raw_location)
    if not location or location in UNKNOWN_LOCATION_STRINGS:
        return True
    if location in US_LOCATION_ALIASES:
        return True
    if 'united states' in location or 'usa' in location or 'u.s.' in location:
        return True
    if is_remote_location(location):
        return not any(hint in location for hint in NON_US_HINTS)
    tokens = re.sub(r'[.,]', ' ', location).split()
    if any(token in US_STATE_CODES for token in tokens):
        return True
    if any(name in location for name in US_STATE_NAMES):
        return True
    if any(city in location for city in US_CITY_HINTS):
        return True
    return False


def parse_posted_date(job):
    for field in POSTED_FIELDS:
        value = job.get(field)
        if value is None:
            continue
        if isinstance(value, (int, float)):
            try:
                if value > 1e12:
                    return datetime.fromtimestamp(value / 1000.0, timezone.utc)
                return datetime.fromtimestamp(value, timezone.utc)
            except Exception:
                continue
        text = str(value).strip()
        if not text:
            continue
        try:
            parsed = datetime.fromisoformat(text.replace('Z', '+00:00'))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            pass
        try:
            return datetime.fromtimestamp(float(text), timezone.utc)
        except Exception:
            pass
        # last resort: parse common date formats
        for fmt in ('%Y-%m-%d', '%m/%d/%Y', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S'):
            try:
                return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                continue
    return None


def job_location_value(job):
    location = job.get('location', '')
    if isinstance(location, dict):
        return location.get('name', '') or ''
    return location or ''


def is_recruiter_job(job):
    if job.get('is_recruiter') is True:
        return True
    title = str(job.get('title', '')).lower()
    company = str(job.get('company') or job.get('company_slug') or '').lower()
    return any(term in title or term in company for term in RECRUITER_TERMS)


def is_remote_job(job):
    if job.get('remote') is True:
        return True
    location = job_location_value(job)
    if is_remote_location(location):
        return True
    workplace = str(job.get('workplaceType', '')).strip().lower()
    return workplace == 'remote'


def matches_filters(job, *, location_usa=False, freshness_days=None, remote_only=False, hide_recruiters=False):
    if hide_recruiters and is_recruiter_job(job):
        return False

    if remote_only and not is_remote_job(job):
        return False

    if location_usa and not is_us_location(job_location_value(job)):
        return False

    if freshness_days is not None:
        posted = parse_posted_date(job)
        if posted is None:
            return False
        age = datetime.now(timezone.utc) - posted
        if age > timedelta(days=freshness_days):
            return False

    return True


def write_csv(jobs, output_path: Path):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open('w', encoding='utf-8', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Company', 'Title', 'Location', 'Experience Level', 'Date', 'ATS', 'URL'])
        for job in jobs:
            company = job.get('company') or job.get('company_slug') or 'Unknown'
            title = job.get('title') or job.get('job_title') or 'Not specified'
            location = job_location_value(job) or 'Not specified'
            experience = str(job.get('skill_level') or job.get('experience_level') or job.get('level') or job.get('seniority') or '')
            posted = parse_posted_date(job)
            date_value = posted.date().isoformat() if posted else 'N/A'
            ats = str(job.get('ats') or 'unknown')
            url = job.get('absolute_url') or job.get('url') or ''
            writer.writerow([company, title, location, experience, date_value, ats, url])


def main():
    parser = argparse.ArgumentParser(description='Export filtered jobs to CSV without opening the browser.')
    parser.add_argument('--data-dir', default=str(DATA_DIR), help='Path to the job data directory')
    parser.add_argument('--output', default='results.csv', help='Output CSV path')
    parser.add_argument('--location-usa', action='store_true', default=True, help='Filter for US-based jobs')
    parser.add_argument('--freshness-days', type=int, default=1, help='Max age of jobs in days')
    parser.add_argument('--remote-only', action='store_true', default=True, help='Only include remote jobs')
    parser.add_argument('--hide-recruiters', action='store_true', default=True, help='Exclude recruiter-posted jobs')
    args = parser.parse_args()

    jobs = load_manifest_jobs(Path(args.data_dir))
    print(f'Loaded {len(jobs):,} jobs from {args.data_dir}')

    filtered = [job for job in jobs if matches_filters(
        job,
        location_usa=args.location_usa,
        freshness_days=args.freshness_days,
        remote_only=args.remote_only,
        hide_recruiters=args.hide_recruiters,
    )]

    print(f'Filtered down to {len(filtered):,} jobs')
    write_csv(filtered, Path(args.output))
    print(f'Wrote CSV to {args.output}')


if __name__ == '__main__':
    main()
