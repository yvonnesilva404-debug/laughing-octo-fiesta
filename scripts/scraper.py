import requests
import json
import random
import time
import re
import os
import sys
from datetime import timedelta
import gzip
import argparse
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from itertools import islice

# Ensure we can import scripts/builtin.py when running from repo root
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

import builtin

# ============================================================
# CONFIGURATION
# ============================================================

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(SCRIPT_DIR)
GREENHOUSE_FILE = os.path.join(ROOT_DIR, "data", "greenhouse_companies.json")
ASHBY_FILE = os.path.join(ROOT_DIR, "data", "ashby_companies.json")
BAMBOOHR_FILE = os.path.join(ROOT_DIR, "data", "bamboohr_companies.json")
WORKDAY_FILE = os.path.join(ROOT_DIR, "data", "workday_companies.json")
LEVER_FILE = os.path.join(ROOT_DIR, "data", "lever_companies.json")
WORKABLE_FILE = os.path.join(ROOT_DIR, "data", "workable_companies.json")
BUILTIN_FILE = os.path.join(ROOT_DIR, "data", "builtin_companies.json")

ICIMS_FILE = os.path.join(ROOT_DIR, "data", "icims_companies.json")

OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

DEAD_SLUG_DIR = os.path.join(ROOT_DIR, "data", "dead_slugs")
os.makedirs(DEAD_SLUG_DIR, exist_ok=True)

RECRUITER_TERMS = [
    "recruit",
    "recruiting",
    "recruiter",
    "staffing",
    "staff",
    "talent",
    "talenthub",
    "talentgroup",
    "solutions",
    "consulting",
    "placement",
    "search",
    "resources",
    "agency",
]

USER_AGENTS = [
    # Chrome 144 - Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
    # Chrome 144 - macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
    # Chrome 144 - Linux
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
    # Firefox 147 - Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:147.0) Gecko/20100101 Firefox/147.0",
    # Firefox 147 - macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:147.0) Gecko/20100101 Firefox/147.0",
    # Firefox 147 - Linux
    "Mozilla/5.0 (X11; Linux x86_64; rv:147.0) Gecko/20100101 Firefox/147.0",
    # Safari 26 - macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.0 Safari/605.1.15",
    # Edge 144 - Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0",
]

# ============================================================
# LOAD COMPANIES
# ============================================================


def load_companies(filepath):
    """Load companies from JSON file."""
    try:
        with open(filepath, "r") as f:
            companies = set(json.load(f))
        print(f"Loaded {len(companies):,} companies from {filepath}")
        return companies
    except FileNotFoundError:
        print(f"File not found: {filepath}")
        return set()


def get_job_key(job):
    """Dedup key for job entries."""
    url = job.get("url") or job.get("absolute_url")
    if job.get("ats") == "Workday" and url:
        match = re.search(r"/jobs/(\d+)", url)
        if match:
            company = job.get("company", "")
            return f"workday:{company}:{match.group(1)}"
    if url:
        return url

    # fallback to company+title if URL is unavailable
    title = (job.get("title") or "").strip()
    company = job.get("company") or job.get("company_slug")
    if title and company:
        return f"{company}:{title}"

    return None


def load_existing_job_keys():
    """Load existing dedupe keys from data/jobs_manifest.json."""
    existing = set()
    manifest_path = os.path.join(ROOT_DIR, "data", "jobs_manifest.json")
    if not os.path.exists(manifest_path):
        return existing

    try:
        with open(manifest_path, "r") as f:
            manifest = json.load(f)

        for chunk_file in manifest.get("chunks", []):
            chunk_path = os.path.join(ROOT_DIR, "data", chunk_file)
            if not os.path.exists(chunk_path):
                continue
            with gzip.open(chunk_path, "rt", encoding="utf-8") as c:
                jobs = json.load(c)
                for job in jobs:
                    key = get_job_key(job)
                    if key:
                        existing.add(key)
    except Exception as e:
        print(f"Could not load existing job keys: {e}")

    print(f"Loaded {len(existing):,} existing jobs for pre-dedupe")
    return existing


def partition_company_tasks(task_list, chunks, chunk_id):
    """Partition company scrape work into independent chunks."""
    if chunks <= 0 or chunk_id <= 0 or chunk_id > chunks:
        raise ValueError("Invalid --chunks/--chunk-id combination")

    total = len(task_list)
    if total == 0:
        return []

    selected = [task_list[i] for i in range(chunk_id - 1, total, chunks)]
    print(
        f"Running chunk {chunk_id}/{chunks}: {len(selected):,} company tasks out of {total:,} total."
    )
    return selected


# ============================================================
# VERIFY ACTIVE JOBS + FETCH ALL JOBS
# ============================================================

# API requests for testing in browser console
"""
fetch("https://jobs.ashbyhq.com/api/non-user-graphql?op=ApiJobBoardWithTeams", {
  method: "POST",
  headers: {"Content-Type": "application/json"},
  body: JSON.stringify({
    operationName: "ApiJobBoardWithTeams",
    variables: {organizationHostedJobsPageName: "zip"},
    query: "query ApiJobBoardWithTeams($organizationHostedJobsPageName: String!) { jobBoard: jobBoardWithTeams(organizationHostedJobsPageName: $organizationHostedJobsPageName) { jobPostings { id title locationName } } }"
  })
}).then(r => r.json()).then(console.log)

fetch("https://{slug}.bamboohr.com/careers/list"){
    method: "GET",
    headers: {"Content-Type": "application/json"},
}.then(r => r.json()).then(console.log)

}
"""

SOURCE_TYPE = "automated"


def iso_from_epoch_ms(value):
    try:
        ms = int(value)
        # lever returns millisecond epoch
        return datetime.fromtimestamp(ms / 1000.0, timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        return None


def parse_posted_at(value):
    if value is None:
        return None

    if isinstance(value, (int, float)):
        try:
            numeric_value = float(value)
            if numeric_value > 1e12:
                return datetime.fromtimestamp(numeric_value / 1000.0, timezone.utc)
            if numeric_value > 1e9:
                return datetime.fromtimestamp(numeric_value, timezone.utc)
        except Exception:
            return None

    text = str(value).strip()
    if not text:
        return None

    lowered = text.lower()
    now = datetime.now(timezone.utc)
    cleaned = re.sub(
        r"^(posted|posted on|reposted|reposted on)\s+",
        "",
        lowered,
        flags=re.IGNORECASE,
    ).strip()

    if cleaned in ("today", "just now"):
        return now
    if cleaned == "yesterday":
        return now - timedelta(days=1)
    if cleaned in ("an hour ago", "a hour ago"):
        return now - timedelta(hours=1)
    if cleaned == "a day ago":
        return now - timedelta(days=1)
    if cleaned == "a week ago":
        return now - timedelta(weeks=1)

    rel = re.search(
        r"(?:(\d+)|an|a)\+?\s*(minute|minutes|hour|hours|day|days|week|weeks)\s+ago",
        cleaned,
    )
    if rel:
        qty = int(rel.group(1) or 1)
        unit = rel.group(2)
        if "minute" in unit:
            return now - timedelta(minutes=qty)
        if "hour" in unit:
            return now - timedelta(hours=qty)
        if "day" in unit:
            return now - timedelta(days=qty)
        if "week" in unit:
            return now - timedelta(weeks=qty)

    iso_candidate = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(iso_candidate)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        pass

    date_formats = [
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%m/%d/%y",
        "%b %d, %Y",
        "%B %d, %Y",
        "%d %b %Y",
        "%d %B %Y",
    ]
    cleaned_text = re.sub(
        r"^(posted|posted on|reposted|reposted on)\s+",
        "",
        text,
        flags=re.IGNORECASE,
    ).strip()
    for fmt in date_formats:
        try:
            return datetime.strptime(cleaned_text, fmt).replace(tzinfo=timezone.utc)
        except Exception:
            continue

    year_match = re.search(r"(19|20)\d{2}", cleaned_text)
    if year_match:
        try:
            y = int(year_match.group(0))
            return datetime(y, 1, 1, tzinfo=timezone.utc)
        except Exception:
            return None

    return None


def canonical_posted_date(value):
    text = str(value or "").strip()
    if not text:
        return ""

    dt = parse_posted_at(text)
    if dt is None:
        return text

    lowered = text.lower()
    has_subday_precision = any(
        token in lowered
        for token in (
            "minute",
            "minutes",
            "hour",
            "hours",
            "just now",
            "an hour ago",
            "a hour ago",
        )
    )
    has_explicit_time = ("t" in lowered and ":" in text) or bool(
        re.search(r"\b\d{1,2}:\d{2}\b", text)
    )
    dt_utc = dt.astimezone(timezone.utc)

    if has_subday_precision or has_explicit_time:
        return dt_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")

    return dt_utc.date().isoformat()


def get_date_posted_from_source(job, ats):
    raw_value = None

    if ats == "Greenhouse":
        # Greenhouse has first_published or updated_at
        raw_value = job.get("first_published") or job.get("updated_at") or job.get("created_at")

    elif ats == "Ashby":
        # Ashby API currently does not expose a publish date in this endpoint
        raw_value = None

    elif ats == "BambooHR":
        raw_value = job.get("postedDate") or job.get("createdDate") or job.get("updatedDate")

    elif ats == "Lever":
        lever_created = job.get("createdAt") or job.get("updatedAt")
        if lever_created:
            raw_value = iso_from_epoch_ms(lever_created) or lever_created

    elif ats == "Workday":
        raw_value = (
            job.get("postedOn")
            or job.get("postingPublishDate")
            or job.get("postingUpdatedDate")
            or job.get("requisitionPostDate")
        )

    return canonical_posted_date(raw_value) or None


def get_job_metadata():
    """Generate consistent metadata for each job."""
    return {
        "scraped_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "source": SOURCE_TYPE,
    }


def fetch_company_jobs_greenhouse(slug):
    """Fetch all jobs for a company."""
    try:
        url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"
        response = requests.get(url, timeout=30)

        if response.status_code == 200:
            data = response.json()
            jobs = data.get("jobs", [])

            if jobs:
                # Normalize job structure for frontend
                normalized = []
                for job in jobs:
                    location_name = job.get("location", {}).get("name", "Not specified")
                    remote_flag = isinstance(location_name, str) and "remote" in location_name.lower()

                    if not remote_flag:
                        continue

                    normalized.append(
                        {
                            "company": slug,
                            "company_slug": slug,
                            "title": job.get("title"),
                            "location": location_name,
                            "url": job.get("absolute_url"),
                            "absolute_url": job.get("absolute_url"),
                            "departments": [
                                d.get("name") for d in job.get("departments", [])
                            ],
                            "id": job.get("id"),
                            "updated_at": job.get("updated_at"),
                            "date_posted": get_date_posted_from_source(job, "Greenhouse"),
                            "remote": bool(remote_flag),
                            "workplaceType": "remote",
                            "is_recruiter": is_recruiter_company(slug),
                            "ats": "Greenhouse",
                            "skill_level": job_tier_classification(job.get("title", "")),
                            **get_job_metadata(),
                        }
                    )

                return slug, normalized

    except Exception as e:
        print(f"Error fetching Greenhouse for {slug}: {e}")
    return slug, []


def fetch_ashby_job_posted_date(slug, job_id):
    detail_url = "https://jobs.ashbyhq.com/api/non-user-graphql?op=ApiJobPosting"
    detail_query = "query ApiJobPosting($organizationHostedJobsPageName: String!, $jobPostingId: String!) { jobPosting(organizationHostedJobsPageName: $organizationHostedJobsPageName, jobPostingId: $jobPostingId) { id publishedDate locationName compensationTierSummary } }"

    attempts = 3
    for attempt in range(1, attempts + 1):
        detail_headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (compatible; JobFetcher/1.0)",
            "Origin": "https://jobs.ashbyhq.com",
            "Referer": f"https://jobs.ashbyhq.com/{slug}/{job_id}",
        }

        try:
            detail_response = requests.post(
                detail_url,
                json={
                    "operationName": "ApiJobPosting",
                    "query": detail_query,
                    "variables": {
                        "organizationHostedJobsPageName": slug,
                        "jobPostingId": job_id,
                    },
                },
                headers=detail_headers,
                timeout=10,
            )

            if detail_response.status_code == 200:
                job_posting = (detail_response.json().get("data") or {}).get("jobPosting") or {}
                published_date = job_posting.get("publishedDate")
                if published_date:
                    return canonical_posted_date(published_date)
                # if source returns no published date, no need to retry further
                break
            else:
                # Rate limiting or transient ashby error
                time.sleep(0.5)

        except Exception as e:
            if attempt == attempts:
                print(f"Ashby detail failed {slug}/{job_id}: {e}")
            else:
                time.sleep(0.5)

    return None


def fetch_company_jobs_ashby(slug):
    try:
        url = f"https://jobs.ashbyhq.com/api/non-user-graphql?op=ApiJobBoardWithTeams"
        payload = {
            "operationName": "ApiJobBoardWithTeams",
            "variables": {"organizationHostedJobsPageName": slug},
            "query": "query ApiJobBoardWithTeams($organizationHostedJobsPageName: String!) { jobBoard: jobBoardWithTeams(organizationHostedJobsPageName: $organizationHostedJobsPageName) { jobPostings { id title locationName } } }",
        }

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (compatible; JobFetcher/1.0)",
        }

        response = requests.post(url, json=payload, headers=headers, timeout=30)

        if response.status_code == 200:
            data = response.json()
            jobs = (data.get("data") or {}).get("jobBoard") or {}
            jobs = jobs.get("jobPostings") or []

            if jobs:
                normalized = []
                for job in jobs:
                    posting_id = job.get('id')
                    published_date = None
                    if posting_id:
                        published_date = fetch_ashby_job_posted_date(slug, posting_id)

                    normalized.append(
                        {
                            "company": slug,
                            "company_slug": slug,
                            "title": job.get("title", ""),
                            "location": job.get("locationName", "Not specified")[:50],
                            "url": f"https://jobs.ashbyhq.com/{slug}/{posting_id}",
                            "date_posted": published_date,
                            "is_recruiter": is_recruiter_company(slug),
                            "ats": "Ashby",
                            "skill_level": job_tier_classification(
                                job.get("title", "")
                            ),
                            **get_job_metadata(),
                        }
                    )
                return slug, normalized
    except Exception as e:
        print(f"Error fetching Ashby for {slug}: {e}")
    return slug, []


def fetch_company_jobs_bamboohr(slug):
    """https://{slug}.bamboohr.com/careers
    https://{slug}.bamboohr.com/careers/list

    """
    url = f"https://{slug}.bamboohr.com/careers/list"
    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (compatible; ATSProbe/1.0)",
    }

    try:
        response = requests.get(
            url,
            timeout=30,
            headers=headers,
        )

        if response.status_code == 200:

            if "application/json" not in response.headers.get("Content-Type", ""):
                print(
                    f"Unexpected content type for {slug}: {response.headers.get('Content-Type')}"
                )
                return slug, []

            data = response.json()
            jobs = data.get("result", [])

            if jobs:
                normalized = []
                for job in jobs:

                    loc = job.get("location") or {}
                    if isinstance(loc, dict):
                        city = loc.get("city", "")
                        state = loc.get("state", "")
                        location = (
                            ", ".join(filter(None, [city, state])) or "Not specified"
                        )
                    else:
                        location = str(loc) if loc else "Not specified"

                    normalized.append(
                        {
                            "company": slug,
                            "company_slug": slug,
                            "title": job.get("jobOpeningName"),
                            "location": location[:50],
                            "url": f"https://{slug}.bamboohr.com/careers/view/{job.get('id')}",
                            "date_posted": get_date_posted_from_source(job, "BambooHR"),
                            "is_recruiter": is_recruiter_company(slug),
                            "ats": "BambooHR",
                            "skill_level": job_tier_classification(
                                job.get("jobOpeningName", "")
                            ),
                            **get_job_metadata(),
                        }
                    )
                return slug, normalized
    except Exception as e:
        print(f"Error fetching BambooHR for {slug}: {e}")
    return slug, []


def fetch_company_jobs_lever(slug):
    """https://api.lever.co/v0/postings/{slug}"""

    try:
        url = f"https://api.lever.co/v0/postings/{slug}"
        response = requests.get(url, timeout=30)

        if response.status_code == 200:
            jobs = response.json()

            if jobs:
                normalized = []
                for job in jobs:
                    categories = job.get("categories", {})
                    lever_location = categories.get("location", "Not specified")
                    remote_flag = isinstance(lever_location, str) and "remote" in lever_location.lower()

                    if not remote_flag:
                        continue

                    normalized.append(
                        {
                            "company": slug,
                            "company_slug": slug,
                            "title": job.get("text"),
                            "location": lever_location[:50],
                            "url": job.get("hostedUrl"),
                            "date_posted": get_date_posted_from_source(job, "Lever"),
                            "remote": bool(remote_flag),
                            "workplaceType": "remote",
                            "is_recruiter": is_recruiter_company(slug),
                            "ats": "Lever",
                            "skill_level": job_tier_classification(job.get("text", "")),
                            **get_job_metadata(),
                        }
                    )
                return slug, normalized
    except Exception as e:
        print(f"Error fetching Lever for {slug}: {e}")
    return slug, []


def fetch_company_jobs_workday(slug):
    """
    slug format: "company|wd#|site_id" e.g. "kohls|wd1|kohlscareers"
    url: https://{company}.wd{num}.myworkdayjobs.com/wday/cxs/{company}/{site_id}/jobs
    """

    try:
        parts = slug.split("|")
        if len(parts) != 3:
            return slug, []

        company, wd, site_id = parts
        wd_num = wd.replace("wd", "")

        base_url = f"https://{company}.wd{wd_num}.myworkdayjobs.com"
        api_url = f"{base_url}/wday/cxs/{company}/{site_id}/jobs"

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": random.choice(USER_AGENTS),
            "Origin": base_url,
            "Referer": f"{base_url}/{site_id}",
        }

        normalized = []
        offset = 0
        limit = 20
        retries = 0
        max_retries = 2
        observed_total = None

        while True:
            payload = {
                "appliedFacets": {},
                "limit": limit,
                "offset": offset,
                "searchText": "",
            }

            response = requests.post(
                api_url,
                json=payload,
                headers=headers,
                timeout=30,
            )

            if response.status_code != 200:
                if retries < max_retries:
                    retries += 1
                    time.sleep(random.uniform(2.0, 4.0))
                    continue
                break

            data = response.json()
            jobs = data.get("jobPostings", [])
            total = data.get("total", 0)

            # Detect silent blocking / truncation
            if observed_total is None:
                observed_total = total
            elif total != observed_total:
                # Workday sometimes lies mid-pagination when blocking
                break

            if not jobs:
                break

            for job in jobs:
                job_path = job.get("externalPath", "")
                normalized.append(
                    {
                        "company": company,
                        "company_slug": slug,
                        "title": job.get("title"),
                        "location": job.get("locationsText", "Not specified")[:50],
                        "url": f"{base_url}/{site_id}{job_path}",
                        "date_posted": get_date_posted_from_source(job, "Workday"),
                        "is_recruiter": is_recruiter_company(company),
                        "ats": "Workday",
                        "skill_level": job_tier_classification(job.get("title", "")),
                        **get_job_metadata(),
                    }
                )

            offset += limit

            if offset >= total:
                break

            # Jitter between pages (critical)
            time.sleep(random.uniform(0.8, 1.8))

        return slug, normalized

    except Exception:
        return slug, []


def fetch_company_jobs_workable(slug):
    # URL: "https://apply.workable.com/api/v3/accounts/{company}/jobs"

    url = f"https://apply.workable.com/api/v3/accounts/{slug}/jobs"
    headers = {
        "Content-Type": "application/json",
        "Referer": "https://apply.workable.com/{slug}/",
        "Origin": "https://apply.workable.com",
        "User-Agent": random.choice(USER_AGENTS),
    }

    # response format: {"total":6,"results":[{"id":5542984,"shortcode":"72D952483B","title":"Senior Systems Engineer (Linux and Storage)","remote":false
    # {"location": {"country": "Greece", "countryCode": "GR","city": "Athens", "region": "Attica"}}

    while True:
        try:
            payload = {
                "query": "",
                "location": [],
                "department": [],
                "worktype": [],
                # remote-only by default for speed
                "remote": [True],
            }
            response = requests.post(url, json=payload, headers=headers)

            if response.status_code == 200:
                data = response.json()
                jobs = data.get("results", [])

                normalized = []
                for job in jobs:
                    location_info = job.get("location") or {}
                    location = (
                        ", ".join(
                            filter(
                                None,
                                [
                                    location_info.get("city", ""),
                                    location_info.get("region", ""),
                                    location_info.get("country", ""),
                                ],
                            )
                        )
                        or "Not specified"
                    )

                    remote_flag = job.get("remote")
                    workplace_type = None
                    if isinstance(remote_flag, bool):
                        workplace_type = "remote" if remote_flag else None

                    if not remote_flag:
                        continue

                    normalized.append(
                        {
                            "company": slug,
                            "company_slug": slug,
                            "title": job.get("title"),
                            "location": location[:50],
                            "url": f"https://apply.workable.com/{slug}/jobs/{job.get('shortcode')}",
                            "remote": bool(remote_flag),
                            "workplaceType": workplace_type,
                            "is_recruiter": is_recruiter_company(slug),
                            "ats": "Workable",
                            "skill_level": job_tier_classification(
                                job.get("title", "")
                            ),
                            **get_job_metadata(),
                        }
                    )
                return slug, normalized
        except Exception:
            return slug, []


def fetch_company_jobs_icims(slug):

    # URL: https://careers-{company}.icims.com/jobs/search?ss

    return slug, []


def fetch_company_jobs_builtin(category_path):
    """Fetch all jobs for a builtin category path."""

    all_jobs = []
    try:
        results = builtin.pull(category_path, max_pages=5, delay=0.5)
        for r in results:
            if not r:
                continue

            company = r.get("company") or category_path
            is_remote = False
            if r.get("employment_type") and "remote" in str(r.get("employment_type")).lower():
                is_remote = True
            if r.get("location") and "remote" in str(r.get("location")).lower():
                is_remote = True

            normalized = {
                "company": company,
                "company_slug": category_path,
                "title": r.get("title"),
                "location": r.get("location"),
                "url": r.get("url"),
                "date_posted": canonical_posted_date(r.get("posted_at")),
                "remote": is_remote,
                "workplaceType": r.get("employment_type") or ("remote" if is_remote else None),
                "is_recruiter": False,
                "ats": "Builtin",
                "skill_level": job_tier_classification(r.get("title", "")),
                **get_job_metadata(),
            }
            all_jobs.append(normalized)
    except Exception as e:
        print(f"Error fetching Builtin for {category_path}: {e}")

    return category_path, all_jobs, None


def load_dead_slugs(platform):
    """Load cached dead slugs for a platform."""
    filepath = os.path.join(DEAD_SLUG_DIR, f"{platform}.json")
    if not os.path.exists(filepath):
        return set()
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return set(json.load(f))
    except (json.JSONDecodeError, IOError):
        return set()


def save_dead_slugs(platform, slugs):
    """Save cached dead slugs for a platform."""
    filepath = os.path.join(DEAD_SLUG_DIR, f"{platform}.json")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(sorted(slugs), f, indent=2)
    print(f"  Cached {len(slugs):,} dead slugs for {platform}")


def fetch_all_jobs(companies, fetcher, platform="ATS", existing_job_keys=None):
    """Fetch jobs from all companies in parallel."""
    if existing_job_keys is None:
        existing_job_keys = set()

    print("=" * 80)
    print(f"FETCHING JOBS FROM {len(companies):,} COMPANIES FROM PLATFORM: {platform}")
    print("=" * 80 + "\n")

    platform_lower = platform.lower()
    dead_slugs = load_dead_slugs(platform_lower)
    live_companies = [s for s in companies if s not in dead_slugs]

    if dead_slugs:
        print(f"  Skipping {len(dead_slugs):,} known dead slugs")
        print(f"  Checking {len(live_companies):,} potentially active companies\n")

    all_jobs = []
    active_companies = {}
    failed = 0
    new_dead = set()

    MAX_WORKERS = {
        "bamboohr": 20,
        "greenhouse": 30,
        "ashby": 5,
        "lever": 30,
        "workday": 50,
        "builtin": 10,
        "icims": 30,
        "workable": 30,
    }

    max_workers = MAX_WORKERS.get(platform_lower, 30)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetcher, slug): slug for slug in live_companies}

        for i, future in enumerate(as_completed(futures), 1):
            result = future.result()
            slug = result[0]
            jobs = result[1] if len(result) > 1 else []
            status_code = result[2] if len(result) > 2 else None

            if jobs:
                filtered_jobs = []
                for job in jobs:
                    key = get_job_key(job)
                    if key and key in existing_job_keys:
                        continue
                    if key:
                        existing_job_keys.add(key)
                    filtered_jobs.append(job)

                all_jobs.extend(filtered_jobs)
                active_companies[slug] = len(filtered_jobs)
                print(f"  [{i}/{len(live_companies)}] {slug}: {len(filtered_jobs)} jobs (filtered {len(jobs)-len(filtered_jobs)})")
            else:
                failed += 1
                if status_code in (404, 410):
                    new_dead.add(slug)
                if i % 50 == 0:
                    print(f"  [{i}/{len(live_companies)}] Checked... ({failed} inactive)")

    if new_dead:
        save_dead_slugs(platform_lower, dead_slugs | new_dead)

    print(f"\nDETAILED STATS FOR {platform}:")
    print(f"  Companies checked: {len(live_companies)}")
    print(f"  Companies with jobs: {len(active_companies)}")
    print(f"  Failed/empty: {failed}")
    print(f"  Newly dead: {len(new_dead)}")
    print(f"  Total jobs: {len(all_jobs)}")

    return active_companies, all_jobs


# ============================================================
# Helper Functions
# ============================================================


def is_recruiter_company(slug):
    slug = slug.lower()

    # Keyword-based detection
    if any(term in slug for term in RECRUITER_TERMS):
        return True

    return False


def clean_job_data(jobs):
    """Remove invalid/useless job entries."""
    cleaned = []
    skipped_reasons = {"no_title": 0, "no_url": 0, "no_company": 0}

    for job in jobs:
        title = (job.get("title") or "").strip().lower()
        url = job.get("url") or job.get("absolute_url")
        company = job.get("company") or job.get("company_slug")

        # Skip jobs with invalid titles
        if not title or title in ["not specified", "n/a", "unknown", ""]:
            skipped_reasons["no_title"] += 1
            continue

        # Skip jobs without URLs
        if not url:
            skipped_reasons["no_url"] += 1
            continue

        # Skip jobs without company info
        if not company:
            skipped_reasons["no_company"] += 1
            continue

        cleaned.append(job)

    # Print summary
    total_skipped = sum(skipped_reasons.values())
    if total_skipped > 0:
        print(f"\n  Skipped {total_skipped:,} invalid jobs:")
        for reason, count in skipped_reasons.items():
            if count > 0:
                print(f"    - {reason.replace('_', ' ').title()}: {count:,}")

    return cleaned


def job_tier_classification(title):
    """Classify job tier using weighted keyword scoring."""

    title_lower = title.lower()
    score = 0

    # Weights: positive = senior, negative = junior
    keywords = {
        # Strong senior indicators
        r"\b(?:chief|cto|ceo|cfo|vp|vice president|director)\b": 50,  # chief, cto, ceo, cfo, vp, vice president, director
        r"\b(?:principal|distinguished|fellow)\b": 40,  # principal, distinguished, fellow
        r"\b(?:staff|lead|head of)\b": 30,  # staff, lead, head of
        r"\b(?:senior|sr\.?)\b": 20,  # senior, sr.
        r"\b(?:architect|manager)\b": 15,  # architect, manager
        r"\b(?:iii|iv|v|vi)\b": 15,  # Roman numerals, i.e. III, IV, V, VI for levels
        r"\blevel\s*[4-9]\b": 15,  # e.g. Level 4, Level 5, Level 6, Level 7, Level 8, Level 9
        r"\bengr?\s*[4-6]\b": 15,  # e.g. Engr 4, Engr 5, Engr 6
        r"\b(?:counsel|of\s*counsel)\b": 20,  # senior attorney
        r"\b(?:attending|charge)\b": 20,  # attending physician, charge nurse = senior
        # Weak senior indicators
        r"\b(?:ii|2)\b": 5,  # level II or 2
        r"\blevel\s*3\b": 5,  # level 3
        # Entry-level indicators
        r"\b(?:associate)\b": -10,  # associate
        r"\b(?:junior|jr\.?)\b": -20,  # junior, jr.
        r"\b(?:trainee|graduate|new\s*grad)\b": -25,  # trainee, graduate, new grad
        r"\bentry[\s-]?level\b": -25,  # entry-level
        r"\b(?:i|1)\b(?!\s*-|\d)": -15,  # "I" or "1" but not "1-2" or "10"
        r"\b(?:trainee|graduate|new\s*grad)\b": -25,  # trainee, graduate, new grad
        r"\b(?:paralegal|clerk)\b": -15,  # entry-level legal
        r"\b(?:resident|clinical\s*fellow)\b": -15,  # medical residency = entry-ish
        r"\b(?:aide|assistant|tech)\b": -10,  # nurse aide, medical assistant
        # Intern (heavily weighted)
        r"\bintern(?:ship)?\b": -100,  # intern or internship
    }

    # Calculate score
    for pattern, weight in keywords.items():
        if re.search(pattern, title_lower):  # if pattern matches
            score += weight

    # tiers
    if score <= -50:
        return "intern"
    elif score <= -5:
        return "entry"
    elif score >= 15:
        return "senior"
    else:
        return "mid"


# ============================================================
# SAVE RESULTS
# ============================================================


def save_results(all_companies, active_companies, all_jobs, output_prefix="jobs_chunk"):
    """Save all data to JSON files."""
    print("=" * 80)
    print("SAVING RESULTS")
    print("=" * 80 + "\n")

    original_count = len(all_jobs)
    all_jobs = clean_job_data(all_jobs)
    cleaned_count = original_count - len(all_jobs)
    print(f"Removed {cleaned_count:,} invalid jobs (blank/not specified titles)")

    timestamp = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    # Save all companies list
    companies_file = os.path.join(OUTPUT_DIR, "all_companies.json")
    with open(companies_file, "w") as f:
        json.dump(sorted(list(all_companies)), f, indent=2)
    print(f"All companies: {companies_file}")

    # Save active companies with job counts
    active_file = os.path.join(OUTPUT_DIR, "active_companies.json")
    with open(active_file, "w") as f:
        json.dump(active_companies, f, indent=2, sort_keys=True)
    print(f"Active companies: {active_file}")

    # Save all jobs
    all_jobs_file = os.path.join(OUTPUT_DIR, "all_jobs.json")
    with open(all_jobs_file, "w") as f:
        json.dump(all_jobs, f, indent=2)
    print(f"All jobs: {all_jobs_file} ({len(all_jobs):,} jobs)")

    # Build slim version for frontend
    FRONTEND_FIELDS = {
        "title",
        "company",
        "location",
        "url",
        "ats",
        "skill_level",
        "is_recruiter",
        "remote",
        "workplaceType",
        "date_posted",
    }

    slim_jobs = [
        {k: job.get(k) for k in FRONTEND_FIELDS if k in job} for job in all_jobs
    ]

    # Pre-sort by company name for better frontend caching
    slim_jobs.sort(
        key=lambda x: (x.get("company", "").lower(), x.get("title", "").lower())
    )

    # Remove old chunk files to prevent confusion and save space
    for old_chunk in os.listdir(OUTPUT_DIR):
        if old_chunk.startswith("jobs_chunk_") and old_chunk.endswith(".json.gz"):
            os.remove(os.path.join(OUTPUT_DIR, old_chunk))

    # Split into chunks of ~25k for frontend loading (with gzip compression)
    CHUNK_SIZE = 25_000

    chunks = [
        slim_jobs[i : i + CHUNK_SIZE] for i in range(0, len(slim_jobs), CHUNK_SIZE)
    ]

    chunk_filenames = []
    for idx, chunk in enumerate(chunks):
        chunk_file = os.path.join(OUTPUT_DIR, f"{output_prefix}_{idx}.json.gz")
        with gzip.open(chunk_file, "wt", encoding="utf-8") as f:
            json.dump(chunk, f, indent=0)
        chunk_filenames.append(f"{output_prefix}_{idx}.json.gz")
        size_mb = os.path.getsize(chunk_file) / (1024 * 1024)
        print(f"  Chunk {idx}: {len(chunk):,} jobs ({size_mb:.1f}MB)")

    # Manifest so the frontend knows what to load
    manifest = {
        "chunks": chunk_filenames,
        "totalJobs": len(slim_jobs),
        "last_updated": timestamp,
    }
    manifest_name = "jobs_manifest.json" if output_prefix == "jobs_chunk" else f"jobs_manifest_{output_prefix}.json"
    manifest_file = os.path.join(OUTPUT_DIR, manifest_name)
    with open(manifest_file, "w") as f:
        json.dump(manifest, f, indent=2)

    recruiter_jobs = sum(1 for job in all_jobs if job.get("is_recruiter"))

    # Save metadata summary
    metadata = {
        "last_updated": timestamp,
        "total_companies": len(all_companies),
        "active_companies": len(active_companies),
        "total_jobs": len(all_jobs),
        "recruiter_jobs": recruiter_jobs,
        "source_type": SOURCE_TYPE,
        "platforms": "greenhouse_api, ashby_api, bamboohr_api, lever_api, workday_api, builtin_scraper",
    }

    metadata_file = os.path.join(OUTPUT_DIR, "metadata.json")
    with open(metadata_file, "w") as f:
        json.dump(metadata, f, indent=2)
    print(f"Metadata: {metadata_file}")

    print()


# ============================================================
# MAIN
# ============================================================


def main(platform='all', company_limit=0, chunks=0, chunk_id=0):
    print("\n" + "=" * 80)
    print("JOB BOARD AGGREGATOR")
    print("Scraping all jobs from ATS companies")
    print("=" * 80)

    # Load existing companies
    greenhouse_companies = load_companies(GREENHOUSE_FILE)
    ashby_companies = load_companies(ASHBY_FILE)
    bamboohr_companies = load_companies(BAMBOOHR_FILE)
    lever_companies = load_companies(LEVER_FILE)
    workday_companies = load_companies(WORKDAY_FILE)
    builtin_companies = load_companies(BUILTIN_FILE)

    if (
        not greenhouse_companies
        and not ashby_companies
        and not bamboohr_companies
        and not lever_companies
        and not workday_companies
        and not builtin_companies
    ):
        print("Exiting - no companies loaded!")
        return

    # Apply company limit for quick tests
    if company_limit > 0:
        def _limit_set(s):
            return set(islice(sorted(s), company_limit))

        greenhouse_companies = _limit_set(greenhouse_companies)
        ashby_companies = _limit_set(ashby_companies)
        bamboohr_companies = _limit_set(bamboohr_companies)
        lever_companies = _limit_set(lever_companies)
        workday_companies = _limit_set(workday_companies)
        builtin_companies = _limit_set(builtin_companies)
        print(f"LIMITING to {company_limit} companies per platform for quick test run")

    # Pre-dedupe (skip jobs already in existing data)
    existing_job_keys = load_existing_job_keys()

    # Fetch from selected sources in parallel per platform
    sources = set([platform]) if platform != 'all' else {'greenhouse', 'ashby', 'bamboohr', 'lever', 'workday', 'builtin'}

    platform_map = {
        'greenhouse': (greenhouse_companies, fetch_company_jobs_greenhouse, 'GREENHOUSE'),
        'ashby': (ashby_companies, fetch_company_jobs_ashby, 'ASHBY'),
        'bamboohr': (bamboohr_companies, fetch_company_jobs_bamboohr, 'BAMBOOHR'),
        'lever': (lever_companies, fetch_company_jobs_lever, 'LEVER'),
        'workday': (workday_companies, fetch_company_jobs_workday, 'WORKDAY'),
        'builtin': (builtin_companies, fetch_company_jobs_builtin, 'BUILTIN'),
    }

    selected_platforms = [p for p in sources if p in platform_map]
    if not selected_platforms:
        print(f"No valid platforms selected: {sources}")
        return

    if chunks > 0:
        task_list = []
        for platform_name in selected_platforms:
            companies_set = platform_map[platform_name][0]
            for slug in sorted(companies_set):
                task_list.append((platform_name, slug))

        selected_tasks = partition_company_tasks(task_list, chunks, chunk_id)

        limited_sets = {name: set() for name in platform_map}
        for platform_name, slug in selected_tasks:
            limited_sets[platform_name].add(slug)

        greenhouse_companies = limited_sets['greenhouse']
        ashby_companies = limited_sets['ashby']
        bamboohr_companies = limited_sets['bamboohr']
        lever_companies = limited_sets['lever']
        workday_companies = limited_sets['workday']
        builtin_companies = limited_sets['builtin']
        print("Applied chunk partitioning to selected companies")

        platform_map = {
            'greenhouse': (greenhouse_companies, fetch_company_jobs_greenhouse, 'GREENHOUSE'),
            'ashby': (ashby_companies, fetch_company_jobs_ashby, 'ASHBY'),
            'bamboohr': (bamboohr_companies, fetch_company_jobs_bamboohr, 'BAMBOOHR'),
            'lever': (lever_companies, fetch_company_jobs_lever, 'LEVER'),
            'workday': (workday_companies, fetch_company_jobs_workday, 'WORKDAY'),
            'builtin': (builtin_companies, fetch_company_jobs_builtin, 'BUILTIN'),
        }

    platform_jobs = {}
    all_active_companies = {}
    all_jobs = []

    with ThreadPoolExecutor(max_workers=len(selected_platforms)) as platform_executor:
        futures = {
            platform_executor.submit(
                fetch_all_jobs,
                platform_map[platform][0],
                platform_map[platform][1],
                platform_map[platform][2],
                existing_job_keys,
            ): platform
            for platform in selected_platforms
        }

        for future in as_completed(futures):
            platform_name = futures[future]
            active, jobs = future.result()
            platform_jobs[platform_name] = (active, jobs)
            all_active_companies.update(active)
            all_jobs.extend(jobs)
            print(f"\n  >>> {platform_name.upper()} COMPLETE: {len(active):,} active, {len(jobs):,} jobs <<<\n")

    # Combine results
    all_companies = (
        greenhouse_companies
        | ashby_companies
        | bamboohr_companies
        | lever_companies
        | workday_companies
        | builtin_companies
    )

    output_prefix = f"jobs_chunk_partial_{chunk_id}" if chunks > 0 else "jobs_chunk"
    save_results(all_companies, all_active_companies, all_jobs, output_prefix=output_prefix)

    # Final summary
    print("=" * 80)
    print("FINAL SUMMARY")
    print("=" * 80)
    print(f"Total companies:   {len(all_companies):,}")
    print(f"Active companies:  {len(all_active_companies):,}")
    print(f"Total jobs:        {len(all_jobs):,}")
    print(f"\nAll data saved to '{OUTPUT_DIR}/' directory")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Job Board Aggregator Scraper")
    parser.add_argument(
        "--source",
        choices=["automated", "manual"],
        default="automated",
        help="Source type: automated (GitHub Actions) or manual (local run)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit number of companies per platform (for quick test runs, 0 means no limit)",
    )
    parser.add_argument(
        "--chunks",
        type=int,
        default=0,
        help="Total number of scrape partitions for GitHub Actions matrix jobs",
    )
    parser.add_argument(
        "--chunk-id",
        type=int,
        default=0,
        help="1-based chunk index to run when --chunks is set",
    )
    parser.add_argument(
        "--platform",
        choices=["all", "greenhouse", "ashby", "bamboohr", "lever", "workday", "builtin"],
        default="all",
        help="Select one platform only (default: all)",
    )

    args = parser.parse_args()
    SOURCE_TYPE = args.source
    COMPANY_LIMIT = args.limit
    PLATFORM = args.platform

    print(f"\nRunning in {SOURCE_TYPE.upper()} mode\n")

    main(PLATFORM, COMPANY_LIMIT, args.chunks, args.chunk_id)
