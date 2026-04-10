import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from typing import List, Dict, Optional
import time
import re
import json

BASE = "https://builtin.com"

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/121.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9"
})


def _extract_id(url: str) -> str:
    return urlparse(url).path.rstrip("/").split("/")[-1]


def _extract_job_urls(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls = {
        urljoin(BASE, a["href"])
        for a in soup.select("a[href^='/job/'], a[href^='/jobs/']")
        if a.get("href")
    }
    return list(urls)


def _parse_salary(pay_text: Optional[str]):
    salary_min = salary_max = currency = None

    if not pay_text:
        return salary_min, salary_max, currency

    match = re.search(r"(\d+)K-(\d+)K", pay_text)
    if match:
        salary_min = int(match.group(1)) * 1000
        salary_max = int(match.group(2)) * 1000
        currency = "USD"

    return salary_min, salary_max, currency


def _parse_locations(soup: BeautifulSoup):
    locations = []

    tooltip_span = soup.select_one("span[data-bs-toggle='tooltip']")
    if tooltip_span:
        tooltip_html = tooltip_span.get("title", "")
        tooltip_soup = BeautifulSoup(tooltip_html, "html.parser")
        for div in tooltip_soup.find_all("div"):
            text = div.get_text(strip=True)
            if text:
                locations.append(text)

    if not locations:
        for script in soup.select("script[type='application/ld+json']"):
            raw_text = (script.string or script.get_text() or "").strip()
            if not raw_text:
                continue
            try:
                payload = json.loads(raw_text)
            except Exception:
                continue

            records = payload if isinstance(payload, list) else [payload]
            for record in records:
                if not isinstance(record, dict):
                    continue
                job_locations = record.get("jobLocation")
                if not isinstance(job_locations, list):
                    continue
                for item in job_locations:
                    if not isinstance(item, dict):
                        continue
                    addr = item.get("address")
                    if not isinstance(addr, dict):
                        continue
                    parts = [
                        str(addr.get("addressLocality", "") or "").strip(),
                        str(addr.get("addressRegion", "") or "").strip(),
                        str(addr.get("addressCountry", "") or "").strip(),
                    ]
                    text = ", ".join([p for p in parts if p])
                    if text:
                        locations.append(text)

    if not locations:
        candidate_remote = None
        for span in soup.find_all("span"):
            text = span.get_text(strip=True)
            lowered = text.lower()
            if not text or len(text) > 80:
                continue
            if "employee" in lowered:
                continue
            if any(term in lowered for term in ("manager", "engineer", "director", "analyst", "specialist", "intern")):
                continue
            if (
                re.search(r",\s*[A-Z]{2}\b", text)
                or "united states" in lowered
                or "usa" in lowered
            ):
                locations.append(text)
                break
            if lowered == "remote" or lowered.startswith("remote "):
                candidate_remote = text
        if not locations and candidate_remote:
            locations.append(candidate_remote)

    return ", ".join(locations) if locations else None


def _parse_work_type(soup: BeautifulSoup):
    for span in soup.find_all("span"):
        text = span.get_text(strip=True)
        lowered = text.lower()
        if not text:
            continue
        if lowered in {"remote", "hybrid", "onsite", "on-site", "in office", "in-office"}:
            return text
        if "hiring remotely" in lowered:
            return "remote"
    return None


def _parse_detail(url: str) -> Optional[Dict]:
    try:
        r = SESSION.get(url, timeout=(5, 10))
        r.raise_for_status()
    except Exception:
        return None

    soup = BeautifulSoup(r.text, "html.parser")

    title = None
    title_tag = soup.select_one("h1 span")
    if title_tag:
        title = title_tag.get_text(strip=True)
    if not title:
        h1 = soup.find("h1")
        if h1:
            title = h1.get_text(strip=True)

    company = None
    company_tag = soup.select_one("a[href^='/company/'] h2")
    if company_tag:
        company = company_tag.get_text(strip=True)
    if not company:
        company_anchor = soup.select_one("a[href^='/company/']")
        if company_anchor:
            company = company_anchor.get_text(strip=True)

    posted = None
    posted_tag = soup.find("span", string=lambda t: t and ("Posted" in t or "Reposted" in t))
    if posted_tag:
        posted = posted_tag.get_text(strip=True)

    pay_raw = None
    for span in soup.find_all("span"):
        txt = span.get_text(strip=True)
        if "Annually" in txt or "Hourly" in txt:
            pay_raw = txt
            break

    salary_min, salary_max, currency = _parse_salary(pay_raw)

    location = _parse_locations(soup)
    employment_type = _parse_work_type(soup)

    description = None
    desc_div = soup.find("div", id=lambda x: x and x.startswith("job-post-body-"))
    if desc_div:
        description = desc_div.get_text("\n", strip=True)

    if not description:
        desc_alt = soup.select_one(".html-parsed-content")
        if desc_alt:
            description = desc_alt.get_text("\n", strip=True)

    if not description:
        article = soup.find("article")
        if article:
            description = article.get_text("\n", strip=True)

    return {
        "platform": "builtin",
        "external_id": _extract_id(url),
        "url": url,
        "title": title,
        "company": company,
        "location": location,
        "posted_at": posted,
        "employment_type": employment_type,
        "department": None,
        "raw_payload": {"pay_raw": pay_raw},
    }


def pull(category_path: str, max_pages: int = 5, delay: float = 0.5) -> List[Dict]:
    all_jobs = []
    seen_ids = set()

    for page in range(4, max_pages + 1):
        separator = "&" if "?" in category_path else "?"
        url = f"{BASE}{category_path}{separator}page={page}"
        print(f"[builtin] fetching category page: {url}")

        try:
            r = SESSION.get(url, timeout=(5, 10))
            r.raise_for_status()
        except Exception as e:
            print(f"[builtin] failed category page: {url} -> {e}")
            break

        job_urls = _extract_job_urls(r.text)
        if not job_urls:
            break

        for job_url in job_urls:
            job = _parse_detail(job_url)
            if not job:
                continue
            if job["external_id"] in seen_ids:
                continue

            seen_ids.add(job["external_id"])
            job["slug"] = category_path
            all_jobs.append(job)

            time.sleep(delay)

    return all_jobs
