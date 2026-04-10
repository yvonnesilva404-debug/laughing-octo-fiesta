// ============================================================
// FILTERING
// ============================================================

import { escapeRegex } from './ui_utils.js';
import { loadApplicationStatus } from './storage.js';

const US_LOCATION_ALIASES = new Set([
    'us', 'usa', 'u.s.', 'u.s.a.', 'united states', 'united states of america',
]);

const REMOTE_GENERIC_TERMS = new Set([
    'remote', 'anywhere', 'work from home', 'wfh', 'distributed', 'virtual', 'telecommute',
]);

const US_STATE_CODES = new Set([
    'al', 'ak', 'az', 'ar', 'ca', 'co', 'ct', 'de', 'fl', 'ga', 'hi', 'id', 'il', 'in', 'ia', 'ks',
    'ky', 'la', 'me', 'md', 'ma', 'mi', 'mn', 'ms', 'mo', 'mt', 'ne', 'nv', 'nh', 'nj', 'nm', 'ny',
    'nc', 'nd', 'oh', 'ok', 'or', 'pa', 'ri', 'sc', 'sd', 'tn', 'tx', 'ut', 'vt', 'va', 'wa', 'wv',
    'wi', 'wy', 'dc',
]);

// These can appear as normal words ("in", "or", "me", etc.).
const AMBIGUOUS_US_STATE_CODES = new Set(['in', 'or', 'me', 'hi', 'ok']);

const US_STATE_NAMES = [
    'alabama', 'alaska', 'arizona', 'arkansas', 'california', 'colorado', 'connecticut', 'delaware',
    'florida', 'georgia', 'hawaii', 'idaho', 'illinois', 'indiana', 'iowa', 'kansas', 'kentucky',
    'louisiana', 'maine', 'maryland', 'massachusetts', 'michigan', 'minnesota', 'mississippi', 'missouri',
    'montana', 'nebraska', 'nevada', 'new hampshire', 'new jersey', 'new mexico', 'new york',
    'north carolina', 'north dakota', 'ohio', 'oklahoma', 'oregon', 'pennsylvania', 'rhode island',
    'south carolina', 'south dakota', 'tennessee', 'texas', 'utah', 'vermont', 'virginia', 'washington',
    'west virginia', 'wisconsin', 'wyoming', 'district of columbia',
];

const US_CITY_HINTS = new Set([
    'miami', 'washington', 'washington dc', 'washington, dc', 'new york city', 'san francisco', 'los angeles',
    'chicago', 'seattle', 'austin', 'boston', 'atlanta', 'dallas', 'denver', 'philadelphia', 'phoenix',
    'houston', 'san diego', 'portland', 'nashville', 'charlotte', 'detroit', 'minneapolis',
]);

const NON_US_REGIONAL_HINTS = new Set([
    'europe', 'emea', 'apac', 'latam', 'eu', 'global', 'worldwide', 'international',
    'remote-europe', 'remote uk',
]);

const NON_US_SUBNATIONAL_HINTS = new Set([
    // Frequently seen Canada-only hints.
    'ontario', 'quebec', 'british columbia', 'alberta', 'manitoba', 'saskatchewan',
    'nova scotia', 'new brunswick', 'newfoundland', 'prince edward island',
]);

function normalizeLocationText(value) {
    return String(value || '').toLowerCase().trim();
}

function textContainsTerm(text, term) {
    if (!term) return false;
    if (term.includes(' ') || term.includes('-')) {
        return text.includes(term);
    }
    const pattern = new RegExp(`(^|[^a-z])${escapeRegex(term)}([^a-z]|$)`, 'i');
    return pattern.test(text);
}

function buildNonUSCountryHints() {
    const hints = new Set([
        // Common aliases not guaranteed by Intl display names.
        'uk', 'u.k.', 'united kingdom', 'turkiye',
    ]);

    try {
        if (typeof Intl !== 'undefined' && typeof Intl.DisplayNames === 'function') {
            const regionCodes = (typeof Intl.supportedValuesOf === 'function')
                ? Intl.supportedValuesOf('region')
                : [];
            const regionNames = new Intl.DisplayNames(['en'], { type: 'region' });

            for (const code of regionCodes) {
                const upper = String(code || '').toUpperCase();
                if (!upper || upper === 'US') continue;
                const name = regionNames.of(upper);
                if (!name) continue;
                const normalized = normalizeLocationText(name);
                if (!normalized || US_LOCATION_ALIASES.has(normalized)) continue;
                hints.add(normalized);
            }
        }
    } catch {
        // Keep fallback hints only when Intl APIs are unavailable.
    }

    return hints;
}

const NON_US_COUNTRY_HINTS = buildNonUSCountryHints();

function hasNonUSHint(location) {
    const text = normalizeLocationText(location);
    if (!text) return false;

    for (const hint of NON_US_REGIONAL_HINTS) {
        if (textContainsTerm(text, hint)) return true;
    }

    for (const hint of NON_US_SUBNATIONAL_HINTS) {
        if (textContainsTerm(text, hint)) return true;
    }

    for (const hint of NON_US_COUNTRY_HINTS) {
        if (textContainsTerm(text, hint)) return true;
    }

    return false;
}

function isRemoteOnlyLocation(location) {
    if (!location) return false;
    return location.toLowerCase().includes('remote');
}

function hasUSStateCode(rawLocation, normalizedLocation) {
    const rawTokens = String(rawLocation)
        .split(/[^A-Za-z]/)
        .filter(Boolean);

    for (const rawToken of rawTokens) {
        if (rawToken.length !== 2) continue;
        const token = rawToken.toLowerCase();
        if (!US_STATE_CODES.has(token)) continue;

        // Avoid false positives like "remote in canada".
        if (!AMBIGUOUS_US_STATE_CODES.has(token)) return true;

        // Accept ambiguous codes only when they look intentionally abbreviated.
        if (rawToken === rawToken.toUpperCase()) return true;

        const punctuatedCodePattern = new RegExp(`(^|[,/()\\-]\\s*)${escapeRegex(token)}(\\s*[,/()\\-]|$)`, 'i');
        if (punctuatedCodePattern.test(normalizedLocation)) return true;
    }

    return false;
}

const UNKNOWN_LOCATION_STRINGS = new Set([
    'not specified', 'n/a', 'not available', 'unknown', 'unspecified', 'none', '-',
]);

function isUSLocation(rawLocation) {
    // Blank/null/unknown → treat as unspecified → assume US
    if (!rawLocation) return true;
    const location = String(rawLocation).toLowerCase().trim();
    if (!location || UNKNOWN_LOCATION_STRINGS.has(location)) return true;

    if (US_LOCATION_ALIASES.has(location)) return true;
    if (location.includes('united states') || location.includes('usa') || location.includes('u.s.')) return true;

    if (isRemoteOnlyLocation(location)) {
        if (!hasNonUSHint(location)) return true;
    }

    if (hasUSStateCode(rawLocation, location)) return true;

    if (US_STATE_NAMES.some(name => location.includes(name))) return true;
    for (const city of US_CITY_HINTS) {
        if (location.includes(city)) return true;
    }

    return false;
}

function normalizeFilterLocation(rawLocation) {
    if (!rawLocation || typeof rawLocation !== 'string') return '';
    const location = rawLocation.toLowerCase().trim();
    if (US_LOCATION_ALIASES.has(location)) return 'united states';
    return location;
}

function shouldMatchByUSCanonical(filterLocation, jobLocation) {
    return filterLocation === 'united states' && isUSLocation(jobLocation);
}

function toDateUsingPosted(job) {
    const candidates = [
        job.date_posted,
        job.posted_at,
        job.posted_on,
        job.postedDate,
        job.postedOn,
        job.postedDate,
        job.datePosted,
        job.publish_date,
        job.published_at,
        job.startDate,
    ];

    for (const raw of candidates) {
        if (!raw) continue;
        const value = typeof raw === 'string' ? raw.trim() : String(raw).trim();
        if (!value) continue;

        const dt = new Date(value);
        if (!Number.isNaN(dt.getTime())) {
            return dt;
        }
    }
    return null;
}

/**
 * Read current filter values from the DOM.
 * @returns {object} Filter state object
 */
export function readFilterInputs() {
    return {
        hideRecruiters: document.getElementById('filter-hide-recruiters').checked,
        remoteOnly: document.getElementById('filter-remote-only').checked,
        hideApplied: document.getElementById('filter-hide-applied').checked,
        title: document.getElementById('filter-title').value.toLowerCase().trim(),
        company: document.getElementById('filter-company').value.toLowerCase().trim(),
        location: document.getElementById('filter-location').value.toLowerCase().trim(),
        freshness: document.getElementById('filter-freshness').value,
        status: document.getElementById('filter-status').value,
        ats: document.getElementById('filter-ats').value,
        skill_level: document.getElementById('filter-skill-level').value,
        exclude: document.getElementById('filter-exclude').value.toLowerCase().trim(),
    };
}

/**
 * Filter the full jobs array based on the current filter inputs.
 * @param {Array} allJobs - The complete jobs array
 * @returns {{ filteredJobs: Array, filterState: object }}
 */
export function filterJobs(allJobs) {
    const f = readFilterInputs();
    const apps = loadApplicationStatus();

    const titleRegex = f.title ? new RegExp(`\\b${escapeRegex(f.title)}\\b`, 'i') : null;
    const companyRegex = f.company ? new RegExp(`\\b${escapeRegex(f.company)}\\b`, 'i') : null;
    const normalizedFilterLocation = normalizeFilterLocation(f.location);
    const isUSLocationFilter = normalizedFilterLocation === 'united states';
    const locationRegex = (f.location && !isUSLocationFilter)
        ? new RegExp(`\\b${escapeRegex(f.location)}\\b`, 'i')
        : null;
    const atsLower = f.ats ? f.ats.toLowerCase() : null;
    const excludeTerms = f.exclude ? f.exclude.split(',').map(t => t.trim()).filter(Boolean) : null;
    let freshnessMaxDays = null;
    const freshnessNow = f.freshness ? Date.now() : 0;
    if (f.freshness) {
        const n = parseInt(f.freshness, 10);
        if (!Number.isNaN(n) && n > 0) freshnessMaxDays = Math.min(n, 14);
    }

    const filterState = {
        title: f.title,
        company: f.company,
        location: f.location,
        freshness: f.freshness,
        remoteOnly: f.remoteOnly,
        status: f.status,
        ats: f.ats,
        skill_level: f.skill_level,
        exclude: f.exclude
    };

    const filteredJobs = allJobs.filter(job => {
        // Recruiter filter
        if (f.hideRecruiters && job.is_recruiter === true) return false;

        // Application status
        const url = job.url;
        const jobStatus = apps[url]?.status || '';

        if (f.hideApplied && (jobStatus === 'applied' || jobStatus === 'ignored')) return false;
        if (f.status && jobStatus !== f.status) return false;

        // Text fields
        const title = (job.title || '').toLowerCase();
        const company = ((job.company || job.company_slug) || '').toLowerCase();
        let location = '';
        if (job.location) {
            location = typeof job.location === 'object'
                ? (job.location.name || '').toLowerCase()
                : (job.location || '').toLowerCase();
        }

        // Remote only
        if (f.remoteOnly) {
            const isRemote = job.remote === true
                || location.includes('remote')
                || (job.workplaceType && job.workplaceType.toLowerCase() === 'remote');
            if (!isRemote) return false;
        }

        // ATS
        if (atsLower) {
            if ((job.ats || '').toLowerCase() !== atsLower) return false;
        }

        // Freshness / age filter (uses posted-date fields only; missing dates are treated as unavailable)
        if (freshnessMaxDays !== null) {
            const postedAt = toDateUsingPosted(job);
            if (!postedAt) return false;
            const ageDays = (freshnessNow - postedAt.getTime()) / 86400000;
            if (ageDays > freshnessMaxDays) return false;
        }

        // Skill level
        if (f.skill_level) {
            const jobSkillLevel = (job.skill_level || '').toLowerCase();
            if (jobSkillLevel !== f.skill_level.toLowerCase()) return false;
        }

        // Exclude title keywords
        if (excludeTerms && excludeTerms.some(term => title.includes(term))) return false;

        if (isUSLocationFilter) {
            if (!isUSLocation(location)) return false;
        } else if (locationRegex) {
            if (!locationRegex.test(location)) return false;
        }

        return (
            (!titleRegex || titleRegex.test(title)) &&
            (!companyRegex || companyRegex.test(company))
        );
    });

    return { filteredJobs, filterState };
}

/** Reset all filter DOM inputs to defaults */
export function clearFilterInputs() {
    document.getElementById('filter-title').value = '';
    document.getElementById('filter-company').value = '';
    document.getElementById('filter-location').value = '';
    document.getElementById('filter-status').value = '';
    document.getElementById('filter-ats').value = '';
    document.getElementById('filter-skill-level').value = '';
    document.getElementById('filter-freshness').value = '';
    document.getElementById('filter-hide-recruiters').checked = false;
    document.getElementById('filter-remote-only').checked = false;
    document.getElementById('filter-hide-applied').checked = false;
}
