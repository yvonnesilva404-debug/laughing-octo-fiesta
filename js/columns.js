// ============================================================
// COLUMN CONFIGURATION
// ============================================================

import { escape } from './ui_utils.js';
import { loadApplicationStatus } from './storage.js';

/** Build and return the column definitions for the job table */
export function createColumns() {
    return [
        { key: 'company', label: 'Company', sortable: false },
        { key: 'title', label: 'Title', sortable: false },
        { key: 'location', label: 'Location', sortable: false },
        {
            key: 'skill_level',
            label: 'Experience Level',
            sortable: false,
            render: job => {
                const rawLevel = job.skill_level || job.experience_level || job.level || job.seniority || '';
                const level = String(rawLevel).trim();
                if (!level) {
                    return '<span class="text-muted">Not specified</span>';
                }
                const display = level.charAt(0).toUpperCase() + level.slice(1).toLowerCase();
                return `<span class="experience-label">${escape(display)}</span>`;
            }
        },
        {
            key: 'date_posted',
            label: 'Date',
            sortable: false,
            render: job => {
                const rawDate = job.date_posted || job.posted_at || job.posted_on;
                if (rawDate) {
                    const parsed = new Date(rawDate);
                    if (!Number.isNaN(parsed.getTime())) {
                        return parsed.toLocaleDateString();
                    }
                    return rawDate;
                }

                const scrubDate = job.scraped_at;
                if (scrubDate) {
                    const parsed = new Date(scrubDate);
                    if (!Number.isNaN(parsed.getTime())) {
                        return `${parsed.toLocaleDateString()} (scraped)`;
                    }
                    return `${scrubDate} (scraped)`;
                }

                return 'N/A';
            }
        },
        {
            key: 'ats',
            label: 'ATS',
            sortable: false,
            render: job => {
                const ats = job.ats || 'unknown';
                const colors = {
                    'greenhouse': 'success',
                    'lever': 'primary',
                    'workday': 'warning',
                    'ashby': 'info',
                    'icms': 'secondary',
                    'bamboohr': 'danger',
                    'workable': 'dark',
                    'unknown': 'primary'
                };
                const color = colors[ats.toLowerCase()] || 'light';
                return `<span class="badge bg-${color}">${escape(ats)}</span>`;
            }
        },
        {
            key: 'url',
            label: 'Apply',
            sortable: false,
            render: job => {
                const url = job.absolute_url || job.url;
                return url
                    ? `<a href="${escape(url)}" target="_blank" rel="noopener noreferrer" class="btn btn-sm btn-outline-primary">Apply</a>`
                    : 'N/A';
            }
        },
        {
            key: 'actions',
            label: 'Actions',
            sortable: false,
            render: job => {
                const url = job.absolute_url || job.url;
                return `
                    <div class="btn-group" role="group">
                        <input type="checkbox" class="btn-check save-checkbox"
                               id="save-${escape(url)}"
                               data-job-url="${escape(url)}">
                        <label class="btn btn-sm btn-outline-primary" for="save-${escape(url)}">Saved</label>

                        <input type="checkbox" class="btn-check apply-checkbox"
                               id="apply-${escape(url)}"
                               data-job-url="${escape(url)}">
                        <label class="btn btn-sm btn-outline-success" for="apply-${escape(url)}">Applied</label>

                        <input type="checkbox" class="btn-check ignored-checkbox"
                               id="ignore-${escape(url)}"
                               data-job-url="${escape(url)}">
                        <label class="btn btn-sm btn-outline-secondary" for="ignore-${escape(url)}">Ignored</label>
                    </div>`;
            }
        }
    ];
}