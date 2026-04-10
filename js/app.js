// ============================================================
// JOB BOARD APP 
// ============================================================

import { showToast, showLoadingToast, setUIBusy, updateFABVisibility } from './ui_utils.js';
import { saveApplicationStatus } from './storage.js';
import { createColumns } from './columns.js';
import { loadJobsProgressive, updateStats } from './jobs_loader.js';
import { filterJobs, clearFilterInputs } from './filters.js';
import { render } from './renderer.js';
import { updateURL, loadFromURL } from './url_state.js';
import { setupEventListeners } from './events.js';
import { applySorting } from './sorting.js';

class JobBoardApp {
    constructor() {
        this.allJobs = [];
        this.filteredJobs = [];
        this.currentPage = 1;
        this.perPage = window.innerWidth <= 900 ? 25 : 100;
        this.sortState = { key: null, direction: 'asc' };

        this.filterState = {
            title: '', company: '', location: '', freshness: '', status: '',
            ats: '', skill_level: '', remoteOnly: false
        };

        this.debounceTimer = null;
        this.columns = createColumns();
    }

    // ── Initialization ───────────────────────────────────────────
    async init() {
        await this.loadJobs();
        setupEventListeners(this);
        const loadResult = this.loadFromURL();
        if (loadResult.autoRun || loadResult.preset === 'usa-remote-1d') {
            this.applyPresetFilters();
        }
        this.render();
    }

    // ── Data Loading ───────────────────────────────────────────
    async loadJobs() {
        const loadingEl = document.getElementById('loading');
        const resultsEl = document.getElementById('results');

        try {
            await loadJobsProgressive(this);

            this.sortState = { key: null, direction: 'asc' };

            loadingEl.style.display = 'none';
            resultsEl.style.display = 'block';

            console.log(`Loaded ${this.allJobs.length} jobs (more loading...)`);

        } catch (error) {
            console.error('Error loading jobs:', error);
            showToast('Error loading job data.', 'danger');
            loadingEl.textContent = 'Failed to load job data.';
        }
    }

    // ── Rendering ────────────────────────────────────────────
    render() {
        render(this);
    }

    debounceRender() {
        clearTimeout(this.debounceTimer);
        this.debounceTimer = setTimeout(() => this.render(), 300);
    }

    // ── Filtering ────────────────────────────────────────────
    applyFilters() {
        const { filteredJobs, filterState } = filterJobs(this.allJobs);
        this.filteredJobs = filteredJobs;
        this.filterState = filterState;
        this.currentPage = 1;
        updateURL(this.filterState, this.currentPage, this.sortState);
        this.render();

        if (this.filteredJobs.length > 0) {
            setTimeout(() => this.exportResults(), 0);
        }
    }

    clearFilters() {
        clearFilterInputs();
        this.filterState = {
            title: '', company: '', location: '', status: '',
            ats: '', skill_level: '', remoteOnly: false
        };
        this.filteredJobs = [...this.allJobs];
        this.currentPage = 1;
        updateURL(this.filterState, this.currentPage, this.sortState);
        this.render();
    }

    applyPresetFilters() {
        document.getElementById('filter-title').value = '';
        document.getElementById('filter-company').value = '';
        document.getElementById('filter-location').value = 'usa';
        document.getElementById('filter-freshness').value = '1';
        document.getElementById('filter-hide-recruiters').checked = true;
        document.getElementById('filter-remote-only').checked = true;
        document.getElementById('filter-status').value = '';
        document.getElementById('filter-ats').value = '';
        document.getElementById('filter-skill-level').value = '';
        document.getElementById('filter-exclude').value = '';
        this.applyFilters();
    }

    refilter() {
        if (this.hasActiveFilters()) {
            this.applyFilters();
        } else {
            this.filteredJobs = this.allJobs;
        }
    }

    hasActiveFilters() {
        const f = this.filterState;
        return f.title || f.company || f.location || f.freshness || f.status ||
            f.ats || f.skill_level || f.remoteOnly;
    }

    // ── Sorting ──────────────────────────────────────────────
    handleSort(key) {
        if (this.sortState.key === key) {
            this.sortState.direction = this.sortState.direction === 'asc' ? 'desc' : 'asc';
        } else {
            this.sortState.key = key;
            this.sortState.direction = 'asc';
        }
        this.currentPage = 1;
        updateURL(this.filterState, this.currentPage, this.sortState);
        this.sortAndRender();
    }

    sortAndRender() {
        const loader = showLoadingToast('Sorting...');
        setTimeout(() => {
            this.render();
            loader.hide();
        }, 100);
    }

    // ── Pagination ───────────────────────────────────────────
    previousPage() {
        if (this.currentPage > 1) {
            this.currentPage--;
            this.render();
            window.scrollTo(0, 0);
        }
    }

    nextPage() {
        const totalPages = Math.ceil(this.filteredJobs.length / this.perPage);
        if (this.currentPage < totalPages) {
            this.currentPage++;
            this.render();
            window.scrollTo(0, 0);
        }
    }

    // ── URL State ────────────────────────────────────────────
    loadFromURL() {
        const { hasFilters, page } = loadFromURL();
        this.currentPage = page;
        if (hasFilters) this.applyFilters();
    }

    // ── Batch Processing ─────────────────────────────────────
    handleBatch() {
        const selected = document.querySelectorAll('.save-checkbox:checked, .apply-checkbox:checked, .ignored-checkbox:checked');
        if (selected.length === 0) {
            showToast('Please select at least one job first.', 'warning');
            return;
        }

        setUIBusy(true);

        try {
            document.querySelectorAll('.save-checkbox:checked').forEach(box => {
                if (box.dataset.jobUrl) saveApplicationStatus(box.dataset.jobUrl, 'saved');
            });
            document.querySelectorAll('.apply-checkbox:checked').forEach(box => {
                if (box.dataset.jobUrl) saveApplicationStatus(box.dataset.jobUrl, 'applied');
            });
            document.querySelectorAll('.ignored-checkbox:checked').forEach(box => {
                if (box.dataset.jobUrl) saveApplicationStatus(box.dataset.jobUrl, 'ignored');
            });

            showToast(`Updated ${selected.length} job(s) successfully!`, 'success');
            updateFABVisibility();
            this.render();

        } catch (err) {
            showToast('Error updating job status.', 'danger');
            console.error(err);
        } finally {
            setUIBusy(false);
        }
    }

    // ── Export filtered jobs to CSV ───────────────────────────
    exportResults() {
        const jobs = (Array.isArray(this.filteredJobs) && this.filteredJobs.length > 0)
            ? this.filteredJobs
            : (Array.isArray(this.allJobs) ? this.allJobs : []);

        console.log('Export results invoked', {
            filteredJobs: this.filteredJobs?.length ?? 0,
            allJobs: this.allJobs?.length ?? 0,
            usedJobs: jobs.length
        });

        if (jobs.length === 0) {
            showToast('No jobs available to export.', 'warning');
            console.warn('exportResults: no rows available for CSV export');
            return;
        }

        const quote = value => `"${String(value || '').replace(/"/g, '""')}"`;

        const formatDate = job => {
            const rawDate = job.date_posted || job.posted_at || job.posted_on;

            if (rawDate) {
                const parsed = new Date(rawDate);
                if (!Number.isNaN(parsed.getTime())) return parsed.toLocaleDateString();
                return rawDate;
            }

            const scraped = job.scraped_at;
            if (scraped) {
                const parsed = new Date(scraped);
                if (!Number.isNaN(parsed.getTime())) return parsed.toLocaleDateString();
                return scraped;
            }

            return 'N/A';
        };

        const headers = ['Company', 'Title', 'Location', 'Experience Level', 'Date', 'ATS', 'URL'];
        const rows = jobs.map(job => {
            const company = job.company || job.company_slug || 'Unknown';
            const title = job.title || job.job_title || 'Not specified';
            const locationRaw = job.location && typeof job.location === 'object' ? job.location.name : job.location;
            const location = locationRaw || 'Not specified';
            const experience = (job.skill_level || job.experience_level || job.level || job.seniority || '').toString();
            const ats = (job.ats || 'unknown').toString();
            const url = job.absolute_url || job.url || '';
            return [company, title, location, experience, formatDate(job), ats, url];
        });

        const csvContent = [headers, ...rows].map(row => row.map(quote).join(',')).join('\r\n');

        const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
        const objectUrl = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.setAttribute('href', objectUrl);
        link.setAttribute('download', 'job-results.csv');
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(objectUrl);

        showToast(`Exported ${jobs.length} job(s).`, 'success');
    }
}

// ============================================================
// INITIALIZE APP
// ============================================================
document.addEventListener('DOMContentLoaded', () => {
    const app = new JobBoardApp();
    app.init();
});