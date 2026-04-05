/**
 * JIP Pipeline Dashboard — auto-refresh, silent data updates, detail panels
 */

(function () {
    "use strict";

    const CONFIG = window.DASHBOARD_CONFIG || { refreshInterval: 30 };
    let refreshTimer = null;
    let refreshCountdown = CONFIG.refreshInterval;

    // ------------------------------------------------------------------ helpers

    function formatDateIST(isoString) {
        if (!isoString) return "—";
        try {
            const dt = new Date(isoString);
            const opts = {
                day: "2-digit",
                month: "short",
                year: "numeric",
                hour: "2-digit",
                minute: "2-digit",
                timeZone: "Asia/Kolkata",
                hour12: false,
            };
            return dt.toLocaleString("en-IN", opts).replace(",", "");
        } catch {
            return isoString;
        }
    }

    function formatTimeIST(isoString) {
        if (!isoString) return "—";
        try {
            const dt = new Date(isoString);
            return dt.toLocaleTimeString("en-IN", {
                hour: "2-digit",
                minute: "2-digit",
                timeZone: "Asia/Kolkata",
                hour12: false,
            });
        } catch {
            return isoString;
        }
    }

    function formatNumber(n) {
        if (n === null || n === undefined) return "—";
        return Number(n).toLocaleString("en-IN");
    }

    function durationStr(startedAt, completedAt) {
        if (!startedAt) return "—";
        const end = completedAt ? new Date(completedAt) : new Date();
        const start = new Date(startedAt);
        const secs = Math.round((end - start) / 1000);
        if (secs < 60) return `${secs}s`;
        const mins = Math.floor(secs / 60);
        const rem = secs % 60;
        if (mins < 60) return `${mins}m ${rem}s`;
        const hrs = Math.floor(mins / 60);
        const minsRem = mins % 60;
        return `${hrs}h ${minsRem}m`;
    }

    function statusLabel(status) {
        const labels = {
            success: "Complete",
            running: "Running",
            failed: "Failed",
            partial: "Partial",
            pending: "Pending",
            skipped: "Skipped",
            holiday_skip: "Holiday",
        };
        return labels[status] || status || "Unknown";
    }

    function escape(str) {
        if (!str) return "";
        return String(str)
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;");
    }

    // ------------------------------------------------------------------ pipeline status

    async function loadPipelineStatus() {
        try {
            const res = await fetch("/api/pipelines");
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            const data = await res.json();

            // data may be {pipelines: [...]} or a list
            const pipelines = Array.isArray(data)
                ? data
                : data.pipelines || [];

            // Index by pipeline_name
            const byName = {};
            pipelines.forEach((p) => {
                byName[p.pipeline_name] = p;
            });

            // Update pipeline cards
            document.querySelectorAll(".pipeline-card").forEach((card) => {
                const pname = card.dataset.pipeline;
                const p = byName[pname] || {};
                const status = p.status || "unknown";

                card.className = `pipeline-card status-${status}`;

                const badge = card.querySelector(".pipeline-status-badge");
                if (badge) badge.textContent = statusLabel(status);

                const meta = card.querySelector(".pipeline-rows");
                if (meta) {
                    const rows = p.rows_processed;
                    const failed = p.rows_failed;
                    if (rows !== undefined && rows !== null) {
                        meta.textContent = `${formatNumber(rows)} rows` +
                            (failed ? ` / ${formatNumber(failed)} failed` : "");
                    } else {
                        meta.textContent = p.started_at ? formatTimeIST(p.started_at) : "—";
                    }
                }
            });

            // Update date
            const dateEl = document.getElementById("pipeline-date");
            if (dateEl && pipelines.length > 0) {
                const bd = pipelines[0].business_date;
                dateEl.textContent = bd || new Date().toLocaleDateString("en-IN", {
                    timeZone: "Asia/Kolkata",
                });
            }
        } catch (err) {
            console.error("loadPipelineStatus error:", err);
        }
    }

    // ------------------------------------------------------------------ health

    async function loadHealth() {
        const setValue = (id, value, cls) => {
            const el = document.getElementById(id);
            if (!el) return;
            el.textContent = value;
            el.className = `health-value ${cls}`;
        };

        try {
            const res = await fetch("/api/health");
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            const data = await res.json();

            const de = data.data_engine || data.status || "unknown";
            setValue("health-de", de, de === "healthy" ? "ok" : "error");

            const redis = data.redis;
            if (redis === "ok" || redis === "healthy") {
                setValue("health-redis", "OK", "ok");
            } else if (redis === "unavailable" || redis === "error") {
                setValue("health-redis", "Error", "error");
            } else {
                setValue("health-redis", redis || "—", "unknown");
            }

            const db = data.db;
            if (db === "ok" || db === "healthy") {
                setValue("health-db", "OK", "ok");
            } else if (db === "unavailable" || db === "error") {
                setValue("health-db", "Error", "error");
            } else {
                setValue("health-db", db || "—", "unknown");
            }

            const disk = data.disk_percent;
            if (disk !== undefined && disk !== null) {
                const cls = disk > 85 ? "error" : disk > 70 ? "warn" : "ok";
                setValue("health-disk", `${disk}%`, cls);
            } else {
                setValue("health-disk", "—", "unknown");
            }

            const conns = data.db_connections;
            setValue(
                "health-db-conns",
                conns !== undefined && conns !== null ? formatNumber(conns) : "—",
                "ok"
            );

            const latency = data.redis_latency_ms;
            setValue(
                "health-redis-latency",
                latency !== undefined && latency !== null ? `${latency}ms` : "—",
                latency !== undefined && latency < 5 ? "ok" : latency !== undefined ? "warn" : "unknown"
            );
        } catch (err) {
            console.error("loadHealth error:", err);
            ["health-de", "health-redis", "health-db", "health-disk", "health-db-conns", "health-redis-latency"]
                .forEach((id) => {
                    const el = document.getElementById(id);
                    if (el) { el.textContent = "Error"; el.className = "health-value error"; }
                });
        }
    }

    // ------------------------------------------------------------------ SLA

    async function loadSLA() {
        try {
            const res = await fetch("/api/sla");
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            const data = await res.json();

            const dateEl = document.getElementById("sla-date");
            if (dateEl) dateEl.textContent = data.date || "—";

            const tbody = document.getElementById("sla-tbody");
            if (!tbody) return;

            const rows = (data.sla || []).map((item) => {
                const statusCls = {
                    on_time: "sla-on-time",
                    breached: "sla-breached",
                    pending: "sla-pending",
                    running: "sla-running",
                }[item.sla_status] || "sla-pending";

                const statusText = {
                    on_time: "On Time",
                    breached: "Breached",
                    pending: "Pending",
                    running: "Running",
                }[item.sla_status] || item.sla_status;

                return `<tr>
                    <td class="col-text">${escape(item.label)}</td>
                    <td class="col-num">${escape(item.deadline)}</td>
                    <td class="col-num">${escape(item.actual_time)}</td>
                    <td class="col-text"><span class="${statusCls}">${statusText}</span></td>
                </tr>`;
            });

            tbody.innerHTML = rows.length
                ? rows.join("")
                : '<tr><td colspan="4" class="loading-cell">No SLA data available.</td></tr>';
        } catch (err) {
            console.error("loadSLA error:", err);
        }
    }

    // ------------------------------------------------------------------ anomalies

    const SEVERITY_ORDER = { critical: 0, high: 1, medium: 2, low: 3 };
    let anomalyData = [];

    async function loadAnomalies() {
        try {
            const res = await fetch("/api/anomalies?resolved=false");
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            const data = await res.json();

            const items = Array.isArray(data) ? data : data.anomalies || [];
            anomalyData = items.slice().sort(
                (a, b) =>
                    (SEVERITY_ORDER[a.severity] ?? 99) -
                    (SEVERITY_ORDER[b.severity] ?? 99)
            );

            const countEl = document.getElementById("anomaly-count");
            if (countEl) countEl.textContent = anomalyData.length;

            renderAnomalies();
        } catch (err) {
            console.error("loadAnomalies error:", err);
        }
    }

    function renderAnomalies() {
        const tbody = document.getElementById("anomaly-tbody");
        if (!tbody) return;

        if (!anomalyData.length) {
            tbody.innerHTML = '<tr><td colspan="8" class="loading-cell">No unresolved anomalies today.</td></tr>';
            return;
        }

        const rows = anomalyData.map((a, idx) => {
            const sev = a.severity || "low";
            const entity = a.ticker || a.mstar_id || (a.instrument_id ? a.instrument_id.slice(0, 8) + "…" : "—");
            const resolvedText = a.is_resolved ? "Resolved" : "Open";
            return `<tr onclick="window.dashboard.showAnomalyDetail(${idx})" data-idx="${idx}">
                <td class="col-text"><span class="severity-badge ${sev}">${sev}</span></td>
                <td class="col-text">${escape(a.pipeline_name)}</td>
                <td class="col-text">${escape(entity)}</td>
                <td class="col-text">${escape(a.anomaly_type || "—")}</td>
                <td class="col-text">${escape(a.expected_range || "—")}</td>
                <td class="col-text">${escape(a.actual_value || "—")}</td>
                <td class="col-num">${formatDateIST(a.detected_at)}</td>
                <td class="col-text">${resolvedText}</td>
            </tr>`;
        });

        tbody.innerHTML = rows.join("");
    }

    function showAnomalyDetail(idx) {
        const a = anomalyData[idx];
        if (!a) return;
        const panel = document.getElementById("anomaly-detail-panel");
        const content = document.getElementById("anomaly-detail-content");
        if (!panel || !content) return;

        const entity = a.ticker || a.mstar_id || a.instrument_id || "—";
        const lines = [
            `ID:             ${a.id || "—"}`,
            `Pipeline:       ${a.pipeline_name || "—"}`,
            `Business Date:  ${a.business_date || "—"}`,
            `Entity Type:    ${a.entity_type || "—"}`,
            `Entity:         ${entity}`,
            `Anomaly Type:   ${a.anomaly_type || "—"}`,
            `Severity:       ${a.severity || "—"}`,
            `Expected Range: ${a.expected_range || "—"}`,
            `Actual Value:   ${a.actual_value || "—"}`,
            `Detected At:    ${formatDateIST(a.detected_at)}`,
            `Status:         ${a.is_resolved ? "Resolved" : "Open"}`,
        ];
        if (a.resolved_by) lines.push(`Resolved By:    ${a.resolved_by}`);
        if (a.resolution_note) lines.push(`Resolution:     ${a.resolution_note}`);

        content.textContent = lines.join("\n");
        panel.classList.remove("hidden");
        panel.scrollIntoView({ behavior: "smooth", block: "nearest" });
    }

    function closeAnomalyDetail() {
        const panel = document.getElementById("anomaly-detail-panel");
        if (panel) panel.classList.add("hidden");
    }

    // ------------------------------------------------------------------ history

    async function loadHistory() {
        const pipeline = document.getElementById("history-pipeline-filter")?.value || "";
        const dateFrom = document.getElementById("history-date-from")?.value || "";
        const dateTo = document.getElementById("history-date-to")?.value || "";

        const params = new URLSearchParams({ limit: "100" });
        if (pipeline) params.append("pipeline_name", pipeline);
        if (dateFrom) params.append("date_from", dateFrom);
        if (dateTo) params.append("date_to", dateTo);

        const tbody = document.getElementById("history-tbody");
        if (tbody) tbody.innerHTML = '<tr><td colspan="8" class="loading-cell">Loading...</td></tr>';

        try {
            const res = await fetch(`/api/history?${params}`);
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            const data = await res.json();

            const items = Array.isArray(data) ? data : data.runs || data.history || [];

            if (!tbody) return;

            if (!items.length) {
                tbody.innerHTML = '<tr><td colspan="8" class="loading-cell">No pipeline runs found for the selected filters.</td></tr>';
                return;
            }

            const rows = items.map((run, idx) => {
                const statusCls = `text-${run.status || "muted"}`;
                const hasTrackStatus = !!run.track_status;
                return `<tr onclick="${hasTrackStatus ? `window.dashboard.showHistoryDetail(${idx})` : ""}" data-run-idx="${idx}" style="${hasTrackStatus ? "cursor:pointer" : "cursor:default"}">
                    <td class="col-text">${escape(run.pipeline_name)}</td>
                    <td class="col-num">${escape(run.business_date || "—")}</td>
                    <td class="col-text"><span class="${statusCls}">${statusLabel(run.status)}</span></td>
                    <td class="col-num">${formatNumber(run.rows_processed)}</td>
                    <td class="col-num">${formatNumber(run.rows_failed)}</td>
                    <td class="col-num">${formatDateIST(run.started_at)}</td>
                    <td class="col-num">${formatDateIST(run.completed_at)}</td>
                    <td class="col-num">${durationStr(run.started_at, run.completed_at)}</td>
                </tr>`;
            });

            tbody.innerHTML = rows.join("");
            window._historyData = items;
        } catch (err) {
            console.error("loadHistory error:", err);
            if (tbody) tbody.innerHTML = '<tr><td colspan="8" class="loading-cell">Error loading history.</td></tr>';
        }
    }

    function showHistoryDetail(idx) {
        const run = (window._historyData || [])[idx];
        if (!run || !run.track_status) return;

        const panel = document.getElementById("history-detail-panel");
        const content = document.getElementById("history-detail-content");
        if (!panel || !content) return;

        content.textContent = JSON.stringify(run.track_status, null, 2);
        panel.classList.remove("hidden");
        panel.scrollIntoView({ behavior: "smooth", block: "nearest" });
    }

    function closeHistoryDetail() {
        const panel = document.getElementById("history-detail-panel");
        if (panel) panel.classList.add("hidden");
    }

    // ------------------------------------------------------------------ last updated

    function updateTimestamp() {
        const el = document.getElementById("last-updated");
        if (!el) return;
        const now = new Date();
        el.textContent = now.toLocaleTimeString("en-IN", {
            hour: "2-digit",
            minute: "2-digit",
            second: "2-digit",
            timeZone: "Asia/Kolkata",
            hour12: false,
        });
    }

    // ------------------------------------------------------------------ countdown

    function startCountdown() {
        refreshCountdown = CONFIG.refreshInterval;
        const el = document.getElementById("refresh-countdown");
        const tick = setInterval(() => {
            refreshCountdown -= 1;
            if (el) el.textContent = refreshCountdown;
            if (refreshCountdown <= 0) {
                clearInterval(tick);
            }
        }, 1000);
    }

    // ------------------------------------------------------------------ refresh all

    async function refreshAll() {
        await Promise.allSettled([
            loadPipelineStatus(),
            loadHealth(),
            loadSLA(),
            loadAnomalies(),
        ]);
        updateTimestamp();
        startCountdown();
    }

    // ------------------------------------------------------------------ init

    function init() {
        refreshAll();
        refreshTimer = setInterval(refreshAll, CONFIG.refreshInterval * 1000);
    }

    // Public API
    window.dashboard = {
        showAnomalyDetail,
        closeAnomalyDetail,
        showHistoryDetail,
        closeHistoryDetail,
        loadHistory,
        refresh: refreshAll,
    };

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", init);
    } else {
        init();
    }
})();
