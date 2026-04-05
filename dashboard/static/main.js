/**
 * JIP Pipeline Dashboard — main.js
 * Auto-refresh every 30 s via setInterval. No build step, no framework.
 * All fetches go to /api/* on the dashboard server (port 8099) which
 * proxies to the main engine (port 8010).
 */

"use strict";

// ---- State ----------------------------------------------------------------

const state = {
  anomalyFilter: "all",     // "all" | "critical" | "warning" | "info"
  detailItem: null,          // currently open anomaly detail
  historyFrom: "",           // date string YYYY-MM-DD
  historyTo: "",
  lastRefresh: null,
  refreshTimer: null,
  REFRESH_MS: 30_000,
};

// SLA human labels
const SLA_LABELS = {
  pre_market: "Pre-Market",
  equity_eod: "Equity EOD",
  mf_nav: "MF NAV",
  fii_dii: "FII/DII",
  rs: "RS Score",
  regime: "Regime",
};

// ---- Helpers ---------------------------------------------------------------

function fmtRows(n) {
  if (n == null) return "—";
  return formatIndian(n);
}

/** Indian lakh/crore formatting (mirrors dashboard/api.py) */
function formatIndian(n) {
  const neg = n < 0;
  const abs = Math.abs(Math.round(n));
  const s = String(abs);
  if (s.length <= 3) return (neg ? "-" : "") + s;
  const last3 = s.slice(-3);
  let rest = s.slice(0, -3);
  const groups = [];
  while (rest.length > 2) {
    groups.unshift(rest.slice(-2));
    rest = rest.slice(0, -2);
  }
  if (rest) groups.unshift(rest);
  return (neg ? "-" : "") + groups.join(",") + "," + last3;
}

function statusBadge(status) {
  if (!status) return "";
  const map = {
    success: ["badge-success", "Success"],
    failed: ["badge-failed", "Failed"],
    partial: ["badge-partial", "Partial"],
    running: ["badge-running", "Running"],
    pending: ["badge-pending", "Pending"],
    skipped: ["badge-skipped", "Skipped"],
    holiday_skip: ["badge-holiday", "Holiday"],
  };
  const [cls, label] = map[status] || ["badge-pending", status];
  return `<span class="badge ${cls}"><span class="badge-dot"></span>${label}</span>`;
}

function slaCell(sla) {
  if (!sla || sla.sla_key == null) return `<span class="sla-na">—</span>`;
  if (sla.met === null) {
    // in-progress
    if (sla.overdue_minutes != null && sla.overdue_minutes > 0) {
      return `<span class="sla-missed">OVERDUE<span class="sla-overdue-minutes">+${sla.overdue_minutes}m</span></span>`;
    }
    return `<span class="sla-na">${sla.deadline_str}</span>`;
  }
  if (sla.met) return `<span class="sla-met">Met (${sla.deadline_str})</span>`;
  return `<span class="sla-missed">Missed<span class="sla-overdue-minutes">+${sla.overdue_minutes}m</span></span>`;
}

function elapsedStr(startedAt, completedAt) {
  if (!startedAt) return "—";
  const start = new Date(startedAt);
  const end = completedAt ? new Date(completedAt) : new Date();
  const secs = Math.round((end - start) / 1000);
  if (secs < 60) return `${secs}s`;
  if (secs < 3600) return `${Math.floor(secs / 60)}m ${secs % 60}s`;
  return `${Math.floor(secs / 3600)}h ${Math.floor((secs % 3600) / 60)}m`;
}

function esc(s) {
  if (s == null) return "";
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function failRatio(rowsProcessed, rowsFailed) {
  if (!rowsProcessed || !rowsFailed) return 0;
  return Math.min(1, rowsFailed / rowsProcessed);
}

// ---- Fetch ----------------------------------------------------------------

async function fetchJSON(url) {
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`HTTP ${resp.status} — ${url}`);
  return resp.json();
}

// ---- Render: summary stats row -------------------------------------------

function renderStats(runs) {
  const counts = { success: 0, failed: 0, running: 0, partial: 0, pending: 0, skipped: 0 };
  let totalRows = 0, totalFailed = 0;
  for (const r of runs) {
    const s = r.status || "pending";
    if (s in counts) counts[s]++;
    totalRows += r.rows_processed || 0;
    totalFailed += r.rows_failed || 0;
  }

  document.getElementById("stat-success").textContent = counts.success;
  document.getElementById("stat-failed").textContent = counts.failed + counts.partial;
  document.getElementById("stat-running").textContent = counts.running + counts.pending;
  document.getElementById("stat-total-rows").textContent = formatIndian(totalRows);
  document.getElementById("stat-failed-rows").textContent = formatIndian(totalFailed);

  const failEl = document.getElementById("stat-card-failed");
  failEl.classList.toggle("stat-error", (counts.failed + counts.partial) > 0);
  failEl.classList.toggle("stat-ok", (counts.failed + counts.partial) === 0);
}

// ---- Render: pipeline table -----------------------------------------------

function renderPipelineTable(runs) {
  const tbody = document.getElementById("pipeline-tbody");
  if (!runs || runs.length === 0) {
    tbody.innerHTML = `<tr><td colspan="8" class="empty-state">No pipeline runs found for today.</td></tr>`;
    return;
  }

  tbody.innerHTML = runs.map((r) => {
    const ratio = failRatio(r.rows_processed, r.rows_failed);
    const barClass = ratio > 0.05 ? "failed" : ratio > 0 ? "warning" : "";
    const pct = Math.round(ratio * 100);
    return `
      <tr data-run-id="${esc(r.id)}">
        <td class="font-mono">${esc(r.pipeline_name || "—")}</td>
        <td>${r.display?.business_date_fmt || esc(r.business_date) || "—"}</td>
        <td>${statusBadge(r.status)}</td>
        <td class="num">${r.display?.rows_processed_fmt || fmtRows(r.rows_processed)}</td>
        <td class="num">
          ${r.display?.rows_failed_fmt || fmtRows(r.rows_failed)}
          ${ratio > 0 ? `<span class="progress-bar-wrap"><span class="progress-bar-fill ${barClass}" style="width:${pct}%"></span></span>` : ""}
        </td>
        <td>${r.display?.started_at_fmt || esc(r.started_at) || "—"}</td>
        <td>${elapsedStr(r.started_at, r.completed_at)}</td>
        <td>${slaCell(r.sla)}</td>
      </tr>`;
  }).join("");
}

// ---- Render: SLA tracker --------------------------------------------------

async function renderSLA(runs) {
  const now = new Date();
  const todayRuns = {};
  for (const r of runs) {
    const name = r.pipeline_name;
    if (!todayRuns[name] || r.run_number > todayRuns[name].run_number) {
      todayRuns[name] = r;
    }
  }

  let confData;
  try {
    confData = await fetchJSON("/api/sla-config");
  } catch (_) {
    confData = { deadlines: {}, pipeline_map: {} };
  }

  const deadlines = confData.deadlines || {};
  const pipelineMap = confData.pipeline_map || {};

  // Build SLA key → status
  const slaStatus = {};
  for (const [pname, run] of Object.entries(todayRuns)) {
    const slaKey = pipelineMap[pname];
    if (!slaKey) continue;
    if (!slaStatus[slaKey]) slaStatus[slaKey] = { met: null, overdue: null };
    const sla = run.sla || {};
    if (sla.met === true) slaStatus[slaKey] = { met: true, overdue: null };
    else if (sla.met === false && slaStatus[slaKey].met !== true) {
      slaStatus[slaKey] = { met: false, overdue: sla.overdue_minutes };
    }
  }

  const container = document.getElementById("sla-grid");
  container.innerHTML = Object.entries(deadlines).map(([key, time]) => {
    const label = SLA_LABELS[key] || key;
    const st = slaStatus[key];
    let statusHtml, barClass, barPct;

    if (!st || st.met === null) {
      // Compute time progress
      const [hh, mm] = time.split(":").map(Number);
      const deadlineMins = hh * 60 + mm;
      const nowMins = now.getHours() * 60 + now.getMinutes();
      barPct = Math.min(100, Math.round((nowMins / deadlineMins) * 100));
      barClass = barPct >= 100 ? "warning" : "";
      statusHtml = `<span class="sla-na">${time}</span>`;
    } else if (st.met) {
      barPct = 100;
      barClass = "";
      statusHtml = `<span class="sla-met">Met</span>`;
    } else {
      barPct = 100;
      barClass = "missed";
      statusHtml = `<span class="sla-missed">+${st.overdue}m late</span>`;
    }

    return `
      <div class="sla-row">
        <span class="sla-name">${esc(label)}</span>
        <span class="sla-deadline">${esc(time)}</span>
        <div class="sla-bar-wrap">
          <div class="sla-bar-fill ${barClass}" style="width:${barPct}%"></div>
        </div>
        <span class="sla-status-text">${statusHtml}</span>
      </div>`;
  }).join("");
}

// ---- Render: anomalies ----------------------------------------------------

function renderAnomalies(grouped, counts) {
  // Update tab badges
  for (const sev of ["critical", "warning", "info"]) {
    const badge = document.getElementById(`anomaly-count-${sev}`);
    if (badge) badge.textContent = counts[sev] || 0;
  }

  const filter = state.anomalyFilter;
  let items = [];
  if (filter === "all") {
    items = [...(grouped.critical || []), ...(grouped.warning || []), ...(grouped.info || [])];
  } else {
    items = grouped[filter] || [];
  }

  const container = document.getElementById("anomaly-list");
  if (items.length === 0) {
    container.innerHTML = `<div class="empty-state">No ${filter === "all" ? "" : filter + " "}anomalies found.</div>`;
    return;
  }

  container.innerHTML = items.map((a, idx) => {
    const sev = a.severity || "info";
    const title = esc(a.title || a.anomaly_type || a.message || "Anomaly");
    const detail = esc(a.detail || a.description || "");
    const timeStr = a.detected_at || a.created_at || "";
    return `
      <div class="anomaly-item sev-${sev}" data-anomaly-idx="${idx}" data-sev="${sev}">
        <div class="anomaly-header">
          <span class="badge badge-${sev}"><span class="badge-dot"></span>${sev}</span>
          <span class="anomaly-title">${title}</span>
          <span class="anomaly-time">${esc(timeStr.slice(11, 16)) || ""}</span>
        </div>
        ${detail ? `<div class="anomaly-detail">${detail}</div>` : ""}
      </div>`;
  }).join("");

  // Click → detail panel
  container.querySelectorAll(".anomaly-item").forEach((el) => {
    el.addEventListener("click", () => {
      const sev = el.dataset.sev;
      const idx = parseInt(el.dataset.anomalyIdx, 10);
      const list = filter === "all"
        ? [...(grouped.critical || []), ...(grouped.warning || []), ...(grouped.info || [])]
        : (grouped[sev] || []);
      openDetailPanel(list[idx]);
    });
  });
}

// ---- Render: system health ------------------------------------------------

function renderHealth(health) {
  const set = (id, val, cls) => {
    const el = document.getElementById(id);
    if (!el) return;
    el.textContent = val;
    el.className = `health-value ${cls}`;
  };

  if (health.error) {
    set("health-redis", "ERR", "error");
    set("health-db-conns", "ERR", "error");
    set("health-disk", "ERR", "error");
    set("health-engine", "ERR", "error");
    return;
  }

  const redis = health.redis_ping;
  set("health-redis", redis ? "OK" : "FAIL", redis ? "ok" : "error");
  document.getElementById("health-redis-sub").textContent = redis ? "Connected" : "Unreachable";

  const dbConns = health.db_connections;
  if (dbConns != null) {
    const cls = dbConns > 80 ? "warn" : "ok";
    set("health-db-conns", dbConns, cls);
    document.getElementById("health-db-sub").textContent = `of ${health.db_max_connections || "?"} max`;
  } else {
    set("health-db-conns", "—", "neutral");
  }

  const disk = health.disk_used_pct;
  if (disk != null) {
    const pct = Math.round(disk);
    const cls = pct > 85 ? "error" : pct > 70 ? "warn" : "ok";
    set("health-disk", pct + "%", cls);
    document.getElementById("health-disk-sub").textContent = `${health.disk_free_gb || "?"} GB free`;
  } else {
    set("health-disk", "—", "neutral");
  }

  const eng = health.engine_version || health.version;
  set("health-engine", eng ? `v${eng}` : "OK", "ok");
  document.getElementById("health-engine-sub").textContent = health.engine_uptime_human || "";
}

// ---- Detail panel ---------------------------------------------------------

function openDetailPanel(item) {
  state.detailItem = item;
  const overlay = document.getElementById("detail-overlay");
  const title = document.getElementById("detail-panel-title");
  const body = document.getElementById("detail-panel-body");

  title.textContent = item.title || item.anomaly_type || item.message || "Anomaly Detail";
  overlay.classList.add("open");

  const rows = Object.entries(item)
    .filter(([k]) => !["title", "anomaly_type"].includes(k))
    .map(([k, v]) => {
      const display = typeof v === "object" && v !== null
        ? `<pre>${esc(JSON.stringify(v, null, 2))}</pre>`
        : esc(String(v ?? ""));
      return `<div class="detail-row"><span class="detail-key">${esc(k)}</span><span class="detail-val">${display}</span></div>`;
    });

  body.innerHTML = rows.join("") || `<div class="empty-state">No additional detail.</div>`;
}

function closeDetailPanel() {
  document.getElementById("detail-overlay").classList.remove("open");
  state.detailItem = null;
}

// ---- History table --------------------------------------------------------

function renderHistoryTable(runs) {
  const tbody = document.getElementById("history-tbody");
  if (!runs || runs.length === 0) {
    tbody.innerHTML = `<tr><td colspan="7" class="empty-state">No history found for this range.</td></tr>`;
    return;
  }

  tbody.innerHTML = runs.map((r) => `
    <tr>
      <td>${r.display?.business_date_fmt || esc(r.business_date) || "—"}</td>
      <td class="font-mono">${esc(r.pipeline_name || "—")}</td>
      <td>${statusBadge(r.status)}</td>
      <td class="num">${r.display?.rows_processed_fmt || fmtRows(r.rows_processed)}</td>
      <td class="num">${r.display?.rows_failed_fmt || fmtRows(r.rows_failed)}</td>
      <td>${elapsedStr(r.started_at, r.completed_at)}</td>
      <td>${slaCell(r.sla)}</td>
    </tr>`).join("");
}

// ---- Error banner ---------------------------------------------------------

function showError(msg) {
  const el = document.getElementById("error-banner");
  if (!el) return;
  el.textContent = msg;
  el.classList.add("visible");
}

function clearError() {
  const el = document.getElementById("error-banner");
  if (!el) return;
  el.classList.remove("visible");
}

// ---- Main refresh ---------------------------------------------------------

async function refresh() {
  // Pulse dot
  const dot = document.getElementById("refresh-dot");
  dot.classList.remove("pulsing");
  void dot.offsetWidth; // reflow to re-trigger animation
  dot.classList.add("pulsing");

  clearError();

  try {
    // Today's runs
    const [runsData, anomalyData, healthData] = await Promise.all([
      fetchJSON("/api/pipeline-runs"),
      fetchJSON("/api/anomalies"),
      fetchJSON("/api/system-health"),
    ]);

    const runs = runsData.runs || [];
    renderStats(runs);
    renderPipelineTable(runs);
    await renderSLA(runs);

    const grouped = anomalyData.grouped || { critical: [], warning: [], info: [] };
    const counts = anomalyData.counts || {};
    renderAnomalies(grouped, counts);

    renderHealth(healthData);

    if (runsData.error || anomalyData.error) {
      showError("Warning: main engine returned an error — some data may be stale.");
    }
  } catch (err) {
    console.error("refresh error", err);
    showError("Could not connect to the main engine. Retrying in 30s.");
  }

  state.lastRefresh = new Date();
  updateTimestamp();
}

function updateTimestamp() {
  const el = document.getElementById("last-refresh");
  if (!el || !state.lastRefresh) return;
  const d = state.lastRefresh;
  const pad = (n) => String(n).padStart(2, "0");
  el.textContent = `Last updated ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

// ---- History load ---------------------------------------------------------

async function loadHistory() {
  const from = document.getElementById("hist-from").value;
  const to = document.getElementById("hist-to").value;
  if (!from || !to) return;

  state.historyFrom = from;
  state.historyTo = to;

  try {
    const data = await fetchJSON(`/api/pipeline-runs?date_from=${from}&date_to=${to}`);
    renderHistoryTable(data.runs || []);
  } catch (err) {
    console.error("history load error", err);
    showError("Failed to load history: " + err.message);
  }
}

// ---- Init -----------------------------------------------------------------

function init() {
  // Anomaly tab buttons
  document.querySelectorAll(".anomaly-tab").forEach((btn) => {
    btn.addEventListener("click", () => {
      document.querySelectorAll(".anomaly-tab").forEach((b) => b.classList.remove("active"));
      btn.classList.add("active");
      state.anomalyFilter = btn.dataset.filter || "all";
      // Re-render with cached data without re-fetching
      const cached = window.__anomalyCache;
      if (cached) renderAnomalies(cached.grouped, cached.counts);
    });
  });

  // Detail panel close
  document.getElementById("detail-close-btn")?.addEventListener("click", closeDetailPanel);
  document.getElementById("detail-overlay")?.addEventListener("click", (e) => {
    if (e.target === e.currentTarget) closeDetailPanel();
  });

  // History load button
  document.getElementById("load-history-btn")?.addEventListener("click", loadHistory);

  // Default history range: last 7 days
  const today = new Date();
  const todayStr = today.toISOString().slice(0, 10);
  const weekAgo = new Date(today);
  weekAgo.setDate(weekAgo.getDate() - 7);
  const weekAgoStr = weekAgo.toISOString().slice(0, 10);
  const fromEl = document.getElementById("hist-from");
  const toEl = document.getElementById("hist-to");
  if (fromEl && !fromEl.value) fromEl.value = weekAgoStr;
  if (toEl && !toEl.value) toEl.value = todayStr;

  // Patch renderAnomalies to cache
  const origRender = renderAnomalies;
  window.renderAnomalies = (grouped, counts) => {
    window.__anomalyCache = { grouped, counts };
    origRender(grouped, counts);
  };

  // Initial load then schedule
  refresh();
  state.refreshTimer = setInterval(refresh, state.REFRESH_MS);

  // Update timestamp every second
  setInterval(updateTimestamp, 1000);
}

document.addEventListener("DOMContentLoaded", init);
