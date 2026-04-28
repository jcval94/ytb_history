import { renderHorizontalBars } from "./charts.js";
import { formatDate, formatNumber, formatPercent, asNumber, escapeHtml } from "./formatters.js";
import { renderTable, sortRows } from "./tables.js";

const DATA_FILES = {
  manifest: "./data/site_manifest.json",
  dashboardIndex: "./data/dashboard_index.json",
  latestVideoMetrics: "./data/latest_video_metrics.json",
  latestChannelMetrics: "./data/latest_channel_metrics.json",
  latestVideoScores: "./data/latest_video_scores.json",
  latestVideoAdvancedMetrics: "./data/latest_video_advanced_metrics.json",
  latestTitleMetrics: "./data/latest_title_metrics.json",
  latestMetricEligibility: "./data/latest_metric_eligibility.json",
  periodDailyVideo: "./data/period_daily_video_metrics.json",
  periodWeeklyVideo: "./data/period_weekly_video_metrics.json",
  periodMonthlyVideo: "./data/period_monthly_video_metrics.json",
  periodDailyChannel: "./data/period_daily_channel_metrics.json",
  periodWeeklyChannel: "./data/period_weekly_channel_metrics.json",
  periodMonthlyChannel: "./data/period_monthly_channel_metrics.json",
  latestAlerts: "./data/latest_alerts.json",
  alertSummary: "./data/alert_summary.json",
  latestVideoSignals: "./data/latest_video_signals.json",
  latestChannelSignals: "./data/latest_channel_signals.json",
  latestSignalCandidates: "./data/latest_signal_candidates.json",
  latestModelManifest: "./data/latest_model_manifest.json",
  latestModelLeaderboard: "./data/latest_model_leaderboard.json",
  latestFeatureImportance: "./data/latest_feature_importance.json",
  latestFeatureDirection: "./data/latest_feature_direction.json",
  latestModelSuiteReportHtml: "./data/latest_model_suite_report.html",
  latestVideoNlpFeatures: "./data/latest_video_nlp_features.json",
  latestTitleNlpFeatures: "./data/latest_title_nlp_features.json",
  latestSemanticClusters: "./data/latest_semantic_clusters.json",
  nlpFeatureSummary: "./data/nlp_feature_summary.json",
  latestVideoTopics: "./data/latest_video_topics.json",
  latestTopicMetrics: "./data/latest_topic_metrics.json",
  latestTitlePatternMetrics: "./data/latest_title_pattern_metrics.json",
  latestKeywordMetrics: "./data/latest_keyword_metrics.json",
  latestTopicOpportunities: "./data/latest_topic_opportunities.json",
  topicIntelligenceSummary: "./data/topic_intelligence_summary.json",
  latestContentDriverLeaderboard: "./data/latest_content_driver_leaderboard.json",
  latestContentDriverFeatureImportance: "./data/latest_content_driver_feature_importance.json",
  latestContentDriverFeatureDirection: "./data/latest_content_driver_feature_direction.json",
  latestContentDriverGroupImportance: "./data/latest_content_driver_group_importance.json",
  latestContentDriverReportHtml: "./data/latest_content_driver_report.html",
  latestWeeklyBriefJson: "./data/latest_weekly_brief.json",
  latestWeeklyBriefHtml: "./data/latest_weekly_brief.html"
};

const state = {
  data: {},
  filterText: "",
  channel: "",
  duration: "",
  horizon: "all"
};

init().catch((error) => {
  pushWarning(`Unexpected error: ${error instanceof Error ? error.message : String(error)}`);
  setDataStatus("error");
});

async function init() {
  bindTabs();
  bindFilters();

  const manifest = await fetchJson(DATA_FILES.manifest, { required: true });
  state.data.manifest = manifest ?? {};
  if (!manifest) return;

  setGeneratedAt(manifest.generated_at || "");
  setDataStatus(Array.isArray(manifest.warnings) && manifest.warnings.length ? "warning" : "ready");
  (manifest.warnings || []).forEach(pushWarning);

  for (const [key, path] of Object.entries(DATA_FILES)) {
    if (key === "manifest") continue;
    if (key === "latestWeeklyBriefHtml") {
      state.data[key] = await fetchText(path);
      continue;
    }
    state.data[key] = await fetchJson(path);
  }

  populateFilters();
  renderAll();
}

async function fetchJson(path, { required = false } = {}) {
  try {
    const response = await fetch(path, { cache: "no-store" });
    if (!response.ok) throw new Error(`${response.status} ${response.statusText}`);
    return await response.json();
  } catch (error) {
    const label = `Could not load ${path}: ${error instanceof Error ? error.message : String(error)}`;
    if (required) {
      pushWarning(`Required file missing. ${label}`);
      const panel = document.querySelector("#tab-overview");
      if (panel) panel.innerHTML = `<p>${escapeHtml(label)}</p>`;
    } else {
      pushWarning(label);
    }
    return null;
  }
}

async function fetchText(path, { required = false } = {}) {
  try {
    const response = await fetch(path, { cache: "no-store" });
    if (!response.ok) throw new Error(`${response.status} ${response.statusText}`);
    return await response.text();
  } catch (error) {
    const label = `Could not load ${path}: ${error instanceof Error ? error.message : String(error)}`;
    if (required) {
      pushWarning(`Required file missing. ${label}`);
    } else {
      pushWarning(label);
    }
    return "";
  }
}

function bindTabs() {
  const tabs = document.querySelector("#tabs");
  if (!tabs) return;
  tabs.querySelectorAll("button").forEach((button) => {
    button.addEventListener("click", () => {
      const tab = button.getAttribute("data-tab");
      if (!tab) return;
      activateTab(tab);
    });
  });
}

function activateTab(tab) {
  const tabs = document.querySelector("#tabs");
  if (!tabs) return;
  tabs.querySelectorAll("button").forEach((item) => item.classList.remove("active"));
  document.querySelectorAll(".tab-panel").forEach((panel) => panel.classList.remove("active"));
  tabs.querySelector(`button[data-tab="${tab}"]`)?.classList.add("active");
  document.querySelector(`#tab-${tab}`)?.classList.add("active");
}

function bindFilters() {
  const filterInput = document.querySelector("#global-filter");
  const channelFilter = document.querySelector("#channel-filter");
  const durationFilter = document.querySelector("#duration-filter");
  const horizonFilter = document.querySelector("#horizon-filter");
  const resetButton = document.querySelector("#reset-filters");

  filterInput?.addEventListener("input", () => {
    state.filterText = String(filterInput.value || "").toLowerCase();
    renderAll();
  });
  channelFilter?.addEventListener("change", () => {
    state.channel = String(channelFilter.value || "");
    renderAll();
  });
  durationFilter?.addEventListener("change", () => {
    state.duration = String(durationFilter.value || "");
    renderAll();
  });
  horizonFilter?.addEventListener("change", () => {
    state.horizon = String(horizonFilter.value || "all");
    renderAll();
  });
  resetButton?.addEventListener("click", () => {
    state.filterText = "";
    state.channel = "";
    state.duration = "";
    state.horizon = "all";
    if (filterInput) filterInput.value = "";
    if (channelFilter) channelFilter.value = "";
    if (durationFilter) durationFilter.value = "";
    if (horizonFilter) horizonFilter.value = "all";
    renderAll();
  });
}

function populateFilters() {
  const channelFilter = document.querySelector("#channel-filter");
  if (!channelFilter) return;

  const rows = tableRows("latestVideoMetrics");
  const channels = [...new Set(rows.map((row) => row.channel_name).filter(Boolean))].sort();
  const options = channels
    .map((channel) => `<option value="${escapeHtml(channel)}">${escapeHtml(channel)}</option>`)
    .join("");
  channelFilter.insertAdjacentHTML("beforeend", options);
}

function renderAll() {
  const videos = applyFilters(tableRows("latestVideoMetrics"));
  const channels = applyFilters(tableRows("latestChannelMetrics"), { skipDuration: true });
  const scores = applyFilters(tableRows("latestVideoScores"));
  const advanced = applyFilters(tableRows("latestVideoAdvancedMetrics"));
  const titles = applyFilters(tableRows("latestTitleMetrics"));
  const quality = applyFilters(tableRows("latestMetricEligibility"), { skipDuration: true });

  renderHeader(videos, channels);
  renderKpis(videos, channels, scores, advanced);
  renderOverview(videos, channels, scores);
  renderVideos(videos);
  renderChannels(channels);
  renderScores(scores);
  renderAdvanced(advanced);
  renderTitles(titles);
  renderPeriods();
  renderAlerts();
  renderDataQuality(quality, advanced);
  renderModels();
  renderTopics();
  renderNlp();
  renderContentDrivers();
  renderBrief();
}

function tableRows(key) {
  const table = state.data[key];
  return Array.isArray(table?.rows) ? table.rows : [];
}

function applyFilters(rows, { skipDuration = false } = {}) {
  return rows.filter((row) => {
    if (state.channel && row.channel_name !== state.channel) return false;
    if (!skipDuration && state.duration && row.duration_bucket !== state.duration) return false;
    if (state.filterText) {
      const haystack = `${row.title || ""} ${row.channel_name || ""} ${row.video_id || ""}`.toLowerCase();
      if (!haystack.includes(state.filterText)) return false;
    }
    if (state.horizon !== "all") {
      const flag = `${state.horizon}_eligible`;
      if (Object.prototype.hasOwnProperty.call(row, flag)) {
        if (row[flag] !== true) return false;
      } else if (Object.prototype.hasOwnProperty.call(row, "success_horizon_label")) {
        if (row.success_horizon_label !== state.horizon) return false;
      }
    }
    return true;
  });
}

function renderHeader(videos, channels) {
  const videosCount = document.querySelector("#videos-count");
  const channelsCount = document.querySelector("#channels-count");
  if (videosCount) videosCount.textContent = `Videos: ${videos.length}`;
  if (channelsCount) channelsCount.textContent = `Channels: ${channels.length}`;
}

function renderKpis(videos, channels, scores, advanced) {
  const totalViewsDelta = videos.reduce((acc, row) => acc + asNumber(row.views_delta), 0);
  const totalLikesDelta = videos.reduce((acc, row) => acc + asNumber(row.likes_delta), 0);
  const totalCommentsDelta = videos.reduce((acc, row) => acc + asNumber(row.comments_delta), 0);
  const avgEngagementRate = videos.length
    ? videos.reduce((acc, row) => acc + asNumber(row.engagement_rate), 0) / videos.length
    : 0;
  const topAlpha = sortRows(scores, "alpha_score", "desc")[0] || {};
  const topChannel = sortRows(channels, "total_views_delta", "desc")[0] || {};
  const lowConfidence = advanced.filter((row) => asNumber(row.metric_confidence_score) < 50).length;

  const cards = [
    ["videos_total", formatNumber(videos.length)],
    ["channels_total", formatNumber(channels.length)],
    ["total_views_delta", formatNumber(totalViewsDelta)],
    ["total_likes_delta", formatNumber(totalLikesDelta)],
    ["total_comments_delta", formatNumber(totalCommentsDelta)],
    ["avg_engagement_rate", formatPercent(avgEngagementRate)],
    ["top alpha video", topAlpha.title || topAlpha.video_id || "--"],
    ["top channel by growth", topChannel.channel_name || "--"],
    ["low confidence rows", formatNumber(lowConfidence)]
  ];

  const html = cards
    .map(([label, value]) => `<article class="kpi-card"><h3>${escapeHtml(label)}</h3><p>${escapeHtml(value)}</p></article>`)
    .join("");
  const container = document.querySelector("#kpis");
  if (container) container.innerHTML = html;
}

function renderOverview(videos, channels, scores) {
  const panel = document.querySelector("#tab-overview");
  if (!panel) return;

  panel.innerHTML = `
    <div id="ov-videos" class="grid-two"></div>
    <div id="ov-channels" class="grid-two"></div>
    <div id="ov-alerts"></div>
    <div id="ov-brief"></div>
  `;

  const topViews = sortRows(videos, "views_delta", "desc").slice(0, 10);
  const topAlpha = sortRows(scores, "alpha_score", "desc").slice(0, 10);
  const topChannels = sortRows(channels, "total_views_delta", "desc").slice(0, 10);

  const viewsContainer = document.createElement("div");
  const alphaContainer = document.createElement("div");
  const channelsContainer = document.createElement("div");
  const bucketsContainer = document.createElement("div");

  renderHorizontalBars(viewsContainer, topViews, {
    labelKey: "title",
    valueKey: "views_delta",
    title: "Top 10 videos por views_delta"
  });
  renderHorizontalBars(alphaContainer, topAlpha, {
    labelKey: "title",
    valueKey: "alpha_score",
    title: "Top 10 videos por alpha_score"
  });
  renderHorizontalBars(channelsContainer, topChannels, {
    labelKey: "channel_name",
    valueKey: "total_views_delta",
    title: "Top canales por total_views_delta"
  });

  const distribution = Object.entries(
    videos.reduce((acc, row) => {
      const key = row.duration_bucket || "unknown";
      acc[key] = (acc[key] || 0) + 1;
      return acc;
    }, {})
  ).map(([bucket, count]) => ({ bucket, count }));

  renderHorizontalBars(bucketsContainer, distribution, {
    labelKey: "bucket",
    valueKey: "count",
    title: "Distribución por duration_bucket"
  });

  panel.querySelector("#ov-videos")?.append(viewsContainer, alphaContainer);
  panel.querySelector("#ov-channels")?.append(channelsContainer, bucketsContainer);

  const topAlertsWrap = document.createElement("div");
  const topAlerts = topAlertsBySeverity(5);
  if (!topAlerts.length) {
    topAlertsWrap.innerHTML = "<h3 class=\"section-title\">Signals to watch</h3><p>No alerts generated yet</p>";
  } else {
    const rows = topAlerts
      .map((alert) => `<li>${escapeHtml(alert.signal_type)} · ${severityBadge(alert.severity)} · ${escapeHtml(alert.title || alert.channel_name || "--")} · score ${escapeHtml(String(alert.adjusted_signal_score ?? "--"))}</li>`)
      .join("");
    topAlertsWrap.innerHTML = `<h3 class="section-title">Signals to watch</h3><ul>${rows}</ul>`;
  }
  panel.querySelector("#ov-alerts")?.append(topAlertsWrap);

  const briefWrap = document.createElement("div");
  const briefJson = state.data.latestWeeklyBriefJson;
  const summary = Array.isArray(briefJson?.executive_summary) ? briefJson.executive_summary.slice(0, 3) : [];
  if (!summary.length) {
    briefWrap.innerHTML = "<h3 class=\"section-title\">Weekly Brief Highlights</h3><p>No weekly brief generated yet</p>";
  } else {
    const items = summary.map((item) => `<li>${escapeHtml(String(item))}</li>`).join("");
    briefWrap.innerHTML = `
      <h3 class="section-title">Weekly Brief Highlights</h3>
      <ul>${items}</ul>
      <p><button id="go-to-brief" type="button">Go to Brief tab</button></p>
    `;
    briefWrap.querySelector("#go-to-brief")?.addEventListener("click", () => activateTab("brief"));
  }
  panel.querySelector("#ov-brief")?.append(briefWrap);
}

function renderVideos(videos) {
  renderTable(document.querySelector("#tab-videos"), [
    "title", "channel_name", "views_delta", "engagement_rate", "video_age_days", "duration_bucket"
  ], videos, { initialSortKey: "views_delta", title: "Videos" });
}

function renderChannels(channels) {
  renderTable(document.querySelector("#tab-channels"), [
    "channel_name", "total_views_delta", "avg_engagement_rate", "channel_momentum_score", "videos_tracked"
  ], channels, { initialSortKey: "total_views_delta", title: "Channels" });
}

function renderScores(scores) {
  renderTable(document.querySelector("#tab-scores"), [
    "title", "channel_name", "alpha_score", "opportunity_score", "anomaly_score"
  ], scores, { initialSortKey: "alpha_score", title: "Scores" });
}

function renderAdvanced(advanced) {
  renderTable(document.querySelector("#tab-advanced"), [
    "title", "short_term_success_score", "mid_term_success_score", "long_term_success_score", "trend_burst_score",
    "evergreen_score", "packaging_problem_score", "metric_confidence_score"
  ], advanced, { initialSortKey: "short_term_success_score", title: "Advanced" });
}

function renderTitles(titles) {
  renderTable(document.querySelector("#tab-titles"), [
    "title", "has_number", "has_question", "has_ai_word", "has_finance_word", "views_delta"
  ], titles, { initialSortKey: "views_delta", title: "Titles" });
}

function renderPeriods() {
  const panel = document.querySelector("#tab-periods");
  if (!panel) return;
  const selectorId = "period-grain";
  panel.innerHTML = `
    <h3 class="section-title">Periods</h3>
    <label for="${selectorId}">grain</label>
    <select id="${selectorId}">
      <option value="daily">daily</option>
      <option value="weekly">weekly</option>
      <option value="monthly">monthly</option>
    </select>
    <div id="period-video-table"></div>
    <div id="period-channel-table"></div>
  `;

  const redraw = () => {
    const grain = document.querySelector(`#${selectorId}`)?.value || "daily";
    const videoRows = tableRows(`period${capitalize(grain)}Video`);
    const channelRows = tableRows(`period${capitalize(grain)}Channel`);
    renderTable(document.querySelector("#period-video-table"), [
      "period_start", "title", "period_views_delta", "period_avg_engagement_rate"
    ], videoRows, { initialSortKey: "period_views_delta", title: `Video period metrics (${grain})` });
    renderTable(document.querySelector("#period-channel-table"), [
      "period_start", "channel_name", "period_views_delta", "period_avg_engagement_rate"
    ], channelRows, { initialSortKey: "period_views_delta", title: `Channel period metrics (${grain})` });
  };

  document.querySelector(`#${selectorId}`)?.addEventListener("change", redraw);
  redraw();
}

function renderAlerts() {
  const panel = document.querySelector("#tab-alerts");
  if (!panel) return;
  const alertPayload = state.data.latestAlerts || {};
  const summary = state.data.alertSummary || {};
  const alerts = Array.isArray(alertPayload.alerts) ? alertPayload.alerts : [];
  const signalCandidates = tableRows("latestSignalCandidates");
  const counts = summary.severity_counts || {};

  panel.innerHTML = `
    <div class="kpi-grid" id="alerts-kpis"></div>
    <h3 class="section-title">Top alerts</h3>
    <div id="alerts-top"></div>
    <h3 class="section-title">Filters</h3>
    <div class="filters" id="alerts-filters">
      <select id="alerts-severity-filter"><option value="">All severities</option></select>
      <select id="alerts-signal-filter"><option value="">All signal types</option></select>
      <select id="alerts-entity-filter"><option value="">All entities</option></select>
    </div>
    <div id="alerts-table"></div>
    <h3 class="section-title">Signal candidates</h3>
    <div id="signal-candidates-table"></div>
  `;

  const cards = [
    ["total_alerts", summary.total_alerts ?? alertPayload.alert_count ?? alerts.length ?? 0],
    ["critical", counts.critical ?? 0],
    ["high", counts.high ?? 0],
    ["medium", counts.medium ?? 0],
    ["low", counts.low ?? 0]
  ];
  document.querySelector("#alerts-kpis").innerHTML = cards
    .map(([label, value]) => `<article class="kpi-card"><h3>${escapeHtml(label)}</h3><p>${escapeHtml(String(value))}</p></article>`)
    .join("");

  if (!alerts.length) {
    document.querySelector("#alerts-top").innerHTML = "<p>No alerts generated yet</p>";
    document.querySelector("#alerts-table").innerHTML = "<p>No alerts generated yet</p>";
  } else {
    const topRows = topAlertsBySeverity(10);
    renderTable(document.querySelector("#alerts-top"), [
      "signal_type", "severity", "title", "channel_name", "adjusted_signal_score", "confidence_level", "recommended_action"
    ], topRows, { initialSortKey: "adjusted_signal_score", title: "Top alerts" });
  }

  hydrateAlertsFilters(alerts);
  const redraw = () => {
    const severity = document.querySelector("#alerts-severity-filter")?.value || "";
    const signalType = document.querySelector("#alerts-signal-filter")?.value || "";
    const entityType = document.querySelector("#alerts-entity-filter")?.value || "";
    const filtered = alerts.filter((row) => {
      if (severity && row.severity !== severity) return false;
      if (signalType && row.signal_type !== signalType) return false;
      if (entityType && row.entity_type !== entityType) return false;
      return true;
    });
    renderTable(document.querySelector("#alerts-table"), [
      "signal_type", "severity", "entity_type", "title", "channel_name", "adjusted_signal_score", "confidence_level", "recommended_action"
    ], filtered, { initialSortKey: "adjusted_signal_score", title: "Alerts" });
  };
  document.querySelector("#alerts-severity-filter")?.addEventListener("change", redraw);
  document.querySelector("#alerts-signal-filter")?.addEventListener("change", redraw);
  document.querySelector("#alerts-entity-filter")?.addEventListener("change", redraw);
  redraw();

  renderTable(document.querySelector("#signal-candidates-table"), [
    "entity_type", "entity_id", "signal_type", "triggered", "raw_signal_score", "adjusted_signal_score", "confidence_level"
  ], signalCandidates, { initialSortKey: "adjusted_signal_score", title: "Signal candidates" });
}

function hydrateAlertsFilters(alerts) {
  const severityOptions = [...new Set(alerts.map((row) => row.severity).filter(Boolean))].sort();
  const signalOptions = [...new Set(alerts.map((row) => row.signal_type).filter(Boolean))].sort();
  const entityOptions = [...new Set(alerts.map((row) => row.entity_type).filter(Boolean))].sort();
  const writeOptions = (selector, values) => {
    const element = document.querySelector(selector);
    if (!element) return;
    element.innerHTML = `<option value="">${escapeHtml(element.options[0]?.text || "All")}</option>` +
      values.map((value) => `<option value="${escapeHtml(value)}">${escapeHtml(value)}</option>`).join("");
  };
  writeOptions("#alerts-severity-filter", severityOptions);
  writeOptions("#alerts-signal-filter", signalOptions);
  writeOptions("#alerts-entity-filter", entityOptions);
}

function topAlertsBySeverity(limit = 5) {
  const alertPayload = state.data.latestAlerts || {};
  const alerts = Array.isArray(alertPayload.alerts) ? alertPayload.alerts : [];
  const severityOrder = { critical: 0, high: 1, medium: 2, low: 3 };
  return [...alerts]
    .sort((a, b) => {
      const left = severityOrder[a.severity] ?? 99;
      const right = severityOrder[b.severity] ?? 99;
      if (left !== right) return left - right;
      return asNumber(b.adjusted_signal_score) - asNumber(a.adjusted_signal_score);
    })
    .slice(0, limit);
}

function severityBadge(severity) {
  const value = String(severity || "low").toLowerCase();
  return `<span class="severity-badge severity-${escapeHtml(value)}">${escapeHtml(value)}</span>`;
}

function renderBrief() {
  const panel = document.querySelector("#tab-brief");
  if (!panel) return;

  const briefJson = state.data.latestWeeklyBriefJson;
  const briefHtml = state.data.latestWeeklyBriefHtml;

  if (briefJson && typeof briefJson === "object" && Object.keys(briefJson).length) {
    const summary = Array.isArray(briefJson.executive_summary) ? briefJson.executive_summary : [];
    const keyMetrics = briefJson.key_metrics && typeof briefJson.key_metrics === "object" ? briefJson.key_metrics : {};
    const actions = Array.isArray(briefJson.top_actions_this_week) ? briefJson.top_actions_this_week : [];
    const content = Array.isArray(briefJson.top_content_opportunities) ? briefJson.top_content_opportunities : [];
    const watchlist = Array.isArray(briefJson.watchlist_recommendations) ? briefJson.watchlist_recommendations : [];
    const alerts = Array.isArray(briefJson.top_alerts) ? briefJson.top_alerts : [];
    const qualityNotes = Array.isArray(briefJson.data_quality_notes) ? briefJson.data_quality_notes : [];

    const keyMetricsRows = Object.entries(keyMetrics)
      .map(([metric, value]) => `<tr><td>${escapeHtml(metric)}</td><td>${escapeHtml(String(value))}</td></tr>`)
      .join("");

    panel.innerHTML = `
      <h2>Weekly Brief</h2>
      <h3>Executive Summary</h3>
      ${summary.length ? `<ul>${summary.map((item) => `<li>${escapeHtml(String(item))}</li>`).join("")}</ul>` : "<p>No executive summary available</p>"}

      <h3>Key Metrics</h3>
      ${keyMetricsRows ? `<table><thead><tr><th>metric</th><th>value</th></tr></thead><tbody>${keyMetricsRows}</tbody></table>` : "<p>No key metrics available</p>"}

      <h3>What Actions Should I Take This Week?</h3>
      <div id="brief-actions"></div>

      <h3>Top Content Opportunities</h3>
      <div id="brief-content"></div>

      <h3>Watchlist</h3>
      <div id="brief-watchlist"></div>

      <h3>Alerts to Watch</h3>
      <div id="brief-alerts"></div>

      <h3>Data Quality Notes</h3>
      ${qualityNotes.length ? `<ul>${qualityNotes.map((item) => `<li>${escapeHtml(String(item))}</li>`).join("")}</ul>` : "<p>No data quality notes</p>"}
    `;

    renderTable(panel.querySelector("#brief-actions"), [
      "priority", "action_type", "recommended_action", "reason", "confidence_level", "decision_score"
    ], actions, { initialSortKey: "decision_score", title: "Actions" });

    renderTable(panel.querySelector("#brief-content"), [
      "content_strategy", "source_title", "why_it_matters", "evidence_score", "recommended_timeframe"
    ], content, { initialSortKey: "evidence_score", title: "Content opportunities" });

    renderTable(panel.querySelector("#brief-watchlist"), [
      "entity_type", "entity_id", "title", "reason", "watch_priority"
    ], watchlist, { initialSortKey: "watch_priority", title: "Watchlist" });

    renderTable(panel.querySelector("#brief-alerts"), [
      "severity", "signal_type", "entity_id", "adjusted_signal_score"
    ], alerts, { initialSortKey: "adjusted_signal_score", title: "Alerts" });
    return;
  }

  if (typeof briefHtml === "string" && briefHtml.trim()) {
    panel.innerHTML = briefHtml;
    return;
  }

  panel.innerHTML = "<p>No weekly brief generated yet</p>";
}

function renderDataQuality(quality, advanced) {
  const panel = document.querySelector("#tab-data-quality");
  if (!panel) return;
  const lowConfidenceRows = advanced.filter((row) => asNumber(row.metric_confidence_score) < 50);

  panel.innerHTML = '<div id="dq-metric"></div><div id="dq-low"></div>';
  renderTable(document.querySelector("#dq-metric"), [
    "video_id", "channel_id", "short_term_eligible", "mid_term_eligible", "long_term_eligible", "confidence_reason"
  ], quality, { initialSortKey: "video_id", title: "metric eligibility" });

  renderTable(document.querySelector("#dq-low"), [
    "title", "channel_name", "metric_confidence_score"
  ], lowConfidenceRows, { initialSortKey: "metric_confidence_score", title: "low confidence rows" });
}

function renderModels() {
  const panel = document.querySelector("#tab-models");
  if (!panel) return;

  const manifest = state.data.latestModelManifest || {};
  const leaderboard = tableRows("latestModelLeaderboard");
  const importanceRows = tableRows("latestFeatureImportance");
  const directionRows = tableRows("latestFeatureDirection");
  const suiteReportHtml = typeof state.data.latestModelSuiteReportHtml === "string" ? state.data.latestModelSuiteReportHtml : "";

  panel.innerHTML = `
    <h2>Models</h2>
    <div id="models-status" class="kpi-grid"></div>
    <h3 class="section-title">Leaderboard</h3>
    <div id="models-leaderboard"></div>
    <h3 class="section-title">Feature Importance</h3>
    <div class="filters">
      <select id="models-target-filter"><option value="">All targets</option></select>
      <select id="models-family-filter"><option value="">All families</option></select>
    </div>
    <div id="models-importance"></div>
    <h3 class="section-title">Linear coefficients</h3>
    <div id="models-linear-coeff"></div>
    <h3 class="section-title">Random Forest</h3>
    <p class="warning">RF importance does not imply direction; direction is estimated with prediction-based directional analysis.</p>
    <div id="models-rf"></div>
    <h3 class="section-title">Shallow Tree Rules</h3>
    <div id="models-tree-rules"></div>
  `;

  const statusCards = [
    ["suite_id", manifest.suite_id || "--"],
    ["artifact_name", manifest.artifact_name || "--"],
    ["workflow_run_id", manifest.workflow_run_id || "--"],
    ["expires_at_estimate", manifest.expires_at_estimate || "--"]
  ];
  const statusHtml = statusCards
    .map(([label, value]) => `<article class="kpi-card"><h3>${escapeHtml(label)}</h3><p>${escapeHtml(String(value))}</p></article>`)
    .join("");
  const status = panel.querySelector("#models-status");
  if (status) status.innerHTML = statusHtml;

  renderTable(panel.querySelector("#models-leaderboard"), [
    "model_family", "target", "champion_metric", "champion_metric_value", "selected_as_champion", "lift_vs_best_baseline"
  ], leaderboard, { initialSortKey: "champion_metric_value", title: "Model leaderboard" });

  const targets = [...new Set(importanceRows.map((row) => row.target).filter(Boolean))].sort();
  const families = [...new Set(importanceRows.map((row) => row.model_family).filter(Boolean))].sort();
  const targetFilter = panel.querySelector("#models-target-filter");
  const familyFilter = panel.querySelector("#models-family-filter");
  if (targetFilter) {
    targetFilter.insertAdjacentHTML("beforeend", targets.map((target) => `<option value="${escapeHtml(target)}">${escapeHtml(target)}</option>`).join(""));
  }
  if (familyFilter) {
    familyFilter.insertAdjacentHTML("beforeend", families.map((family) => `<option value="${escapeHtml(family)}">${escapeHtml(family)}</option>`).join(""));
  }

  const redrawImportance = () => {
    const target = targetFilter?.value || "";
    const family = familyFilter?.value || "";
    const filtered = importanceRows.filter((row) => {
      if (target && row.target !== target) return false;
      if (family && row.model_family !== family) return false;
      return true;
    });
    const topRows = sortRows(filtered, "importance_rank", "asc").slice(0, 20);
    renderTable(panel.querySelector("#models-importance"), [
      "target", "model_family", "feature", "importance_type", "importance_value", "importance_rank", "direction"
    ], topRows, { initialSortKey: "importance_rank", title: "Top variables" });

    const linearRows = sortRows(
      filtered.filter((row) => row.model_family === "linear_regularized"),
      "importance_rank",
      "asc"
    ).slice(0, 20);
    renderTable(panel.querySelector("#models-linear-coeff"), [
      "feature", "standardized_coefficient", "direction", "importance_rank"
    ], linearRows, { initialSortKey: "importance_rank", title: "Linear coefficients" });

    const rfRows = sortRows(
      directionRows.filter((row) => row.model_family === "random_forest").filter((row) => !target || row.target === target),
      "direction_score",
      "desc"
    ).slice(0, 20);
    renderTable(panel.querySelector("#models-rf"), [
      "feature", "direction", "direction_score", "direction_method", "low_bin_prediction", "high_bin_prediction"
    ], rfRows, { initialSortKey: "direction_score", title: "RF permutation importance & estimated direction" });
  };

  targetFilter?.addEventListener("change", redrawImportance);
  familyFilter?.addEventListener("change", redrawImportance);
  redrawImportance();

  if (suiteReportHtml.trim()) {
    panel.querySelector("#models-tree-rules").innerHTML = suiteReportHtml;
  } else {
    panel.querySelector("#models-tree-rules").innerHTML = "<p>No suite report available</p>";
  }
}

function renderTopics() {
  const panel = document.querySelector("#tab-topics");
  if (!panel) return;
  panel.innerHTML = `
    <h2>Topics</h2>
    <div id="topics-opportunities"></div>
    <div id="topics-metrics"></div>
    <div id="topics-patterns"></div>
    <div id="topics-keywords"></div>
  `;
  renderTable(panel.querySelector("#topics-opportunities"), [
    "topic", "opportunity_type", "topic_opportunity_score", "topic_saturation_score", "topic_velocity_score", "recommended_action"
  ], tableRows("latestTopicOpportunities"), { initialSortKey: "topic_opportunity_score", title: "Topic opportunities" });
  renderTable(panel.querySelector("#topics-metrics"), [
    "topic", "video_count", "channel_count", "avg_views_delta", "avg_engagement_rate", "topic_velocity_score", "topic_saturation_score", "topic_opportunity_score"
  ], tableRows("latestTopicMetrics"), { initialSortKey: "topic_opportunity_score", title: "Topic metrics" });
  renderTable(panel.querySelector("#topics-patterns"), [
    "title_pattern", "video_count", "avg_views_delta", "avg_engagement_rate", "title_pattern_success_score", "example_titles"
  ], tableRows("latestTitlePatternMetrics"), { initialSortKey: "title_pattern_success_score", title: "Title pattern metrics" });
  renderTable(panel.querySelector("#topics-keywords"), [
    "keyword", "semantic_group", "video_count", "total_views_delta", "avg_engagement_rate", "top_video_title"
  ], tableRows("latestKeywordMetrics"), { initialSortKey: "video_count", title: "Keyword metrics" });
}

function renderNlp() {
  const panel = document.querySelector("#tab-nlp");
  if (!panel) return;
  const clusters = tableRows("latestSemanticClusters");
  const videos = tableRows("latestVideoNlpFeatures");
  const titles = tableRows("latestTitleNlpFeatures");

  panel.innerHTML = `
    <h2>NLP</h2>
    <div id="nlp-clusters-bars"></div>
    <div id="nlp-clusters-table"></div>
    <div id="nlp-video-semantic"></div>
    <div id="nlp-title-features"></div>
  `;

  const byCluster = Object.values(videos.reduce((acc, row) => {
    const key = row.semantic_cluster_label || "unknown";
    if (!acc[key]) acc[key] = { label: key, views_delta: 0 };
    acc[key].views_delta += asNumber(row.views_delta);
    return acc;
  }, {})).sort((a, b) => b.views_delta - a.views_delta).slice(0, 10);
  const barsWrap = panel.querySelector("#nlp-clusters-bars");
  if (barsWrap) {
    renderHorizontalBars(barsWrap, byCluster, { labelKey: "label", valueKey: "views_delta", title: "Top semantic clusters by views_delta" });
  }
  renderTable(panel.querySelector("#nlp-clusters-table"), [
    "video_id", "semantic_cluster_id", "semantic_cluster_size", "semantic_cluster_label", "cluster_top_terms"
  ], clusters, { initialSortKey: "semantic_cluster_size", title: "Semantic clusters" });
  renderTable(panel.querySelector("#nlp-video-semantic"), [
    "title", "channel_name", "ai_semantic_score", "finance_semantic_score", "productivity_semantic_score", "tutorial_semantic_score", "news_semantic_score", "views_delta"
  ], videos, { initialSortKey: "views_delta", title: "Semantic scores por video" });
  renderTable(panel.querySelector("#nlp-title-features"), [
    "title", "title_length_chars", "title_word_count", "title_has_number", "title_has_question", "hook_semantic_type", "dominant_semantic_score"
  ], titles, { initialSortKey: "dominant_semantic_score", title: "Title NLP features" });
}

function renderContentDrivers() {
  const panel = document.querySelector("#tab-content-drivers");
  if (!panel) return;
  const leaderboard = tableRows("latestContentDriverLeaderboard");
  const importance = tableRows("latestContentDriverFeatureImportance");
  const directions = tableRows("latestContentDriverFeatureDirection");
  const groups = tableRows("latestContentDriverGroupImportance");
  const reportHtml = typeof state.data.latestContentDriverReportHtml === "string" ? state.data.latestContentDriverReportHtml : "";

  panel.innerHTML = `
    <h2>Content Drivers</h2>
    <p class="warning">Estas importancias son predictivas, no causales.</p>
    <div id="cd-leaderboard"></div>
    <div id="cd-importance"></div>
    <div id="cd-direction"></div>
    <div id="cd-groups"></div>
    <h3 class="section-title">Reporte HTML</h3>
    <div id="cd-report"></div>
  `;

  renderTable(panel.querySelector("#cd-leaderboard"), [
    "target", "model_family", "mae_log", "rmse_log", "spearman_corr", "top_10_overlap_with_actual", "precision_at_top_decile_regression"
  ], leaderboard, { initialSortKey: "spearman_corr", title: "Leaderboard por target" });
  renderTable(panel.querySelector("#cd-importance"), [
    "target", "model_family", "feature", "feature_group", "importance_type", "importance_value", "importance_rank", "direction"
  ], importance, { initialSortKey: "importance_rank", title: "Top features por target/model" });
  renderTable(panel.querySelector("#cd-direction"), [
    "target", "model_family", "feature", "feature_group", "direction", "direction_score", "direction_method", "low_bin_prediction", "high_bin_prediction"
  ], directions, { initialSortKey: "direction_score", title: "Feature directions" });
  renderTable(panel.querySelector("#cd-groups"), [
    "target", "model_family", "feature_group", "group_importance", "feature_count"
  ], groups, { initialSortKey: "group_importance", title: "Group importance" });

  const reportNode = panel.querySelector("#cd-report");
  if (reportNode) {
    reportNode.innerHTML = reportHtml.trim() || "<p>No content driver report available.</p>";
  }
}

function setGeneratedAt(value) {
  const element = document.querySelector("#generated-at");
  if (element) element.textContent = `Generated: ${formatDate(value)}`;
}

function setDataStatus(value) {
  const element = document.querySelector("#data-status");
  if (element) element.textContent = `Data status: ${value}`;
}

function pushWarning(message) {
  const container = document.querySelector("#warnings");
  if (!container) return;
  const div = document.createElement("div");
  div.className = "warning";
  div.textContent = message;
  container.append(div);
}

function capitalize(value) {
  return value.slice(0, 1).toUpperCase() + value.slice(1);
}
