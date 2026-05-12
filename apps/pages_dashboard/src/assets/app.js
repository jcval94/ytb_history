import { renderFunnel, renderGauge, renderHeatmap, renderHorizontalBars, renderLineChart, renderScatterPlot } from "./charts.js";
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
  latestModelReadinessDiagnostics: "./data/latest_model_readiness_diagnostics.json",
  latestTargetCoverageReport: "./data/latest_target_coverage_report.json",
  latestTrainingGapReport: "./data/latest_training_gap_report.json",
  latestModelReadinessReportHtml: "./data/latest_model_readiness_report.html",
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
  latestCreativePackages: "./data/latest_creative_packages.json",
  latestTitleCandidates: "./data/latest_title_candidates.json",
  latestHookCandidates: "./data/latest_hook_candidates.json",
  latestThumbnailBriefs: "./data/latest_thumbnail_briefs.json",
  latestScriptOutlines: "./data/latest_script_outlines.json",
  latestOriginalityChecks: "./data/latest_originality_checks.json",
  latestProductionChecklist: "./data/latest_production_checklist.json",
  creativePackagesSummary: "./data/creative_packages_summary.json",
  transcriptSelectionReport: "./data/transcript_selection_report.json",
  transcriptionRunReport: "./data/transcription_run_report.json",
  transcriptInsightsRunReport: "./data/transcript_insights_run_report.json",
  transcriptRegistry: "./data/transcript_registry.json",
  transcriptInsightsIndex: "./data/transcript_insights_index.json",
  latestWeeklyBriefJson: "./data/latest_weekly_brief.json",
  latestWeeklyBriefHtml: "./data/latest_weekly_brief.html",
  latestProcessStatus: "./data/latest_process_status.json",
  processCatalog: "./data/process_catalog.json",
  operationSummary: "./data/operation_summary.json",
  dashboardImpactMatrix: "./data/dashboard_impact_matrix.json"
};

const state = {
  data: {},
  filterText: "",
  channel: "",
  duration: "",
  horizon: "all",
  renderedTabs: new Set()
};

init().catch((error) => {
  pushWarning(`Unexpected error: ${error instanceof Error ? error.message : String(error)}`);
  setDomainStatus("operational_data_status", { state: "generation_error", message: "Error real de generación" });
  setDomainStatus("ml_data_status", { state: "generation_error", message: "Error real de generación" });
});

async function init() {
  bindTabs();
  bindFilters();
  bindChartReplayControls();

  const manifest = await fetchJson(DATA_FILES.manifest, { required: true });
  state.data.manifest = manifest ?? {};
  if (!manifest) return;

  setGeneratedAt(manifest.generated_at || "");
  setDataStatus(manifest.warnings || [], manifest.notices || []);
  const freshness = manifest.data_freshness || {};
  setDomainStatus("operational_data_status", freshness.operational_data_status);
  setDomainStatus("ml_data_status", freshness.ml_data_status);
  (manifest.warnings || []).forEach(pushWarning);
  (manifest.notices || []).forEach(pushNotice);

  const TEXT_DATA_KEYS = new Set([
    "latestWeeklyBriefHtml",
    "latestModelSuiteReportHtml",
    "latestContentDriverReportHtml",
    "latestModelReadinessReportHtml"
  ]);

  for (const [key, path] of Object.entries(DATA_FILES)) {
    if (key === "manifest") continue;
    if (TEXT_DATA_KEYS.has(key)) {
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

const TAB_RENDERERS = {
  videos: () => renderVideos(applyFilters(tableRows("latestVideoMetrics"))),
  channels: () => renderChannels(applyFilters(tableRows("latestChannelMetrics"), { skipDuration: true })),
  scores: () => renderScores(applyFilters(tableRows("latestVideoScores"))),
  advanced: () => renderAdvanced(applyFilters(tableRows("latestVideoAdvancedMetrics"))),
  titles: () => renderTitles(applyFilters(tableRows("latestTitleMetrics"))),
  periods: () => renderPeriods(),
  alerts: () => renderAlerts(),
  "data-quality": () => renderDataQuality(
    applyFilters(tableRows("latestMetricEligibility"), { skipDuration: true }),
    applyFilters(tableRows("latestVideoAdvancedMetrics"))
  ),
  models: () => renderModels(),
  topics: () => renderTopics(),
  nlp: () => renderNlp(),
  "content-drivers": () => renderContentDrivers(),
  creative: () => renderCreativePackages(),
  transcripts: () => renderTranscripts(),
  brief: () => renderBrief(),
  operations: () => renderOperations()
};

function activateTab(tab) {
  const tabs = document.querySelector("#tabs");
  if (!tabs) return;
  tabs.querySelectorAll("button").forEach((item) => item.classList.remove("active"));
  document.querySelectorAll(".tab-panel").forEach((panel) => panel.classList.remove("active"));
  tabs.querySelector(`button[data-tab="${tab}"]`)?.classList.add("active");
  document.querySelector(`#tab-${tab}`)?.classList.add("active");
  renderActiveTab(tab);
}

function renderActiveTab(tab) {
  if (state.renderedTabs.has(tab)) return;
  const renderer = TAB_RENDERERS[tab];
  if (!renderer) return;
  renderer();
  state.renderedTabs.add(tab);
}

function getActiveTab() {
  return document.querySelector("#tabs button.active")?.getAttribute("data-tab") || "overview";
}

function renderAfterGlobalFilterChange() {
  state.renderedTabs.clear();
  renderAll();
  renderActiveTab(getActiveTab());
}

function bindFilters() {
  const filterInput = document.querySelector("#global-filter");
  const channelFilter = document.querySelector("#channel-filter");
  const durationFilter = document.querySelector("#duration-filter");
  const horizonFilter = document.querySelector("#horizon-filter");
  const resetButton = document.querySelector("#reset-filters");

  filterInput?.addEventListener("input", () => {
    state.filterText = String(filterInput.value || "").toLowerCase();
    renderAfterGlobalFilterChange();
  });
  channelFilter?.addEventListener("change", () => {
    state.channel = String(channelFilter.value || "");
    renderAfterGlobalFilterChange();
  });
  durationFilter?.addEventListener("change", () => {
    state.duration = String(durationFilter.value || "");
    renderAfterGlobalFilterChange();
  });
  horizonFilter?.addEventListener("change", () => {
    state.horizon = String(horizonFilter.value || "all");
    renderAfterGlobalFilterChange();
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
    renderAfterGlobalFilterChange();
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

  renderHeader(videos, channels);
  renderAnalysisDateRange(videos);
  renderKpis(videos, channels, scores, advanced);
  renderOverview(videos, channels, scores);
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

function renderAnalysisDateRange(videos) {
  const target = document.querySelector("#analysis-date-range");
  if (!target) return;

  const uploadDates = videos
    .map((row) => normalizeDateValue(row.upload_date))
    .filter(Boolean)
    .sort();
  const executionDates = videos
    .map((row) => normalizeDateValue(row.execution_date))
    .filter(Boolean)
    .sort();

  const uploadLabel = formatDateRangeLabel(uploadDates);
  const executionLabel = formatDateRangeLabel(executionDates);

  const parts = [];
  if (uploadLabel) parts.push(`Upload: ${uploadLabel}`);
  if (executionLabel) parts.push(`Snapshots: ${executionLabel}`);

  target.textContent = parts.length ? `Analysis window: ${parts.join(" | ")}` : "Analysis window: --";
}

function normalizeDateValue(value) {
  if (!value) return "";
  if (typeof value !== "string") return "";
  const trimmed = value.trim();
  if (!trimmed) return "";
  return trimmed.length >= 10 ? trimmed.slice(0, 10) : "";
}

function formatDateRangeLabel(sortedDates) {
  if (!sortedDates.length) return "";
  const first = sortedDates[0];
  const last = sortedDates[sortedDates.length - 1];
  if (first === last) return first;
  return `${first} → ${last}`;
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
    <div id="ov-decision-lab" class="chart-grid"></div>
    <div id="ov-videos" class="grid-two"></div>
    <div id="ov-channels" class="grid-two"></div>
    <div id="ov-alerts"></div>
    <div id="ov-creative"></div>
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
  renderOverviewCharts(panel, videos, scores);

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

  const creativeWrap = document.createElement("div");
  const creativeTop = sortRows(tableRows("latestCreativePackages"), "creative_execution_score", "desc").slice(0, 3);
  if (!creativeTop.length) {
    creativeWrap.innerHTML = '<h3 class="section-title">Top Creative Packages</h3><p>No creative packages generated yet</p>';
  } else {
    const items = creativeTop
      .map((row) => `<li>${escapeHtml(String(row.topic || "--"))} · ${escapeHtml(String(row.package_type || "--"))} · score ${escapeHtml(String(row.creative_execution_score || "--"))}</li>`)
      .join("");
    creativeWrap.innerHTML = `<h3 class="section-title">Top Creative Packages</h3><ul>${items}</ul>`;
  }
  panel.querySelector("#ov-creative")?.append(creativeWrap);

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

function bindChartReplayControls() {
  document.addEventListener("click", (event) => {
    const button = event.target instanceof Element ? event.target.closest("[data-chart-play]") : null;
    if (!button) return;
    const card = button.closest(".chart-card");
    if (!card) return;
    card.classList.remove("is-replaying");
    void card.offsetWidth;
    card.classList.add("is-replaying");
  });
}

function renderOverviewCharts(panel, videos, scores) {
  const chartGrid = panel.querySelector("#ov-decision-lab");
  if (!chartGrid) return;
  chartGrid.innerHTML = `
    <div id="ov-performance-scatter"></div>
    <div id="ov-opportunity-matrix"></div>
    <div id="ov-signal-funnel"></div>
  `;

  const scoreByVideo = new Map(scores.map((row) => [String(row.video_id || ""), row]));
  const performanceRows = videos
    .map((row) => {
      const score = scoreByVideo.get(String(row.video_id || "")) || {};
      return {
        ...row,
        alpha_score: asNumber(score.alpha_score ?? row.alpha_score),
        opportunity_score: asNumber(score.opportunity_score ?? row.opportunity_score)
      };
    })
    .filter((row) => asNumber(row.views_delta) > 0 || asNumber(row.engagement_rate) > 0);

  renderScatterPlot(chartGrid.querySelector("#ov-performance-scatter"), performanceRows, {
    xKey: "views_delta",
    yKey: "engagement_rate",
    sizeKey: "alpha_score",
    colorKey: "channel_name",
    labelKey: "title",
    title: "Reach vs engagement map",
    subtitle: "Separates high-reach videos from high-quality-audience videos.",
    xLabel: "views_delta",
    yLabel: "engagement_rate",
    formatY: formatPercent,
    tooltipKeys: ["channel_name", "video_age_days", "duration_bucket", "alpha_score", "opportunity_score"],
    pointLabelKeys: ["title"],
    maxPointLabels: 5
  });

  renderScatterPlot(chartGrid.querySelector("#ov-opportunity-matrix"), getOpportunityMatrixRows(), {
    xKey: "avg_confidence_score",
    yKey: "avg_decision_score",
    sizeKey: "candidates_count",
    colorKey: "action_type",
    labelKey: "recommended_focus",
    title: "Opportunity vs confidence",
    subtitle: "Prioritize the upper-right: strong score with enough evidence.",
    xLabel: "avg confidence",
    yLabel: "avg decision",
    tooltipKeys: ["action_type", "candidates_count", "recommended_focus"],
    pointLabelKeys: ["recommended_focus", "action_type"],
    maxPointLabels: 4
  });

  renderFunnel(chartGrid.querySelector("#ov-signal-funnel"), buildSignalFunnelSteps(), {
    title: "Signals to action funnel",
    subtitle: "A quick audit of how many signals survive into alerts and actions."
  });
}

function renderVideos(videos) {
  const panel = document.querySelector("#tab-videos");
  if (!panel) return;
  panel.innerHTML = `
    <div id="videos-visuals" class="chart-grid"></div>
    <div id="videos-table"></div>
  `;
  renderVideoCharts(panel.querySelector("#videos-visuals"), videos);
  renderTable(panel.querySelector("#videos-table"), [
    "title", "channel_name", "views_delta", "engagement_rate", "video_age_days", "duration_bucket"
  ], videos, { initialSortKey: "views_delta", title: "Videos", pageSize: 25 });
}

function renderChannels(channels) {
  const panel = document.querySelector("#tab-channels");
  if (!panel) return;
  panel.innerHTML = `
    <div id="channels-visuals" class="chart-grid"></div>
    <div id="channels-table"></div>
  `;
  renderChannelCharts(panel.querySelector("#channels-visuals"), channels);
  renderTable(panel.querySelector("#channels-table"), [
    "channel_name", "total_views_delta", "avg_engagement_rate", "channel_momentum_score", "videos_tracked"
  ], channels, { initialSortKey: "total_views_delta", title: "Channels", pageSize: 10 });
}

function renderScores(scores) {
  const panel = document.querySelector("#tab-scores");
  if (!panel) return;
  panel.innerHTML = `
    <div id="scores-visuals" class="chart-grid"></div>
    <div id="scores-table"></div>
  `;
  renderScoreCharts(panel.querySelector("#scores-visuals"), scores);
  renderTable(panel.querySelector("#scores-table"), [
    "title", "channel_name", "alpha_score", "opportunity_score", "anomaly_score"
  ], scores, { initialSortKey: "alpha_score", title: "Scores", pageSize: 25 });
}

function renderAdvanced(advanced) {
  const panel = document.querySelector("#tab-advanced");
  if (!panel) return;
  panel.innerHTML = `
    <div id="advanced-visuals" class="chart-grid"></div>
    <div id="advanced-table"></div>
  `;
  renderAdvancedCharts(panel.querySelector("#advanced-visuals"), advanced);
  renderTable(panel.querySelector("#advanced-table"), [
    "title", "short_term_success_score", "mid_term_success_score", "long_term_success_score", "trend_burst_score",
    "evergreen_score", "packaging_problem_score", "metric_confidence_score"
  ], advanced, { initialSortKey: "short_term_success_score", title: "Advanced", pageSize: 25 });
}

function renderTitles(titles) {
  const panel = document.querySelector("#tab-titles");
  if (!panel) return;
  panel.innerHTML = `
    <div id="titles-visuals" class="chart-grid"></div>
    <div id="titles-table"></div>
  `;
  renderTitleCharts(panel.querySelector("#titles-visuals"), titles);
  renderTable(panel.querySelector("#titles-table"), [
    "title", "has_number", "has_question", "has_ai_word", "has_finance_word", "views_delta"
  ], titles, { initialSortKey: "views_delta", title: "Titles", pageSize: 25 });
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
    <div id="period-growth-line" class="chart-grid chart-grid-wide"></div>
    <div id="period-video-table"></div>
    <div id="period-channel-table"></div>
  `;

  const redraw = () => {
    const grain = document.querySelector(`#${selectorId}`)?.value || "daily";
    const videoRows = tableRows(`period${capitalize(grain)}Video`);
    const channelRows = tableRows(`period${capitalize(grain)}Channel`);
    renderLineChart(document.querySelector("#period-growth-line"), channelRows, {
      xKey: "period_start",
      yKey: "period_views_delta",
      groupKey: "channel_name",
      title: `Growth time-series by channel (${grain})`,
      subtitle: "Use this to spot sustained momentum versus one-period spikes.",
      xLabel: "period_start",
      yLabel: "views_delta",
      maxGroups: 6
    });
    renderTable(document.querySelector("#period-video-table"), [
      "period_start", "title", "period_views_delta", "period_avg_engagement_rate"
    ], videoRows, { initialSortKey: "period_views_delta", title: `Video period metrics (${grain})`, pageSize: 25 });
    renderTable(document.querySelector("#period-channel-table"), [
      "period_start", "channel_name", "period_views_delta", "period_avg_engagement_rate"
    ], channelRows, { initialSortKey: "period_views_delta", title: `Channel period metrics (${grain})`, pageSize: 25 });
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
    <div class="chart-grid"><div id="alerts-flow"></div></div>
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
  renderFunnel(document.querySelector("#alerts-flow"), buildSignalFunnelSteps(), {
    title: "Candidate to alert funnel",
    subtitle: "Shows how strict the alerting layer is on this run."
  });

  if (!alerts.length) {
    document.querySelector("#alerts-top").innerHTML = "<p>No alerts generated yet</p>";
    document.querySelector("#alerts-table").innerHTML = "<p>No alerts generated yet</p>";
  } else {
    const topRows = topAlertsBySeverity(10);
    renderTable(document.querySelector("#alerts-top"), [
      "signal_type", "severity", "title", "channel_name", "adjusted_signal_score", "confidence_level", "recommended_action"
    ], topRows, { initialSortKey: "adjusted_signal_score", title: "Top alerts", pageSize: 10 });
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
    ], filtered, { initialSortKey: "adjusted_signal_score", title: "Alerts", pageSize: 25 });
  };
  document.querySelector("#alerts-severity-filter")?.addEventListener("change", redraw);
  document.querySelector("#alerts-signal-filter")?.addEventListener("change", redraw);
  document.querySelector("#alerts-entity-filter")?.addEventListener("change", redraw);
  redraw();

  renderTable(document.querySelector("#signal-candidates-table"), [
    "entity_type", "entity_id", "signal_type", "triggered", "raw_signal_score", "adjusted_signal_score", "confidence_level"
  ], signalCandidates, { initialSortKey: "adjusted_signal_score", title: "Signal candidates", pageSize: 25 });
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

function renderCreativePackages() {
  const panel = document.querySelector("#tab-creative");
  if (!panel) return;
  const packages = tableRows("latestCreativePackages");
  const titles = tableRows("latestTitleCandidates");
  const hooks = tableRows("latestHookCandidates");
  const thumbs = tableRows("latestThumbnailBriefs");
  const outlines = tableRows("latestScriptOutlines");
  const originality = tableRows("latestOriginalityChecks");
  const checklist = tableRows("latestProductionChecklist");
  const summary = state.data.creativePackagesSummary || {};

  const typeCounts = summary.package_type_counts || {};
  const topPackageType = Object.keys(typeCounts).sort((a, b) => asNumber(typeCounts[b]) - asNumber(typeCounts[a]))[0] || "--";
  const cards = [
    ["total_packages", summary.total_packages ?? packages.length],
    ["avg_originality_score", summary.avg_originality_score ?? "--"],
    ["high_copy_risk_count", summary.high_copy_risk_count ?? 0],
    ["top_package_type", topPackageType]
  ].map(([k,v]) => `<article class="kpi-card"><h3>${escapeHtml(String(k))}</h3><p>${escapeHtml(String(v))}</p></article>`).join("");

  panel.innerHTML = `<div class="kpi-grid">${cards}</div><div id="creative-visuals" class="chart-grid"></div><div id="creative-filters"></div><div id="creative-tables"></div>`;

  const packageTypes = [...new Set(packages.map((r) => String(r.package_type || "")).filter(Boolean))].sort();
  const topics = [...new Set(packages.map((r) => String(r.topic || "")).filter(Boolean))].sort();
  const timeframes = [...new Set(packages.map((r) => String(r.recommended_timeframe || "")).filter(Boolean))].sort();
  const statuses = [...new Set(titles.map((r) => String(r.originality_status || "")).filter(Boolean))].sort();

  const filtersHtml = `
    <h3 class="section-title">Creative Filters</h3>
    <div class="filters">
      <select id="creative-package-type"><option value="">All package types</option>${packageTypes.map((v) => `<option value="${escapeHtml(v)}">${escapeHtml(v)}</option>`).join("")}</select>
      <select id="creative-topic"><option value="">All topics</option>${topics.map((v) => `<option value="${escapeHtml(v)}">${escapeHtml(v)}</option>`).join("")}</select>
      <select id="creative-originality-status"><option value="">All originality</option>${statuses.map((v) => `<option value="${escapeHtml(v)}">${escapeHtml(v)}</option>`).join("")}</select>
      <select id="creative-timeframe"><option value="">All timeframes</option>${timeframes.map((v) => `<option value="${escapeHtml(v)}">${escapeHtml(v)}</option>`).join("")}</select>
    </div>
  `;
  panel.querySelector("#creative-filters").innerHTML = filtersHtml;

  const renderTables = () => {
    const fType = document.querySelector("#creative-package-type")?.value || "";
    const fTopic = document.querySelector("#creative-topic")?.value || "";
    const fStatus = document.querySelector("#creative-originality-status")?.value || "";
    const fTime = document.querySelector("#creative-timeframe")?.value || "";
    const filteredPackages = packages.filter((r) => (!fType || r.package_type === fType) && (!fTopic || r.topic === fTopic) && (!fTime || r.recommended_timeframe === fTime));
    const pkgIds = new Set(filteredPackages.map((r) => String(r.creative_package_id || "")));
    const filteredTitles = titles.filter((r) => pkgIds.has(String(r.creative_package_id || "")) && (!fStatus || r.originality_status === fStatus));
    const filteredHooks = hooks.filter((r) => pkgIds.has(String(r.creative_package_id || "")));
    const filteredThumbs = thumbs.filter((r) => pkgIds.has(String(r.creative_package_id || "")));
    const filteredOutlines = outlines.filter((r) => pkgIds.has(String(r.creative_package_id || "")));
    const filteredOriginality = originality.filter((r) => pkgIds.has(String(r.creative_package_id || "")) && (!fStatus || r.originality_status === fStatus));
    const filteredChecklist = checklist.filter((r) => pkgIds.has(String(r.creative_package_id || "")));

    renderCreativeCharts(panel.querySelector("#creative-visuals"), filteredPackages, filteredOriginality);

    const target = panel.querySelector("#creative-tables");
    target.innerHTML = '<div id="creative-packages"></div><div id="creative-titles"></div><div id="creative-hooks"></div><div id="creative-thumbs"></div><div id="creative-outlines"></div><div id="creative-originality"></div><div id="creative-checklist"></div>';
    renderTable(target.querySelector("#creative-packages"), ["package_type", "topic", "creative_angle", "recommended_format", "creative_execution_score", "transcript_available", "transcript_summary", "recommended_timeframe"], sortRows(filteredPackages, "creative_execution_score", "desc"), { title: "Top Creative Packages", pageSize: 25 });
    renderTable(target.querySelector("#creative-titles"), ["title_candidate", "title_pattern", "estimated_strength", "originality_status", "copy_risk_score"], sortRows(filteredTitles, "estimated_strength", "desc"), { title: "Title Candidates", pageSize: 25 });
    renderTable(target.querySelector("#creative-hooks"), ["hook_text", "hook_type", "expected_use", "risk", "transcript_aware"], filteredHooks, { title: "Hooks", pageSize: 25 });
    renderTable(target.querySelector("#creative-thumbs"), ["main_text", "visual_metaphor", "emotion", "layout_suggestion", "transcript_aware"], filteredThumbs, { title: "Thumbnail Briefs", pageSize: 25 });
    renderTable(target.querySelector("#creative-outlines"), ["structure_type", "intro", "section_1", "section_2", "section_3", "closing", "transcript_aware"], filteredOutlines, { title: "Script Outlines", pageSize: 25 });
    renderTable(target.querySelector("#creative-originality"), ["candidate_type", "copy_risk_score", "originality_status"], sortRows(filteredOriginality, "copy_risk_score", "desc"), { title: "Originality Checks", pageSize: 25 });
    renderTable(target.querySelector("#creative-checklist"), ["production_step", "estimated_effort", "required_input"], filteredChecklist, { title: "Production Checklist", pageSize: 10 });
  };

  ["#creative-package-type", "#creative-topic", "#creative-originality-status", "#creative-timeframe"].forEach((sel) => {
    panel.querySelector(sel)?.addEventListener("change", renderTables);
  });
  renderTables();
}

function renderTranscripts() {
  const panel = document.querySelector("#tab-transcripts");
  if (!panel) return;

  const selection = state.data.transcriptSelectionReport || {};
  const transcription = state.data.transcriptionRunReport || {};
  const insightsRun = state.data.transcriptInsightsRunReport || {};
  const registryRows = tableRows("transcriptRegistry");
  const insightsRows = tableRows("transcriptInsightsIndex");

  const cards = [
    ["selected_count", selection.selected_count ?? 0],
    ["selected_forced_count", selection.selected_forced_count ?? 0],
    ["selected_ranked_count", selection.selected_ranked_count ?? 0],
    ["transcribed_success", transcription.transcribed_success ?? 0],
    ["skipped_no_audio_source", transcription.skipped_no_audio_source ?? 0],
    ["insights_generated", insightsRun.generated_success ?? insightsRun.generated ?? 0]
  ].map(([label, value]) => `<article class="kpi-card"><h3>${escapeHtml(String(label))}</h3><p>${escapeHtml(String(value))}</p></article>`).join("");

  panel.innerHTML = `
    <div class="kpi-grid">${cards}</div>
    <p class="notice">Audio sources are not stored in the repo. Place authorized audio files in data/audio_sources/ locally or in the workflow environment.</p>
    <div id="transcript-registry-table"></div>
    <div id="transcript-insights-table"></div>
  `;

  renderTable(panel.querySelector("#transcript-registry-table"), [
    "video_id", "channel_name", "title", "status", "source_type", "transcription_model", "text_char_count"
  ], registryRows, { initialSortKey: "text_char_count", title: "Transcript Registry", pageSize: 25 });
  renderTable(panel.querySelector("#transcript-insights-table"), [
    "video_id", "summary", "main_topics", "status"
  ], insightsRows, { title: "Transcript Insights", pageSize: 25 });
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
      <div id="brief-scorecard" class="kpi-grid"></div>
      ${keyMetricsRows ? `<table><thead><tr><th>metric</th><th>value</th></tr></thead><tbody>${keyMetricsRows}</tbody></table>` : "<p>No key metrics available</p>"}
      <div id="brief-visuals" class="chart-grid"></div>

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

    renderBriefScorecard(panel.querySelector("#brief-scorecard"), keyMetrics);
    renderBriefCharts(panel.querySelector("#brief-visuals"), keyMetrics, actions, content, alerts);

    renderTable(panel.querySelector("#brief-actions"), [
      "priority", "action_type", "recommended_action", "reason", "confidence_level", "decision_score"
    ], actions, { initialSortKey: "decision_score", title: "Actions", pageSize: 10 });

    renderTable(panel.querySelector("#brief-content"), [
      "content_strategy", "source_title", "why_it_matters", "evidence_score", "recommended_timeframe"
    ], content, { initialSortKey: "evidence_score", title: "Content opportunities", pageSize: 10 });

    renderTable(panel.querySelector("#brief-watchlist"), [
      "entity_type", "entity_id", "title", "reason", "watch_priority"
    ], watchlist, { initialSortKey: "watch_priority", title: "Watchlist", pageSize: 10 });

    renderTable(panel.querySelector("#brief-alerts"), [
      "severity", "signal_type", "entity_id", "adjusted_signal_score"
    ], alerts, { initialSortKey: "adjusted_signal_score", title: "Alerts", pageSize: 10 });
    return;
  }

  if (typeof briefHtml === "string" && briefHtml.trim()) {
    panel.innerHTML = briefHtml;
    return;
  }

  panel.innerHTML = "<p>No weekly brief generated yet</p>";
}

function renderOperations() {
  const panel = document.querySelector("#tab-operations");
  if (!panel) return;

  const processPayload = state.data.latestProcessStatus || {};
  const processes = Array.isArray(processPayload.processes) ? processPayload.processes : [];
  const impactRows = tableRows("dashboardImpactMatrix");
  const summary = state.data.operationSummary || {};

  const cards = [
    ["processes_total", summary.processes_total ?? processes.length],
    ["healthy", summary.healthy_count ?? countByStatus(processes, "success")],
    ["warnings", summary.warning_count ?? countByStatus(processes, "success_with_warnings")],
    ["stale", summary.stale_count ?? countByStatus(processes, "stale")],
    ["failed", summary.failed_count ?? countByStatus(processes, "failed")],
    ["not_initialized", summary.not_initialized_count ?? countByStatus(processes, "not_initialized")]
  ].map(([label, value]) => `<article class="kpi-card"><h3>${escapeHtml(String(label))}</h3><p>${escapeHtml(String(value ?? "--"))}</p></article>`).join("");

  panel.innerHTML = `
    <h2>Operations</h2>
    <div class="kpi-grid">${cards}</div>
    <div id="operations-visuals" class="chart-grid"></div>
    <section class="filters operations-filters">
      <select id="operations-domain-filter"><option value="">All domains</option>${selectOptions(uniqueValues(processes, "domain"))}</select>
      <select id="operations-status-filter"><option value="">All statuses</option>${selectOptions(uniqueValues(processes, "status"))}</select>
      <select id="operations-tab-filter"><option value="">All impacted tabs</option>${selectOptions(uniqueTabValues(processes))}</select>
      <button id="operations-reset-filters" type="button">Reset filters</button>
    </section>
    <div id="operations-stale-list"></div>
    <div id="operations-process-table"></div>
    <div id="operations-artifact-table"></div>
    <div id="operations-impact-table"></div>
  `;

  renderOperationsCharts(panel.querySelector("#operations-visuals"), processes, summary);

  const redraw = () => {
    const domain = panel.querySelector("#operations-domain-filter")?.value || "";
    const status = panel.querySelector("#operations-status-filter")?.value || "";
    const tab = panel.querySelector("#operations-tab-filter")?.value || "";
    const filtered = processes.filter((process) => {
      if (domain && process.domain !== domain) return false;
      if (status && process.status !== status) return false;
      if (tab && !(process.dashboard_tabs || []).includes(tab)) return false;
      return true;
    });
    const processIds = new Set(filtered.map((process) => process.process_id));
    const filteredImpact = impactRows.filter((row) => processIds.has(row.process_id) && (!tab || row.dashboard_tab === tab));
    const artifactRows = filtered.flatMap(operationArtifactRows);
    const staleRows = filtered.filter((process) => ["stale", "failed", "success_with_warnings", "not_initialized"].includes(process.status));

    renderTable(panel.querySelector("#operations-stale-list"), [
      "process_id", "name", "domain", "status", "base_status", "age_hours", "status_detail"
    ], operationProcessRows(staleRows), { initialSortKey: "status", title: "Action queue", pageSize: 10 });

    renderTable(panel.querySelector("#operations-process-table"), [
      "process_id", "name", "domain", "process_type", "cadence", "status", "base_status", "last_run_at", "age_hours", "sla_hours", "dashboard_tabs", "depends_on"
    ], operationProcessRows(filtered), { initialSortKey: "status", title: "Process status", pageSize: 25 });

    renderTable(panel.querySelector("#operations-artifact-table"), [
      "process_id", "artifact_path", "exists", "required", "status", "observed_at", "age_hours", "warnings_count", "errors_count", "failure_count"
    ], artifactRows, { initialSortKey: "process_id", title: "Artifact trace", pageSize: 25 });

    renderTable(panel.querySelector("#operations-impact-table"), [
      "process_id", "name", "domain", "process_type", "cadence", "dashboard_tab", "impact_type", "status", "last_run_at", "is_stale"
    ], filteredImpact, { initialSortKey: "dashboard_tab", title: "Dashboard impact matrix", pageSize: 25 });
  };

  ["#operations-domain-filter", "#operations-status-filter", "#operations-tab-filter"].forEach((selector) => {
    panel.querySelector(selector)?.addEventListener("change", redraw);
  });
  panel.querySelector("#operations-reset-filters")?.addEventListener("click", () => {
    ["#operations-domain-filter", "#operations-status-filter", "#operations-tab-filter"].forEach((selector) => {
      const node = panel.querySelector(selector);
      if (node) node.value = "";
    });
    redraw();
  });
  redraw();
}

function renderDataQuality(quality, advanced) {
  const panel = document.querySelector("#tab-data-quality");
  if (!panel) return;
  const lowConfidenceRows = advanced.filter((row) => asNumber(row.metric_confidence_score) < 50);

  panel.innerHTML = '<div id="dq-visuals" class="chart-grid"></div><div id="dq-metric"></div><div id="dq-low"></div>';
  renderDataQualityCharts(panel.querySelector("#dq-visuals"), quality, advanced);
  renderTable(panel.querySelector("#dq-metric"), [
    "video_id", "channel_id", "short_term_eligible", "mid_term_eligible", "long_term_eligible", "confidence_reason"
  ], quality, { initialSortKey: "video_id", title: "metric eligibility", pageSize: 25 });

  renderTable(panel.querySelector("#dq-low"), [
    "title", "channel_name", "metric_confidence_score"
  ], lowConfidenceRows, { initialSortKey: "metric_confidence_score", title: "low confidence rows", pageSize: 25 });
}

function renderModels() {
  const panel = document.querySelector("#tab-models");
  if (!panel) return;

  const manifest = state.data.latestModelManifest || {};
  const leaderboard = tableRows("latestModelLeaderboard");
  const importanceRows = tableRows("latestFeatureImportance");
  const directionRows = tableRows("latestFeatureDirection");
  const suiteReportHtml = typeof state.data.latestModelSuiteReportHtml === "string" ? state.data.latestModelSuiteReportHtml : "";
  const readiness = state.data.latestModelReadinessDiagnostics || {};
  const gap = state.data.latestTrainingGapReport || {};
  const targetCoverageRows = tableRows("latestTargetCoverageReport");
  const readinessReportHtml = typeof state.data.latestModelReadinessReportHtml === "string" ? state.data.latestModelReadinessReportHtml : "";

  panel.innerHTML = `
    <h2>Models</h2>
    <div id="models-status" class="kpi-grid"></div>
    <h3 class="section-title">Leaderboard</h3>
    <div id="models-leaderboard"></div>
    <div id="models-leaderboard-visuals" class="chart-grid"></div>
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
    <h3 class="section-title">Model Readiness</h3>
    <div id="models-readiness" class="kpi-grid"></div>
    <div id="models-readiness-visuals" class="chart-grid"></div>
    <div id="models-readiness-message"></div>
    <div id="models-target-coverage"></div>
    <div id="models-readiness-html"></div>
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
  ], leaderboard, { initialSortKey: "champion_metric_value", title: "Model leaderboard", pageSize: 10 });
  renderModelLeaderboardCharts(panel.querySelector("#models-leaderboard-visuals"), leaderboard);

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
    ], topRows, { initialSortKey: "importance_rank", title: "Top variables", pageSize: 10 });

    const linearRows = sortRows(
      filtered.filter((row) => row.model_family === "linear_regularized"),
      "importance_rank",
      "asc"
    ).slice(0, 20);
    renderTable(panel.querySelector("#models-linear-coeff"), [
      "feature", "standardized_coefficient", "direction", "importance_rank"
    ], linearRows, { initialSortKey: "importance_rank", title: "Linear coefficients", pageSize: 10 });

    const rfRows = sortRows(
      directionRows.filter((row) => row.model_family === "random_forest").filter((row) => !target || row.target === target),
      "direction_score",
      "desc"
    ).slice(0, 20);
    renderTable(panel.querySelector("#models-rf"), [
      "feature", "direction", "direction_score", "direction_method", "low_bin_prediction", "high_bin_prediction"
    ], rfRows, { initialSortKey: "direction_score", title: "RF permutation importance & estimated direction", pageSize: 10 });
  };

  targetFilter?.addEventListener("change", redrawImportance);
  familyFilter?.addEventListener("change", redrawImportance);
  redrawImportance();

  if (suiteReportHtml.trim()) {
    panel.querySelector("#models-tree-rules").innerHTML = suiteReportHtml;
  } else {
    panel.querySelector("#models-tree-rules").innerHTML = "<p>No suite report available</p>";
  }

  const readinessCards = [
    ["recommended_status", readiness.recommended_status || "--"],
    ["can_train_now", String(Boolean(readiness.can_train_now))],
    ["trainable_examples", String(readiness.trainable_examples ?? "--")],
    ["missing_exploratory", String(readiness.examples_missing_for_exploratory ?? "--")],
    ["missing_baseline", String(readiness.examples_missing_for_baseline ?? "--")],
    ["primary_blocker", gap.primary_blocker || "--"],
    ["forecast", (readiness.forecast || {}).status || "--"],
    ["recommended_next_steps", Array.isArray(readiness.recommended_next_steps) ? readiness.recommended_next_steps.join(" | ") : "--"]
  ];
  const readinessHtml = readinessCards
    .map(([label, value]) => `<article class="kpi-card"><h3>${escapeHtml(label)}</h3><p>${escapeHtml(String(value))}</p></article>`)
    .join("");
  const readinessWrap = panel.querySelector("#models-readiness");
  if (readinessWrap) readinessWrap.innerHTML = readinessHtml;
  renderModelReadinessCharts(panel.querySelector("#models-readiness-visuals"), readiness, gap, targetCoverageRows);

  const msg = panel.querySelector("#models-readiness-message");
  if (msg && readiness.can_train_now === false) {
    msg.innerHTML = "<p class=\"warning\">El entrenamiento todavía no está listo porque faltan ejemplos con observación futura. Sigue ejecutando YouTube Monitor diariamente.</p>";
  }
  renderTable(panel.querySelector("#models-target-coverage"), ["target_name", "coverage_pct", "trainable_rows", "blocker", "status"], targetCoverageRows, { initialSortKey: "coverage_pct", title: "Target Coverage", pageSize: 10 });
  if (readinessReportHtml.trim()) {
    panel.querySelector("#models-readiness-html").innerHTML = readinessReportHtml;
  }
}

function renderTopics() {
  const panel = document.querySelector("#tab-topics");
  if (!panel) return;
  const opportunities = tableRows("latestTopicOpportunities");
  const metrics = tableRows("latestTopicMetrics");
  const patterns = tableRows("latestTitlePatternMetrics");
  const keywords = tableRows("latestKeywordMetrics");

  panel.innerHTML = `
    <h2>Topics</h2>
    <div id="topics-visuals" class="chart-grid"></div>
    <div id="topics-opportunities"></div>
    <div id="topics-metrics"></div>
    <div id="topics-patterns"></div>
    <div id="topics-keywords"></div>
  `;
  renderTable(panel.querySelector("#topics-opportunities"), [
    "topic", "opportunity_type", "topic_opportunity_score", "topic_saturation_score", "topic_velocity_score", "recommended_action"
  ], opportunities, { initialSortKey: "topic_opportunity_score", title: "Topic opportunities", pageSize: 10 });
  renderTable(panel.querySelector("#topics-metrics"), [
    "topic", "video_count", "channel_count", "avg_views_delta", "avg_engagement_rate", "topic_velocity_score", "topic_saturation_score", "topic_opportunity_score"
  ], metrics, { initialSortKey: "topic_opportunity_score", title: "Topic metrics", pageSize: 25 });
  renderTable(panel.querySelector("#topics-patterns"), [
    "title_pattern", "video_count", "avg_views_delta", "avg_engagement_rate", "title_pattern_success_score", "example_titles"
  ], patterns, { initialSortKey: "title_pattern_success_score", title: "Title pattern metrics", pageSize: 25 });
  renderTable(panel.querySelector("#topics-keywords"), [
    "keyword", "semantic_group", "video_count", "total_views_delta", "avg_engagement_rate", "top_video_title"
  ], keywords, { initialSortKey: "video_count", title: "Keyword metrics", pageSize: 25 });
  renderTopicCharts(panel.querySelector("#topics-visuals"), opportunities, metrics, patterns);
}

function renderNlp() {
  const panel = document.querySelector("#tab-nlp");
  if (!panel) return;
  const clusters = tableRows("latestSemanticClusters");
  const videos = tableRows("latestVideoNlpFeatures");
  const titles = tableRows("latestTitleNlpFeatures");

  panel.innerHTML = `
    <h2>NLP</h2>
    <div id="nlp-visuals" class="chart-grid"></div>
    <div id="nlp-clusters-table"></div>
    <div id="nlp-video-semantic"></div>
    <div id="nlp-title-features"></div>
  `;

  renderNlpCharts(panel.querySelector("#nlp-visuals"), videos);
  renderTable(panel.querySelector("#nlp-clusters-table"), [
    "video_id", "semantic_cluster_id", "semantic_cluster_size", "semantic_cluster_label", "cluster_top_terms"
  ], clusters, { initialSortKey: "semantic_cluster_size", title: "Semantic clusters", pageSize: 25 });
  renderTable(panel.querySelector("#nlp-video-semantic"), [
    "title", "channel_name", "ai_semantic_score", "finance_semantic_score", "productivity_semantic_score", "tutorial_semantic_score", "news_semantic_score", "views_delta"
  ], videos, { initialSortKey: "views_delta", title: "Semantic scores por video", pageSize: 25 });
  renderTable(panel.querySelector("#nlp-title-features"), [
    "title", "title_length_chars", "title_word_count", "title_has_number", "title_has_question", "hook_semantic_type", "dominant_semantic_score"
  ], titles, { initialSortKey: "dominant_semantic_score", title: "Title NLP features", pageSize: 25 });
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
    <div id="cd-visuals" class="chart-grid chart-grid-focus"></div>
    <div id="cd-leaderboard"></div>
    <div id="cd-importance"></div>
    <div id="cd-direction"></div>
    <div id="cd-groups"></div>
    <h3 class="section-title">Reporte HTML</h3>
    <div id="cd-report"></div>
  `;

  renderTable(panel.querySelector("#cd-leaderboard"), [
    "target", "model_family", "mae_log", "rmse_log", "spearman_corr", "top_10_overlap_with_actual", "precision_at_top_decile_regression"
  ], leaderboard, { initialSortKey: "spearman_corr", title: "Leaderboard por target", pageSize: 10 });
  renderContentDriverCharts(panel.querySelector("#cd-visuals"), leaderboard, importance, directions, groups);
  renderTable(panel.querySelector("#cd-importance"), [
    "target", "model_family", "feature", "feature_group", "importance_type", "importance_value", "importance_rank", "direction"
  ], importance, { initialSortKey: "importance_rank", title: "Top features por target/model", pageSize: 25 });
  renderTable(panel.querySelector("#cd-direction"), [
    "target", "model_family", "feature", "feature_group", "direction", "direction_score", "direction_method", "low_bin_prediction", "high_bin_prediction"
  ], directions, { initialSortKey: "direction_score", title: "Feature directions", pageSize: 25 });
  renderTable(panel.querySelector("#cd-groups"), [
    "target", "model_family", "feature_group", "group_importance", "feature_count"
  ], groups, { initialSortKey: "group_importance", title: "Group importance", pageSize: 10 });

  const reportNode = panel.querySelector("#cd-report");
  if (reportNode) {
    reportNode.innerHTML = reportHtml.trim() || "<p>No content driver report available.</p>";
  }
}

function renderVideoCharts(container, videos) {
  if (!container) return;
  container.innerHTML = `
    <div id="videos-reach-map"></div>
    <div id="videos-age-velocity"></div>
  `;

  renderScatterPlot(container.querySelector("#videos-reach-map"), videos, {
    xKey: "views_delta",
    yKey: "engagement_rate",
    sizeKey: "views_per_day_since_upload",
    colorKey: "duration_bucket",
    labelKey: "title",
    title: "Video reach vs engagement",
    subtitle: "Highlights videos that combine growth with strong audience reaction.",
    xLabel: "views_delta",
    yLabel: "engagement_rate",
    formatY: formatPercent,
    tooltipKeys: ["channel_name", "video_age_days", "duration_bucket", "likes_delta", "comments_delta"],
    pointLabelKeys: ["title"],
    maxPointLabels: 5
  });

  renderScatterPlot(container.querySelector("#videos-age-velocity"), videos, {
    xKey: "video_age_days",
    yKey: "views_per_day_since_upload",
    sizeKey: "views_delta",
    colorKey: "channel_name",
    labelKey: "title",
    title: "Freshness vs velocity",
    subtitle: "Finds young videos earning attention faster than the catalog baseline.",
    xLabel: "age days",
    yLabel: "views per day",
    tooltipKeys: ["channel_name", "upload_date", "duration_bucket", "views_delta"],
    pointLabelKeys: ["title"],
    maxPointLabels: 4
  });
}

function renderChannelCharts(container, channels) {
  if (!container) return;
  container.innerHTML = `
    <div id="channels-momentum-map"></div>
    <div id="channels-format-mix"></div>
  `;

  renderScatterPlot(container.querySelector("#channels-momentum-map"), channels, {
    xKey: "total_views_delta",
    yKey: "avg_engagement_rate",
    sizeKey: "videos_tracked",
    colorKey: "channel_name",
    labelKey: "channel_name",
    title: "Channel growth vs engagement",
    subtitle: "Separates volume leaders from channels with concentrated engagement.",
    xLabel: "views_delta",
    yLabel: "avg engagement",
    formatY: formatPercent,
    tooltipKeys: ["videos_tracked", "new_videos", "top_video_title", "top_video_views_delta"],
    pointLabelKeys: ["channel_name"],
    maxPointLabels: 5
  });

  const formatRows = [
    { label: "shorts", value: sumRows(channels, "shorts_count") },
    { label: "mid", value: sumRows(channels, "mid_count") },
    { label: "long", value: sumRows(channels, "long_count") }
  ];
  renderHorizontalBars(container.querySelector("#channels-format-mix"), formatRows, {
    labelKey: "label",
    valueKey: "value",
    title: "Tracked format mix",
    subtitle: "Checks whether channel comparisons are driven by format mix.",
  });
}

function renderScoreCharts(container, scores) {
  if (!container) return;
  container.innerHTML = `
    <div id="scores-opportunity-map"></div>
    <div id="scores-anomaly-bars"></div>
  `;

  renderScatterPlot(container.querySelector("#scores-opportunity-map"), scores, {
    xKey: "alpha_score",
    yKey: "opportunity_score",
    sizeKey: "anomaly_score",
    colorKey: "channel_name",
    labelKey: "title",
    title: "Alpha vs opportunity",
    subtitle: "Upper-right videos are strong candidates for deeper review.",
    xLabel: "alpha_score",
    yLabel: "opportunity_score",
    tooltipKeys: ["channel_name", "views_delta", "engagement_rate", "relative_growth_percentile"],
    pointLabelKeys: ["title"],
    maxPointLabels: 5
  });

  const anomalyRows = sortRows(scores, "anomaly_score", "desc").slice(0, 7);
  renderHorizontalBars(container.querySelector("#scores-anomaly-bars"), anomalyRows, {
    labelKey: "title",
    valueKey: "anomaly_score",
    title: "Highest anomaly scores",
    subtitle: "Use this as a QA list before acting on surprising score spikes."
  });
}

function renderAdvancedCharts(container, advanced) {
  if (!container) return;
  container.innerHTML = `
    <div id="advanced-confidence-map"></div>
    <div id="advanced-horizon-bars"></div>
  `;

  const enriched = advanced.map((row) => ({
    ...row,
    best_success_score: Math.max(
      asNumber(row.short_term_success_score),
      asNumber(row.mid_term_success_score),
      asNumber(row.long_term_success_score),
      asNumber(row.overall_success_score)
    )
  }));
  renderScatterPlot(container.querySelector("#advanced-confidence-map"), enriched, {
    xKey: "metric_confidence_score",
    yKey: "best_success_score",
    sizeKey: "trend_burst_score",
    colorKey: "success_horizon_label",
    labelKey: "title",
    title: "Success score vs confidence",
    subtitle: "Prioritize high-success videos that also have reliable measurement.",
    xLabel: "metric confidence",
    yLabel: "best success score",
    tooltipKeys: ["channel_name", "views_delta", "duration_bucket", "evergreen_score", "packaging_problem_score"],
    pointLabelKeys: ["title"],
    maxPointLabels: 5
  });

  const horizonRows = groupCountRows(advanced, "success_horizon_label", "horizon", "count").slice(0, 7);
  renderHorizontalBars(container.querySelector("#advanced-horizon-bars"), horizonRows, {
    labelKey: "horizon",
    valueKey: "count",
    title: "Success horizon distribution",
    subtitle: "Shows whether current opportunities are near-term or compounding bets."
  });
}

function renderTitleCharts(container, titles) {
  if (!container) return;
  container.innerHTML = `
    <div id="titles-pattern-bars"></div>
    <div id="titles-length-map"></div>
  `;

  const featureRows = [
    { label: "number", value: countTruthyRows(titles, "has_number") },
    { label: "question", value: countTruthyRows(titles, "has_question") },
    { label: "colon", value: countTruthyRows(titles, "has_colon") },
    { label: "promise", value: countTruthyRows(titles, "has_promise_word") },
    { label: "urgency", value: countTruthyRows(titles, "has_urgency_word") },
    { label: "AI word", value: countTruthyRows(titles, "has_ai_word") },
    { label: "finance word", value: countTruthyRows(titles, "has_finance_word") }
  ];
  renderHorizontalBars(container.querySelector("#titles-pattern-bars"), featureRows, {
    labelKey: "label",
    valueKey: "value",
    title: "Title signal frequency",
    subtitle: "Keeps copy-pattern interpretation grounded in actual counts."
  });

  renderScatterPlot(container.querySelector("#titles-length-map"), titles, {
    xKey: "title_word_count",
    yKey: "views_delta",
    sizeKey: "engagement_rate",
    colorKey: "channel_name",
    labelKey: "title",
    title: "Title length vs growth",
    subtitle: "Labels only standout titles so the shape stays readable.",
    xLabel: "word count",
    yLabel: "views_delta",
    tooltipKeys: ["channel_name", "has_number", "has_question", "has_ai_word", "engagement_rate"],
    pointLabelKeys: ["title"],
    maxPointLabels: 5
  });
}

function renderDataQualityCharts(container, quality, advanced) {
  if (!container) return;
  container.innerHTML = `
    <div id="dq-confidence-gauge"></div>
    <div id="dq-reason-bars"></div>
  `;

  const avgConfidence = averageRows(advanced, "metric_confidence_score");
  renderGauge(container.querySelector("#dq-confidence-gauge"), {
    title: "Average metric confidence",
    value: avgConfidence,
    max: 100,
    label: `${formatNumber(advanced.filter((row) => asNumber(row.metric_confidence_score) < 50).length)} low-confidence rows`,
    subtitle: "Flags whether the current run is strong enough for decisions."
  });

  const reasonRows = groupCountRows(quality, "confidence_reason", "reason", "count").slice(0, 7);
  renderHorizontalBars(container.querySelector("#dq-reason-bars"), reasonRows, {
    labelKey: "reason",
    valueKey: "count",
    title: "Confidence blockers",
    subtitle: "Shows why rows are excluded or downgraded."
  });
}

function renderTopicCharts(container, opportunities, metrics, patterns) {
  if (!container) return;
  container.innerHTML = `
    <div id="topics-opportunity-map"></div>
    <div id="topics-pattern-bars"></div>
  `;

  renderScatterPlot(container.querySelector("#topics-opportunity-map"), metrics, {
    xKey: "topic_saturation_score",
    yKey: "topic_velocity_score",
    sizeKey: "topic_opportunity_score",
    colorKey: "top_channel_name",
    labelKey: "topic",
    title: "Topic velocity vs saturation",
    subtitle: "Upper-left tends to mean fast topics with more room to move.",
    xLabel: "saturation",
    yLabel: "velocity",
    tooltipKeys: ["video_count", "channel_count", "avg_views_delta", "top_video_title", "top_channel_name"],
    pointLabelKeys: ["topic"],
    maxPointLabels: 5
  });

  const patternRows = sortRows(patterns, "title_pattern_success_score", "desc").slice(0, 7);
  renderHorizontalBars(container.querySelector("#topics-pattern-bars"), patternRows, {
    labelKey: "title_pattern",
    valueKey: "title_pattern_success_score",
    title: "Best title patterns",
    subtitle: opportunities.length ? "Compared against current topic opportunities." : "Uses historical title-pattern signal only."
  });
}

function renderNlpCharts(container, videos) {
  if (!container) return;
  container.innerHTML = `
    <div id="nlp-clusters-bars"></div>
    <div id="nlp-semantic-map"></div>
  `;

  const byCluster = Object.values(videos.reduce((acc, row) => {
    const key = row.semantic_cluster_label || "unknown";
    if (!acc[key]) acc[key] = { label: key, views_delta: 0 };
    acc[key].views_delta += asNumber(row.views_delta);
    return acc;
  }, {})).sort((a, b) => b.views_delta - a.views_delta).slice(0, 10);
  renderHorizontalBars(container.querySelector("#nlp-clusters-bars"), byCluster, {
    labelKey: "label",
    valueKey: "views_delta",
    title: "Top semantic clusters by views_delta",
    subtitle: "Capped width keeps this readable without stretching across the page."
  });

  renderScatterPlot(container.querySelector("#nlp-semantic-map"), videos, {
    xKey: "ai_semantic_score",
    yKey: "views_delta",
    sizeKey: "dominant_semantic_score",
    colorKey: "semantic_cluster_label",
    labelKey: "title",
    title: "AI semantic signal vs growth",
    subtitle: "Tests whether a theme score is associated with actual traction.",
    xLabel: "AI semantic score",
    yLabel: "views_delta",
    tooltipKeys: ["channel_name", "semantic_cluster_label", "finance_semantic_score", "productivity_semantic_score"],
    pointLabelKeys: ["title"],
    maxPointLabels: 5
  });
}

function renderCreativeCharts(container, packages, originality) {
  if (!container) return;
  container.innerHTML = `
    <div id="creative-execution-map"></div>
    <div id="creative-risk-bars"></div>
  `;

  renderScatterPlot(container.querySelector("#creative-execution-map"), packages, {
    xKey: "originality_score",
    yKey: "creative_execution_score",
    sizeKey: "confidence_score",
    colorKey: "package_type",
    labelKey: "topic",
    title: "Originality vs execution",
    subtitle: "Prioritize ideas with high execution value and low copy risk.",
    xLabel: "originality",
    yLabel: "execution score",
    tooltipKeys: ["source_channel_name", "source_title", "package_type", "recommended_timeframe", "copy_risk_score"],
    pointLabelKeys: ["topic", "source_title"],
    maxPointLabels: 5
  });

  const riskRows = sortRows(originality, "copy_risk_score", "desc").slice(0, 7).map((row) => ({
    ...row,
    label: row.candidate_type || row.creative_package_id || "--"
  }));
  renderHorizontalBars(container.querySelector("#creative-risk-bars"), riskRows, {
    labelKey: "label",
    valueKey: "copy_risk_score",
    title: "Highest copy-risk checks",
    subtitle: "Review these before using titles, hooks, or thumbnails."
  });
}

function renderBriefScorecard(container, keyMetrics) {
  if (!container) return;
  const cards = [
    ["videos_total", keyMetrics.videos_total ?? "--"],
    ["total_views_delta", formatNumber(keyMetrics.total_views_delta)],
    ["avg_engagement_rate", formatPercent(keyMetrics.avg_engagement_rate)],
    ["total_alerts", keyMetrics.total_alerts ?? "--"],
    ["action_candidates", keyMetrics.total_action_candidates ?? "--"],
    ["high_priority_actions", keyMetrics.high_priority_actions ?? "--"]
  ];
  container.innerHTML = cards
    .map(([label, value]) => `<article class="kpi-card"><h3>${escapeHtml(label)}</h3><p>${escapeHtml(String(value))}</p></article>`)
    .join("");
}

function renderBriefCharts(container, keyMetrics, actions, content, alerts) {
  if (!container) return;
  container.innerHTML = `
    <div id="brief-action-matrix"></div>
    <div id="brief-content-bubbles"></div>
    <div id="brief-alert-funnel"></div>
  `;

  const actionRows = actions.map((row) => ({
    ...row,
    confidence_score: asNumber(row.metric_confidence_score) || confidenceScore(row.confidence_level),
    impact_score: asNumber(row.expected_value_score) || asNumber(row.decision_score)
  }));
  renderScatterPlot(container.querySelector("#brief-action-matrix"), actionRows, {
    xKey: "confidence_score",
    yKey: "decision_score",
    sizeKey: "impact_score",
    colorKey: "action_type",
    labelKey: "recommended_action",
    title: "Action priority vs confidence",
    subtitle: "Upper-right actions are stronger bets; bubble size estimates expected value.",
    xLabel: "confidence",
    yLabel: "decision_score",
    tooltipKeys: ["priority", "action_type", "reason", "expected_value_score"],
    pointLabelKeys: ["recommended_action", "action_type"],
    maxPointLabels: 4
  });

  const opportunityRows = content.map((row) => ({
    ...row,
    urgency_score: timeframeScore(row.recommended_timeframe),
    impact_score: asNumber(row.evidence_score)
  }));
  renderScatterPlot(container.querySelector("#brief-content-bubbles"), opportunityRows, {
    xKey: "evidence_score",
    yKey: "urgency_score",
    sizeKey: "impact_score",
    colorKey: "recommended_timeframe",
    labelKey: "source_title",
    title: "Content opportunity bubbles",
    subtitle: "Evidence on X, urgency on Y, size by impact signal.",
    xLabel: "evidence_score",
    yLabel: "timeframe urgency",
    tooltipKeys: ["content_strategy", "recommended_timeframe", "why_it_matters"],
    pointLabelKeys: ["source_title", "content_strategy"],
    maxPointLabels: 4
  });

  const actionTotal = asNumber(keyMetrics.total_action_candidates) || actions.length;
  renderFunnel(container.querySelector("#brief-alert-funnel"), [
    { label: "Signals", value: tableRows("latestSignalCandidates").length, detail: "raw candidates" },
    { label: "Alerts", value: alerts.length, detail: "brief watch items" },
    { label: "Actions", value: actionTotal, detail: "decision candidates" },
    { label: "Top actions", value: actions.length, detail: "shown this week" }
  ], {
    title: "Weekly signal flow",
    subtitle: "Connects monitoring noise to concrete recommended actions."
  });
}

function renderModelLeaderboardCharts(container, leaderboard) {
  if (!container) return;
  container.innerHTML = `<div id="models-champion-bars"></div>`;
  const championRows = leaderboard
    .filter((row) => isTruthy(row.selected_as_champion))
    .concat(leaderboard.filter((row) => !leaderboard.some((candidate) => isTruthy(candidate.selected_as_champion))))
    .map((row) => ({
      ...row,
      target_label: `${row.target || "--"} / ${row.model_family || "--"}`
    }));
  renderHorizontalBars(container.querySelector("#models-champion-bars"), championRows, {
    labelKey: "target_label",
    valueKey: "champion_metric_value",
    title: "Champion metric by target"
  });
}

function renderModelReadinessCharts(container, readiness, gap, targetCoverageRows) {
  if (!container) return;
  container.innerHTML = `
    <div id="models-readiness-gauge"></div>
    <div id="models-gap-burn"></div>
    <div id="models-coverage-bars"></div>
  `;

  renderGauge(container.querySelector("#models-readiness-gauge"), {
    title: "Readiness gauge",
    value: readinessPercent(readiness, gap),
    max: 100,
    label: readiness.recommended_status || readiness.status || "unknown",
    subtitle: "Progress toward baseline-ready training volume."
  });

  const trainable = asNumber(readiness.trainable_examples ?? gap.current_trainable_examples);
  const exploratoryNeed = trainable + asNumber(readiness.examples_missing_for_exploratory ?? gap.examples_missing_for_exploratory);
  const baselineNeed = trainable + asNumber(readiness.examples_missing_for_baseline ?? gap.examples_missing_for_baseline);
  renderFunnel(container.querySelector("#models-gap-burn"), [
    { label: "Trainable now", value: trainable, detail: "current examples" },
    { label: "Exploratory target", value: exploratoryNeed || trainable, detail: "minimum useful signal" },
    { label: "Baseline target", value: baselineNeed || trainable, detail: "stable baseline goal" }
  ], {
    title: "Training gap burn-down",
    subtitle: "Shows how many examples are still needed for stronger ML use."
  });

  const coverageRows = targetCoverageRows.map((row) => ({
    ...row,
    target_label: row.target_name || row.target || "--",
    coverage_pct: asNumber(row.coverage_pct)
  }));
  renderHorizontalBars(container.querySelector("#models-coverage-bars"), coverageRows, {
    labelKey: "target_label",
    valueKey: "coverage_pct",
    title: "Coverage by target"
  });
}

function renderContentDriverCharts(container, leaderboard, importance, directions, groups) {
  if (!container) return;
  container.innerHTML = `
    <div id="cd-group-heatmap"></div>
    <div id="cd-top-features"></div>
    <div id="cd-direction-map"></div>
    <div id="cd-model-bars"></div>
  `;

  renderHeatmap(container.querySelector("#cd-group-heatmap"), groups, {
    xKey: "feature_group",
    yKey: "target",
    valueKey: "group_importance",
    title: "Target x feature-group heatmap",
    subtitle: "Predictive importance by target. This is not causal evidence."
  });

  const topFeatures = sortRows(importance, "importance_rank", "asc").slice(0, 18).map((row) => ({
    ...row,
    feature_label: `${row.target || "--"} / ${row.feature || "--"}`,
    importance_magnitude: Math.abs(asNumber(row.importance_value))
  }));
  renderHorizontalBars(container.querySelector("#cd-top-features"), topFeatures, {
    labelKey: "feature_label",
    valueKey: "importance_magnitude",
    title: "Top predictive features"
  });

  const directionRows = directions.map((row) => ({
    ...row,
    direction_score_abs: Math.abs(asNumber(row.direction_score))
  }));
  renderScatterPlot(container.querySelector("#cd-direction-map"), directionRows, {
    xKey: "low_bin_prediction",
    yKey: "high_bin_prediction",
    sizeKey: "direction_score_abs",
    colorKey: "direction",
    labelKey: "feature",
    title: "Driver direction map",
    subtitle: "Compares low-bin and high-bin predictions; direction remains predictive.",
    xLabel: "low bin prediction",
    yLabel: "high bin prediction",
    tooltipKeys: ["target", "model_family", "feature_group", "direction_score", "direction_method"],
    pointLabelKeys: ["feature"],
    maxPointLabels: 5
  });

  const modelRows = leaderboard.map((row) => ({
    ...row,
    model_label: `${row.target || "--"} / ${row.model_family || "--"}`
  }));
  renderHorizontalBars(container.querySelector("#cd-model-bars"), modelRows, {
    labelKey: "model_label",
    valueKey: "spearman_corr",
    title: "Model quality by target"
  });
}

function renderOperationsCharts(container, processes, summary) {
  if (!container) return;
  container.innerHTML = `
    <div id="operations-health-gauge"></div>
    <div id="operations-status-bars"></div>
    <div id="operations-domain-bars"></div>
    <div id="operations-age-bars"></div>
  `;

  const total = processes.length || asNumber(summary.processes_total);
  const healthy = asNumber(summary.healthy_count ?? countByStatus(processes, "success"));
  renderGauge(container.querySelector("#operations-health-gauge"), {
    title: "Operations health",
    value: total ? (healthy / total) * 100 : 0,
    max: 100,
    label: `${formatNumber(healthy)} / ${formatNumber(total)} healthy`,
    subtitle: "Durable process telemetry status."
  });

  const statusCounts = summary.status_counts && typeof summary.status_counts === "object"
    ? summary.status_counts
    : processes.reduce((acc, process) => {
        const key = process.status || "unknown";
        acc[key] = (acc[key] || 0) + 1;
        return acc;
      }, {});
  const statusRows = Object.entries(statusCounts)
    .map(([status, count]) => ({ status, count: asNumber(count) }))
    .sort((a, b) => b.count - a.count);
  renderHorizontalBars(container.querySelector("#operations-status-bars"), statusRows, {
    labelKey: "status",
    valueKey: "count",
    title: "Process status mix",
    subtitle: "Normalized states across configured workflows and CLI jobs."
  });

  const domainRows = groupCountRows(processes, "domain", "domain", "count").slice(0, 10);
  renderHorizontalBars(container.querySelector("#operations-domain-bars"), domainRows, {
    labelKey: "domain",
    valueKey: "count",
    title: "Processes by domain",
    subtitle: "Operational ownership surface."
  });

  const ageRows = processes
    .filter((process) => process.age_hours !== null && process.age_hours !== undefined)
    .map((process) => ({ label: process.process_id, age_hours: asNumber(process.age_hours) }))
    .sort((a, b) => b.age_hours - a.age_hours)
    .slice(0, 10);
  renderHorizontalBars(container.querySelector("#operations-age-bars"), ageRows, {
    labelKey: "label",
    valueKey: "age_hours",
    title: "Oldest durable artifacts",
    subtitle: "A quick stale-risk scan by process age.",
    formatValue: (value) => `${formatNumber(value)}h`
  });
}

function operationProcessRows(processes) {
  return processes.map((process) => ({
    process_id: process.process_id || "",
    name: process.name || "",
    domain: process.domain || "",
    process_type: process.process_type || "",
    cadence: process.cadence || "",
    status: process.status || "unknown",
    base_status: process.base_status || "",
    last_run_at: process.last_run_at || "",
    age_hours: process.age_hours ?? "",
    sla_hours: process.sla_hours ?? "",
    dashboard_tabs: joinList(process.dashboard_tabs),
    depends_on: joinList(process.depends_on),
    status_detail: process.status_detail || ""
  }));
}

function operationArtifactRows(process) {
  const artifacts = Array.isArray(process.artifacts) ? process.artifacts : [];
  return artifacts.map((artifact) => ({
    process_id: process.process_id || "",
    artifact_path: artifact.resolved_path || artifact.path || "",
    exists: String(Boolean(artifact.exists)),
    required: String(Boolean(artifact.required)),
    status: artifact.status || "unknown",
    observed_at: artifact.observed_at_iso || "",
    age_hours: artifact.age_hours ?? "",
    warnings_count: artifact.warnings_count ?? 0,
    errors_count: artifact.errors_count ?? 0,
    failure_count: artifact.failure_count ?? 0
  }));
}

function uniqueValues(rows, key) {
  return [...new Set(rows.map((row) => row?.[key]).filter(Boolean))].sort();
}

function uniqueTabValues(processes) {
  return [...new Set(processes.flatMap((process) => Array.isArray(process.dashboard_tabs) ? process.dashboard_tabs : []))]
    .filter(Boolean)
    .sort();
}

function selectOptions(values) {
  return values.map((value) => `<option value="${escapeHtml(String(value))}">${escapeHtml(String(value))}</option>`).join("");
}

function joinList(value) {
  return Array.isArray(value) ? value.join(", ") : "";
}

function countByStatus(processes, status) {
  return processes.filter((process) => process.status === status).length;
}

function sumRows(rows, key) {
  return rows.reduce((total, row) => total + asNumber(row?.[key]), 0);
}

function averageRows(rows, key) {
  const values = rows.map((row) => asNumber(row?.[key])).filter((value) => Number.isFinite(value));
  if (!values.length) return 0;
  return values.reduce((total, value) => total + value, 0) / values.length;
}

function countTruthyRows(rows, key) {
  return rows.filter((row) => isTruthy(row?.[key])).length;
}

function groupCountRows(rows, sourceKey, labelKey, valueKey) {
  const counts = rows.reduce((acc, row) => {
    const label = String(row?.[sourceKey] || "unknown");
    acc[label] = (acc[label] || 0) + 1;
    return acc;
  }, {});
  return Object.entries(counts)
    .map(([label, count]) => ({ [labelKey]: label, [valueKey]: count }))
    .sort((a, b) => asNumber(b[valueKey]) - asNumber(a[valueKey]));
}

function getOpportunityMatrixRows() {
  const brief = state.data.latestWeeklyBriefJson;
  return Array.isArray(brief?.opportunity_matrix) ? brief.opportunity_matrix : [];
}

function buildSignalFunnelSteps() {
  const signalCandidates = tableRows("latestSignalCandidates");
  const triggered = signalCandidates.filter((row) => isTruthy(row.triggered)).length;
  const alertPayload = state.data.latestAlerts || {};
  const alerts = Array.isArray(alertPayload.alerts) ? alertPayload.alerts.length : asNumber(alertPayload.alert_count);
  const brief = state.data.latestWeeklyBriefJson || {};
  const actionTotal = asNumber(brief?.key_metrics?.total_action_candidates) || (Array.isArray(brief?.top_actions_this_week) ? brief.top_actions_this_week.length : 0);
  return [
    { label: "Candidates", value: signalCandidates.length, detail: "scored signals" },
    { label: "Triggered", value: triggered, detail: "above threshold" },
    { label: "Alerts", value: alerts, detail: "surfaced risks" },
    { label: "Actions", value: actionTotal, detail: "recommended next moves" }
  ];
}

function confidenceScore(value) {
  const normalized = String(value || "").toLowerCase();
  if (normalized === "high") return 90;
  if (normalized === "medium") return 60;
  if (normalized === "low") return 30;
  return 45;
}

function timeframeScore(value) {
  const normalized = String(value || "").toLowerCase();
  const mapping = {
    now: 100,
    next_3_days: 92,
    this_week: 78,
    next_run: 66,
    next_2_weeks: 54,
    this_month: 42,
    backlog: 25
  };
  return mapping[normalized] ?? 45;
}

function readinessPercent(readiness, gap) {
  const trainable = asNumber(readiness.trainable_examples ?? gap.current_trainable_examples);
  const baseline = asNumber(readiness.min_trainable_examples_baseline ?? gap.needed_for_baseline);
  const exploratory = asNumber(readiness.min_trainable_examples_exploratory ?? gap.needed_for_exploratory);
  const target = baseline || exploratory || (readiness.can_train_now ? trainable : 100);
  if (!target) return readiness.can_train_now ? 100 : 0;
  return Math.min(100, (trainable / target) * 100);
}

function isTruthy(value) {
  if (value === true) return true;
  if (typeof value === "number") return value !== 0;
  return ["true", "1", "yes"].includes(String(value || "").toLowerCase());
}

function setGeneratedAt(value) {
  const element = document.querySelector("#generated-at");
  if (element) element.textContent = `Generated: ${formatDate(value)}`;
}

function setDataStatus(warnings, notices) {
  const statusNode = document.querySelector("#data-status");
  const warningCount = Array.isArray(warnings) ? warnings.length : 0;
  const noticeCount = Array.isArray(notices) ? notices.length : 0;
  const value = warningCount > 0 ? "warning" : noticeCount > 0 ? "ready_with_notices" : "ready";
  if (statusNode) statusNode.textContent = `Data status: ${value}`;
}

function setDomainStatus(domain, block) {
  const mapping = {
    operational_data_status: "#operational-data-status",
    ml_data_status: "#ml-data-status"
  };
  const label = domain === "operational_data_status" ? "Operational" : "ML";
  const element = document.querySelector(mapping[domain]);
  if (!element) return;
  if (!block || typeof block !== "object") {
    element.textContent = `${label}: no metadata`;
    return;
  }
  const state = block.state || "unknown";
  const message = block.message || "Estado desconocido";
  element.textContent = `${label}: ${state} · ${message}`;
}

function pushWarning(message) {
  const container = document.querySelector("#warnings");
  if (!container) return;
  const div = document.createElement("div");
  div.className = "warning";
  div.textContent = message;
  container.append(div);
}

function pushNotice(message) {
  const container = document.querySelector("#notices");
  if (!container) return;
  const div = document.createElement("div");
  div.className = "warning";
  div.textContent = `Info: ${message}`;
  container.append(div);
}

function capitalize(value) {
  return value.slice(0, 1).toUpperCase() + value.slice(1);
}
