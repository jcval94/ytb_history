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
  periodMonthlyChannel: "./data/period_monthly_channel_metrics.json"
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

function bindTabs() {
  const tabs = document.querySelector("#tabs");
  if (!tabs) return;
  tabs.querySelectorAll("button").forEach((button) => {
    button.addEventListener("click", () => {
      const tab = button.getAttribute("data-tab");
      if (!tab) return;
      tabs.querySelectorAll("button").forEach((item) => item.classList.remove("active"));
      document.querySelectorAll(".tab-panel").forEach((panel) => panel.classList.remove("active"));
      button.classList.add("active");
      document.querySelector(`#tab-${tab}`)?.classList.add("active");
    });
  });
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
  renderDataQuality(quality, advanced);
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

  panel.innerHTML = '<div id="ov-videos" class="grid-two"></div><div id="ov-channels" class="grid-two"></div>';

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
