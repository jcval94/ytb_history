import { asNumber, escapeHtml, formatNumber, formatPercent } from "./formatters.js";

const PALETTE = ["#2563eb", "#0891b2", "#16a34a", "#f97316", "#dc2626", "#7c3aed", "#0f766e", "#be123c"];
const NEUTRAL_POINT_COLOR = "#2563eb";

export function renderHorizontalBars(container, rows, { labelKey, valueKey, title, subtitle = "", formatValue = formatNumber }) {
  if (!container) return;
  const max = Math.max(...rows.map((row) => asNumber(row?.[valueKey])), 1);
  const useDistinctColors = shouldUseDistinctColors(rows.length);
  const items = rows
    .map((row, index) => {
      const label = escapeHtml(row?.[labelKey] ?? "--");
      const raw = asNumber(row?.[valueKey]);
      const pct = Math.max(0, Math.min(100, (raw / max) * 100));
      const color = useDistinctColors ? PALETTE[index % PALETTE.length] : "";
      const style = color ? `width:${pct}%;background:${color};` : `width:${pct}%;`;
      const tooltip = `${row?.[labelKey] ?? "--"}: ${formatValue(raw)}`;
      return `
        <div class="bar-row" title="${escapeHtml(tooltip)}">
          <span title="${label}">${label}</span>
          <div class="progress"><span style="${style}"></span></div>
          <strong>${formatValue(raw)}</strong>
        </div>
      `;
    })
    .join("");
  container.innerHTML = chartShell(title, items || emptyChart("No data"), subtitle);
}

export function renderScatterPlot(
  container,
  rows,
  {
    xKey,
    yKey,
    labelKey,
    colorKey,
    sizeKey,
    title,
    subtitle = "",
    xLabel = "",
    yLabel = "",
    formatX = formatNumber,
    formatY = formatNumber,
    maxItems = 80,
    tooltipKeys = [],
    pointLabelKeys = [],
    maxPointLabels = 5,
    labelImportantPoints = true
  }
) {
  if (!container) return;
  const parsed = rows
    .filter((row) => row && Object.prototype.hasOwnProperty.call(row, xKey) && Object.prototype.hasOwnProperty.call(row, yKey))
    .map((row) => ({
      row,
      x: asNumber(row[xKey]),
      y: asNumber(row[yKey]),
      size: sizeKey ? Math.max(0, asNumber(row[sizeKey])) : 1,
      category: String(colorKey ? row[colorKey] || "--" : "all")
    }))
    .filter((point) => Number.isFinite(point.x) && Number.isFinite(point.y))
    .sort((a, b) => b.size - a.size)
    .slice(0, maxItems);

  if (!parsed.length) {
    container.innerHTML = chartShell(title, emptyChart("No plottable data"), subtitle);
    return;
  }

  const width = 760;
  const height = 390;
  const pad = { top: 24, right: 28, bottom: 58, left: 74 };
  const plotW = width - pad.left - pad.right;
  const plotH = height - pad.top - pad.bottom;
  const xDomain = paddedDomain(parsed.map((point) => point.x));
  const yDomain = paddedDomain(parsed.map((point) => point.y));
  const sizeDomain = paddedDomain(parsed.map((point) => point.size));
  const categories = [...new Set(parsed.map((point) => point.category))];
  const useDistinctColors = Boolean(colorKey) && shouldUseDistinctColors(categories.length);
  const colorByCategory = new Map(
    categories.map((category, index) => [category, useDistinctColors ? PALETTE[index % PALETTE.length] : NEUTRAL_POINT_COLOR])
  );
  const xTicks = makeTicks(xDomain);
  const yTicks = makeTicks(yDomain);

  const grid = [
    ...xTicks.map((tick) => {
      const x = scale(tick, xDomain, [pad.left, pad.left + plotW]);
      return `<line class="chart-grid-line" x1="${x}" y1="${pad.top}" x2="${x}" y2="${pad.top + plotH}"></line>`;
    }),
    ...yTicks.map((tick) => {
      const y = scale(tick, yDomain, [pad.top + plotH, pad.top]);
      return `<line class="chart-grid-line" x1="${pad.left}" y1="${y}" x2="${pad.left + plotW}" y2="${y}"></line>`;
    })
  ].join("");

  const tickLabels = [
    ...xTicks.map((tick) => {
      const x = scale(tick, xDomain, [pad.left, pad.left + plotW]);
      return `<text class="chart-axis-label" x="${x}" y="${height - 24}" text-anchor="middle">${escapeHtml(formatX(tick))}</text>`;
    }),
    ...yTicks.map((tick) => {
      const y = scale(tick, yDomain, [pad.top + plotH, pad.top]);
      return `<text class="chart-axis-label" x="${pad.left - 12}" y="${y + 4}" text-anchor="end">${escapeHtml(formatY(tick))}</text>`;
    })
  ].join("");

  const plotted = parsed.map((point) => {
    const x = scale(point.x, xDomain, [pad.left, pad.left + plotW]);
    const y = scale(point.y, yDomain, [pad.top + plotH, pad.top]);
    const radius = sizeKey ? scale(point.size, sizeDomain, [5, 17]) : 7;
    return {
      ...point,
      xPlot: x,
      yPlot: y,
      radius,
      color: colorByCategory.get(point.category) || NEUTRAL_POINT_COLOR,
      tooltip: buildScatterTooltip(point, { xKey, yKey, xLabel, yLabel, sizeKey, colorKey, labelKey, tooltipKeys, formatX, formatY })
    };
  });

  const points = plotted
    .map((point) => {
      const radius = sizeKey ? scale(point.size, sizeDomain, [5, 17]) : 7;
      return `<circle class="chart-point" cx="${point.xPlot}" cy="${point.yPlot}" r="${radius}" fill="${point.color}"><title>${escapeHtml(point.tooltip)}</title></circle>`;
    })
    .join("");

  const pointLabels = labelImportantPoints
    ? renderImportantScatterLabels(selectImportantScatterLabels(plotted, maxPointLabels), { labelKey, pointLabelKeys, pad, plotW, plotH })
    : "";
  const legend = useDistinctColors && categories.length > 1 ? renderLegend(categories, colorByCategory) : "";
  const svg = `
    <svg class="chart-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="${escapeHtml(title)}">
      ${grid}
      <line class="chart-axis" x1="${pad.left}" y1="${pad.top + plotH}" x2="${pad.left + plotW}" y2="${pad.top + plotH}"></line>
      <line class="chart-axis" x1="${pad.left}" y1="${pad.top}" x2="${pad.left}" y2="${pad.top + plotH}"></line>
      ${tickLabels}
      <text class="chart-axis-title" x="${pad.left + plotW / 2}" y="${height - 4}" text-anchor="middle">${escapeHtml(xLabel || xKey)}</text>
      <text class="chart-axis-title" x="18" y="${pad.top + plotH / 2}" transform="rotate(-90 18 ${pad.top + plotH / 2})" text-anchor="middle">${escapeHtml(yLabel || yKey)}</text>
      ${points}
      ${pointLabels}
    </svg>
  `;
  container.innerHTML = chartShell(title, svg + legend, subtitle);
}

export function renderLineChart(
  container,
  rows,
  { xKey, yKey, groupKey, title, subtitle = "", xLabel = "", yLabel = "", maxGroups = 6, formatY = formatNumber }
) {
  if (!container) return;
  const records = rows
    .filter((row) => row && row[xKey] !== undefined && row[groupKey] !== undefined)
    .map((row) => ({
      x: String(row[xKey]),
      y: asNumber(row[yKey]),
      group: String(row[groupKey] || "--")
    }));
  if (!records.length) {
    container.innerHTML = chartShell(title, emptyChart("No time-series data"), subtitle);
    return;
  }

  const totals = new Map();
  records.forEach((record) => totals.set(record.group, (totals.get(record.group) || 0) + record.y));
  const groups = [...totals.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, maxGroups)
    .map(([group]) => group);
  const useDistinctColors = shouldUseDistinctColors(groups.length);
  const selected = records.filter((record) => groups.includes(record.group));
  const xLabels = [...new Set(selected.map((record) => record.x))].sort();
  const valueMap = new Map();
  selected.forEach((record) => {
    const key = `${record.group}\u0000${record.x}`;
    valueMap.set(key, (valueMap.get(key) || 0) + record.y);
  });

  const values = [...valueMap.values()];
  const yDomain = [Math.min(0, ...values), Math.max(1, ...values)];
  const width = 760;
  const height = 360;
  const pad = { top: 22, right: 28, bottom: 68, left: 76 };
  const plotW = width - pad.left - pad.right;
  const plotH = height - pad.top - pad.bottom;

  const yTicks = makeTicks(yDomain);
  const grid = yTicks
    .map((tick) => {
      const y = scale(tick, yDomain, [pad.top + plotH, pad.top]);
      return `<line class="chart-grid-line" x1="${pad.left}" y1="${y}" x2="${pad.left + plotW}" y2="${y}"></line>
        <text class="chart-axis-label" x="${pad.left - 12}" y="${y + 4}" text-anchor="end">${escapeHtml(formatY(tick))}</text>`;
    })
    .join("");

  const paths = groups
    .map((group, index) => {
      const color = useDistinctColors ? PALETTE[index % PALETTE.length] : NEUTRAL_POINT_COLOR;
      const points = xLabels.map((xValue, xIndex) => {
        const x = xLabels.length === 1 ? pad.left + plotW / 2 : pad.left + (plotW * xIndex) / (xLabels.length - 1);
        const y = scale(valueMap.get(`${group}\u0000${xValue}`) || 0, yDomain, [pad.top + plotH, pad.top]);
        return { x, y, value: valueMap.get(`${group}\u0000${xValue}`) || 0, xValue };
      });
      const d = points.map((point, pointIndex) => `${pointIndex === 0 ? "M" : "L"} ${point.x} ${point.y}`).join(" ");
      const circles = points
        .map((point) => `<circle class="chart-line-point" cx="${point.x}" cy="${point.y}" r="4" fill="${color}"><title>${escapeHtml(`${group}: ${point.xValue} ${formatY(point.value)}`)}</title></circle>`)
        .join("");
      return `<path class="chart-line" d="${d}" stroke="${color}"></path>${circles}`;
    })
    .join("");

  const xTickStep = Math.max(1, Math.ceil(xLabels.length / 6));
  const xTicks = xLabels
    .filter((_, index) => index % xTickStep === 0 || index === xLabels.length - 1)
    .map((xValue) => {
      const index = xLabels.indexOf(xValue);
      const x = xLabels.length === 1 ? pad.left + plotW / 2 : pad.left + (plotW * index) / (xLabels.length - 1);
      return `<text class="chart-axis-label" x="${x}" y="${height - 30}" text-anchor="middle">${escapeHtml(xValue)}</text>`;
    })
    .join("");

  const legend = useDistinctColors ? renderLegend(groups, new Map(groups.map((group, index) => [group, PALETTE[index % PALETTE.length]]))) : "";
  const svg = `
    <svg class="chart-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="${escapeHtml(title)}">
      ${grid}
      <line class="chart-axis" x1="${pad.left}" y1="${pad.top + plotH}" x2="${pad.left + plotW}" y2="${pad.top + plotH}"></line>
      <line class="chart-axis" x1="${pad.left}" y1="${pad.top}" x2="${pad.left}" y2="${pad.top + plotH}"></line>
      ${paths}
      ${xTicks}
      <text class="chart-axis-title" x="${pad.left + plotW / 2}" y="${height - 6}" text-anchor="middle">${escapeHtml(xLabel || xKey)}</text>
      <text class="chart-axis-title" x="18" y="${pad.top + plotH / 2}" transform="rotate(-90 18 ${pad.top + plotH / 2})" text-anchor="middle">${escapeHtml(yLabel || yKey)}</text>
    </svg>
  `;
  container.innerHTML = chartShell(title, svg + legend, subtitle);
}

export function renderHeatmap(container, rows, { xKey, yKey, valueKey, title, subtitle = "", formatValue = formatNumber }) {
  if (!container) return;
  const valueMap = new Map();
  rows.forEach((row) => {
    const x = String(row?.[xKey] || "--");
    const y = String(row?.[yKey] || "--");
    const value = asNumber(row?.[valueKey]);
    const key = `${y}\u0000${x}`;
    valueMap.set(key, Math.max(valueMap.get(key) || 0, value));
  });
  const xLabels = [...new Set(rows.map((row) => String(row?.[xKey] || "--")))].sort();
  const yLabels = [...new Set(rows.map((row) => String(row?.[yKey] || "--")))].sort();
  if (!xLabels.length || !yLabels.length) {
    container.innerHTML = chartShell(title, emptyChart("No heatmap data"), subtitle);
    return;
  }
  const maxLog = Math.max(...[...valueMap.values()].map((value) => Math.log1p(Math.abs(value))), 1);
  const header = `<div class="heatmap-corner"></div>${xLabels.map((label) => `<div class="heatmap-head">${escapeHtml(label)}</div>`).join("")}`;
  const cells = yLabels
    .map((y) => {
      const rowCells = xLabels
        .map((x) => {
          const value = valueMap.get(`${y}\u0000${x}`) || 0;
          const intensity = Math.max(0.08, Math.log1p(Math.abs(value)) / maxLog);
          const textColor = intensity > 0.58 ? "#ffffff" : "#152238";
          return `<div class="heatmap-cell" style="background:rgba(37,99,235,${intensity});color:${textColor};" title="${escapeHtml(`${y} / ${x}: ${formatValue(value)}`)}">${escapeHtml(formatValue(value))}</div>`;
        })
        .join("");
      return `<div class="heatmap-y">${escapeHtml(y)}</div>${rowCells}`;
    })
    .join("");

  const columns = `minmax(120px,1.2fr) repeat(${xLabels.length}, minmax(86px,1fr))`;
  const body = `<div class="heatmap" style="grid-template-columns:${columns};">${header}${cells}</div>`;
  container.innerHTML = chartShell(title, body, subtitle);
}

export function renderFunnel(container, steps, { title, subtitle = "" }) {
  if (!container) return;
  const max = Math.max(...steps.map((step) => asNumber(step.value)), 1);
  const useDistinctColors = shouldUseDistinctColors(steps.length);
  const body = steps
    .map((step, index) => {
      const value = asNumber(step.value);
      const pct = Math.max(4, Math.min(100, (value / max) * 100));
      const color = useDistinctColors ? PALETTE[index % PALETTE.length] : NEUTRAL_POINT_COLOR;
      const tooltip = `${step.label}: ${formatNumber(value)}${step.detail ? ` (${step.detail})` : ""}`;
      return `
        <div class="funnel-row" title="${escapeHtml(tooltip)}">
          <div class="funnel-label">
            <strong>${escapeHtml(step.label)}</strong>
            <span>${escapeHtml(step.detail || "")}</span>
          </div>
          <div class="funnel-track"><span style="width:${pct}%;background:${color};"></span></div>
          <strong>${formatNumber(value)}</strong>
        </div>
      `;
    })
    .join("");
  container.innerHTML = chartShell(title, body || emptyChart("No funnel data"), subtitle);
}

export function renderGauge(container, { title, value, max = 100, label = "", subtitle = "" }) {
  if (!container) return;
  const pct = max > 0 ? Math.max(0, Math.min(100, (asNumber(value) / max) * 100)) : 0;
  const body = `
    <div class="gauge-wrap">
      <div class="gauge" style="--pct:${pct};">
        <span>${formatNumber(pct)}%</span>
      </div>
      <p>${escapeHtml(label)}</p>
    </div>
  `;
  container.innerHTML = chartShell(title, body, subtitle);
}

function chartShell(title, body, subtitle = "") {
  return `
    <section class="chart-card">
      <header>
        <div>
          <h3 class="section-title">${escapeHtml(title)}</h3>
          ${subtitle ? `<p>${escapeHtml(subtitle)}</p>` : ""}
        </div>
        <button class="chart-play" type="button" data-chart-play aria-label="Play ${escapeHtml(title)} animation">Play</button>
      </header>
      ${body}
    </section>
  `;
}

function emptyChart(message) {
  return `<p class="chart-empty">${escapeHtml(message)}</p>`;
}

function paddedDomain(values) {
  const finite = values.filter((value) => Number.isFinite(value));
  const min = Math.min(...finite, 0);
  const max = Math.max(...finite, 1);
  if (min === max) return [min - 1, max + 1];
  const pad = (max - min) * 0.08;
  return [min - pad, max + pad];
}

function makeTicks([min, max]) {
  const tickCount = 4;
  if (min === max) return [min];
  return Array.from({ length: tickCount + 1 }, (_, index) => min + ((max - min) * index) / tickCount);
}

function shouldUseDistinctColors(itemCount) {
  return itemCount > 1 && itemCount < 8;
}

function buildScatterTooltip(point, { xKey, yKey, xLabel, yLabel, sizeKey, colorKey, labelKey, tooltipKeys, formatX, formatY }) {
  const row = point.row || {};
  const lines = [];
  const primary = row[labelKey] || row.title || row.channel_name || row.topic || row.feature || row.video_id || point.category;
  if (primary) lines.push(String(primary));

  [
    ["channel", row.channel_name],
    ["video_id", row.video_id],
    [xLabel || xKey, formatX(point.x)],
    [yLabel || yKey, formatY(point.y)],
    [sizeKey, sizeKey ? formatNumber(point.size) : ""],
    [colorKey, colorKey ? point.category : ""]
  ].forEach(([key, value]) => {
    if (key && value !== undefined && value !== null && value !== "") lines.push(`${key}: ${value}`);
  });

  tooltipKeys.forEach((key) => {
    if ([xKey, yKey, sizeKey, colorKey, labelKey, "channel_name", "video_id"].includes(key)) return;
    const value = row[key];
    if (value !== undefined && value !== null && value !== "") lines.push(`${key}: ${formatTooltipValue(value)}`);
  });

  return [...new Set(lines)].join("\n");
}

function formatTooltipValue(value) {
  return typeof value === "number" ? formatNumber(value) : String(value);
}

function selectImportantScatterLabels(points, maxPointLabels) {
  if (!points.length || maxPointLabels <= 0) return [];
  const selected = new Set();
  const addTop = (sorter) => {
    const point = [...points].sort(sorter)[0];
    if (point) selected.add(point);
  };

  addTop((a, b) => b.y - a.y);
  addTop((a, b) => b.x - a.x);
  addTop((a, b) => b.size - a.size);

  const xDomain = paddedDomain(points.map((point) => point.x));
  const yDomain = paddedDomain(points.map((point) => point.y));
  const sizeDomain = paddedDomain(points.map((point) => point.size));
  [...points]
    .map((point) => ({
      point,
      score:
        normalize(point.x, xDomain) * 0.36 +
        normalize(point.y, yDomain) * 0.44 +
        normalize(point.size, sizeDomain) * 0.2
    }))
    .sort((a, b) => b.score - a.score)
    .slice(0, maxPointLabels)
    .forEach(({ point }) => selected.add(point));

  return [...selected].slice(0, maxPointLabels);
}

function renderImportantScatterLabels(points, { labelKey, pointLabelKeys, pad, plotW, plotH }) {
  return points
    .map((point, index) => {
      const label = scatterLabelText(point.row, labelKey, pointLabelKeys);
      if (!label) return "";
      const nearRight = point.xPlot > pad.left + plotW * 0.72;
      const x = nearRight ? point.xPlot - point.radius - 7 : point.xPlot + point.radius + 7;
      const yOffset = index % 2 === 0 ? -8 : 12;
      const y = Math.max(pad.top + 12, Math.min(pad.top + plotH - 6, point.yPlot + yOffset));
      const anchor = nearRight ? "end" : "start";
      return `<text class="chart-point-label" x="${x}" y="${y}" text-anchor="${anchor}"><title>${escapeHtml(point.tooltip)}</title>${escapeHtml(label)}</text>`;
    })
    .join("");
}

function scatterLabelText(row, labelKey, pointLabelKeys) {
  const sourceKeys = pointLabelKeys.length ? pointLabelKeys : [labelKey, "title", "channel_name", "topic", "feature", "video_id"];
  const value = sourceKeys.map((key) => row?.[key]).find((item) => item !== undefined && item !== null && item !== "");
  if (!value) return "";
  const text = String(value);
  return text.length > 34 ? `${text.slice(0, 31)}...` : text;
}

function normalize(value, [min, max]) {
  if (max === min) return 0;
  return Math.max(0, Math.min(1, (value - min) / (max - min)));
}

function scale(value, [inMin, inMax], [outMin, outMax]) {
  if (inMax === inMin) return (outMin + outMax) / 2;
  const pct = (value - inMin) / (inMax - inMin);
  return outMin + pct * (outMax - outMin);
}

function renderLegend(labels, colorMap) {
  const items = labels
    .slice(0, 12)
    .map((label) => `<span><i style="background:${colorMap.get(label) || PALETTE[0]};"></i>${escapeHtml(label)}</span>`)
    .join("");
  return `<div class="chart-legend">${items}</div>`;
}
