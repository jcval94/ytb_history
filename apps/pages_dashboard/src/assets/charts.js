import { asNumber, escapeHtml, formatNumber, formatPercent } from "./formatters.js";

const PALETTE = ["#2563eb", "#0891b2", "#16a34a", "#f97316", "#dc2626", "#7c3aed", "#0f766e", "#be123c"];

export function renderHorizontalBars(container, rows, { labelKey, valueKey, title }) {
  if (!container) return;
  const max = Math.max(...rows.map((row) => asNumber(row?.[valueKey])), 1);
  const items = rows
    .map((row) => {
      const label = escapeHtml(row?.[labelKey] ?? "--");
      const raw = asNumber(row?.[valueKey]);
      const pct = Math.max(0, Math.min(100, (raw / max) * 100));
      return `
        <div class="bar-row">
          <span title="${label}">${label}</span>
          <div class="progress"><span style="width:${pct}%;"></span></div>
          <strong>${formatNumber(raw)}</strong>
        </div>
      `;
    })
    .join("");
  container.innerHTML = chartShell(title, items || emptyChart("No data"));
}

export function renderScatterPlot(
  container,
  rows,
  { xKey, yKey, labelKey, colorKey, sizeKey, title, subtitle = "", xLabel = "", yLabel = "", formatX = formatNumber, formatY = formatNumber, maxItems = 80 }
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
  const colorByCategory = new Map(categories.map((category, index) => [category, PALETTE[index % PALETTE.length]]));
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

  const points = parsed
    .map((point) => {
      const x = scale(point.x, xDomain, [pad.left, pad.left + plotW]);
      const y = scale(point.y, yDomain, [pad.top + plotH, pad.top]);
      const radius = sizeKey ? scale(point.size, sizeDomain, [5, 17]) : 7;
      const color = colorByCategory.get(point.category) || PALETTE[0];
      const label = point.row?.[labelKey] || point.category;
      const tooltip = `${label}: ${xLabel || xKey} ${formatX(point.x)}, ${yLabel || yKey} ${formatY(point.y)}`;
      return `<circle class="chart-point" cx="${x}" cy="${y}" r="${radius}" fill="${color}"><title>${escapeHtml(tooltip)}</title></circle>`;
    })
    .join("");

  const legend = categories.length > 1 ? renderLegend(categories, colorByCategory) : "";
  const svg = `
    <svg class="chart-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="${escapeHtml(title)}">
      ${grid}
      <line class="chart-axis" x1="${pad.left}" y1="${pad.top + plotH}" x2="${pad.left + plotW}" y2="${pad.top + plotH}"></line>
      <line class="chart-axis" x1="${pad.left}" y1="${pad.top}" x2="${pad.left}" y2="${pad.top + plotH}"></line>
      ${tickLabels}
      <text class="chart-axis-title" x="${pad.left + plotW / 2}" y="${height - 4}" text-anchor="middle">${escapeHtml(xLabel || xKey)}</text>
      <text class="chart-axis-title" x="18" y="${pad.top + plotH / 2}" transform="rotate(-90 18 ${pad.top + plotH / 2})" text-anchor="middle">${escapeHtml(yLabel || yKey)}</text>
      ${points}
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
      const color = PALETTE[index % PALETTE.length];
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

  const legend = renderLegend(groups, new Map(groups.map((group, index) => [group, PALETTE[index % PALETTE.length]])));
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
  const body = steps
    .map((step, index) => {
      const value = asNumber(step.value);
      const pct = Math.max(4, Math.min(100, (value / max) * 100));
      const color = PALETTE[index % PALETTE.length];
      return `
        <div class="funnel-row">
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
        <h3 class="section-title">${escapeHtml(title)}</h3>
        ${subtitle ? `<p>${escapeHtml(subtitle)}</p>` : ""}
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
