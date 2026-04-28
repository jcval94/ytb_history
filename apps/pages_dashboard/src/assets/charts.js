import { asNumber, escapeHtml, formatNumber } from "./formatters.js";

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
          <span>${label}</span>
          <div class="progress"><span style="width:${pct}%;"></span></div>
          <strong>${formatNumber(raw)}</strong>
        </div>
      `;
    })
    .join("");
  container.innerHTML = `<h3 class="section-title">${escapeHtml(title)}</h3>${items || "<p>No data</p>"}`;
}
