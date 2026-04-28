export const asNumber = (value) => (typeof value === "number" && Number.isFinite(value) ? value : 0);

export function formatNumber(value) {
  const number = asNumber(value);
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 }).format(number);
}

export function formatPercent(value) {
  const number = asNumber(value);
  return `${(number * 100).toFixed(2)}%`;
}

export function formatDate(value) {
  if (!value) return "--";
  const dt = new Date(value);
  return Number.isNaN(dt.getTime()) ? String(value) : dt.toLocaleString();
}

export function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}
