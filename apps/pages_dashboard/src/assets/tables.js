import { escapeHtml, formatNumber } from "./formatters.js";

export function sortRows(rows, key, direction = "desc") {
  const mult = direction === "asc" ? 1 : -1;
  return [...rows].sort((a, b) => {
    const left = a?.[key];
    const right = b?.[key];
    if (typeof left === "number" && typeof right === "number") return (left - right) * mult;
    return String(left ?? "").localeCompare(String(right ?? "")) * mult;
  });
}

export function renderTable(container, columns, rows, { initialSortKey = "", title = "Table" } = {}) {
  if (!container) return;
  let currentKey = initialSortKey || columns[0] || "";
  let direction = "desc";

  const draw = () => {
    const sorted = currentKey ? sortRows(rows, currentKey, direction) : [...rows];
    const head = columns
      .map((column) => `<th data-col="${escapeHtml(column)}">${escapeHtml(column)}</th>`)
      .join("");

    const body = sorted
      .map((row) => {
        const cells = columns
          .map((column) => {
            const value = row?.[column];
            const rendered = typeof value === "number" ? formatNumber(value) : escapeHtml(value ?? "");
            return `<td>${rendered}</td>`;
          })
          .join("");
        return `<tr>${cells}</tr>`;
      })
      .join("");

    container.innerHTML = `
      <h3 class="section-title">${escapeHtml(title)}</h3>
      <div class="table-wrap">
        <table>
          <thead><tr>${head}</tr></thead>
          <tbody>${body || `<tr><td colspan="${columns.length}">No rows</td></tr>`}</tbody>
        </table>
      </div>
    `;

    container.querySelectorAll("th").forEach((th) => {
      th.addEventListener("click", () => {
        const key = th.getAttribute("data-col") || "";
        if (!key) return;
        if (currentKey === key) direction = direction === "asc" ? "desc" : "asc";
        else {
          currentKey = key;
          direction = "desc";
        }
        draw();
      });
    });
  };

  draw();
}
