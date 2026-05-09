import { escapeHtml, formatNumber } from "./formatters.js";

const DEFAULT_PAGE_SIZE = 50;
const MAX_PAGE_SIZE = 50;
const PAGE_SIZE_OPTIONS = [10, 25, 50];

export function sortRows(rows, key, direction = "desc") {
  const mult = direction === "asc" ? 1 : -1;
  return [...rows].sort((a, b) => {
    const left = a?.[key];
    const right = b?.[key];
    if (typeof left === "number" && typeof right === "number") return (left - right) * mult;
    return String(left ?? "").localeCompare(String(right ?? "")) * mult;
  });
}

function normalizePageSize(value, fallback = DEFAULT_PAGE_SIZE) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric <= 0) return fallback;
  return Math.min(Math.max(1, Math.trunc(numeric)), MAX_PAGE_SIZE);
}

function pageSizeOptions(currentPageSize) {
  return [...new Set([...PAGE_SIZE_OPTIONS, currentPageSize])]
    .filter((value) => value > 0 && value <= MAX_PAGE_SIZE)
    .sort((a, b) => a - b);
}

export function renderTable(
  container,
  columns,
  rows,
  {
    initialSortKey = "",
    title = "Table",
    pageSize,
    initialLimit,
    showPagination = true,
    emptyLabel = "No rows"
  } = {}
) {
  if (!container) return;

  let currentKey = initialSortKey || columns[0] || "";
  let direction = "desc";
  let currentPage = 1;
  let currentPageSize = normalizePageSize(pageSize ?? initialLimit ?? DEFAULT_PAGE_SIZE);

  const draw = () => {
    const sorted = currentKey ? sortRows(rows, currentKey, direction) : [...rows];
    const totalRows = sorted.length;
    const totalPages = Math.max(1, Math.ceil(totalRows / currentPageSize));
    currentPage = Math.min(Math.max(1, currentPage), totalPages);
    const pageStart = (currentPage - 1) * currentPageSize;
    const pageEnd = pageStart + currentPageSize;
    const pageRows = sorted.slice(pageStart, pageEnd);
    const firstVisible = totalRows === 0 ? 0 : pageStart + 1;
    const lastVisible = Math.min(pageEnd, totalRows);

    const head = columns
      .map((column) => {
        const isSorted = currentKey === column;
        const sortLabel = isSorted ? (direction === "asc" ? " ▲" : " ▼") : "";
        const ariaSort = isSorted ? (direction === "asc" ? "ascending" : "descending") : "none";
        return `<th data-col="${escapeHtml(column)}" aria-sort="${ariaSort}">${escapeHtml(column)}${sortLabel}</th>`;
      })
      .join("");

    const body = pageRows
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

    const controls = showPagination ? `
      <div class="table-controls">
        <button type="button" data-action="prev" ${currentPage <= 1 ? "disabled" : ""}>Anterior</button>
        <span class="table-count">${formatNumber(firstVisible)}-${formatNumber(lastVisible)} de ${formatNumber(totalRows)} filas · Página ${formatNumber(currentPage)} de ${formatNumber(totalPages)}</span>
        <label>
          Filas por página
          <select data-action="page-size">
            ${pageSizeOptions(currentPageSize)
              .map((value) => `<option value="${value}" ${value === currentPageSize ? "selected" : ""}>${value}</option>`)
              .join("")}
          </select>
        </label>
        <button type="button" data-action="next" ${currentPage >= totalPages ? "disabled" : ""}>Siguiente</button>
      </div>
    ` : "";

    container.innerHTML = `
      <h3 class="section-title">${escapeHtml(title)}</h3>
      <div class="table-wrap">
        <table>
          <thead><tr>${head}</tr></thead>
          <tbody>${body || `<tr><td colspan="${Math.max(columns.length, 1)}">${escapeHtml(emptyLabel)}</td></tr>`}</tbody>
        </table>
      </div>
      ${controls}
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
        currentPage = 1;
        draw();
      });
    });

    container.querySelector('[data-action="prev"]')?.addEventListener("click", () => {
      currentPage = Math.max(1, currentPage - 1);
      draw();
    });
    container.querySelector('[data-action="next"]')?.addEventListener("click", () => {
      currentPage = Math.min(totalPages, currentPage + 1);
      draw();
    });
    container.querySelector('[data-action="page-size"]')?.addEventListener("change", (event) => {
      currentPageSize = normalizePageSize(event.target?.value, currentPageSize);
      currentPage = 1;
      draw();
    });
  };

  draw();
}
