"""Build lightweight NLP features from local analytics CSV artifacts."""

from __future__ import annotations

import csv
import json
import math
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SEMANTIC_DICTIONARIES: dict[str, list[str]] = {
    "ai": [
        "ia",
        "ai",
        "inteligencia artificial",
        "chatgpt",
        "gemini",
        "claude",
        "agente",
        "automatizar",
        "herramientas de ia",
        "machine learning",
    ],
    "finance": [
        "dinero",
        "finanzas",
        "inversión",
        "invertir",
        "ahorro",
        "deuda",
        "crédito",
        "banco",
        "ingresos",
        "rendimiento",
    ],
    "productivity": ["productividad", "tiempo", "hábitos", "eficiencia", "organización", "automatización"],
    "urgency": ["hoy", "ahora", "urgente", "nuevo", "última hora", "acaba de", "ya"],
    "warning": ["error", "errores", "cuidado", "peligro", "evita", "nunca", "no hagas", "peor"],
    "promise": ["gana", "ganar", "ahorra", "ahorrar", "mejora", "aprende", "fácil", "rápido"],
    "curiosity": ["nadie", "secreto", "la verdad", "por qué", "lo que no sabes", "sorpresa"],
    "tutorial": ["cómo", "guía", "tutorial", "paso a paso", "aprende"],
    "news": ["noticia", "actualización", "nuevo", "lanzamiento", "acaba de"],
}

VIDEO_OUTPUT_COLUMNS = [
    "execution_date",
    "video_id",
    "channel_id",
    "channel_name",
    "title",
    "description",
    "title_length_chars",
    "title_word_count",
    "description_length_chars",
    "description_word_count",
    "title_has_number",
    "title_has_question",
    "title_has_currency",
    "title_has_year",
    "ai_semantic_score",
    "finance_semantic_score",
    "productivity_semantic_score",
    "urgency_semantic_score",
    "warning_semantic_score",
    "promise_semantic_score",
    "curiosity_semantic_score",
    "tutorial_semantic_score",
    "news_semantic_score",
    "semantic_cluster_id",
    "semantic_cluster_label",
]

TITLE_OUTPUT_COLUMNS = [
    "video_id",
    "title",
    "title_length_chars",
    "title_word_count",
    "title_has_number",
    "title_has_question",
    "title_has_currency",
    "title_has_year",
    "hook_semantic_type",
    "dominant_semantic_score",
]

CLUSTER_OUTPUT_COLUMNS = [
    "video_id",
    "semantic_cluster_id",
    "semantic_cluster_size",
    "semantic_cluster_label",
    "cluster_top_terms",
]

_HOOK_PRIORITY = ["warning", "promise", "curiosity", "tutorial", "news", "ai", "finance"]


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


def _normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _normalize_text(text: str) -> str:
    return _normalize_whitespace(text.lower())


def _aux_token_text(text: str) -> str:
    text_norm = _normalize_text(text)
    return _normalize_whitespace(re.sub(r"[^\w\sáéíóúüñ]", " ", text_norm, flags=re.IGNORECASE))


def _contains_year(text: str) -> bool:
    return bool(re.search(r"\b(19|20)\d{2}\b", text))


def _uppercase_ratio(text: str) -> float:
    letters = [ch for ch in text if ch.isalpha()]
    if not letters:
        return 0.0
    upper = sum(1 for ch in letters if ch.isupper())
    return round((upper / len(letters)) * 100.0, 4)


def _compute_semantic_scores(text: str) -> dict[str, float]:
    normalized = _normalize_text(text)
    scores: dict[str, float] = {}
    for category, terms in SEMANTIC_DICTIONARIES.items():
        if not terms:
            scores[f"{category}_semantic_score"] = 0.0
            continue
        matches = sum(1 for term in terms if term in normalized)
        scores[f"{category}_semantic_score"] = round((matches / len(terms)) * 100.0, 4)
    return scores


def _hook_semantic_type(row: dict[str, Any]) -> tuple[str, float]:
    values = {key: float(row.get(f"{key}_semantic_score", 0.0) or 0.0) for key in _HOOK_PRIORITY}
    best = max(values.values()) if values else 0.0
    if best <= 0:
        return "unknown", 0.0
    for key in _HOOK_PRIORITY:
        if values[key] == best:
            return key, best
    return "unknown", 0.0


def _safe_sqrt_clusters(n_rows: int) -> int:
    if n_rows < 4:
        return 1
    max_by_n = int(math.sqrt(n_rows))
    if max_by_n < 2:
        return 1
    return max(2, min(8, max_by_n))


def build_nlp_features(*, data_dir: str | Path = "data") -> dict[str, Any]:
    data_root = Path(data_dir)
    analytics_latest = data_root / "analytics" / "latest"
    output_dir = data_root / "nlp_features"
    output_dir.mkdir(parents=True, exist_ok=True)

    warnings: list[str] = []

    try:
        from sklearn.cluster import KMeans
        from sklearn.decomposition import TruncatedSVD
        from sklearn.feature_extraction.text import TfidfVectorizer

        sklearn_available = True
    except ModuleNotFoundError:
        sklearn_available = False
        warnings.append("scikit-learn is not available; TF-IDF/LSA/KMeans steps will be skipped.")
    sources = {
        "video_metrics": analytics_latest / "latest_video_metrics.csv",
        "title_metrics": analytics_latest / "latest_title_metrics.csv",
        "video_scores": analytics_latest / "latest_video_scores.csv",
        "video_advanced": analytics_latest / "latest_video_advanced_metrics.csv",
    }

    loaded: dict[str, list[dict[str, str]]] = {}
    for key, path in sources.items():
        if not path.exists():
            warnings.append(f"Missing source file: {path}")
            loaded[key] = []
            continue
        loaded[key] = _read_csv(path)

    base_rows = loaded["video_metrics"]
    if not base_rows:
        warnings.append("No rows found in latest_video_metrics.csv.")

    title_by_video = {row.get("video_id", ""): row for row in loaded["title_metrics"] if row.get("video_id")}
    scores_by_video = {row.get("video_id", ""): row for row in loaded["video_scores"] if row.get("video_id")}
    advanced_by_video = {row.get("video_id", ""): row for row in loaded["video_advanced"] if row.get("video_id")}

    assembled: list[dict[str, Any]] = []
    text_corpus: list[str] = []

    for row in base_rows:
        video_id = str(row.get("video_id", "")).strip()
        if not video_id:
            continue
        merged = {**row, **scores_by_video.get(video_id, {}), **advanced_by_video.get(video_id, {}), **title_by_video.get(video_id, {})}

        title = str(merged.get("title", "") or "")
        description = str(merged.get("description", "") or "")
        if not description:
            description = str(merged.get("video_description", "") or "")
        if not description:
            description = title
            warnings.append(f"Description missing for video_id={video_id}; using title as fallback text.")

        title_clean = _normalize_whitespace(title)
        desc_clean = _normalize_whitespace(description)
        full_text = _normalize_text(f"{title_clean} {desc_clean}".strip())
        aux_title = _aux_token_text(title_clean)

        assembled_row: dict[str, Any] = {
            "execution_date": merged.get("execution_date") or datetime.now(timezone.utc).isoformat(),
            "video_id": video_id,
            "channel_id": str(merged.get("channel_id", "") or ""),
            "channel_name": str(merged.get("channel_name", "") or ""),
            "title": title_clean,
            "description": desc_clean,
            "title_length_chars": len(title_clean),
            "title_word_count": len([part for part in title_clean.split(" ") if part]),
            "description_length_chars": len(desc_clean),
            "description_word_count": len([part for part in desc_clean.split(" ") if part]),
            "title_has_number": int(bool(re.search(r"\d", title_clean))),
            "title_has_question": int("?" in title_clean or "¿" in title_clean),
            "title_has_currency": int(bool(re.search(r"[$€£¥]|\b(usd|eur|mxn|cop|ars)\b", aux_title, flags=re.IGNORECASE))),
            "title_has_year": int(_contains_year(title_clean)),
            "title_uppercase_ratio": _uppercase_ratio(title),
        }
        assembled_row.update(_compute_semantic_scores(full_text))
        assembled.append(assembled_row)
        text_corpus.append(full_text)

    lsa_rows: list[dict[str, Any]] = []
    lsa_columns = ["video_id"]
    cluster_by_video: dict[str, int] = {}
    cluster_size_by_id: dict[int, int] = {}
    cluster_terms_by_id: dict[int, str] = {}

    if sklearn_available and assembled and len(text_corpus) >= 3:
        word_vectorizer = TfidfVectorizer(ngram_range=(1, 2), min_df=1)
        char_vectorizer = TfidfVectorizer(analyzer="char_wb", ngram_range=(3, 5), min_df=1)
        word_matrix = word_vectorizer.fit_transform(text_corpus)
        char_matrix = char_vectorizer.fit_transform(text_corpus)

        from scipy.sparse import hstack  # local import keeps dependency surface focused

        combined = hstack([word_matrix, char_matrix])
        max_components = min(20, combined.shape[0] - 1, combined.shape[1] - 1)

        if max_components >= 1:
            svd = TruncatedSVD(n_components=max_components, random_state=42)
            matrix_lsa = svd.fit_transform(combined)
            lsa_columns = ["video_id", *[f"lsa_{idx}" for idx in range(1, max_components + 1)]]
            for row, vector in zip(assembled, matrix_lsa):
                item = {"video_id": row["video_id"]}
                for idx in range(1, max_components + 1):
                    item[f"lsa_{idx}"] = round(float(vector[idx - 1]), 8)
                lsa_rows.append(item)
        else:
            warnings.append("LSA skipped because TF-IDF matrix does not have enough rank for components.")
            lsa_rows = [{"video_id": row["video_id"]} for row in assembled]

        n_rows = len(assembled)
        n_clusters = _safe_sqrt_clusters(n_rows)
        if n_clusters >= 2:
            kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
            cluster_ids = kmeans.fit_predict(word_matrix)
            feature_names = word_vectorizer.get_feature_names_out()
            cluster_counts = Counter(int(cluster_id) for cluster_id in cluster_ids)

            centroid_order = kmeans.cluster_centers_.argsort(axis=1)[:, ::-1]
            for cluster_id in range(n_clusters):
                terms = [feature_names[idx] for idx in centroid_order[cluster_id][:3] if idx < len(feature_names)]
                top_terms = " ".join(terms).strip() or "general"
                cluster_terms_by_id[cluster_id] = top_terms
            for row, cluster_id in zip(assembled, cluster_ids):
                cid = int(cluster_id)
                cluster_by_video[row["video_id"]] = cid
                cluster_size_by_id[cid] = cluster_counts[cid]
        else:
            warnings.append("Clustering fallback to single cluster because there are too few rows.")
            for row in assembled:
                cluster_by_video[row["video_id"]] = 0
            cluster_size_by_id[0] = len(assembled)
            cluster_terms_by_id[0] = "general"
    else:
        if assembled:
            if sklearn_available:
                warnings.append("LSA skipped because fewer than 3 texts are available.")
            else:
                warnings.append("LSA skipped because scikit-learn is unavailable.")
            warnings.append("Clustering fallback to single cluster because there are too few rows or missing dependencies.")
            lsa_rows = [{"video_id": row["video_id"]} for row in assembled]
            for row in assembled:
                cluster_by_video[row["video_id"]] = 0
            cluster_size_by_id[0] = len(assembled)
            cluster_terms_by_id[0] = "general"
        else:
            lsa_rows = []

    cluster_rows: list[dict[str, Any]] = []
    for row in assembled:
        video_id = row["video_id"]
        cluster_id = cluster_by_video.get(video_id, 0)
        top_terms = cluster_terms_by_id.get(cluster_id, "general")
        label = "_".join(top_terms.split()) if top_terms else "general"
        row["semantic_cluster_id"] = cluster_id
        row["semantic_cluster_label"] = label
        cluster_rows.append(
            {
                "video_id": video_id,
                "semantic_cluster_id": cluster_id,
                "semantic_cluster_size": cluster_size_by_id.get(cluster_id, len(assembled)),
                "semantic_cluster_label": label,
                "cluster_top_terms": top_terms,
            }
        )

    title_rows: list[dict[str, Any]] = []
    for row in assembled:
        hook_type, dominant_score = _hook_semantic_type(row)
        title_rows.append(
            {
                "video_id": row["video_id"],
                "title": row["title"],
                "title_length_chars": row["title_length_chars"],
                "title_word_count": row["title_word_count"],
                "title_has_number": row["title_has_number"],
                "title_has_question": row["title_has_question"],
                "title_has_currency": row["title_has_currency"],
                "title_has_year": row["title_has_year"],
                "hook_semantic_type": hook_type,
                "dominant_semantic_score": round(float(dominant_score), 4),
            }
        )

    _write_csv(output_dir / "latest_video_nlp_features.csv", VIDEO_OUTPUT_COLUMNS, assembled)
    _write_csv(output_dir / "latest_title_nlp_features.csv", TITLE_OUTPUT_COLUMNS, title_rows)
    _write_csv(output_dir / "latest_semantic_vectors.csv", lsa_columns, lsa_rows)
    _write_csv(output_dir / "latest_semantic_clusters.csv", CLUSTER_OUTPUT_COLUMNS, cluster_rows)

    top_labels = Counter(row.get("semantic_cluster_label", "general") for row in cluster_rows)
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_videos": len(assembled),
        "lsa_components": max(0, len(lsa_columns) - 1),
        "clusters": len(cluster_size_by_id) if cluster_size_by_id else 0,
        "top_cluster_labels": [label for label, _ in top_labels.most_common(5)],
        "warnings": warnings,
        "nlp_mode": "tfidf_lsa_dictionary_v1",
        "output_dir": str(output_dir),
    }

    (output_dir / "nlp_feature_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    summary["status"] = "success_with_warnings" if warnings else "success"
    return summary
