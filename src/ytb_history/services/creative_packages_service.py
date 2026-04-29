"""Creative execution packages generated from existing intelligence layers."""

from __future__ import annotations

import csv
import hashlib
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

CREATIVE_PACKAGES_COLUMNS = [
    "creative_package_id", "generated_at", "source_action_id", "source_opportunity_id", "source_video_id", "source_channel_name", "source_title", "topic", "package_type", "creative_angle", "recommended_format", "recommended_timeframe", "source_decision_score", "topic_opportunity_score", "title_pattern_success_score", "originality_score", "copy_risk_score", "production_feasibility_score", "creative_execution_score", "confidence_score", "evidence_json", "dashboard_tab", "recommended_next_step",
]
TITLE_COLUMNS = ["creative_package_id", "title_candidate_id", "title_candidate", "title_pattern", "title_pattern_success_score", "copy_risk_score", "originality_score", "originality_status", "estimated_strength", "notes"]
HOOK_COLUMNS = ["creative_package_id", "hook_id", "hook_text", "hook_type", "expected_use", "risk"]
THUMB_COLUMNS = ["creative_package_id", "thumbnail_brief_id", "main_text", "visual_metaphor", "emotion", "layout_suggestion", "risk_notes"]
OUTLINE_COLUMNS = ["creative_package_id", "outline_id", "structure_type", "intro", "section_1", "section_2", "section_3", "closing", "cta"]
ORIGINALITY_COLUMNS = ["creative_package_id", "source_title", "candidate_text", "candidate_type", "lexical_similarity", "token_overlap_ratio", "copy_risk_score", "originality_score", "originality_status"]
CHECKLIST_COLUMNS = ["creative_package_id", "step_order", "production_step", "estimated_effort", "required_input", "done_default"]

TITLE_TEMPLATES = {
    "fast_reaction_package": ["Lo que significa {topic} esta semana", "{topic}: la señal que conviene vigilar ahora", "Por qué {topic} está ganando atención"],
    "evergreen_explainer_package": ["Cómo entender {topic} sin complicarte", "{topic}: guía simple para empezar", "Lo básico de {topic} explicado fácil"],
    "contrarian_package": ["El problema con la forma en que todos hablan de {topic}", "La verdad incómoda sobre {topic}", "Por qué {topic} no es tan simple como parece"],
    "tutorial_package": ["Cómo usar {topic} paso a paso", "Guía rápida para aplicar {topic}", "{topic}: tutorial práctico desde cero"],
    "comparison_package": ["{topic}: comparación simple para decidir mejor", "Qué opción conviene más dentro de {topic}", "{topic}: diferencias que sí importan"],
    "repackage_package": ["Una mejor forma de explicar {topic}", "El ángulo de {topic} que casi nadie está usando", "Cómo replantear {topic} para hacerlo más claro"],
    "watchlist_package": ["{topic}: lo que conviene observar antes de decidir", "La señal de {topic} que todavía necesita más datos", "{topic}: tendencia o ruido, qué vigilar"],
}

PRODUCTION_FEASIBILITY = {
    "fast_reaction_package": 75.0,
    "tutorial_package": 65.0,
    "evergreen_explainer_package": 80.0,
    "comparison_package": 60.0,
    "contrarian_package": 70.0,
    "repackage_package": 75.0,
    "watchlist_package": 50.0,
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in columns})


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def _tokens(text: str) -> set[str]:
    return {token for token in re.findall(r"[a-z0-9áéíóúñ]+", text.lower()) if token}


def _jaccard(a: str, b: str) -> float:
    ta, tb = _tokens(a), _tokens(b)
    if not ta and not tb:
        return 1.0
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)


def _originality_status(copy_risk_score: float) -> str:
    if copy_risk_score >= 70:
        return "risky"
    if copy_risk_score >= 40:
        return "review"
    return "safe"


def _package_type(row: dict[str, str]) -> str:
    action_type = (row.get("action_type") or "").strip()
    opp_type = (row.get("opportunity_type") or "").strip()
    saturation = _safe_float(row.get("topic_saturation_score")) or 0.0
    opp_score = _safe_float(row.get("topic_opportunity_score")) or 0.0
    tutorial = _safe_float(row.get("tutorial_semantic_score")) or 0.0
    hook_semantic_type = (row.get("hook_semantic_type") or "").strip()
    title_pattern = (row.get("title_pattern") or "").strip()

    if action_type == "create_fast_reaction" or opp_type == "emerging_topic":
        return "fast_reaction_package"
    if action_type == "create_evergreen" or opp_type == "evergreen_angle":
        return "evergreen_explainer_package"
    if action_type == "repackage_idea":
        return "repackage_package"
    if saturation >= 70 and opp_score >= 50:
        return "contrarian_package"
    if tutorial >= 60 or hook_semantic_type == "tutorial" or title_pattern == "tutorial_how_to":
        return "tutorial_package"
    if action_type in {"monitor_next_run", "wait_for_confidence"}:
        return "watchlist_package"
    return "comparison_package"


def _weighted_score(values: dict[str, float | None]) -> float:
    weights = {
        "source_decision_score": 0.35,
        "topic_opportunity_score": 0.20,
        "title_pattern_success_score": 0.15,
        "originality_score": 0.15,
        "production_feasibility_score": 0.10,
        "confidence_score": 0.05,
    }
    available = {k: w for k, w in weights.items() if values.get(k) is not None}
    if not available:
        return 0.0
    normalizer = sum(available.values())
    score = sum((values[k] or 0.0) * (w / normalizer) for k, w in available.items())
    return round(max(0.0, min(100.0, score)), 4)


def generate_creative_packages(*, data_dir: str | Path = "data") -> dict[str, Any]:
    root = Path(data_dir)
    out_dir = root / "creative_packages"
    warnings: list[str] = []
    now = _now_iso()

    action_path = root / "decision" / "latest_action_candidates.csv"
    opp_path = root / "decision" / "latest_content_opportunities.csv"
    topic_path = root / "topic_intelligence" / "latest_topic_opportunities.csv"
    pattern_path = root / "topic_intelligence" / "latest_title_pattern_metrics.csv"

    actions = _read_csv(action_path) if action_path.exists() else []
    opportunities = _read_csv(opp_path) if opp_path.exists() else []
    topics = _read_csv(topic_path) if topic_path.exists() else []
    patterns = _read_csv(pattern_path) if pattern_path.exists() else []

    if not action_path.exists():
        warnings.append("missing_input:decision/latest_action_candidates.csv")
    if not opp_path.exists():
        warnings.append("missing_input:decision/latest_content_opportunities.csv")
    if not topic_path.exists():
        warnings.append("missing_input:topic_intelligence/latest_topic_opportunities.csv")
    if not patterns:
        warnings.append("missing_or_empty:topic_intelligence/latest_title_pattern_metrics.csv")

    opp_by_video = {r.get("source_video_id", ""): r for r in opportunities}
    topic_by_video = {r.get("video_id", ""): r for r in topics}
    pattern_success = _safe_float(patterns[0].get("success_score")) if patterns else None

    creative_rows: list[dict[str, Any]] = []
    title_rows: list[dict[str, Any]] = []
    hook_rows: list[dict[str, Any]] = []
    thumb_rows: list[dict[str, Any]] = []
    outline_rows: list[dict[str, Any]] = []
    originality_rows: list[dict[str, Any]] = []
    checklist_rows: list[dict[str, Any]] = []

    for idx, action in enumerate(actions, start=1):
        video_id = action.get("video_id") or action.get("entity_id") or ""
        opp = opp_by_video.get(video_id, {})
        topic_row = topic_by_video.get(video_id, {})
        merged = {**action, **opp, **topic_row}
        package_type = _package_type(merged)
        topic = topic_row.get("topic") or action.get("topic") or opp.get("opportunity_type") or "tema clave"
        source_title = action.get("title") or opp.get("source_title") or ""
        source_score = _safe_float(action.get("decision_score"))
        topic_score = _safe_float(topic_row.get("topic_opportunity_score")) or _safe_float(opp.get("topic_opportunity_score"))
        pattern_score = _safe_float(topic_row.get("title_pattern_success_score")) or pattern_success or 50.0
        confidence = _safe_float(action.get("metric_confidence_score"))

        creative_package_id = f"cp_{idx}_{hashlib.sha1((action.get('action_id','') + video_id).encode()).hexdigest()[:10]}"
        feasibility = PRODUCTION_FEASIBILITY[package_type]

        templates = TITLE_TEMPLATES[package_type]
        candidate_scores: list[tuple[float, float]] = []
        for title_idx, template in enumerate(templates, start=1):
            candidate = template.format(topic=topic)
            if candidate.strip().lower() == source_title.strip().lower():
                candidate = f"{candidate} - análisis actualizado"
            overlap = _jaccard(source_title, candidate)
            copy_risk = round(overlap * 100, 4)
            originality = round(max(0.0, 100.0 - copy_risk), 4)
            status = _originality_status(copy_risk)
            candidate_scores.append((copy_risk, originality))

            title_rows.append({
                "creative_package_id": creative_package_id,
                "title_candidate_id": f"{creative_package_id}_t{title_idx}",
                "title_candidate": candidate,
                "title_pattern": package_type,
                "title_pattern_success_score": pattern_score,
                "copy_risk_score": copy_risk,
                "originality_score": originality,
                "originality_status": status,
                "estimated_strength": round((0.6 * pattern_score) + (0.4 * originality), 4),
                "notes": "deterministic_template",
            })
            originality_rows.append({
                "creative_package_id": creative_package_id,
                "source_title": source_title,
                "candidate_text": candidate,
                "candidate_type": "title",
                "lexical_similarity": round(overlap, 4),
                "token_overlap_ratio": round(overlap, 4),
                "copy_risk_score": copy_risk,
                "originality_score": originality,
                "originality_status": status,
            })

        avg_copy_risk = round(sum(score[0] for score in candidate_scores) / len(candidate_scores), 4)
        avg_originality = round(sum(score[1] for score in candidate_scores) / len(candidate_scores), 4)

        score_inputs = {
            "source_decision_score": source_score,
            "topic_opportunity_score": topic_score,
            "title_pattern_success_score": pattern_score,
            "originality_score": avg_originality,
            "production_feasibility_score": feasibility,
            "confidence_score": confidence,
        }

        creative_rows.append({
            "creative_package_id": creative_package_id,
            "generated_at": now,
            "source_action_id": action.get("action_id", ""),
            "source_opportunity_id": opp.get("opportunity_id", ""),
            "source_video_id": video_id,
            "source_channel_name": action.get("channel_name") or opp.get("source_channel", ""),
            "source_title": source_title,
            "topic": topic,
            "package_type": package_type,
            "creative_angle": f"Enfoque {package_type.replace('_package', '').replace('_', ' ')} para {topic}",
            "recommended_format": "video corto" if package_type == "fast_reaction_package" else "video explicativo",
            "recommended_timeframe": action.get("timeframe") or opp.get("recommended_timeframe") or "this_week",
            "source_decision_score": source_score if source_score is not None else "",
            "topic_opportunity_score": topic_score if topic_score is not None else "",
            "title_pattern_success_score": pattern_score,
            "originality_score": avg_originality,
            "copy_risk_score": avg_copy_risk,
            "production_feasibility_score": feasibility,
            "creative_execution_score": _weighted_score(score_inputs),
            "confidence_score": confidence if confidence is not None else "",
            "evidence_json": json.dumps({"action_type": action.get("action_type", ""), "opportunity_type": opp.get("opportunity_type", "")}, ensure_ascii=False),
            "dashboard_tab": "creative_execution",
            "recommended_next_step": "mantener_watchlist" if package_type == "watchlist_package" else "iniciar_preproduccion",
        })

        hooks = [
            ("question_hook", f"¿Qué cambia realmente con {topic}?", "intro", "low"),
            ("contrast_hook", f"Muchos simplifican {topic}, pero hay matices críticos.", "intro", "medium"),
            ("data_hook", f"Los datos recientes de {topic} muestran una señal accionable.", "intro", "low"),
            ("mistake_hook", f"Error común: ejecutar {topic} sin validar evidencia.", "middle", "medium"),
            ("promise_hook", f"En este video saldrás con un plan claro sobre {topic}.", "intro", "low"),
            ("curiosity_hook", f"Hay un detalle de {topic} que cambia toda la decisión.", "intro", "medium"),
        ]
        for i, (hook_type, text, expected_use, risk) in enumerate(hooks, start=1):
            hook_rows.append({"creative_package_id": creative_package_id, "hook_id": f"{creative_package_id}_h{i}", "hook_text": text, "hook_type": hook_type, "expected_use": expected_use, "risk": risk})

        thumb_rows.append({
            "creative_package_id": creative_package_id,
            "thumbnail_brief_id": f"{creative_package_id}_tb1",
            "main_text": topic[:48],
            "visual_metaphor": "señal vs ruido",
            "emotion": "urgencia" if package_type == "fast_reaction_package" else "claridad",
            "layout_suggestion": "texto corto lado izquierdo + elemento visual lado derecho",
            "risk_notes": "evitar promesas absolutas y claims no verificables",
        })

        structure_map = {
            "fast_reaction_package": "quick_reaction",
            "comparison_package": "compare_options",
            "tutorial_package": "tutorial_steps",
            "contrarian_package": "myth_vs_reality",
            "repackage_package": "case_breakdown",
        }
        structure = structure_map.get(package_type, "explain_problem_solution")
        outline_rows.append({
            "creative_package_id": creative_package_id,
            "outline_id": f"{creative_package_id}_o1",
            "structure_type": structure,
            "intro": f"Por qué {topic} importa ahora",
            "section_1": "Contexto y señal principal",
            "section_2": "Interpretación para audiencia objetivo",
            "section_3": "Plan de ejecución inmediato",
            "closing": "Riesgos y próximos pasos",
            "cta": "Comenta qué ángulo quieres profundizar",
        })

        for step_order, step in enumerate(["revisar evidencia", "elegir título", "definir hook", "preparar guion", "preparar miniatura", "publicar/monitorear"], start=1):
            checklist_rows.append({
                "creative_package_id": creative_package_id,
                "step_order": step_order,
                "production_step": step,
                "estimated_effort": "bajo" if step_order <= 3 else "medio",
                "required_input": "artifacts",
                "done_default": "false",
            })

    _write_csv(out_dir / "latest_creative_packages.csv", CREATIVE_PACKAGES_COLUMNS, creative_rows)
    _write_csv(out_dir / "latest_title_candidates.csv", TITLE_COLUMNS, title_rows)
    _write_csv(out_dir / "latest_hook_candidates.csv", HOOK_COLUMNS, hook_rows)
    _write_csv(out_dir / "latest_thumbnail_briefs.csv", THUMB_COLUMNS, thumb_rows)
    _write_csv(out_dir / "latest_script_outlines.csv", OUTLINE_COLUMNS, outline_rows)
    _write_csv(out_dir / "latest_originality_checks.csv", ORIGINALITY_COLUMNS, originality_rows)
    _write_csv(out_dir / "latest_production_checklist.csv", CHECKLIST_COLUMNS, checklist_rows)

    outputs = {
        "creative_packages": str(out_dir / "latest_creative_packages.csv"),
        "title_candidates": str(out_dir / "latest_title_candidates.csv"),
        "hook_candidates": str(out_dir / "latest_hook_candidates.csv"),
        "thumbnail_briefs": str(out_dir / "latest_thumbnail_briefs.csv"),
        "script_outlines": str(out_dir / "latest_script_outlines.csv"),
        "originality_checks": str(out_dir / "latest_originality_checks.csv"),
        "production_checklist": str(out_dir / "latest_production_checklist.csv"),
        "summary": str(out_dir / "creative_packages_summary.json"),
    }

    summary = {
        "generated_at": now,
        "total_packages": len(creative_rows),
        "package_type_counts": dict(Counter(row["package_type"] for row in creative_rows)),
        "top_packages": sorted([
            {"creative_package_id": row["creative_package_id"], "creative_execution_score": row["creative_execution_score"]}
            for row in creative_rows
        ], key=lambda row: float(row["creative_execution_score"]), reverse=True)[:5],
        "avg_originality_score": round(sum(float(row["originality_score"]) for row in creative_rows) / len(creative_rows), 4) if creative_rows else 0.0,
        "high_copy_risk_count": sum(1 for row in originality_rows if float(row["copy_risk_score"]) >= 70),
        "warnings": warnings,
    }
    (out_dir / "creative_packages_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        "status": "warning" if warnings else "success",
        "creative_packages_dir": str(out_dir),
        "total_packages": len(creative_rows),
        "outputs": outputs,
        "warnings": warnings,
    }


def build_creative_packages(*, data_dir: str | Path = "data") -> dict[str, Any]:
    """Backward compatible alias."""
    return generate_creative_packages(data_dir=data_dir)
