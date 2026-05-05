# ytb_history

Sistema en **Python 3.11+** para monitorear canales de YouTube por ejecución periódica, detectar videos recientes desde playlists de uploads, actualizar métricas y mantener historial en snapshots/deltas comprimidos.

## 1) Descripción

Este proyecto:
- Monitorea una lista de canales de YouTube.
- Detecta videos recientes desde el **uploads playlist** de cada canal.
- Actualiza métricas usando `videos.list` en lotes.
- Guarda historial inmutable en snapshots y cambios en deltas.

## 2) Arquitectura

El flujo está organizado por responsabilidades:
- **resolver**: resuelve `channel_id` y `uploads_playlist_id`.
- **discovery**: detecta videos desde `playlistItems.list`.
- **tracking**: mantiene y actualiza el catálogo de videos rastreados.
- **enrichment**: consulta detalles/métricas por lotes vía `videos.list`.
- **snapshot/delta storage**: persiste histórico comprimido (`JSONL.GZ`).
- **reports**: genera reportes de ejecución, descubrimiento, cuota y errores.
- **orchestrator**: coordina `run` y `dry-run`.
- **GitHub Actions**: automatiza validación (CI) y ejecución diaria.

## 3) Por qué no se usa `search.list`

`search.list` tiene costo de cuota significativamente más alto y no es necesario para el flujo normal.

Este sistema usa:
- `channels.list` (resolución de canal y playlist de uploads).
- `playlistItems.list` (descubrimiento incremental de videos).
- `videos.list` (enriquecimiento/métricas en batch).

## 4) Cuota

Fórmula de estimación por corrida:
- `channels.list = canales no cacheados`
- `playlistItems.list = canales OK * páginas revisadas`
- `videos.list = ceil(videos_to_track / 50)`

Ejemplo:
- 100 canales
- 0 no cacheados
- 1 página por canal
- 1200 videos a enriquecer

Resultado:
- `channels.list = 0`
- `playlistItems.list = 100`
- `videos.list = 24`
- `total = 124`

## 5) Instalación local

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .
python -m pip install -r requirements.txt
```

## 6) Configurar API key local

```bash
export YOUTUBE_API_KEY="..."
```

También puedes copiar `.env.example` y cargarlo en tu entorno local.

## 7) Ejecutar local

```bash
python -m ytb_history.cli dry-run
python -m ytb_history.cli run
```

## 8) Validar última corrida

```bash
python -m ytb_history.cli validate-latest
```


## 9) Exportar última corrida

```bash
python -m ytb_history.cli export-latest
```

Genera una carpeta particionada en `data/exports/dt=YYYY-MM-DD/run=HHMMSSZ|HHMMSS±ZZZZ/` con:
- `latest_snapshots.csv`
- `latest_deltas.csv`
- `video_growth_summary.csv`
- `export_summary.json`

Notas:
- No llama la API de YouTube.
- No modifica snapshots, deltas ni catálogo actual.
- `tags` en `latest_snapshots.csv` se exporta como **JSON string** estable UTF-8 (por ejemplo: `["python", "datos"]`).


## 10) Construir capa analítica

```bash
python -m ytb_history.cli build-analytics
```

Genera artefactos analíticos en `data/analytics/`:
- `latest/latest_video_metrics.csv`
- `latest/latest_channel_metrics.csv`
- `latest/latest_title_metrics.csv`
- `latest/latest_video_scores.csv` (scoring robusto con percentiles + robust_z)
- `latest/latest_video_advanced_metrics.csv`
- `latest/latest_channel_advanced_metrics.csv`
- `latest/latest_metric_eligibility.csv`
- `baselines/channel_baselines.csv`
- `baselines/video_lifecycle_metrics.csv`
- agregaciones temporales en:
  - `periods/grain=daily/video_metrics.csv`
  - `periods/grain=weekly/video_metrics.csv`
  - `periods/grain=monthly/video_metrics.csv`
  - `periods/grain=daily/channel_metrics.csv`
  - `periods/grain=weekly/channel_metrics.csv`
  - `periods/grain=monthly/channel_metrics.csv`
- `latest/latest_run_metrics.json`
- `latest/dashboard_index.json` (índice dashboard-ready para consumo de UI)
- `latest/analytics_manifest.json`

Además, incluye scores robustos (percentiles + robust_z), métricas de éxito por horizonte (corto/medio/largo), señales de `trend_burst`, `evergreen_score` y `metric_confidence_score`.

`dashboard_index.json` funciona como contrato de integración para el próximo **Dashboard MVP**: el dashboard debe leer este índice para descubrir tablas, rutas, vistas recomendadas, ordenamientos por defecto y KPIs sugeridos, en lugar de hardcodear rutas/columnas.


## 11) Construir dashboard estático

```bash
python -m ytb_history.cli build-pages-dashboard
```

Lee `data/analytics/` y genera artefactos JSON en `site/data/` listos para publicar en GitHub Pages. Además copia el Dashboard MVP estático (`HTML/CSS/JS` vanilla) desde `apps/pages_dashboard/src/` a `site/` (`index.html` y `assets/`) usando rutas relativas compatibles con subpath de GitHub Pages.

El dashboard no usa Streamlit ni backend: consume exclusivamente `./data/*.json`, tolera faltantes mostrando warnings visuales y mantiene navegación por secciones (Overview, Videos, Channels, Scores, Advanced, Titles, Periods, Alerts y Data Quality).


## 12) Generar señales y alertas

```bash
python -m ytb_history.cli generate-alerts
```

Este comando lee exclusivamente tablas existentes en `data/analytics/latest/` y genera señales/alertas accionables en `data/signals/` y `data/alerts/`.

Señales destacadas:
- `alpha_breakout`: detecta videos con `alpha_score` alto para referencia competitiva.
- `trend_burst`: identifica videos con estallido de tendencia reciente.
- `evergreen_candidate`: sugiere contenido con potencial de rendimiento sostenido.
- `packaging_problem`: marca videos con señal de interés pero posible problema de empaque.
- `channel_momentum_up`: detecta canales con momentum alto.
- `metric_confidence_score`: ajusta la confianza para priorizar alertas más sólidas y reducir decisiones sobre métricas débiles.

## 12.1) Construir capa de decisión

```bash
python -m ytb_history.cli build-decision-layer
```

Lee `data/analytics/`, `data/signals/` y `data/alerts/` para generar candidatos de acción priorizados, matriz de oportunidad, oportunidades de contenido y watchlist en `data/decision/`.

Esta capa solo convierte señales con `triggered=true` en action candidates reales; señales no disparadas se ignoran y se contabilizan para trazabilidad.


## 12.2) Generar brief semanal

```bash
python -m ytb_history.cli generate-weekly-brief
```

Genera un brief semanal determinístico en `data/briefs/` usando únicamente artefactos existentes de `analytics`, `signals`, `alerts` y `decision`:
- `latest_weekly_brief.md`
- `latest_weekly_brief.html`
- `latest_weekly_brief.json`
- versión particionada por semana ISO en `week=YYYY-WW/`

## 12.3) Construir dataset supervisado model-ready

```bash
python -m ytb_history.cli build-model-dataset
```

Genera artefactos de preparación para modelado supervisado en `data/modeling/`:
- `supervised_examples.csv`
- `feature_dictionary.json`
- `target_dictionary.json`
- `leakage_audit.json`
- `model_readiness_report.json`

Este comando prepara dataset supervisado y auditorías de readiness, pero **no entrena** modelos productivos todavía.



## 12.3.1) Analizar model readiness diagnostics

```bash
python -m ytb_history.cli analyze-model-readiness --data-dir data
```

Genera diagnóstico explícito de madurez de entrenamiento en `data/modeling/`:
- `latest_model_readiness_diagnostics.json`
- `latest_model_readiness_timeline.csv`
- `latest_target_coverage_report.csv`
- `latest_training_gap_report.json`
- `latest_model_readiness_report.md`
- `latest_model_readiness_report.html`

Este comando **explica por qué el entrenamiento está bloqueado**, no llama YouTube API y **no entrena modelos**.

## 12.4) Construir capa NLP liviana

```bash
python -m ytb_history.cli build-nlp-features
```

Genera artefactos reproducibles en `data/nlp_features/` usando diccionarios semánticos + `TF-IDF` (word 1-2 / char 3-5) + `LSA` (`TruncatedSVD`) + clustering `KMeans`:
- `latest_video_nlp_features.csv`
- `latest_title_nlp_features.csv`
- `latest_semantic_vectors.csv`
- `latest_semantic_clusters.csv`
- `nlp_feature_summary.json`

Esta capa no llama YouTube API, no usa `search.list`, no usa LLMs y no usa embeddings externos pesados.


## 12.5) Construir Topic & Title Intelligence

```bash
python -m ytb_history.cli build-topic-intelligence
```

Lee `data/nlp_features/` + `data/analytics/latest/` y, si existen, `data/decision/` y `data/model_intelligence/` para generar inteligencia temática en `data/topic_intelligence/`:
- `latest_video_topics.csv`
- `latest_topic_metrics.csv`
- `latest_title_pattern_metrics.csv`
- `latest_keyword_metrics.csv`
- `latest_topic_opportunities.csv`
- `topic_intelligence_summary.json`

Este comando no llama YouTube API, no usa `search.list`, no usa LLMs ni embeddings externos pesados.


## 12.6) Entrenar Content Driver Models supervisados

```bash
python -m ytb_history.cli train-content-driver-models
```

Entrena modelos supervisados (Random Forest, lineal regularizado y árbol shallow) con split temporal usando `data/modeling/supervised_examples.csv` + features NLP/tópicas cuando existen.

Genera reportes en `data/model_reports/`:
- `latest_content_driver_leaderboard.csv`
- `latest_content_driver_feature_importance.csv`
- `latest_content_driver_feature_direction.csv`
- `latest_content_driver_group_importance.csv`
- `latest_content_driver_report.md`
- `latest_content_driver_report.html`

Y artefactos fuera de `data/` en `build/content_driver_artifact/` (no se deben versionar modelos en Git).

## 12.7) Smoke test de entrenamiento con dataset sintético

```bash
python -m ytb_history.cli smoke-test-model-training --output-dir build/model_smoke_test
```

Ejecuta un smoke test end-to-end de entrenamiento + predicción usando datos sintéticos determinísticos (`random_state=42`) sin tocar `data/` real.


## 12.9) Generar paquetes creativos

```bash
python -m ytb_history.cli generate-creative-packages
```

Genera una capa de ejecución creativa en `data/creative_packages/` a partir de outputs existentes de `decision`, `topic_intelligence`, `model_reports`, `model_intelligence` y `briefs`, sin recalcular fórmulas de decision/topic/model.

Archivos generados:
- `data/creative_packages/latest_creative_packages.csv`
- `data/creative_packages/latest_title_candidates.csv`
- `data/creative_packages/latest_hook_candidates.csv`
- `data/creative_packages/latest_thumbnail_briefs.csv`
- `data/creative_packages/latest_script_outlines.csv`
- `data/creative_packages/latest_originality_checks.csv`
- `data/creative_packages/latest_production_checklist.csv`
- `data/creative_packages/creative_packages_summary.json`

## 13) Dashboard en GitHub Pages

1. Activa GitHub Pages en `Settings > Pages`.
2. En `Source`, selecciona `GitHub Actions`.
3. Ejecuta el workflow **Deploy Dashboard to GitHub Pages** (manual o por cambios en rutas configuradas).
4. URL esperada del dashboard publicado:
   - `https://jcval94.github.io/ytb_history/`
5. El dashboard se reconstruye automáticamente cuando cambia `data/analytics/**` o `apps/pages_dashboard/**` (además del builder/CLI/workflow de Pages).

El workflow de Pages construye `build-analytics` → `build-nlp-features` → `generate-alerts` → `build-decision-layer` → `build-model-intelligence` → `build-topic-intelligence` → `generate-creative-packages` → `generate-weekly-brief` → `build-pages-dashboard` y publica únicamente el artefacto `site/` (incluye tab Creative).

## 14) Tests

```bash
python -m compileall src tests
pytest -q
```

## 15) Configuración de canales

Editar `config/channels.py`:

```python
CHANNEL_URLS = [
    # URLs de canales a monitorear
]
```

## 16) Configuración de settings

Editar `config/settings.yaml`:
- `discovery_window_days`
- `tracking_window_days`
- `youtube_batch_size`
- `operational_quota_limit`
- `max_pages_per_channel`
- `execution_timezone` (`local` por defecto, o zona IANA como `America/Mexico_City`)

## 17) Archivos generados

- `data/state/channel_registry.jsonl`
- `data/state/tracked_videos_catalog.jsonl`
- `data/snapshots/dt=YYYY-MM-DD/run=HHMMSSZ|HHMMSS±ZZZZ/snapshots.jsonl.gz`
- `data/deltas/dt=YYYY-MM-DD/run=HHMMSSZ|HHMMSS±ZZZZ/deltas.jsonl.gz`
- `data/reports/dt=YYYY-MM-DD/run=HHMMSSZ|HHMMSS±ZZZZ/quota_report.json`
- `data/reports/dt=YYYY-MM-DD/run=HHMMSSZ|HHMMSS±ZZZZ/run_summary.json`
- `data/reports/dt=YYYY-MM-DD/run=HHMMSSZ|HHMMSS±ZZZZ/discovery_report.jsonl`
- `data/reports/dt=YYYY-MM-DD/run=HHMMSSZ|HHMMSS±ZZZZ/channel_errors.jsonl`
- `data/signals/latest_video_signals.csv`
- `data/signals/latest_channel_signals.csv`
- `data/signals/latest_signal_candidates.csv`
- `data/signals/signal_summary.json`
- `data/alerts/latest_alerts.jsonl`
- `data/alerts/latest_alerts.json`
- `data/alerts/latest_alerts.md`
- `data/alerts/alert_summary.json`
- `data/decision/latest_action_candidates.csv`
- `data/decision/latest_opportunity_matrix.csv`
- `data/decision/latest_content_opportunities.csv`
- `data/decision/latest_watchlist_recommendations.csv`
- `data/decision/latest_decision_context.json`
- `data/nlp_features/latest_video_nlp_features.csv`
- `data/nlp_features/latest_title_nlp_features.csv`
- `data/nlp_features/latest_semantic_vectors.csv`
- `data/nlp_features/latest_semantic_clusters.csv`
- `data/nlp_features/nlp_feature_summary.json`
- `data/topic_intelligence/latest_video_topics.csv`
- `data/topic_intelligence/latest_topic_metrics.csv`
- `data/topic_intelligence/latest_title_pattern_metrics.csv`
- `data/topic_intelligence/latest_keyword_metrics.csv`
- `data/topic_intelligence/latest_topic_opportunities.csv`
- `data/topic_intelligence/topic_intelligence_summary.json`
- `data/decision/decision_summary.json`
- `data/briefs/latest_weekly_brief.md`
- `data/briefs/latest_weekly_brief.html`
- `data/briefs/latest_weekly_brief.json`
- `data/briefs/week=YYYY-WW/weekly_brief.md`
- `data/briefs/week=YYYY-WW/weekly_brief.html`
- `data/briefs/week=YYYY-WW/weekly_brief.json`
- `data/modeling/supervised_examples.csv`
- `data/modeling/feature_dictionary.json`
- `data/modeling/target_dictionary.json`
- `data/modeling/leakage_audit.json`
- `data/modeling/model_readiness_report.json`
- `data/model_reports/latest_content_driver_leaderboard.csv`
- `data/model_reports/latest_content_driver_feature_importance.csv`
- `data/model_reports/latest_content_driver_feature_direction.csv`
- `data/model_reports/latest_content_driver_group_importance.csv`
- `data/model_reports/latest_content_driver_report.md`
- `data/model_reports/latest_content_driver_report.html`

## 18) GitHub Actions

### CI (`.github/workflows/ci.yml`)
- Corre en `push` y `pull_request`.
- Instala el paquete con `src layout` usando `python -m pip install -e .`.
- Ejecuta compilación, tests y `dry-run`.
- No requiere `YOUTUBE_API_KEY`.

### Monitor (`.github/workflows/monitor.yml`)

El monitor ejecuta la cadena de transcripción (`select-transcription-candidates` → `transcribe-selected-videos` → `generate-transcript-insights` → `transcript-registry-report`) antes de la capa de inteligencia/brief.
- Corre manual (`workflow_dispatch`) y diario (`schedule`).
- Cron configurado: `17 9 * * *` (UTC).
  - Referencia: **09:17 UTC** ≈ **03:17 en America/Matamoros** dependiendo del horario local.
- Ejecuta en orden: `compile`, `pytest -q`, `dry-run`, `run`, `validate-latest`, `export-latest`, `build-analytics`, `build-nlp-features`, `generate-alerts`, `build-decision-layer`, `select-transcription-candidates`, `install yt-dlp`, `install ffmpeg`, `yt-dlp --version`, `ffmpeg -version`, `transcribe-selected-videos`, `generate-transcript-insights`, `transcript-registry-report`, `build-model-intelligence`, `build-topic-intelligence`, `generate-creative-packages`, `generate-weekly-brief`.
- Usa `YOUTUBE_API_KEY` desde GitHub Secrets **solo** en el paso `run`.
- Usa `OPENAI_API_KEY` en los pasos de transcripción/insights.
- Hace commit únicamente cuando hay cambios en `data/` (stagea solo `data/`).
- La transcripción es idempotente: si el top diario repite videos ya exitosos, se saltan y puede no generarse trabajo nuevo ese día.

Configurar el secret en GitHub:
1. `Settings` > `Secrets and variables` > `Actions`
2. `New repository secret`
3. Name: `YOUTUBE_API_KEY`
4. Value: tu API key

Para transcripción/insights:
1. `Settings` > `Secrets and variables` > `Actions`
2. `New repository secret`
3. Name: `OPENAI_API_KEY`
4. Value: tu API key de OpenAI

Para habilitar cookies de `yt-dlp` en CI (opcional):
1. Exporta un `cookies.txt` vigente desde tu navegador/perfil autorizado.
2. `Settings` > `Secrets and variables` > `Actions`
3. `New repository secret`
4. Name: `YTDLP_COOKIES_TXT`
5. Value: contenido completo del archivo `cookies.txt` (texto plano, multi-línea).

Rotación recomendada de `YTDLP_COOKIES_TXT`:
- Reemplazar el secret cuando expire la sesión/cookie o falle descarga por autenticación.
- Rotar preventivamente (por ejemplo mensual) y después de cambios de contraseña o eventos de seguridad.
- Validar el siguiente run de `monitor.yml`; si falta el secret, el workflow continúa con warning y fallback sin cookies.

Prerrequisito local para transcripción (mismo entorno virtual del proyecto):
```bash
python -m pip install yt-dlp
yt-dlp --version
```

Uso local recomendado cuando `yt-dlp` requiere autenticación/cookies:
```bash
python -m ytb_history.cli transcribe-selected-videos \
  --data-dir data \
  --audio-source-dir data/audio_sources \
  --ytdlp-cookies-file /ruta/local/cookies.txt
```

Opcional en entorno local (usar cookies del navegador):
```bash
python -m ytb_history.cli transcribe-selected-videos \
  --data-dir data \
  --ytdlp-browser firefox \
  --ytdlp-extra-args "--proxy http://127.0.0.1:8080"
```

Ejemplo CI (ruta de cookies inyectada por secret/file mount):
```bash
python -m ytb_history.cli transcribe-selected-videos \
  --data-dir data \
  --ytdlp-cookies-file "$YTDLP_COOKIES_FILE"
```

⚠️ **Seguridad**: nunca commitear `cookies.txt` ni credenciales derivadas. Mantener estos archivos fuera del repositorio y cargarlos desde secretos/variables de entorno en CI.

Además, instalar `ffmpeg` en el sistema y verificar disponibilidad en PATH:
```bash
# Ubuntu/Debian
sudo apt-get update
sudo apt-get install -y ffmpeg
ffmpeg -version
```

## 19) Interpretación de status

- `success`
- `success_with_warnings`
- `aborted_quota_guardrail`
- `failed`

## 20) Troubleshooting

- **Missing YOUTUBE_API_KEY**: define variable local o secret en Actions.
- **quota guardrail abort**: revisa `operational_quota_limit` y tamaño de corrida.
- **canal no resoluble**: valida URL/ID del canal en `config/channels.py`.
- **video unavailable/private/deleted**: revisar `channel_errors.jsonl` y reportes.
- **Errores de descarga en transcripción (`yt-dlp`)**:
  - `failed_audio_download_auth_required`: normalmente requiere cookies/sesión (`--ytdlp-cookies-file` o `--ytdlp-browser`).
  - `failed_audio_download_video_unavailable`: video privado/no disponible/restringido.
  - `failed_audio_download_network_or_rate_limit`: red inestable, timeout o rate limit (`429`).
  - `failed_audio_download`: fallback genérico cuando no se puede clasificar.
  - `skipped_missing_ytdlp`: falta binario `yt-dlp` en el entorno.
  - Para diagnóstico agregado: ejecutar `python -m ytb_history.cli transcript-registry-report` y revisar `status_counts` + `error_category_counts`.
- **no changes to commit**: comportamiento esperado si no hubo cambios en `data/`.

## 21) Seguridad

- No guardar API keys en el repositorio.
- No imprimir secrets en logs.
- No usar `search.list` en flujo normal.
