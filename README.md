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

Si además vas a transcribir audio autorizado y generar insights localmente:

```bash
python -m pip install -e ".[transcription]"
```

En **Windows PowerShell**:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e .
python -m pip install -e ".[transcription]"
```


## 6) Configurar API key local

```bash
export YOUTUBE_API_KEY="..."
export OPENAI_API_KEY="..."
```

En **Windows PowerShell**:

```powershell
$env:YOUTUBE_API_KEY="..."
$env:OPENAI_API_KEY="..."
```

`OPENAI_API_KEY` solo es necesaria para transcripción e insights. También puedes copiar `.env.example` y cargarlo en tu entorno local.

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

Dashboard publicado: [https://jcval94.github.io/ytb_history/](https://jcval94.github.io/ytb_history/)

```bash
python -m ytb_history.cli build-pages-dashboard
```

Lee `data/analytics/` y genera artefactos JSON en `site/data/` listos para publicar en GitHub Pages. Además copia el Dashboard MVP estático (`HTML/CSS/JS` vanilla) desde `apps/pages_dashboard/src/` a `site/` (`index.html` y `assets/`) usando rutas relativas compatibles con subpath de GitHub Pages.

El dashboard no usa Streamlit ni backend: consume exclusivamente `./data/*.json`, tolera faltantes mostrando warnings visuales y mantiene navegación por secciones (Overview, Videos, Channels, Scores, Advanced, Titles, Periods, Alerts y Data Quality).

### 11.1) Estandar de visualizacion del dashboard

Cada pestaña debe tener grafica solo cuando agrega una lectura que la tabla no entrega rapido. Las pestañas de detalle mantienen tablas, pero se agregan graficas de diagnostico cuando ayudan a detectar outliers, trade-offs, concentracion, calidad de datos u oportunidades accionables.

Reglas para graficas actuales y futuras:
- Usar dimensiones balanceadas: evitar tarjetas demasiado pequeñas como miniaturas, graficas de una sola columna que ocupen todo el ancho, y visuales excesivamente altos. El layout base limita el ancho de cada grafica y Content Drivers usa una grilla mas amplia y separada.
- En scatter plots, etiquetar solo puntos importantes: maximos, outliers o puntos con mayor combinacion de señal. El resto debe identificarse por hover.
- Cada punto debe exponer en hover el identificador y la informacion mas valiosa disponible: titulo/canal/video, variables X/Y, tamaño, categoria y metricas de contexto.
- Usar colores categoricos solo cuando hay menos de 8 puntos, series o elementos distinguibles. Con 8 o mas categorias, usar color unico/acento y dejar la distincion fina al tooltip, filtros o tablas.
- No usar color como unica codificacion de importancia: combinar posicion, tamaño, orden, etiquetas selectivas o tabla de respaldo.
- Mantener titulos y subtitulos orientados a decision: explicar que se puede leer del grafico, no solo repetir nombres de columnas.
- Evitar graficas decorativas. Si una pestaña no tiene suficiente variacion o el patron ya esta mejor cubierto por la tabla, dejarla como tabla.
- Mantener branding sobrio de producto de inteligencia: superficie clara, tinta fuerte, acentos azul/teal/rose/ambar, radios de 8px o menos y sin fondos decorativos dominantes.
- Cada grafica incluye un control `Play` para repetir una animacion ligera. La animacion debe reforzar lectura temporal/progresiva, respetar `prefers-reduced-motion` y no ser requisito para entender el dato.
- Remotion queda como capa futura para piezas exportables o videos de reporte; el dashboard runtime debe seguir siendo estatico, liviano y sin dependencia de render de video.

### 11.2) Observabilidad operativa

El registro central de procesos vive en `config/operations.yaml`. Cada proceso declara `process_id`, dominio, cadencia, SLA, comando/workflow, entradas, salidas, secretos requeridos, artefactos esperados, dependencias y tabs del dashboard impactadas.

```bash
python -m ytb_history.cli build-operations
```

El comando genera vistas versionadas en `data/operations/`:
- `latest_process_status.json`: estado normalizado por proceso.
- `process_catalog.json`: catalogo declarativo listo para dashboard.
- `dashboard_impact_matrix.csv`: matriz proceso -> tab.
- `operation_summary.json`: KPIs operativos y procesos que requieren atencion.
- `runs/dt=YYYY-MM-DD/run=HHMMSSZ/`: snapshot historico append-only.

Estados normalizados:
- `success`: artefacto durable encontrado y dentro de SLA.
- `success_with_warnings`: el proceso corrio pero reporto warnings, errores parciales o fallos tolerados.
- `failed`: el artefacto reporta fallo real o JSON invalido.
- `skipped`: ejecucion omitida de forma esperada.
- `stale`: el ultimo artefacto excede su SLA.
- `not_initialized`: proceso registrado sin artefacto requerido todavia.
- `unknown`: proceso configurado sin artefacto durable disponible.

Para procesos futuros, agregar una entrada en `config/operations.yaml` y preferir que el comando emita JSON con `status`, `generated_at`, `outputs`, `warnings` y `errors`/conteos. No publicar secretos, stdout completo ni logs crudos en los artefactos operativos.


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

El workflow de Pages construye `build-analytics` → `build-nlp-features` → `generate-alerts` → `build-decision-layer` → `build-model-intelligence` → `build-topic-intelligence` → `generate-creative-packages` → `generate-weekly-brief` → `build-operations` → `build-pages-dashboard` y publica únicamente el artefacto `site/` (incluye tabs Creative y Operations).

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
- Ejecuta compilacion, tests y `dry-run`.
- No requiere `YOUTUBE_API_KEY`.

### Monitor (`.github/workflows/monitor.yml`)

- Corre manual (`workflow_dispatch`) y diario (`schedule`).
- Cron configurado: `17 9 * * *` (UTC).
- Ejecuta en orden: `compile`, `pytest -q`, `dry-run`, `run`, `validate-latest`, `export-latest`, `build-analytics`, `build-nlp-features`, `generate-alerts`, `build-decision-layer`, `build-model-intelligence`, `build-topic-intelligence`, `generate-creative-packages`, `generate-weekly-brief`, `select-transcription-candidates`.
- Valida el secret `YOUTUBE_API_KEY` y lo usa desde GitHub Secrets solo en el paso `run`.
- No transcribe, no genera insights con OpenAI y no descarga audio.
- Hace commit unicamente cuando hay cambios en `data/`.

Configurar el secret de YouTube en GitHub:
1. `Settings` > `Secrets and variables` > `Actions`
2. `New repository secret`
3. Name: `YOUTUBE_API_KEY`
4. Value: tu API key

### Transcript Intelligence (`.github/workflows/transcripts.yml`)

Workflow manual para seleccionar candidatos, transcribir desde audio local autorizado, generar insights, enriquecer Creative Packages, actualizar el brief y reconstruir el dashboard.

- Trigger: `workflow_dispatch`, sin schedule.
- Inputs: `select_limit`, `include_forced`, `transcription_model`, `insights_model`, `dry_run`, `generate_insights`.
- `dry_run=true` no requiere `OPENAI_API_KEY` y no llama OpenAI.
- `dry_run=false` usa `OPENAI_API_KEY` solo en los pasos de transcripcion e insights.
- No usa `YOUTUBE_API_KEY`, no ejecuta collector, no entrena modelos y no toca `data/audio_sources/`.
- Staging explicito: `data/transcripts/`, `data/creative_packages/`, `data/briefs/` y `site/data/` si existe. No usa `git add .`.

## 19) Transcript Intelligence

### 19.1) Configurar canales forzados de transcripcion

Editar `config/transcription_channels.py`:

```python
TRANSCRIPTION_CHANNEL_URLS = [
    "https://www.youtube.com/@bilinkis",
    "https://www.youtube.com/veritasium",
]
```

Los videos nuevos observados localmente de estos canales entran primero en la cola como `forced_channel_new_video`. No cuentan dentro del top 10 diario y se deduplican si tambien aparecen como ranked.

Defaults en `config/settings.yaml`:

```yaml
transcription:
  daily_ranked_limit: 10
  forced_channels_enabled: true
  forced_channels_max_per_run: 50
  forced_channels_new_video_window_days: 14
  retry_cooldown_days: 7
  default_transcription_model: "gpt-4o-mini-transcribe"
  default_insights_model: "gpt-5.5-mini"
  max_transcriptions_per_run: 60
```

### 19.2) Seleccionar candidatos

```bash
python -m ytb_history.cli select-transcription-candidates --data-dir data --limit 10
```

La seleccion consume capas locales existentes en `data/analytics`, `data/decision`, `data/model_intelligence`, `data/topic_intelligence` y `data/creative_packages`. No llama YouTube API, no usa `search.list`, no llama OpenAI y no descarga audio/video.

### 19.3) Resolucion de media

La transcripcion local resuelve media en este orden:

1. Usa audio existente en `data/audio_sources/<video_id>.*`.
2. Si existe video local en `data/video_sources/<video_id>.*`, extrae audio con `ffmpeg` o `imageio-ffmpeg`.
3. Si no hay media local y el fallback esta habilitado, intenta descargar audio con `yt-dlp`.

```text
data/audio_sources/<video_id>.mp3
data/audio_sources/<video_id>.m4a
data/audio_sources/<video_id>.wav
data/audio_sources/<video_id>.webm
data/audio_sources/<video_id>.mp4
data/video_sources/<video_id>.mp4
data/video_sources/<video_id>.webm
data/video_sources/<video_id>.mkv
data/video_sources/<video_id>.mov
```

`data/audio_sources/` y `data/video_sources/` estan en `.gitignore`; audio y video no se guardan en Git.

### 19.4) Transcribir

```bash
python -m ytb_history.cli transcribe-selected-videos \
  --data-dir data \
  --include-forced \
  --ranked-limit 10 \
  --audio-source-dir data/audio_sources \
  --video-source-dir data/video_sources \
  --model gpt-4o-mini-transcribe
```

Opciones utiles para descargas locales con `yt-dlp`:

```bash
python -m ytb_history.cli transcribe-selected-videos \
  --data-dir data \
  --limit 10 \
  --ytdlp-browser chrome \
  --ytdlp-extra-args "--force-ipv4"
```

Tambien acepta `--ytdlp-cookies-file`, `--ytdlp-cookies-b64` o la variable `YTDLP_COOKIES_B64`. Usa `--no-ytdlp-fallback` si quieres exigir solo media local.

Para validar sin OpenAI:

```bash
python -m ytb_history.cli transcribe-selected-videos --data-dir data --limit 10 --dry-run
```

Si falta `OPENAI_API_KEY` y no es dry-run, el comando devuelve `skipped_missing_api_key` con exit code 0.

### 19.5) Generar insights

```bash
python -m ytb_history.cli generate-transcript-insights --data-dir data --limit 10 --model gpt-5.5-mini
```

Dry-run:

```bash
python -m ytb_history.cli generate-transcript-insights --data-dir data --limit 10 --model gpt-5.5-mini --dry-run
```

Los insights usan Structured Outputs con schema `transcript_insights_v1`, cache por `text_sha256` y no modifican `transcript.txt`.

### 19.6) Reporte de registry

```bash
python -m ytb_history.cli transcript-registry-report --data-dir data
```

### 19.7) Archivos generados

- `data/transcripts/transcript_registry.jsonl`
- `data/transcripts/transcript_queue.jsonl`
- `data/transcripts/transcript_selection_report.json`
- `data/transcripts/transcription_run_report.json`
- `data/transcripts/transcript_insights_index.jsonl`
- `data/transcripts/transcript_insights_run_report.json`
- `data/transcripts/videos/<video_id>/transcript.txt`
- `data/transcripts/videos/<video_id>/transcript_metadata.json`
- `data/transcripts/videos/<video_id>/transcript_insights.json`

Los TXT, metadata e insights se guardan permanentemente en el repo. El audio no.

## 20) Interpretacion de status

- `success`
- `success_with_warnings`
- `skipped_missing_api_key`
- `aborted_quota_guardrail`
- `failed`

## 21) Troubleshooting

- **Missing YOUTUBE_API_KEY**: define variable local o secret en Actions para el monitor.
- **Missing OPENAI_API_KEY**: define variable local o secret en Actions para transcripcion/insights reales; dry-run no la necesita.
- **quota guardrail abort**: revisa `operational_quota_limit` y tamano de corrida.
- **canal no resoluble**: valida URL/ID del canal en `config/channels.py`.
- **video unavailable/private/deleted**: revisar `channel_errors.jsonl` y reportes.
- **skipped_no_audio_source**: coloca audio en `data/audio_sources/`, video en `data/video_sources/`, revisa `yt-dlp`/cookies o ejecuta el diagnostico local.
- **ffmpeg_not_available**: instala `ffmpeg` o instala el extra `transcription` para usar `imageio-ffmpeg`.
- **auth_required en yt-dlp**: usa `--ytdlp-browser chrome`, `--ytdlp-cookies-file` o `YTDLP_COOKIES_B64`.
- **no changes to commit**: comportamiento esperado si no hubo cambios publicables.

## 22) Seguridad

- No guardar API keys en el repositorio.
- No imprimir secrets en logs.
- No usar `search.list` en flujo normal.
- Descargar audio/video solo en ejecuciones locales autorizadas y dentro de rutas ignoradas.
- No guardar audio ni video en Git.

## 23) Automatizacion Local De Transcripcion

Para encadenar seleccion, transcripcion desde fuentes locales autorizadas, insights y sincronizacion con Git desde tu entorno local en una sola corrida manual:

```bash
python -m ytb_history.cli run-local-transcription-automation \
  --repo-dir . \
  --data-dir data \
  --skip-youtube-refresh \
  --limit 10
```

La automatizacion intenta sincronizar el repo al inicio con modo seguro. Si el worktree esta limpio, usa `git pull --rebase --autostash`; si hay cambios locales, no hace pull y deja el reporte en `build/local_automation/latest_sync_report.json`. Por defecto bloquea la transcripcion con `blocked_dirty_worktree`; con `--allow-stale-repo` continua usando la copia local, pero no publica cambios con Git.

Comandos de apoyo:

```bash
python -m ytb_history.cli sync-local-repo --repo-dir . --check-only
python -m ytb_history.cli diagnose-local-transcription --repo-dir .
```

Para registrar la ejecucion automatica local en Windows Task Scheduler:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\register_local_transcription_task.ps1
```

El script ejecutado por la tarea es `scripts/run_local_transcription_automation.ps1`. Mantiene `schedule_state.json`, `latest_sync_report.json`, `latest_run_report.json` y logs en `build/local_automation/`, evita ejecuciones solapadas con un lock local y no guarda secretos. El registro crea sync diario a las 08:00, sync lunes/jueves a las 08:45, catch-up al iniciar sesion, intento al desbloquear sesion y heartbeat cada 3 horas.
