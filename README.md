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

Si además vas a descargar audio y generar transcripciones/insights localmente:

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

## 12.2.1) Generar Opportunity Radar comercial

```bash
python -m ytb_history.cli generate-opportunity-radar
```

Genera un entregable comercial en `data/commercial_radar/<perfil>/` usando solo artefactos ya existentes. El radar esta pensado para vender claridad editorial semanal, no datos crudos:
- `latest_opportunity_radar.md`
- `latest_opportunity_radar.html`
- `latest_opportunity_radar.json`

La implementacion comercial completa esta documentada en [`README_OPPORTUNITY_RADAR.md`](README_OPPORTUNITY_RADAR.md). El dashboard web tambien incluye una pestaña **Radar** que muestra la interfaz de usuario final para prospectos y clientes.

La configuracion comercial vive en `config/commercial_radar.yaml`. El perfil inicial es `spanish_business_ai` y representa el paquete **Weekly YouTube Opportunity Radar** para negocios, finanzas e IA en espanol.

Opciones utiles:

```bash
python -m ytb_history.cli generate-opportunity-radar --profile spanish_business_ai
python -m ytb_history.cli generate-opportunity-radar --anonymize --output-dir data/commercial_radar/demo
```

Notas comerciales y de compliance:
- El radar expone insights derivados: oportunidades, senales, patrones, paquetes creativos y quota proxy.
- No debe venderse como feed crudo de YouTube ni como garantia de views.
- La transcripcion queda fuera del entregable base y solo debe usarse con videos propios, autorizados o provistos por el cliente.

## 12.3) Construir dataset supervisado model-ready

```bash
python -m ytb_history.cli build-model-dataset
python -m ytb_history.cli build-model-dataset --content-format shorts
python -m ytb_history.cli build-model-dataset --content-format videos
```

Genera artefactos de preparación para modelado supervisado en `data/modeling/`:
- `supervised_examples.csv`
- `feature_dictionary.json`
- `target_dictionary.json`
- `leakage_audit.json`
- `model_readiness_report.json`
- `latest_inference_examples.csv`

Este comando prepara dataset supervisado y auditorías de readiness, pero **no entrena** modelos productivos todavía.



Por defecto tambien genera contratos separados en `data/modeling/formats/shorts/` y `data/modeling/formats/videos/`. `content_format` segmenta el dataset, no se usa como feature, y `is_short` queda excluido de ML.

Regla conservadora: `shorts` si `duration_seconds <= 60`; `shorts` si `61..180`, `upload_date >= 2024-10-15` y metadata/titulo/tags indican Shorts; `videos` para el resto con duracion valida; `unknown` si falta duracion. Referencia: [YouTube Help sobre Shorts de 3 minutos](https://support.google.com/youtube/answer/15424877?hl=en).

## 12.3.1) Analizar model readiness diagnostics

```bash
python -m ytb_history.cli analyze-model-readiness --data-dir data
python -m ytb_history.cli analyze-model-readiness --data-dir data --content-format shorts
```

Genera diagnóstico explícito de madurez de entrenamiento en `data/modeling/`:
- `latest_model_readiness_diagnostics.json`
- `latest_model_readiness_timeline.csv`
- `latest_target_coverage_report.csv`
- `latest_training_gap_report.json`
- `latest_model_readiness_report.md`
- `latest_model_readiness_report.html`

Este comando **explica por qué el entrenamiento está bloqueado**, no llama YouTube API y **no entrena modelos**.

Cuando existen splits, tambien escribe diagnosticos en `data/modeling/formats/shorts/` y `data/modeling/formats/videos/`.

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

Contrato Shorts/Videos: entrena desde `data/modeling/formats/shorts/` y `data/modeling/formats/videos/`; no mezcla formatos para entrenamiento, ranking predictivo ni explicabilidad.

```bash
python -m ytb_history.cli train-content-driver-models
```

Entrena modelos supervisados (Random Forest, lineal regularizado y árbol shallow) con split temporal usando los datasets por formato + features NLP/tópicas cuando existen. El CSV raiz `data/modeling/supervised_examples.csv` queda como vista combinada/compatibilidad, no como entrada productiva para mezclar Shorts con videos.

Genera reportes en `data/model_reports/`:
- `latest_content_driver_leaderboard.csv`
- `latest_content_driver_feature_importance.csv`
- `latest_content_driver_feature_direction.csv`
- `latest_content_driver_group_importance.csv`
- `latest_content_driver_report.md`
- `latest_content_driver_report.html`

Los artefactos por formato viven en `build/content_driver_artifact/formats/shorts/` y `build/content_driver_artifact/formats/videos/`; los reportes raiz son vistas combinadas con columna `content_format`.

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

El workflow de Pages construye `build-analytics` → `build-nlp-features` → `generate-alerts` → `build-decision-layer` → `build-model-intelligence` → `build-topic-intelligence` → `generate-creative-packages` → `generate-weekly-brief` → `generate-opportunity-radar` → `build-operations` → `build-pages-dashboard` y publica únicamente el artefacto `site/` (incluye tabs Radar, Creative y Operations).

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
- `data/commercial_radar/<perfil>/latest_opportunity_radar.md`
- `data/commercial_radar/<perfil>/latest_opportunity_radar.html`
- `data/commercial_radar/<perfil>/latest_opportunity_radar.json`
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

GitHub Actions ya no ejecuta transcripción local ni pasos dependientes de `yt-dlp`, `ffmpeg`, cookies de navegador u `OPENAI_API_KEY`. La transcripción, sus insights y la regeneración del registro de transcripciones quedan bajo responsabilidad del equipo en un entorno local/controlado.
- Corre manual (`workflow_dispatch`) y diario (`schedule`).
- Cron configurado: `17 9 * * *` (UTC).
  - Referencia: **09:17 UTC** ≈ **03:17 en America/Matamoros** dependiendo del horario local.
- Ejecuta en orden: `compile`, `pytest -q`, `dry-run`, `run`, `validate-latest`, `export-latest`, `build-analytics`, `build-nlp-features`, `generate-alerts`, `build-decision-layer`, `build-model-intelligence`, `build-topic-intelligence`, `generate-creative-packages`, `generate-weekly-brief`, `generate-opportunity-radar`, `select-transcription-candidates`.
- Valida únicamente el secret `YOUTUBE_API_KEY` y lo usa desde GitHub Secrets **solo** en el paso `run`.
- Hace commit únicamente cuando hay cambios en `data/` (stagea solo `data/`).

Configurar el secret en GitHub:
1. `Settings` > `Secrets and variables` > `Actions`
2. `New repository secret`
3. Name: `YOUTUBE_API_KEY`
4. Value: tu API key

### Transcripción local (responsabilidad del equipo)

La transcripción ya no forma parte de `.github/workflows/monitor.yml`. Si el equipo necesita transcribir, generar insights o reconstruir el registro, debe ejecutar localmente los comandos correspondientes y gestionar de forma segura `OPENAI_API_KEY`, `yt-dlp`, `ffmpeg`, cookies y argumentos adicionales fuera de GitHub Actions.

Prerrequisito local para transcripción (mismo entorno virtual del proyecto):
```bash
python -m pip install -e ".[transcription]"
yt-dlp --version
```

La extra `transcription` instala `openai`, `yt-dlp` e `imageio-ffmpeg`. El runner intenta resolver `yt-dlp` desde `PATH` o desde el mismo entorno Python (`python -m yt_dlp`) y usa `ffmpeg` del sistema cuando existe; si no, puede reutilizar el binario gestionado por `imageio-ffmpeg`. Si prefieres un binario propio, puedes definir `YTDLP_FFMPEG_LOCATION`.

GitHub Actions puede dejar preparada la cola diaria (`data/transcripts/transcript_queue.jsonl`) con `select-transcription-candidates`, pero la descarga de audio, transcripciÃ³n e insights siguen ejecutÃ¡ndose solo en entorno local/controlado.

Flujo local sugerido para mantener artefactos de transcripción/insights y registro:
```bash
python -m ytb_history.cli transcribe-selected-videos --data-dir data --limit 10 --audio-source-dir data/audio_sources
python -m ytb_history.cli generate-transcript-insights --data-dir data --limit 10
python -m ytb_history.cli transcript-registry-report --data-dir data
```

Si por alguna razÃ³n necesitas regenerar la cola manualmente fuera de Actions:
```bash
python -m ytb_history.cli select-transcription-candidates --data-dir data --limit 10
```

El fallback automático de descarga de audio empieza con los defaults nativos de `yt-dlp` (equivalente al patrón Colab `yt-dlp -x --audio-format mp3 ...`) y luego prueba clientes YouTube explícitos (`android`, `ios`, `mweb`, `tv_simply`, `web`) si hace falta.

Si el reporte muestra `used_cookies_file: true` pero también `ytdlp_auth_required_despite_cookies`, el archivo sí fue pasado a `yt-dlp`, pero YouTube no aceptó esa sesión desde el entorno de ejecución; rota/exporta nuevamente cookies o valida el mismo `cookies.txt` con `yt-dlp --cookies cookies.txt -F URL` en el entorno donde falla. El reporte incluye `ytdlp_cookies_file_diagnostics` sin valores secretos (existencia, tamaño y conteos de filas YouTube/Google/expiradas) para detectar secrets mal codificados o cookies vencidas. Si se repiten fallos de autenticación con cookies, la corrida abre `ytdlp_auth_required_circuit_open` después de 3 intentos consecutivos para no gastar tiempo ni llenar el registro con los 10 videos de la cola hasta que se roten las cookies.

La cola de transcripción descarta IDs con formato de canal (`UC...`) cuando aparecen por error en campos `video_id`/`source_video_id`; esos casos se reportan como `invalid_video_ids_skipped` o `skipped_invalid_video_id` para evitar llamadas inútiles a `yt-dlp` contra URLs `watch?v=<channel_id>`.

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

⚠️ **Seguridad**: nunca commitear `cookies.txt` ni credenciales derivadas. Mantener estos archivos fuera del repositorio y gestionarlos únicamente en entornos locales/controlados.

Si prefieres usar `ffmpeg` del sistema, instalarlo y verificar disponibilidad en PATH:
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
  - `failed_audio_download_auth_required`: normalmente requiere cookies/sesión (`--ytdlp-cookies-file` o `--ytdlp-browser`). En el entorno local/controlado, rota o reexporta `cookies.txt`; revisa `ytdlp_cookies_file_diagnostics`, `ytdlp_auth_required_despite_cookies` y `ytdlp_auth_required_circuit_open` para diferenciar cookies mal formadas de una sesión expirada/no aceptada por YouTube. Estos fallos entran en cooldown para no repetirse cada día sin cookies válidas.
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

## 22) Automatizacion Local De Sync Y Transcripcion

La automatizacion local esta separada en dos pasos para evitar pulls peligrosos mientras el repositorio tiene cambios locales:

1. Sincronizar el repo con un fast-forward seguro.
2. Transcribir usando la cola local ya sincronizada.

Para revisar si hay commits nuevos de `origin/main` y hacer pull solo cuando sea seguro:

```bash
python -m ytb_history.cli sync-local-repo --repo-dir .
```

El sync ejecuta `git ls-remote origin refs/heads/main`, compara contra `HEAD`, bloquea si hay cambios tracked locales o commits locales no publicados, y solo usa `git pull --ff-only` cuando el worktree esta limpio. Nunca ejecuta `reset`, `clean`, `checkout` destructivo ni sobrescribe cambios locales. El reporte queda en `build/local_automation/latest_sync_report.json`.

Para encadenar seleccion, transcripcion, insights y publicacion de transcripciones desde tu entorno local:

```bash
python -m ytb_history.cli run-local-transcription-automation \
  --repo-dir . \
  --data-dir data \
  --skip-youtube-refresh \
  --limit 10
```

La transcripcion ya no hace pull directo. Primero lee `build/local_automation/latest_sync_report.json` y solo continua si el sync termino como `success`, `up_to_date` o `skipped_recent_success`. Si el sync quedo bloqueado por cambios locales, la transcripcion termina como `blocked_sync_dirty_worktree`; para una corrida manual consciente se puede usar `--allow-stale-repo`.

Solo se intenta `commit` + `push` cuando se generan resultados publicables nuevos dentro de `data/transcripts/`. Los audios descargados por `yt-dlp` y los videos locales se tratan como cache local en `data/audio_sources/` y `data/video_sources/`: no se versionan y el repo no depende de que existan.

La resolucion de media sigue esta cascada:

- Audio local en `data/audio_sources/<video_id>.*`.
- Video local en `data/video_sources/<video_id>.*`, extrayendo audio con `ffmpeg` o `imageio-ffmpeg`.
- Descarga de audio con `yt-dlp`.

Si OpenAI rechaza un audio por `input_too_large`, el runner segmenta el audio y concatena las transcripciones parciales. Los errores transitorios de OpenAI se reintentan con backoff.

Para diagnosticar el entorno local sin imprimir secretos:

```bash
python -m ytb_history.cli diagnose-local-transcription --repo-dir .
```

El diagnostico reporta tareas programadas, ultimo sync, SHA local/remoto, estado del worktree, cola, canales obligatorios, audios/videos disponibles, presencia de `OPENAI_API_KEY`, `yt-dlp`, `ffmpeg` e `imageio_ffmpeg`. El JSON queda en `build/local_automation/latest_diagnosis.json`.

Para tener un boton manual de "play" en Windows:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\install_local_play_shortcut.ps1
```

Esto crea un acceso directo en el Escritorio llamado `YTB History - Play Local Automation`. Al abrirlo, ejecuta `scripts/run_local_play.ps1`, que fuerza una corrida manual fuera del horario programado: primero sincroniza el repo de forma segura y despues lanza la transcripcion/publicacion. La ventana queda abierta al final para revisar el resultado. El reporte queda en `build/local_automation/latest_play_report.json` y el log en `build/local_automation/logs/manual_play_*.log`.

El modo "play" muestra progreso en espanol con porcentajes y no imprime el JSON tecnico completo en pantalla. Los JSON completos siguen disponibles para diagnostico:

- `build/local_automation/latest_sync_report.json`: estado de sync y SHA local/remoto.
- `build/local_automation/latest_run_report.json`: seleccion, transcripcion, insights y Git.
- `data/transcripts/transcript_selection_report.json`: por que la cola tuvo o no tuvo `10 + forzados`.
- `data/transcripts/transcription_run_report.json`: por video, origen de audio y errores de `yt-dlp`/OpenAI.

En la seleccion, `--limit 10` significa 10 videos del ranking diario. Los videos de canales forzados se agregan encima de esos 10 cuando existen en los artefactos locales, no estan ya transcritos, no estan en cooldown y caen en la ventana configurada. La automatizacion local transcribe toda la cola seleccionada, asi que una cola de `10 + forzados` ya no se corta artificialmente en 10.

Para registrar la ejecucion automatica local en Windows Task Scheduler:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\register_local_transcription_task.ps1
```

Esto crea cuatro tareas:

- `YtbHistoryLocalRepoSync`: cada 6 horas desde 09:00 hasta 23:00; en la practica 09:00, 15:00 y 21:00.
- `YtbHistoryLocalRepoSyncLogonCatchup`: al iniciar sesion, solo si esta dentro de la ventana 09:00-23:00 y no hubo sync exitoso reciente.
- `YtbHistoryLocalTranscription`: cada 6 horas desde 09:20 hasta 23:00; en la practica 09:20, 15:20 y 21:20.
- `YtbHistoryLocalTranscriptionLogonCatchup`: al iniciar sesion, solo si esta dentro de la ventana 09:00-23:00 y no hubo transcripcion exitosa reciente.

Los scripts ejecutados son `scripts/run_local_repo_sync.ps1` y `scripts/run_local_transcription_automation.ps1`. Mantienen estado local en `build/local_automation/sync_state.json` y `build/local_automation/schedule_state.json`, escriben logs en `build/local_automation/logs/`, evitan ejecuciones solapadas con locks locales y no guardan secretos. Por seguridad, las tareas corren como tu usuario interactivo; si Windows exige correr antes del inicio de sesion, habria que usar una cuenta/credencial administrada por Task Scheduler, no guardarla en el repositorio.

Si `yt-dlp` reporta que no pudo copiar la base de cookies del navegador, primero prueba sin `--ytdlp-browser`; en esta maquina la descarga sin cookies funciono mejor. Usa cookies de navegador o `cookies.txt` solo si YouTube empieza a exigir autenticacion.
