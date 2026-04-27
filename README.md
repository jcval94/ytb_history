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

Genera una carpeta particionada en `data/exports/dt=YYYY-MM-DD/run=HHMMSSZ/` con:
- `latest_snapshots.csv`
- `latest_deltas.csv`
- `video_growth_summary.csv`
- `export_summary.json`

Notas:
- No llama la API de YouTube.
- No modifica snapshots, deltas ni catálogo actual.
- `tags` en `latest_snapshots.csv` se exporta como **JSON string** estable UTF-8 (por ejemplo: `["python", "datos"]`).

## 10) Tests

```bash
python -m compileall src tests
pytest -q
```

## 11) Configuración de canales

Editar `config/channels.py`:

```python
CHANNEL_URLS = [
    # URLs de canales a monitorear
]
```

## 12) Configuración de settings

Editar `config/settings.yaml`:
- `discovery_window_days`
- `tracking_window_days`
- `youtube_batch_size`
- `operational_quota_limit`
- `max_pages_per_channel`

## 13) Archivos generados

- `data/state/channel_registry.jsonl`
- `data/state/tracked_videos_catalog.jsonl`
- `data/snapshots/dt=YYYY-MM-DD/run=HHMMSSZ/snapshots.jsonl.gz`
- `data/deltas/dt=YYYY-MM-DD/run=HHMMSSZ/deltas.jsonl.gz`
- `data/reports/dt=YYYY-MM-DD/run=HHMMSSZ/quota_report.json`
- `data/reports/dt=YYYY-MM-DD/run=HHMMSSZ/run_summary.json`
- `data/reports/dt=YYYY-MM-DD/run=HHMMSSZ/discovery_report.jsonl`
- `data/reports/dt=YYYY-MM-DD/run=HHMMSSZ/channel_errors.jsonl`

## 14) GitHub Actions

### CI (`.github/workflows/ci.yml`)
- Corre en `push` y `pull_request`.
- Instala el paquete con `src layout` usando `python -m pip install -e .`.
- Ejecuta compilación, tests y `dry-run`.
- No requiere `YOUTUBE_API_KEY`.

### Monitor (`.github/workflows/monitor.yml`)
- Corre manual (`workflow_dispatch`) y diario (`schedule`).
- Cron configurado: `17 9 * * *` (UTC).
  - Referencia: **09:17 UTC** ≈ **03:17 en America/Matamoros** dependiendo del horario local.
- Ejecuta en orden: `compile`, `pytest -q`, `dry-run`, `run`, `validate-latest`, `export-latest`.
- Usa `YOUTUBE_API_KEY` desde GitHub Secrets **solo** en el paso `run`.
- Hace commit únicamente cuando hay cambios en `data/` (stagea solo `data/`).

Configurar el secret en GitHub:
1. `Settings` > `Secrets and variables` > `Actions`
2. `New repository secret`
3. Name: `YOUTUBE_API_KEY`
4. Value: tu API key

## 15) Interpretación de status

- `success`
- `success_with_warnings`
- `aborted_quota_guardrail`
- `failed`

## 16) Troubleshooting

- **Missing YOUTUBE_API_KEY**: define variable local o secret en Actions.
- **quota guardrail abort**: revisa `operational_quota_limit` y tamaño de corrida.
- **canal no resoluble**: valida URL/ID del canal en `config/channels.py`.
- **video unavailable/private/deleted**: revisar `channel_errors.jsonl` y reportes.
- **no changes to commit**: comportamiento esperado si no hubo cambios en `data/`.

## 17) Seguridad

- No guardar API keys en el repositorio.
- No imprimir secrets en logs.
- No usar `search.list` en flujo normal.
