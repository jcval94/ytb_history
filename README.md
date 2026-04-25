# ytb_history

Scaffolding inicial para un sistema en Python que monitorea canales de YouTube, detecta videos nuevos y mantiene snapshots históricos y deltas por ejecución.

## Requisitos

- Python 3.11+
- `YOUTUBE_API_KEY` en variables de entorno (no se usa todavía en llamadas reales)

## Instalación

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Comandos iniciales

```bash
# Ejecutar pipeline (scaffold, sin llamadas reales)
python -m ytb_history.cli run

# Ejecutar tests
pytest -q
```

## Estructura

- `config/`: configuración estática del proyecto.
- `src/ytb_history/`: paquete principal.
- `tests/`: pruebas unitarias e integración.
