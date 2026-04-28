# Bootstrap de model reports (GitHub Actions)

Orden recomendado de ejecución manual (`workflow_dispatch`):

1. `YouTube Monitor` (`monitor.yml`)
2. `Train Model Suite` (`train_model.yml`)
3. `Predict With Model Artifact` (`predict_model.yml`)
4. `Deploy Dashboard to GitHub Pages` (`pages.yml`)

## Disparar workflows con GitHub CLI

```bash
gh workflow run monitor.yml
gh workflow run train_model.yml
gh workflow run predict_model.yml
gh workflow run pages.yml
```

## Verificación local de artefactos versionados

```bash
python scripts/verify_model_reports_bootstrap.py
```

El script valida:

- existencia de los 9 archivos requeridos bajo `data/model_reports/`.
- si existe `site/data/site_manifest.json`, muestra total de warnings y los warnings que contengan `model_reports`.

## Logs a revisar si faltan reportes

En `train-model.yml`, revisar especialmente:

- `Train content driver models`
- `Train model suite`
- `Model artifact registry report`
