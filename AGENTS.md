# AGENTS.md

Instrucciones permanentes para este proyecto.

## Objetivo del proyecto
Sistema en **Python** para monitorear canales de YouTube durante 6 meses usando la **YouTube Data API**. Debe detectar videos nuevos de una lista de canales y mantener seguimiento histórico con snapshots periódicos por video.

## Reglas técnicas obligatorias
- No usar `search.list` en el flujo normal (costo alto de cuota: 100 unidades por request).
- Usar `channels.list` solamente para resolver `channel_id` y `uploads_playlist_id`.
- Usar `playlistItems.list` para descubrir videos nuevos desde el uploads playlist del canal.
- Usar `videos.list` para enriquecer y actualizar métricas de videos en batches de hasta 50 IDs.
- En tests, evitar llamadas reales a APIs externas y usar mocks/stubs/fakes.
- Mantener bajo consumo de cuota y registrar un reporte de cuota estimada en cada ejecución.
- No guardar secretos en el repositorio. La API key debe venir de la variable de entorno `YOUTUBE_API_KEY`.
- El código debe ser modular, testeable y claro.
- Toda ejecución debe poder correr localmente y en GitHub Actions.
- Si hay error parcial con un canal, no debe romper toda la ejecución.
- El sistema debe guardar snapshots históricos comprimidos y un catálogo actual.
- No sobrescribir datos históricos.
- No hacer commit si no hay cambios reales.

## Estilo de desarrollo
- Python 3.11+.
- Código simple, robusto y con type hints.
- Evitar dependencias innecesarias.
- Crear tests con `pytest`.
- Documentar decisiones importantes.
- Preferir `JSONL` o `JSONL.GZ` para snapshots históricos.
- Mantener un `README` con instrucciones claras.

## Proceso antes de implementar cambios grandes
1. Leer la estructura actual del repositorio.
2. Proponer un plan corto.
3. Implementar en pasos.
4. Ejecutar tests o validaciones.
5. Resumir qué cambió y qué falta.

## Criterios de resiliencia y datos
- Diseñar el pipeline para tolerar fallos parciales por canal y continuar con el resto.
- Registrar errores por canal con contexto suficiente para reintentos.
- Mantener historial inmutable: agregar nuevos snapshots, nunca reescribir snapshots previos.
- Mantener un catálogo actual derivado del histórico o actualizado incrementalmente sin pérdida de trazabilidad.

## Notas de cuota (guía rápida)
- Priorizar lecturas incrementales y batches máximos permitidos por la API.
- Evitar llamadas redundantes para entidades ya resueltas (cachear `channel_id` y `uploads_playlist_id` cuando aplique).
- Incluir en cada corrida un resumen de cuota estimada por endpoint y total.
