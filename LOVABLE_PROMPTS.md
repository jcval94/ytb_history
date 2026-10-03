# Prompts para construir YTB History en Lovable

Este documento contiene una secuencia de prompts lista para copiar y pegar en Lovable. Deben ejecutarse en orden dentro del mismo proyecto, porque cada prompt asume que los anteriores ya fueron implementados.

## Cómo usar esta secuencia

1. Ejecuta un prompt por vez.
2. Antes de avanzar, revisa que Lovable haya terminado la implementación, aplicado las migraciones necesarias y corregido los errores de compilación.
3. No combines todos los prompts en una sola solicitud: la separación permite validar autenticación, seguridad, datos y módulos de forma incremental.
4. Si Lovable propone cambiar el modelo de datos o los permisos, pídele que respete los contratos definidos aquí salvo que exista un bloqueo técnico concreto.
5. Los prompts evitan indicar posiciones, composiciones visuales o imágenes. Lovable debe conservar la estética del proyecto y decidir la mejor resolución visual.

---

## Prompt 1 — Contexto maestro y reglas no negociables

```text
Quiero construir YTB History como una aplicación SaaS privada de inteligencia para YouTube. Implementa los cambios, no te limites a explicarlos.

Contexto funcional:
- El producto monitorea canales y videos de YouTube durante periodos prolongados.
- Un pipeline externo en Python descubre videos, actualiza métricas, genera snapshots históricos inmutables y produce artefactos analíticos.
- El producto presenta rendimiento de videos y canales, scores, señales, alertas, tendencias, NLP, modelos, oportunidades editoriales, paquetes creativos, briefs semanales y observabilidad de procesos.
- La aplicación será multiempresa: cada cliente trabaja dentro de una organización y nunca debe acceder a datos de otra organización.
- Toda la interfaz, validaciones, errores y mensajes deben estar en español.

Reglas técnicas no negociables:
- Usa React y TypeScript con la base tecnológica ya existente en el proyecto Lovable.
- Usa Supabase para autenticación, base de datos y almacenamiento privado.
- No llames a la YouTube Data API desde el navegador ni desde esta aplicación.
- No implementes search.list ni scraping.
- Nunca expongas YOUTUBE_API_KEY, OPENAI_API_KEY ni SUPABASE_SERVICE_ROLE_KEY en el frontend.
- La clave anon de Supabase puede estar en el cliente; la service role solamente puede existir en funciones seguras o en el pipeline externo.
- El pipeline Python seguirá siendo la fuente de verdad de los datos analíticos.
- Conserva compatibilidad con los contratos analytics_v1 y pages_dashboard_v1.
- El historial analítico es append-only: una corrida publicada no se edita ni se sobrescribe.
- Los fallos parciales y los artefactos faltantes deben mostrarse con contexto, sin romper toda la aplicación.
- No inventes métricas, cifras, clientes, videos, canales, predicciones ni resultados.
- No uses localStorage como fuente de autorización. Los permisos reales deben depender de Supabase Auth y RLS.

Dirección de producto y diseño:
- Conserva la estética que ya exista en el proyecto. No hagas un rediseño gratuito.
- Si necesitas completar elementos nuevos, mantén una apariencia sobria de producto de inteligencia: legible, profesional, con jerarquía tipográfica clara, decoración contenida y visualizaciones orientadas a decisiones.
- No agregues imágenes decorativas, fotografías de stock ni ilustraciones generadas.
- No conviertas tablas densas en colecciones de tarjetas si eso reduce su capacidad de análisis.
- Usa gráficas solamente cuando aporten una lectura que la tabla no ofrece rápidamente.
- No uses color como única forma de comunicar estado o importancia.
- Respeta prefers-reduced-motion y evita animaciones necesarias para entender los datos.

Forma de trabajo:
- Revisa primero el código actual y reutiliza componentes, tokens y convenciones existentes.
- Mantén componentes pequeños, tipos estrictos y responsabilidades separadas.
- Cada cambio debe incluir estados de carga, vacío, error, acceso denegado y éxito cuando correspondan.
- No elimines funcionalidad ya operativa.
- Al finalizar cada fase, ejecuta compilación, lint y pruebas disponibles; corrige cualquier error antes de responder.
- En tu respuesta final de cada fase, enumera cambios realizados, migraciones aplicadas, pruebas ejecutadas y cualquier limitación real pendiente.

En esta primera fase, registra estas reglas como contexto del proyecto, inspecciona la implementación actual y prepara la base para las siguientes fases sin inventar pantallas de demostración ni datos ficticios. Si ya existe una parte de esta arquitectura, consérvala y adáptala en lugar de duplicarla.
```

---

## Prompt 2 — Arquitectura de aplicación y contratos compartidos

```text
Continúa sobre el proyecto actual y aplica todas las reglas del contexto maestro.

Implementa la arquitectura base de la aplicación SaaS. Usa las librerías que ya estén instaladas; si faltan capacidades equivalentes, utiliza React Router para rutas, TanStack Query para datos remotos y Zod para validar payloads en tiempo de ejecución. No reemplaces herramientas equivalentes que ya funcionen.

La arquitectura debe incluir:
- Un proveedor de autenticación que represente claramente loading, authenticated y unauthenticated.
- Un proveedor de organización activa que cargue las membresías del usuario, recuerde de forma segura la preferencia de organización y valide que siga siendo accesible.
- Protección de rutas por sesión, membresía y rol.
- Una capa de acceso a datos independiente de los componentes visuales.
- Un cliente Supabase único y tipado.
- TanStack Query configurado con políticas razonables de reintento, invalidación y caché.
- Un error boundary global y límites de error para módulos de datos pesados.
- Carga diferida por módulo; no cargues todos los artefactos analíticos al iniciar sesión.
- Estado de filtros compartidos serializable en la URL para que una vista filtrada pueda recargarse o compartirse.

Define como mínimo estos tipos de dominio, sin usar any:
- Profile
- Organization
- OrganizationRole: owner | admin | analyst | viewer
- OrganizationMembership
- OrganizationInvitation
- MonitoredChannel
- AnalyticsRunStatus: pending | processing | completed | failed
- AnalyticsRun
- AnalyticsArtifact
- DataReadinessState: ready | stale | partial | not_initialized | failed
- DashboardBundleManifest
- TableArtifact<T> con name, generated_at, row_count, columns y rows

Define guards reutilizables para:
- Requerir sesión.
- Requerir una organización activa.
- Requerir uno o varios roles.
- Mostrar acceso denegado sin ocultar errores reales de carga.

La aplicación debe distinguir entre:
- Error de autenticación.
- Usuario autenticado sin organización.
- Falta de permisos.
- Corrida analítica inexistente.
- Corrida fallida.
- Bundle incompatible.
- Artefacto opcional ausente.
- Datos desactualizados.

No construyas todavía todas las vistas analíticas. Deja la navegación y las rutas preparadas con módulos cargados bajo demanda, sin contenido ficticio. Conserva cualquier vista existente que ya funcione.

Criterio de terminado:
- La aplicación compila con TypeScript estricto.
- Las rutas privadas no se renderizan antes de resolver la sesión.
- Los componentes no consultan Supabase directamente: usan servicios o hooks tipados.
- No existe ninguna service role, API key de YouTube ni secreto incrustado en el bundle del navegador.
```

---

## Prompt 3 — Supabase: modelo multiempresa, RLS y Storage privado

```text
Continúa sobre el proyecto actual y aplica todas las reglas del contexto maestro.

Implementa el modelo de datos multiempresa en Supabase mediante migraciones versionadas. Si alguna tabla equivalente ya existe, migra o amplía con cuidado en lugar de duplicarla.

Modelo mínimo:

1. profiles
- id uuid, primary key y foreign key a auth.users(id) con eliminación en cascada.
- full_name text.
- created_at y updated_at timestamptz.

2. organizations
- id uuid primary key.
- name text obligatorio.
- slug text único y normalizado.
- created_by uuid referenciando profiles.
- created_at y updated_at.

3. organization_members
- organization_id uuid.
- user_id uuid.
- role enum: owner, admin, analyst, viewer.
- created_at.
- primary key compuesta por organization_id y user_id.

4. organization_invitations
- id uuid primary key.
- organization_id uuid.
- email normalizado.
- role enum, sin permitir owner en una invitación normal.
- status enum: pending, accepted, revoked, expired.
- invited_by uuid.
- expires_at, accepted_at y created_at.
- No guardes tokens de invitación en texto plano. Usa un hash o el flujo seguro de Supabase Auth desde una Edge Function.

5. monitored_channels
- id uuid primary key.
- organization_id uuid.
- channel_input text obligatorio para URL, handle o channel_id proporcionado por el usuario.
- channel_id text nullable hasta que el pipeline lo resuelva.
- channel_name text nullable.
- uploads_playlist_id text nullable.
- status enum: pending, active, paused, resolution_failed.
- created_by, created_at y updated_at.
- Restricción única adecuada para evitar duplicados dentro de una organización sin impedir que dos organizaciones monitoreen el mismo canal.

6. analytics_runs
- id uuid primary key.
- organization_id uuid.
- run_key text.
- schema_version text.
- status enum: pending, processing, completed, failed.
- generated_at, completed_at y created_at.
- warning_count y error_count enteros no negativos.
- error_summary text nullable, siempre sanitizado y sin secretos.
- unique organization_id + run_key.

7. analytics_artifacts
- id uuid primary key.
- organization_id uuid.
- run_id uuid.
- artifact_key text.
- storage_path text.
- content_type text.
- row_count bigint no negativo.
- size_bytes bigint no negativo.
- checksum text.
- schema_version text.
- created_at.
- unique run_id + artifact_key.
- El registro es inmutable una vez completada la corrida.

8. notification_preferences
- organization_id y user_id como clave lógica.
- Preferencias explícitas para alertas críticas, resumen semanal y avisos de datos stale.
- created_at y updated_at.

9. audit_events
- id uuid primary key.
- organization_id uuid.
- actor_user_id uuid nullable para acciones del sistema.
- action text.
- target_type y target_id.
- metadata jsonb sanitizado.
- created_at.
- Append-only.

Seguridad y permisos:
- Activa RLS en todas las tablas públicas.
- Crea funciones helper seguras para comprobar membresía y roles, con search_path fijo y sin recursión de políticas.
- owner: control total, incluida transferencia de propiedad y eliminación de la organización.
- admin: administra configuración, canales e integrantes que no sean owner; no puede asignar ni retirar el rol owner.
- analyst: lee todos los datos analíticos y puede administrar la lista de canales monitoreados, pero no usuarios ni configuración sensible.
- viewer: acceso de solo lectura a los datos de su organización.
- Un usuario solamente puede leer su propio perfil y los perfiles de integrantes de organizaciones compartidas.
- analytics_runs y analytics_artifacts solamente pueden escribirse con service role desde el pipeline o una función segura. Los miembros solo pueden leer corridas completed de sus organizaciones.
- audit_events no pueden actualizarse ni eliminarse desde el cliente.
- Evita que cualquier operación deje a una organización sin owner.

Storage:
- Crea un bucket privado llamado analytics-artifacts.
- Usa la ruta {organization_id}/runs/{run_id}/{artifact_key}.{extension}.
- Las políticas de Storage deben validar la membresía a partir del primer segmento de la ruta.
- Los clientes autenticados solo pueden leer objetos de sus organizaciones.
- El navegador no puede subir, reemplazar ni borrar bundles analíticos.
- El pipeline con service role puede publicar nuevos objetos, pero nunca sobrescribir una ruta existente.

Automatización:
- Crea el perfil al registrarse usando un trigger seguro o una función transaccional.
- Añade updated_at donde corresponda.
- Añade índices para membresías, invitaciones pendientes, canales por organización, últimas corridas completed y artefactos por corrida.

Incluye pruebas o verificaciones SQL con dos usuarios de organizaciones distintas. Demuestra que no pueden leer membresías, corridas, artefactos ni objetos de Storage entre organizaciones.
```

---

## Prompt 4 — Autenticación y onboarding

```text
Continúa sobre el proyecto actual y aplica todas las reglas del contexto maestro.

Implementa los flujos completos de autenticación en español usando Supabase Auth:
- Registro con nombre, correo y contraseña.
- Verificación de correo.
- Inicio de sesión.
- Cierre de sesión.
- Solicitud de recuperación de contraseña.
- Establecimiento de nueva contraseña desde un enlace válido.
- Manejo de enlaces vencidos, reutilizados o inválidos.
- Restauración y actualización segura de sesión.
- Aceptación de invitaciones tanto para usuarios existentes como para usuarios nuevos.

Onboarding:
- Un registro directo crea un perfil y permite crear la primera organización mediante una operación transaccional segura.
- El creador se convierte en owner.
- Si el usuario llegó mediante invitación, primero completa su perfil y se integra a la organización invitante; no crees una organización adicional automáticamente.
- Un usuario autenticado sin membresías debe ver una recuperación clara: crear organización o aceptar una invitación válida.
- La creación de organización debe validar nombre y slug, resolver colisiones y evitar estados parcialmente creados.

Seguridad y experiencia:
- No reveles si un correo existe durante la recuperación de contraseña.
- No dependas únicamente de controles ocultos en la UI; todas las operaciones deben estar respaldadas por RLS, RPC o Edge Functions.
- No guardes contraseñas, tokens o sesiones manualmente.
- Conserva returnTo solamente para rutas internas validadas; evita redirecciones abiertas.
- Al expirar una sesión, conserva de forma segura la intención de navegación y solicita iniciar sesión de nuevo.
- Presenta mensajes accionables en español y conserva los detalles técnicos en logs sanitizados, no en la interfaz.
- Deshabilita envíos duplicados mientras una operación esté en curso.
- Incluye confirmación visible de éxito para registro, recuperación y cambio de contraseña.

Implementa pruebas de los caminos principales y de error. Verifica manualmente registro, confirmación, login, logout, recuperación, sesión vencida e invitación.
```

---

## Prompt 5 — Organizaciones, perfiles y gestión de usuarios

```text
Continúa sobre el proyecto actual y aplica todas las reglas del contexto maestro.

Implementa la administración multiempresa y de usuarios sin definir una composición visual rígida. Reutiliza la estética y los componentes existentes.

Capacidades requeridas:
- Consultar y editar el nombre del perfil propio.
- Consultar las organizaciones a las que pertenece el usuario.
- Cambiar de organización activa sin mezclar caché ni datos entre organizaciones.
- Consultar datos generales de la organización activa.
- Para owner y admin: listar integrantes e invitaciones pendientes.
- Invitar por correo con rol admin, analyst o viewer según los permisos del actor.
- Reenviar o revocar una invitación pendiente.
- Cambiar roles respetando la matriz de permisos.
- Retirar integrantes con confirmación explícita.
- Permitir que un usuario abandone una organización si no es su último owner.
- Transferir propiedad mediante una operación explícita y atómica entre dos integrantes existentes.
- Impedir retirar, degradar o abandonar al último owner.
- Registrar en audit_events las invitaciones, revocaciones, cambios de rol, altas, bajas, transferencias y cambios de configuración.

Implementa las operaciones sensibles mediante RPC o Edge Functions transaccionales. No encadenes varias escrituras desde el cliente si un fallo intermedio puede dejar permisos inconsistentes.

Las Edge Functions que usen capacidades administrativas de Supabase Auth deben:
- Verificar el JWT del solicitante.
- Resolver la organización desde el payload y comprobar el rol dentro de la función.
- Validar el cuerpo con un esquema estricto.
- No aceptar actor_user_id ni permisos declarados por el cliente.
- No devolver secretos ni datos de otras organizaciones.
- Ser idempotentes cuando sea razonable.

Incluye estados para organización sin integrantes adicionales, invitación duplicada, correo inválido, invitación vencida, pérdida de permisos durante la operación y conflicto al modificar al último owner.

Al cambiar de organización:
- Cancela o invalida queries de la organización anterior.
- Limpia filtros que hagan referencia a canales inaccesibles.
- Carga de nuevo permisos, corrida actual y preferencias.
- Nunca muestres durante la transición datos de la organización anterior.
```

---

## Prompt 6 — Integración de bundles analíticos privados

```text
Continúa sobre el proyecto actual y aplica todas las reglas del contexto maestro.

Implementa la capa que conecta el frontend con los bundles privados producidos por el pipeline Python.

Flujo esperado:
1. El pipeline crea una fila analytics_runs con status processing usando service role.
2. Publica cada artefacto en una ruta nueva e inmutable del bucket analytics-artifacts.
3. Registra analytics_artifacts con checksum, tamaño, row_count, content_type y schema_version.
4. Cuando todos los artefactos obligatorios fueron verificados, marca la corrida completed.
5. El frontend consulta la corrida completed más reciente de la organización activa.
6. El frontend obtiene cada artefacto de forma diferida mediante acceso privado autorizado o URL firmada de vida corta.

No implementes la publicación con service role dentro del navegador. Documenta el contrato que debe usar el pipeline externo y deja una utilidad o ejemplo server-side separado, sin credenciales reales.

Contratos de lectura:
- Las tablas CSV convertidas a JSON usan el envelope: name, generated_at, row_count, columns, rows.
- Los reportes estructurados pueden ser objetos JSON específicos de dominio.
- Los HTML derivados deben tratarse como contenido no confiable: no renderices HTML arbitrario con dangerouslySetInnerHTML. Prefiere reconstruir la vista desde JSON. Si se necesita visualizar un HTML legado, sanitízalo con una biblioteca mantenida y sin scripts, eventos inline, iframes ni URLs peligrosas.
- Valida todos los payloads con Zod antes de entregarlos a la UI.
- Comprueba schema_version y reporta claramente versiones incompatibles.

Artefactos existentes que la capa debe reconocer:

Analítica principal:
- dashboard_index
- site_manifest
- latest_video_metrics
- latest_channel_metrics
- latest_video_scores
- latest_video_advanced_metrics
- latest_channel_advanced_metrics
- latest_title_metrics
- latest_metric_eligibility
- channel_baselines
- video_lifecycle_metrics
- period_daily_video_metrics
- period_weekly_video_metrics
- period_monthly_video_metrics
- period_daily_channel_metrics
- period_weekly_channel_metrics
- period_monthly_channel_metrics

Señales y alertas:
- latest_video_signals
- latest_channel_signals
- latest_signal_candidates
- signal_summary
- latest_alerts
- alert_summary

Modelos:
- latest_model_manifest
- latest_model_leaderboard
- latest_feature_importance
- latest_feature_direction
- latest_predictions
- latest_hybrid_recommendations
- latest_model_readiness_diagnostics
- latest_target_coverage_report
- latest_training_gap_report

NLP, tópicos y drivers:
- latest_video_nlp_features
- latest_title_nlp_features
- latest_semantic_clusters
- nlp_feature_summary
- latest_video_topics
- latest_topic_metrics
- latest_title_pattern_metrics
- latest_keyword_metrics
- latest_topic_opportunities
- topic_intelligence_summary
- latest_content_driver_leaderboard
- latest_content_driver_feature_importance
- latest_content_driver_feature_direction
- latest_content_driver_group_importance

Ejecución editorial:
- latest_creative_packages
- latest_title_candidates
- latest_hook_candidates
- latest_thumbnail_briefs
- latest_script_outlines
- latest_originality_checks
- latest_production_checklist
- creative_packages_summary
- latest_weekly_brief
- latest_opportunity_radar

Operaciones:
- latest_process_status
- process_catalog
- operation_summary
- dashboard_impact_matrix

Rendimiento:
- No descargues todos estos archivos al iniciar.
- Carga primero manifest, dashboard_index, corrida actual y solamente los artefactos necesarios para la vista activa.
- Algunos artefactos superan varios MB y contienen miles de filas. Usa caché por organization_id + run_id + artifact_key, cancelación de requests, paginación o virtualización y procesamiento memoizado.
- Si el backend no ofrece paginación real, evita duplicar grandes arrays y no mantengas varias transformaciones completas en memoria.
- Prefetch solamente de módulos pequeños y previsibles.
- Las URLs firmadas no deben persistirse en localStorage ni logs.

Frescura y resiliencia:
- Determina ready, stale, partial, not_initialized o failed usando el manifest, la corrida y los artefactos presentes.
- Un artefacto opcional ausente afecta solo a su módulo.
- Un artefacto obligatorio ausente marca la corrida como partial y debe indicar cuál falta.
- Conserva la última corrida completed utilizable si una corrida posterior está processing o failed, mostrando un aviso claro.
- Muestra generated_at, rango de análisis, advertencias y errores sanitizados.

Incluye un adaptador de desarrollo que pueda leer los JSON estáticos actuales solamente en modo local explícito. En producción, el origen obligatorio debe ser Supabase privado; no uses los JSON públicos de GitHub Pages como mecanismo de protección de datos.
```

---

## Prompt 7 — Analítica principal

```text
Continúa sobre el proyecto actual y aplica todas las reglas del contexto maestro.

Implementa los módulos de analítica principal consumiendo exclusivamente la capa de bundles tipada. No consultes Storage ni Supabase directamente desde los componentes visuales.

Filtros compartidos:
- Búsqueda por título, canal o video_id.
- Canal.
- Duración: short, mid o long.
- Formato: Shorts o Videos.
- Horizonte: all, short_term, mid_term o long_term.
- Periodo cuando corresponda.
- Restablecer filtros.
- Persistir filtros relevantes en query params y validar cualquier valor recibido desde la URL.

Overview:
- Videos monitoreados.
- Canales monitoreados.
- Variación total de vistas, likes y comentarios.
- Engagement promedio.
- Video con mayor alpha_score.
- Canal con mayor crecimiento.
- Registros con baja confianza.
- Resumen de frescura, ventana de análisis y estado de la corrida.
- Lecturas ejecutivas de rendimiento, señales y calidad basadas únicamente en datos reales.

Videos:
- Tabla analítica ordenable y paginada o virtualizada.
- Campos principales existentes: execution_date, channel_id, channel_name, video_id, title, upload_date, duration_seconds, duration_bucket, is_short, views, likes, comments, views_delta, likes_delta, comments_delta, engagement_rate, like_rate, comment_rate, video_age_days, views_per_day_since_upload, is_new_video, metadata_changed, growth_rank y engagement_rank.
- Permite inspeccionar el detalle de un video sin perder filtros.
- Si ofreces un enlace a YouTube, constrúyelo de forma segura desde video_id y márcalo como navegación externa.

Channels:
- Comparación por videos_tracked, new_videos, total_views, total_views_delta, likes, comentarios, engagement, mediana de crecimiento y mejor video.
- Detalle de canal con sus videos y evolución disponible.

Scores:
- growth_percentile, engagement_percentile, freshness_score, growth_robust_z, relative_growth_percentile, alpha_score, opportunity_score y anomaly_score.
- Explica en lenguaje claro que son señales relativas, no garantías.

Advanced:
- Métricas de velocidad, crecimiento relativo, aceleración, volatilidad, éxito por horizonte, trend_burst, evergreen, packaging_problem y metric_confidence_score.
- No muestres una métrica como confiable si su elegibilidad o confianza indica lo contrario.

Titles:
- Longitud, número de palabras y señales de título existentes.
- Relación entre patrones de título, crecimiento y engagement, sin afirmar causalidad.

Periods:
- Selector de granularidad diaria, semanal o mensual.
- Evolución por video y canal usando los artefactos period_*.
- Maneja series incompletas y periodos sin observaciones.

Data Quality:
- Elegibilidad por horizonte, confianza, datos faltantes, warnings del manifest y cobertura.
- Distingue datos realmente ausentes de ceros válidos.

Visualización:
- Conserva tablas cuando sean la forma más precisa de comparar.
- Añade gráficas solo para tendencias, dispersión, outliers, concentración o trade-offs que no sean evidentes en una tabla.
- Tooltips con identificador, variables mostradas y contexto útil.
- Etiqueta solo puntos importantes cuando una dispersión tenga muchos elementos.
- Mantén formatos consistentes de fechas, enteros, porcentajes y valores faltantes.
- Cada control debe funcionar con teclado y tener nombre accesible.

Incluye estados vacíos específicos por filtro, módulo sin artefacto, corrida parcial y datos stale. No sustituyas datos ausentes con ejemplos inventados.
```

---

## Prompt 8 — Alertas, Radar, modelos, tópicos, NLP y Content Drivers

```text
Continúa sobre el proyecto actual y aplica todas las reglas del contexto maestro.

Implementa los módulos de inteligencia y decisión usando los artefactos reales y la capa de datos tipada.

Alerts:
- Lista y resumen por severidad, tipo, canal, video, confianza y estado triggered cuando esos campos estén disponibles.
- Filtros por severidad, señal y canal.
- Prioriza alertas accionables sin ocultar las de menor severidad.
- Explica la evidencia y el siguiente paso cuando el artefacto lo incluya.
- Señales principales: alpha_breakout, trend_burst, evergreen_candidate, packaging_problem y channel_momentum_up.
- No conviertas una señal no disparada en una alerta activa.

Opportunity Radar:
- Perfil, periodo, estado y fecha de generación.
- Resumen ejecutivo, métricas clave, oportunidades prioritarias, videos acelerando, canales a observar, temas emergentes, patrones de títulos, paquetes creativos, alertas, acciones comerciales y notas de política de datos.
- Incluye metodología, archivos fuente y cuota estimada cuando existan.
- Deja claro que el radar ofrece insights derivados y no un feed crudo ni una garantía de views.
- Reconstruye la experiencia desde el JSON; no dependas del HTML legado.

Models:
- Manifest del modelo, leaderboard, feature importance, feature direction, predictions, recomendaciones híbridas y diagnóstico de readiness.
- Filtros por formato Shorts/Videos y target cuando estén disponibles.
- Diferencia entrenamiento, predicción y recomendación.
- Muestra cobertura, gaps y bloqueos del modelo.
- Aclara que feature importance no implica dirección ni causalidad.
- No presentes un modelo como listo si los diagnósticos indican lo contrario.

Topics:
- Tópicos por video, métricas agregadas, patrones de título, keywords y oportunidades.
- Permite comprender velocidad, saturación, oportunidad y evidencia disponible.

NLP:
- Features de títulos y videos, clusters semánticos y resumen de cobertura.
- Diferencia señales léxicas determinísticas de inferencias de modelos.

Content Drivers:
- Leaderboard, importancia por feature, dirección estimada y grupos de variables.
- Permite comparar target, formato y familia de variables.
- Comunica limitaciones metodológicas junto a los resultados.

Reglas comunes:
- No inventes explicaciones que no estén respaldadas por campos o metodología disponible.
- En datos con baja confianza, muestra la incertidumbre de forma visible y textual.
- Vincula video_id y channel_id entre módulos para permitir navegación contextual sin duplicar datos.
- Conserva filtros compartidos cuando tengan sentido y descarta los incompatibles de manera explícita.
- Un fallo en Models no debe impedir consultar Alerts o Topics.
- Usa lazy loading y evita descargar artefactos de módulos no visitados.

Prueba cada módulo con datos presentes, artefacto vacío, artefacto ausente, esquema inválido y corrida stale.
```

---

## Prompt 9 — Paquetes creativos y brief semanal

```text
Continúa sobre el proyecto actual y aplica todas las reglas del contexto maestro.

Implementa los módulos de ejecución editorial a partir de los artefactos existentes. No generes contenido nuevo con IA desde el frontend y no simules resultados.

Creative Packages:
- Lista de paquetes priorizados con creative_package_id, fuente, canal, título original, topic, package_type, creative_angle, formato recomendado, timeframe, scores de oportunidad, originalidad, riesgo de copia, factibilidad, ejecución creativa y confianza.
- Relaciona cada paquete con sus title candidates, hook candidates, thumbnail briefs, script outlines, originality checks y production checklist.
- Cuando existan insights de transcripción, muéstralos como evidencia adicional y distingue claramente transcript_available.
- Presenta recommended_next_step y la evidencia disponible.
- Permite copiar campos textuales y exportar un paquete individual en un formato estructurado, siempre preservando su identificador y fecha de generación.
- No permitas editar el artefacto analítico original. Si se requieren notas o estado de trabajo del usuario, guárdalos en una entidad separada y auditable, nunca dentro del bundle.

Weekly Brief:
- Periodo, estado, resumen ejecutivo y métricas clave.
- Acciones principales de la semana.
- Oportunidades de contenido.
- Watchlist y matriz de oportunidad.
- Videos por crecimiento, alpha videos y canales por momentum.
- Tópicos, clusters, content drivers, alertas, paquetes creativos, patrones de títulos, calidad de datos y readiness de modelos.
- Reconstruye el brief desde latest_weekly_brief.json; no dependas del HTML legado.
- Permite una versión imprimible o exportable sin exponer URLs firmadas ni datos de otra organización.

Trazabilidad:
- Cada recomendación debe conservar source_action_id, source_opportunity_id, source_video_id u otros identificadores disponibles.
- Muestra generated_at y la corrida de origen.
- Advierte cuando una sección está incompleta por falta de artefactos.
- No mezcles elementos de corridas distintas dentro del mismo brief o paquete.

Estados y permisos:
- Todos los roles pueden consultar estos módulos si pertenecen a la organización.
- Cualquier futura nota o seguimiento interno debe respetar RLS por organización.
- Viewer no puede ejecutar acciones administrativas ni modificar canales.
- Exportar o copiar debe respetar el estado actual de autorización y no incluir campos internos sensibles.

Incluye pruebas de relación entre artefactos, exportación, contenido faltante, campos opcionales y cambio de organización durante la carga.
```

---

## Prompt 10 — Operaciones, canales y administración técnica

```text
Continúa sobre el proyecto actual y aplica todas las reglas del contexto maestro.

Implementa la observabilidad operativa y la administración de canales sin ejecutar el pipeline desde el navegador.

Operations:
- Consume latest_process_status, process_catalog, operation_summary y dashboard_impact_matrix.
- Presenta proceso, dominio, tipo, cadencia, SLA, estado, última corrida, antigüedad, dependencias, artefactos esperados y módulos afectados.
- Estados normalizados: success, success_with_warnings, failed, skipped, stale, not_initialized y unknown.
- Permite filtrar por dominio, estado, tipo de proceso y módulo afectado.
- Distingue claramente un fallo real, un warning tolerado, una omisión esperada y un artefacto stale.
- Muestra cuota estimada por endpoint y total cuando esté disponible.
- Nunca muestres secretos, stdout completo, cookies, tokens ni logs crudos.
- Un proceso fallido no debe ocultar el último bundle completed todavía utilizable.

Channels:
- owner, admin y analyst pueden solicitar alta, pausa o reactivación de canales monitoreados.
- viewer solo puede consultar.
- Acepta URL, handle o channel_id como channel_input.
- La UI solo registra la intención en monitored_channels.
- La resolución de channel_id y uploads_playlist_id corresponde al pipeline Python mediante channels.list.
- El descubrimiento posterior corresponde al uploads playlist mediante playlistItems.list.
- La actualización de métricas corresponde a videos.list en batches de hasta 50 IDs.
- No implementes search.list como alternativa.
- Muestra pending, active, paused o resolution_failed y el contexto sanitizado del último error.
- Evita duplicados dentro de la misma organización.

Administración técnica:
- Vista de corridas con generated_at, completed_at, schema_version, status, warnings, errores y artefactos.
- Detalle de artefactos con key, row_count, size, checksum y disponibilidad; no expongas la ruta privada completa si no es necesaria.
- Historial de audit_events visible solo para owner y admin.
- Preferencias de notificaciones por usuario y organización.
- No añadas billing, API pública, gestión de secretos ni ejecución arbitraria de comandos.

Audita todas las mutaciones de canales y configuración. Incluye confirmaciones para operaciones destructivas o que cambien acceso. Prueba permisos por rol y aislamiento entre organizaciones.
```

---

## Prompt 11 — Estados, accesibilidad, rendimiento y resiliencia

```text
Continúa sobre el proyecto actual y aplica todas las reglas del contexto maestro.

Realiza una pasada transversal de calidad sobre toda la aplicación. No cambies la estética ni reorganices el producto sin una necesidad funcional.

Estados obligatorios:
- Sesión resolviéndose.
- Sin sesión.
- Sin organización.
- Sin permisos.
- Carga inicial y carga de módulo.
- Sin datos porque no existe una corrida.
- Sin resultados por filtros.
- Artefacto opcional ausente.
- Bundle parcial.
- Corrida failed con fallback a la última completed.
- Datos stale.
- Esquema incompatible.
- Error de red recuperable.
- Error no recuperable.
- Operación exitosa.

Accesibilidad:
- HTML semántico y jerarquía de encabezados coherente.
- Labels accesibles para todos los controles.
- Navegación completa con teclado.
- Foco visible y gestión de foco en diálogos, errores y cambios de ruta.
- Tablas con encabezados y ordenamiento anunciado.
- Gráficas con título, descripción textual y alternativa tabular o resumen equivalente.
- Estados no comunicados solo por color.
- Contraste suficiente.
- Regiones live para confirmaciones y errores asíncronos, sin anuncios excesivos.
- Respeta prefers-reduced-motion.

Rendimiento:
- Lazy loading por ruta y módulo.
- No hagas fetch de artefactos que la vista actual no necesita.
- Cancela requests al cambiar organización, corrida o ruta.
- Evita cascadas de requests y renders derivados innecesarios.
- Virtualiza o pagina tablas grandes.
- Memoriza transformaciones costosas con dependencias correctas.
- No guardes bundles grandes completos en localStorage.
- No registres payloads analíticos completos en consola o servicios de error.
- Libera referencias a datos de una organización al cambiar a otra.

Resiliencia y seguridad del cliente:
- Sanitiza HTML legado o, preferentemente, no lo renderices.
- Valida query params, IDs, redirects y payloads remotos.
- No interpolar contenido no confiable como HTML.
- Evita filtraciones por mensajes de error, breadcrumbs o telemetría.
- Las queries deben incluir organization_id y run_id en su clave de caché.
- Al perder membresía o bajar de rol, invalida inmediatamente permisos y datos protegidos.

Responsive:
- Todas las tareas deben ser utilizables en escritorio y móvil.
- Las tablas pueden usar desplazamiento, columnas priorizadas o vistas de detalle; no ocultes silenciosamente información crítica.
- Evita overflow accidental, controles inaccesibles y texto ilegible.
- No conviertas la interfaz en una versión decorativa diferente según el tamaño.

Ejecuta una auditoría de accesibilidad, rendimiento y errores. Corrige problemas encontrados y documenta únicamente limitaciones que no puedan resolverse dentro del proyecto.
```

---

## Prompt 12 — Pruebas, auditoría de seguridad y cierre

```text
Continúa sobre el proyecto actual y aplica todas las reglas del contexto maestro.

Haz la validación final de producción. Corrige los problemas encontrados; no entregues solo un informe.

Pruebas de autenticación:
- Registro directo y creación transaccional de organización.
- Verificación de correo.
- Login y logout.
- Recuperación y cambio de contraseña.
- Sesión vencida.
- Invitación para usuario nuevo y existente.
- Enlace de invitación inválido o vencido.

Pruebas multiempresa y RLS:
- Crea dos organizaciones y usuarios separados.
- Verifica que no puedan leer perfiles, miembros, invitaciones, canales, corridas, artefactos, auditoría ni objetos Storage ajenos.
- Intenta cambiar organization_id desde requests manipulados.
- Verifica que viewer no pueda mutar.
- Verifica que analyst no pueda administrar usuarios.
- Verifica que admin no pueda asignar, degradar ni eliminar owner.
- Verifica que el último owner no pueda abandonar o quedar eliminado.
- Verifica transferencia de propiedad atómica.
- Verifica que el navegador no pueda escribir analytics_runs, analytics_artifacts ni el bucket.

Pruebas del puente de datos:
- Última corrida completed.
- Corrida processing posterior con fallback seguro.
- Corrida failed posterior con fallback seguro.
- Bundle partial.
- Artefacto opcional ausente.
- Checksum o schema_version inválido.
- Tabla vacía válida.
- Archivo grande.
- URL firmada vencida.
- Cambio de organización durante un fetch.
- HTML legado malicioso bloqueado o sanitizado.

Pruebas funcionales:
- Todos los filtros y su persistencia en URL.
- Navegación contextual por video y canal.
- Ordenamiento, paginación o virtualización.
- Alertas por severidad y triggered.
- Radar, Models, Topics, NLP y Content Drivers con sus estados vacíos.
- Relación de paquetes creativos con sus candidatos y checklists.
- Brief semanal y exportación.
- Operations y permisos de administración de canales.

Pruebas de calidad:
- TypeScript, build, lint y tests sin errores.
- Sin secretos ni service role en el bundle final.
- Sin datos ficticios en producción.
- Sin requests a YouTube desde el frontend.
- Sin uso de search.list.
- Sin HTML inseguro.
- Navegación por teclado y foco.
- Verificación en escritorio y móvil.
- Respeto de prefers-reduced-motion.
- Ausencia de overflow y estados bloqueados.

Documentación final:
- Variables públicas necesarias, únicamente URL y anon key de Supabase.
- Migraciones, Edge Functions y políticas RLS creadas.
- Contrato de publicación para el pipeline Python, indicando que requiere service role fuera del frontend.
- Estructura y versionado de bundles.
- Matriz de roles.
- Procedimiento para añadir un nuevo artifact_key sin romper clientes anteriores.
- Procedimiento de despliegue y rollback.

Entrega un resumen final con evidencia de pruebas. Si algún criterio no pasa, corrígelo antes de dar por terminado el proyecto. No declares seguridad multiempresa completa basándote solamente en controles de interfaz: debe estar demostrada por RLS y pruebas de acceso cruzado.
```

---

## Resultado esperado

Al completar los doce prompts, Lovable debe haber producido una aplicación privada en español con:

- Autenticación completa.
- Organizaciones, invitaciones y roles.
- Aislamiento multiempresa mediante RLS.
- Bundles analíticos privados, inmutables y versionados.
- Todas las áreas funcionales del dashboard existente.
- Administración de canales sin exponer secretos ni llamar YouTube desde el navegador.
- Manejo explícito de datos parciales, stale y fallos por proceso.
- Accesibilidad, rendimiento y pruebas de seguridad proporcionales a un SaaS de datos.

Quedan deliberadamente fuera de alcance el billing self-service, una API pública, el scraping, la ejecución del pipeline desde el navegador y el almacenamiento de secretos en Lovable.
