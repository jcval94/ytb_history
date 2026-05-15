# YouTube Opportunity Radar

README unico para implementar el plan comercial de monetizacion de `ytb_history`.

## 1. Posicionamiento

Vender inteligencia editorial accionable, no datos crudos de YouTube ni un SaaS generico.

**Producto:** Weekly YouTube Opportunity Radar

**Promesa:** detectar videos, canales, temas, titulos y patrones creativos con senales tempranas de crecimiento, y convertirlos en decisiones editoriales concretas.

ICP inicial:
- Agencias de contenido, personal branding, YouTube growth y podcast clips.
- Equipos editoriales de creadores medianos/grandes.
- Medios o research shops que compran reportes por categoria.

Oferta inicial:
- **Pilot Radar:** USD 750/mes, hasta 50 canales, brief semanal.
- **Growth Radar:** USD 1,500/mes, hasta 100 canales, dashboard y alertas.
- **Agency Radar:** USD 3,000+/mes, multiples clientes/categorias y entregables white-label.

## 2. Entregable comercial

Cada semana el cliente recibe:
- Resumen ejecutivo.
- Top videos acelerando contra baseline.
- Canales a observar.
- Temas emergentes.
- Patrones de titulos ganadores.
- 10 oportunidades editoriales priorizadas.
- 5 paquetes creativos accionables: angulo, hook, titulo, formato y checklist.
- Alertas relevantes.
- Dashboard privado como anexo de evidencia.
- Notas de metodologia, cuota y compliance.

El entregable principal vive en:

```bash
data/commercial_radar/<perfil>/latest_opportunity_radar.md
data/commercial_radar/<perfil>/latest_opportunity_radar.html
data/commercial_radar/<perfil>/latest_opportunity_radar.json
```

El dashboard web publica la interfaz completa en la pestaña **Radar** cuando se ejecuta `build-pages-dashboard`.

## 3. Configuracion comercial

Editar:

```bash
config/commercial_radar.yaml
```

Perfil inicial:

```yaml
default_profile: spanish_business_ai
profiles:
  spanish_business_ai:
    client_name: Pilot Agency Prospect
    category_name: Negocios, finanzas e IA en espanol
    package_name: Weekly YouTube Opportunity Radar
    plan_name: Pilot Radar
    monthly_price_usd: 750
    max_channels: 50
```

Para demos comerciales, usar:

```bash
python -m ytb_history.cli generate-opportunity-radar --anonymize --output-dir data/commercial_radar/demo
```

## 4. Flujo tecnico recomendado

El radar usa artefactos locales ya generados. No llama APIs externas y no usa `search.list`.

Secuencia completa:

```bash
python -m ytb_history.cli run
python -m ytb_history.cli validate-latest
python -m ytb_history.cli export-latest
python -m ytb_history.cli build-analytics
python -m ytb_history.cli build-nlp-features
python -m ytb_history.cli generate-alerts
python -m ytb_history.cli build-decision-layer
python -m ytb_history.cli build-model-intelligence
python -m ytb_history.cli build-topic-intelligence
python -m ytb_history.cli generate-creative-packages
python -m ytb_history.cli generate-weekly-brief
python -m ytb_history.cli generate-opportunity-radar
python -m ytb_history.cli build-operations
python -m ytb_history.cli build-pages-dashboard
```

Comando minimo cuando ya existen artefactos de analytics/brief:

```bash
python -m ytb_history.cli generate-opportunity-radar
python -m ytb_history.cli build-pages-dashboard
```

## 5. Interfaz web

La pagina estatica queda en `site/` y carga:

```bash
site/index.html
site/data/latest_opportunity_radar.json
site/data/latest_opportunity_radar.html
```

La pestaña **Radar** muestra:
- posicionamiento del paquete;
- KPIs de alcance, precio, oportunidades, alertas y cuota;
- resumen ejecutivo;
- acciones comerciales;
- graficas de prioridad, patrones de titulos y embudo de senales;
- tablas de oportunidades, videos, canales, temas, titulos, paquetes creativos y alertas;
- metodologia, fuentes trazables y guardrails de cuota.

## 6. Compliance y margen operativo

Reglas:
- No vender un feed crudo de datos de YouTube como producto principal.
- Vender insights derivados: scores, alertas, recomendaciones, patrones y oportunidades.
- No prometer views garantizadas ni causalidad.
- Presentar resultados como senales e hipotesis editoriales.
- No usar `search.list` en el flujo normal.
- Mantener la API key solo en `YOUTUBE_API_KEY`.
- Usar transcripcion solo con videos propios, autorizados o provistos por el cliente.
- Revisar politicas de YouTube API Services antes de exponer API/feed enterprise.

Guardrail de cuota:
- Limitar canales por plan.
- Reportar cuota estimada por perfil/categoria.
- Cobrar mas cuando sube el alcance.

## 7. Roadmap de 30 dias

Semana 1:
- Elegir categoria inicial: negocios, finanzas e IA en espanol.
- Generar una demo anonima.
- Publicar dashboard con pestaña Radar.
- Preparar mini-brief para outreach.

Semana 2:
- Contactar 50 prospectos.
- Ofrecer mini-brief gratuito con 3 oportunidades reales.
- Buscar 5-8 llamadas.
- Cerrar 1-2 pilotos pagados.

Semana 3:
- Entregar el primer radar manualmente.
- Medir que secciones generan respuesta.
- Recortar lo que no ayuda a decidir.

Semana 4:
- Volver repetible el flujo.
- Documentar la plantilla por cliente/categoria.
- Pedir referidos.
- Preparar segundo nicho solo si el primero retiene.

## 8. Criterios de exito del piloto

- 3 clientes piloto en 30-45 dias.
- 50%+ de briefs abiertos o respondidos.
- Cada cliente identifica 3+ ideas utiles por mes.
- Al menos 1 idea del radar se convierte en pieza publicada.
- El cliente acepta renovar despues del primer mes.

## 9. Que no construir todavia

- Login completo.
- Billing self-serve.
- SaaS multi-tenant complejo.
- API publica.
- Scraping fuera de la API oficial.
- Transcripcion masiva de videos ajenos.

Primero vender: brief, dashboard, alertas, paquetes creativos y decision editorial.
