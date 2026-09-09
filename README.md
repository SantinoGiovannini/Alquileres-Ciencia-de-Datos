# Brujula Inmobiliaria

Proyecto Integrador - Ciencia de Datos 2026 (UTN FRM).

## La pregunta

Un aviso de alquiler en Mendoza, ¿esta sobrevalorado, en linea con el
mercado, o es una oportunidad? El modelo es supervisado (regresion sobre
precio_m2 + analisis del residuo); el detalle de la propuesta completa esta
en `Documentacion/Propuestas_Proyecto_Integrador_Inmobiliario.docx`.

## Fuente y unidad de analisis

- **Fuente principal:** Inmoclick, https://inmoclick.com/departamentos-en-alquiler-en-mendoza
  y https://inmoclick.com/casas-en-alquiler-en-mendoza (robots.txt sin
  restricciones, HTML estatico, ~830 avisos verificados entre los dos tipos).
- **Fuentes adicionales reales:** argenprop.com e inmoup.com.ar -- no son
  otro motor sobre Inmoclick, son sitios con HTML y estructura completamente
  distintos, y **corren siempre** (no solo si Inmoclick cae). Ver "Por que
  tres fuentes" mas abajo.
- **Segmento:** departamentos y casas en alquiler, Mendoza.
- **Unidad de analisis:** un aviso publicado. "Una fila es un aviso de
  alquiler -- departamento o casa -- publicado en Inmoclick, argenprop o
  inmoup, para la provincia de Mendoza, en el momento del scraping."
- **Columna objetivo:** `precio_m2` (precio / superficie cubierta),
  calculada en `transform_clean` sobre el DataFrame consolidado -- no
  depende de que la fuente la traiga, asi que es consistente entre las tres.
- **Clave primaria:** `listing_id` (el `kid` interno de Inmoclick,
  `argenprop-<id>`, o `inmoup-<id-agente>-<id-interno>` -- unico dentro de
  cada fuente, y el prefijo evita colisiones entre las tres).

## Arquitectura del pipeline

Modelo medallon, capa bronce (`data/raw/<fecha>/`, HTML crudo tal como
llega) separada de capa plata (`data/processed/`, el CSV final):

```
wait_for_source -> check_source ---> extract_listings -> parse_raw --------+
                               `--> use_frozen_snapshot -----------------+
extract_listings_argenprop -> parse_raw_argenprop -----------------------+--> transform_clean -> quality_check -> export_csv
extract_listings_inmoup -> parse_raw_inmoup ----------------------------/
```

| Tarea | Que hace |
|---|---|
| `wait_for_source` | Sensor. Pide la pagina 1 de Inmoclick; si no responde en 15 min, sigue por el snapshot congelado. |
| `check_source` | Decision **binaria**: Inmoclick vivo (`extract_listings`) o snapshot congelado (`use_frozen_snapshot`). Ver "Por que es binaria" abajo. |
| `extract_listings` | **Bronce, Inmoclick.** Baja listado + fichas para departamentos y casas (dos saltos: antiguedad/ambientes/expensas solo estan en la ficha). Pausa entre pedidos y reintento si una ficha vuelve vacia (ver "Hallazgos de scraping"). |
| `parse_raw` | Recorre el bronce de Inmoclick (sin red) y arma un registro por aviso, para ambos tipos. |
| `extract_listings_argenprop` | **Bronce, argenprop.** Corre siempre, no solo si Inmoclick cae. Descubre por sitemap y baja con Playwright (ver por que abajo). |
| `parse_raw_argenprop` | Parsea el bronce de argenprop, descarta lo que no sea de Mendoza. |
| `extract_listings_inmoup` | **Bronce, inmoup.** Corre siempre. Descubre por sitemap y baja con `requests` -- sin Playwright, ver por que abajo. |
| `parse_raw_inmoup` | Lee el JSON-LD de cada ficha, descarta lo que no sea de Mendoza. |
| `use_frozen_snapshot` | Ultimo recurso para Inmoclick: la semilla versionada en `data/frozen/semilla.csv`. |
| `transform_clean` | **Plata.** Junta lo que aporto cada fuente, tipa columnas, dedupe por `listing_id`, calcula `precio_m2`. |
| `quality_check` | Los siete criterios del Kit de arranque (ver abajo). Reporta, no bloquea -- salvo dataset vacio. |
| `export_csv` | Escribe `data/processed/brujula_inmobiliaria_alquiler_mendoza.csv`, el entregable de la Entrega 1. |

El codigo de scraping/parseo vive en `dags/brujula/` (paquete al lado del
DAG, no en un `include/` -- el `docker-compose.yaml` oficial solo monta
`dags/`, `logs/`, `plugins/`, `config/`).

## Por que tres fuentes (y por que las dos adicionales corren siempre)

Inmoclick solo, aunque se sumen departamentos y casas, da ~830 avisos --
por debajo del piso de **1.000 filas** que pide el criterio "Volumen
suficiente" del Kit de arranque de la catedra. El kit mismo dice que hacer
en ese caso: *"combinar con otra fuente"* (en plural, en este caso: dos).
Por eso ni `extract_listings_argenprop` ni `extract_listings_inmoup`
dependen de que Inmoclick falle: corren siempre, en paralelo, y sus filas
se suman a las de Inmoclick en `transform_clean`. Con las tres fuentes el
dataset llega a **1.188 avisos** en una corrida completa real -- ver
"Nota sobre esta entrega" mas abajo.

El respaldo ante una fuente caida sigue existiendo (`check_source` ->
`use_frozen_snapshot` si Inmoclick no responde), pero es una decision
aparte de si argenprop aporta filas o no -- son dos preguntas distintas
("¿la fuente principal esta viva?" vs. "¿conseguimos suficiente volumen?"),
asi que viven en dos partes del grafo distintas.

**Version anterior, para que quede el registro:** la primera version tenia
`check_source` decidiendo entre tres ramas en un solo lugar (Inmoclick /
argenprop / congelado), porque antes argenprop *si* dependia de que
Inmoclick fallara. Esa version ya resolvia un bug real: con dos
sensores+branches en cascada, el segundo no podia distinguir "nunca hizo
falta intentarlo" de "se intento y se agoto el tiempo" (las dos causas
dejan la tarea previa en `skipped`). Al pasar argenprop a que corra
siempre, ese problema desaparecio solo: ya no hay una decision en cascada
que resolver.

## Hallazgos de scraping (los dos sitios se defienden de pedidos rapidos)

Bajar cientos de avisos seguidos, rapido, dispara defensas anti-scraping
en las dos fuentes -- esto se descubrio corriendo el pipeline completo, no
leyendo documentacion:

- **Inmoclick devuelve `200 OK` con el cuerpo vacio** cuando se le pide
  demasiado rapido y seguido (no es un error HTTP, hay que detectarlo por
  el tamaño de la respuesta). `extract_listings` pausa entre pedidos y
  reintenta con backoff cuando una ficha vuelve vacia.
- **argenprop tiene proteccion AWS WAF** ("Human Verification"): en rafaga
  devuelve una pagina de desafio en vez del contenido real. Con pausa entre
  pedidos (1.2s) el bloqueo baja mucho, pero **no desaparece del todo** --
  scrapear mucho en una misma sesion de pruebas puede dejar el bloqueo mas
  persistente por un rato. Si `extract_listings_argenprop` viene devolviendo
  pocas filas, probablemente sea esto: no es un bug, es que el sitio esta
  bloqueando esta IP por ahora.

## Fuente adicional real: argenprop.com

**Descubrimiento por sitemap, no por buscador.** El `robots.txt` de
argenprop.com limita a 3 paginas la paginacion de resultados de busqueda,
pero declara sitemaps completos sin esa restriccion. La region Cuyo
(Mendoza + San Juan + San Luis) tiene su propio indice
(`sitemaps/sitemap-ficha-cuyo.xml.gz`), que apunta a un sitemap hoja de
alquiler. De ahi sale la lista de fichas, sin tocar el buscador limitado.

**Necesita Playwright, no es opcional.** A diferencia de Inmoclick (todo
en atributos HTML), argenprop renderiza precio y superficie por JavaScript
del lado del cliente -- se verifico bajando una ficha con `requests` liso
y ni el precio ni la superficie aparecen en el documento. Por eso el
`Dockerfile` de este repo instala Chromium (ver "Como levantar el
entorno").

**Ambiguedad de departamentos con nombre repetido.** El sitemap de Cuyo
mezcla tres provincias, y "San Martin" y "Rivadavia" son departamento
tanto de Mendoza como de San Juan. El filtro por nombre en la URL
(`argenprop.DEPARTAMENTOS_MENDOZA`) solo recorta que bajar; la
confirmacion real es la provincia que trae el **breadcrumb estructurado**
de la ficha (`schema.org BreadcrumbList` -- el item cuyo link termina en
`-arg` es la provincia). Se probo primero con `.location-label`, pero ese
selector resulto poco confiable: parece atado a un widget de mapa que se
renderiza en un paso posterior, y en la practica volvia vacio en la
mayoria de las fichas aunque el resto de la pagina ya estuviera lista. El
breadcrumb, al ser markup pensado para SEO, se rellena de forma consistente.

**Que falta frente a Inmoclick.** El mapeo de amenities booleanas
(`transform.AMENITIES_BOOL_ARGENPROP`) es best-effort -- se inspecciono
una sola ficha real al construir el parser y no traia amenities, asi que
las etiquetas se completaron por el nombre esperable de la columna sin
confirmar el texto exacto. Tampoco hay `prp_id`/`usr_id`/`lat`/`lng`.

## Fuente adicional real: inmoup.com.ar

**No necesita Playwright.** A diferencia de argenprop, la ficha de inmoup
trae un bloque `<script type="application/ld+json">` (schema.org
`RealEstateListing`) ya presente en el HTML server-side -- precio, moneda,
direccion, provincia, coordenadas y hasta `datePosted` (fecha de
publicacion real, que ni Inmoclick ni argenprop exponen). Se verifico
bajando una ficha con `requests` liso: todo el JSON-LD ya estaba ahi. Solo
la pagina de *busqueda* de inmoup es client-side (Next.js) -- por eso el
descubrimiento tambien es por sitemap, no por esa pagina.

**Descubrimiento por sitemap.** El `robots.txt` de inmoup permite
`/sitemap/sitemap-*.xml`. Los sitemaps `sitemap-inmuebles-por-provincias-{1..4}.xml`
tienen ~18.000 fichas de todo el pais -- se prefiltra por nombre de
departamento de Mendoza en el slug (mismo criterio y misma ambiguedad con
"San Martin"/"Rivadavia" que argenprop) y se confirma la provincia real
leyendo `address.addressRegion` del JSON-LD, sin heuristicas de HTML.

**Sin problemas de bloqueo, a diferencia de argenprop.** Se probaron 495
fichas de punta a punta con pausas moderadas (0,4s): 0 errores, 0
bloqueos, 488 confirmadas de Mendoza. La ausencia de JavaScript en el
lado del scraper (una llamada HTTP simple, no un navegador completo)
parece ser la diferencia -- no hay certeza de la causa exacta, pero el
contraste con argenprop (mismo tipo de pausa, mismo volumen, bloqueo
persistente) es marcado.

**Que falta frente a Inmoclick.** `barrio` no viene separado de
`localidad` en el JSON-LD (solo hay `addressLocality`). El resto de los
campos (dormitorios, banios, superficie, amenities, antiguedad) vienen
igual de bien o mejor que Inmoclick, con las mismas etiquetas en espanol.

## Fuente de emergencia (Properati/Kaggle) sin implementar

La propuesta la menciona como tercer nivel. El slug correcto del dataset
es `kaggle.com/datasets/properati-data/properties` (no `properati-data` a
secas, que devuelve 404) -- si hace falta esa cuarta fuente, es el punto
de partida.

## ZonaProp, evaluada y descartada

Esta detras de Cloudflare con un desafio JS real -- se detecto pidiendo su
sitemap con `curl` liso, que devolvio una pagina "Just a moment..." en vez
del XML (mismo tipo de proteccion que `sofifa.com` en el ejemplo canonico
de la catedra). No se insistio: pasar ese desafio de forma automatizada
cruza a evasion de deteccion, no scraping educado.

## Como levantar el entorno

```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'
cp .env.example .env
```

Despues, **dos ediciones en el archivo que acabas de bajar** (en el bloque
`x-airflow-common` cerca del principio -- Apache ya deja el comentario
listo para esto):

```yaml
x-airflow-common:
  &airflow-common
  # image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:3.3.1}   # <- comentar esta linea
  build: .                                                # <- descomentar esta
  ...
  volumes:
    - ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags
    - ${AIRFLOW_PROJ_DIR:-.}/logs:/opt/airflow/logs
    - ${AIRFLOW_PROJ_DIR:-.}/config:/opt/airflow/config
    - ${AIRFLOW_PROJ_DIR:-.}/plugins:/opt/airflow/plugins
    - ${AIRFLOW_PROJ_DIR:-.}/data:/opt/airflow/data       # <- agregar esta linea
```

Esto hace que `docker compose build` use el `Dockerfile` de este repo (que
instala Playwright + Chromium, necesarios para el respaldo de argenprop) y
que `data/` quede visible dentro de los contenedores.

```bash
docker compose build
docker compose up airflow-init
docker compose up
# UI en http://localhost:8080 (usuario y contraseña: airflow)
```

> El `docker-compose.yaml` oficial no esta versionado en este repo (ver
> `.gitignore`): cada integrante lo descarga una vez en su maquina y le
> aplica las dos ediciones de arriba.

**Nota sobre esta entrega:** el pipeline completo (Inmoclick departamentos +
casas + argenprop + inmoup, las tres fuentes adicionales en paralelo) se
corrio de punta a punta en modo `full` contra los sitios reales, reusando
un entorno Airflow 3.3 ya probado de la catedra (misma imagen
`apache/airflow:3.3.1` que resuelve hoy el `docker-compose.yaml` oficial).
Resultado real: **1.188 avisos** (660 de Inmoclick, 79 de argenprop, 449 de
inmoup), 46 columnas, `precio_m2` calculado, los siete criterios
verificados -- **supera el piso de 1.000 filas**. El `Dockerfile` de *este*
repo especifico (con Chromium) todavia no se construyo con
`docker compose build` en un entorno limpio desde cero -- hacerlo antes de
dar la Fase 1 por cerrada.

## Lo que hay que saber explicar en la defensa

Ver la seccion 6 de la guia de pasos / plan de vuelo: que es una fila, cual
es la columna objetivo, por que hay nulos, que pasa si se corre de nuevo,
donde queda el dato crudo, y el tema de tidy (amenities como texto libre,
mismo caso que el ejemplo de FIFA de la catedra).

`quality_check` deja un reporte (`data/processed/quality_report_<fecha>.txt`)
con los seis chequeos: clave unica, `len`, `shape`, `dtypes.value_counts()`,
`isna().mean()` por columna, y las columnas siempre nulas. Es lectura
obligatoria antes de la defensa -- ahi se ve, por ejemplo, que columnas
como `barrio_privado` o `recibe_permuta` pueden salir 100% nulas en una
corrida chica.

## Modos de corrida

| Parametro | Valores | Que hace |
|---|---|---|
| `mode` | `subset` / `full` | `subset`: la primera pagina de cada tipo en Inmoclick (~48 avisos) + ~12 fichas de argenprop + ~12 de inmoup. `full`: ~830 avisos de Inmoclick mas hasta 220 fichas de argenprop y hasta 500 de inmoup. |
| `engine` | `auto` / `http` / `browser` | Motor de descarga de Inmoclick. `auto` prueba HTTP y cae a navegador si el sitio empieza a filtrar (no deberia hacer falta, ver `dags/brujula/inmoclick.py`). |

## Probar

```bash
docker compose exec airflow-scheduler airflow dags test brujula_inmobiliaria_pipeline 2026-01-01 --conf '{"mode":"subset"}'
```

## Estado

- [x] Fase 0 - estructura del repo y DAG esqueleto
- [x] Fase 1 - extraccion real (extract_listings, parse_raw) -- validado con datos reales de Inmoclick + argenprop + inmoup
- [x] Fase 2 - transformacion y calidad (transform_clean, quality_check) -- precio_m2 y los siete criterios, validado
- [x] Fase 3 - corrida completa en verde (1.188 avisos, supera el piso de 1.000), visible en el historial de Airflow -- pendiente reproducirla con el `docker compose` de este repo especifico (se corrio en un entorno Airflow 3.3 equivalente)
- [ ] Fase 4 - documentacion final
- [ ] Fase 5 - ensayo cronometrado
