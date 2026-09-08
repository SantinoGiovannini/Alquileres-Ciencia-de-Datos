# Brujula Inmobiliaria

Proyecto Integrador - Ciencia de Datos 2026 (UTN FRM).

## La pregunta

Un aviso de alquiler en Mendoza, ¿esta sobrevalorado, en linea con el
mercado, o es una oportunidad? El modelo es supervisado (regresion sobre
precio_m2 + analisis del residuo); el detalle de la propuesta completa esta
en `Documentacion/Propuestas_Proyecto_Integrador_Inmobiliario.docx`.

## Fuente y unidad de analisis

- **Fuente principal:** Inmoclick, https://inmoclick.com/departamentos-en-alquiler-en-mendoza
  (robots.txt sin restricciones, HTML estatico, 715 avisos verificados).
- **Fuente de respaldo real:** argenprop.com -- no es otro motor sobre
  Inmoclick, es un sitio con HTML y estructura completamente distintos. Ver
  la seccion "Respaldo" mas abajo.
- **Segmento:** departamentos en alquiler, Mendoza.
- **Unidad de analisis:** un aviso publicado. "Una fila es un departamento
  en alquiler publicado en Inmoclick (o, en su defecto, argenprop) para la
  provincia de Mendoza, en el momento del scraping."
- **Columna objetivo:** `precio_m2` (precio / superficie cubierta),
  calculada en `transform_clean` sobre el DataFrame consolidado -- no
  depende de que la fuente la traiga, asi que es consistente entre
  Inmoclick y argenprop.
- **Clave primaria:** `listing_id` (el `kid` interno de Inmoclick, o
  `argenprop-<id>` para los avisos de respaldo -- unico dentro de cada
  fuente, y el prefijo evita colisiones entre las dos).

## Arquitectura del pipeline

Modelo medallon, capa bronce (`data/raw/<fecha>/`, HTML crudo tal como
llega) separada de capa plata (`data/processed/`, el CSV final):

```
wait_for_source -> check_source ---> extract_listings -> parse_raw ------+
                               |--> extract_listings_argenprop           |
                               |        -> parse_raw_argenprop ----------+--> transform_clean -> quality_check -> export_csv
                               `--> use_frozen_snapshot -----------------+
```

| Tarea | Que hace |
|---|---|
| `wait_for_source` | Sensor. Pide la pagina 1 de Inmoclick; si no responde en 15 min, sigue por el respaldo. |
| `check_source` | Decision de **tres** vias en un solo lugar (ver por que abajo): Inmoclick vivo, argenprop vivo, o ninguno de los dos. |
| `extract_listings` | **Bronce, Inmoclick.** Baja listado + fichas (dos saltos: antiguedad/ambientes/expensas solo estan en la ficha). |
| `parse_raw` | Recorre el bronce de Inmoclick (sin red) y arma un registro por aviso. |
| `extract_listings_argenprop` | **Bronce, argenprop (respaldo).** Baja las fichas candidatas con Playwright (ver por que abajo). |
| `parse_raw_argenprop` | Parsea el bronce de argenprop, descarta lo que no sea de Mendoza. |
| `use_frozen_snapshot` | Ultimo recurso: la semilla versionada en `data/frozen/semilla.csv`. |
| `transform_clean` | **Plata.** Tipa columnas, dedupe por `listing_id`, calcula `precio_m2`. |
| `quality_check` | Los seis chequeos de calidad de la catedra (ver abajo). Reporta, no bloquea -- salvo dataset vacio. |
| `export_csv` | Escribe `data/processed/brujula_inmobiliaria_alquiler_mendoza.csv`, el entregable de la Entrega 1. |

El codigo de scraping/parseo vive en `dags/brujula/` (paquete al lado del
DAG, no en un `include/` -- el `docker-compose.yaml` oficial solo monta
`dags/`, `logs/`, `plugins/`, `config/`).

## Por que `check_source` es una sola decision de tres vias

Con dos sensores+branches en cascada (uno para Inmoclick, otro para
argenprop) el segundo no puede distinguir "nunca hizo falta intentarlo"
(Inmoclick ya respondio bien, asi que el branch de argenprop ni se
recorre) de "se intento y se agoto el tiempo" -- las dos causas dejan la
tarea previa en el mismo estado `skipped`, y Airflow no guarda por que.
Se probo en un DAG hermano de este pipeline (`airflow-fifa`/`airflow-inmobiliaria`
de la catedra) y el segundo branch corria igual, disparando el snapshot
congelado sin necesidad. Por eso `check_source` prueba argenprop
*sincronicamente, dentro de si misma* cuando Inmoclick fallo, en vez de
delegarlo a un sensor aparte.

## Respaldo real: argenprop.com

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
confirmacion real es la provincia que trae `.location-label` en la ficha
ya renderizada.

**Que falta frente a Inmoclick.** El mapeo de amenities booleanas
(`transform.AMENITIES_BOOL_ARGENPROP`) es best-effort -- se inspecciono
una sola ficha real al construir el parser y no traia amenities, asi que
las etiquetas se completaron por el nombre esperable de la columna sin
confirmar el texto exacto. Tampoco hay `prp_id`/`usr_id`/`lat`/`lng`.

**Fuente de emergencia (Properati/Kaggle) sin implementar.** La propuesta
la menciona como tercer nivel. El slug correcto del dataset es
`kaggle.com/datasets/properati-data/properties` (no `properati-data` a
secas, que devuelve 404) -- si hace falta esa tercera rama, es el punto de
partida.

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

**Nota sobre esta entrega:** el camino de Inmoclick (`extract_listings` ->
`parse_raw` -> `transform_clean` -> `quality_check` -> `export_csv`) se
valido de punta a punta con datos reales (24 avisos en modo `subset`, 45
columnas, `precio_m2` calculado, reporte de calidad generado), reusando un
entorno Airflow 3.3 ya probado de la catedra. El camino de argenprop se
valido con el mismo metodo en una sesion anterior del mismo pipeline
(sitemap real, Playwright real, filtro de provincia real), pero el
`Dockerfile` de *este* repo especifico (con Chromium) todavia no se
construyo con `docker compose build` en un entorno limpio -- hacerlo antes
de dar la Fase 1 por cerrada.

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
| `mode` | `subset` / `full` | `subset`: la primera pagina del listado (~24 avisos). `full`: los ~715 departamentos en alquiler de toda la provincia. |
| `engine` | `auto` / `http` / `browser` | Motor de descarga de Inmoclick. `auto` prueba HTTP y cae a navegador si el sitio empieza a filtrar (no deberia hacer falta, ver `dags/brujula/inmoclick.py`). |

## Probar

```bash
docker compose exec airflow-scheduler airflow dags test brujula_inmobiliaria_pipeline 2026-01-01 --conf '{"mode":"subset"}'
```

## Estado

- [x] Fase 0 - estructura del repo y DAG esqueleto
- [x] Fase 1 - extraccion real (extract_listings, parse_raw) -- validado con datos reales de Inmoclick
- [x] Fase 2 - transformacion y calidad (transform_clean, quality_check) -- precio_m2 y los seis chequeos, validado
- [ ] Fase 3 - correr el DAG completo con `docker compose` (este repo especifico) y dejarlo en verde
- [ ] Fase 4 - documentacion final
- [ ] Fase 5 - ensayo cronometrado
