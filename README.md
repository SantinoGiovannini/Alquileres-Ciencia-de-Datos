# Brujula Inmobiliaria

Proyecto Integrador - Ciencia de Datos 2026 (UTN FRM).

## La pregunta

Un aviso de alquiler en Mendoza, ¿esta sobrevalorado, en linea con el
mercado, o es una oportunidad? El modelo es supervisado (regresion sobre
precio_m2 + analisis del residuo); el detalle de la propuesta completa esta
en `Documentacion/Propuestas_Proyecto_Integrador_Inmobiliario.docx`.

## Fuente y unidad de analisis

- **Fuente:** Inmoclick, https://inmoclick.com/departamentos-en-alquiler-en-mendoza
  (robots.txt sin restricciones, HTML estatico, 715 avisos verificados).
- **Segmento:** departamentos en alquiler, Mendoza.
- **Unidad de analisis:** un aviso publicado. "Una fila es un departamento
  en alquiler publicado en Inmoclick para la provincia de Mendoza, en el
  momento del scraping."
- **Columna objetivo:** `precio_m2` (precio / superficie).
- **Clave primaria:** el ID del aviso dentro de su URL (ej. el `280` en
  `inmoclick.com/79085-shelter-propiedades/inmuebles/280/ficha/...`) —
  a confirmar que no se repite una vez que se scrapeen varios avisos.

## Arquitectura del pipeline

Cinco tareas de Airflow, capa bronce (`data/raw/`, HTML crudo tal como
llega) separada de capa plata (`data/processed/`, el CSV final):

    extract_listings -> parse_raw -> transform_clean -> quality_check -> export_csv

El DAG esqueleto esta en `dags/brujula_pipeline.py`, con un `TODO` por
tarea. La logica real de scraping y transformacion se completa en las
Fases 1 y 2 de la guia de pasos.

## Como levantar el entorno (Fase 0)

```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'
cp .env.example .env
docker compose up airflow-init
docker compose up
# UI en http://localhost:8080 (usuario y contraseña: airflow)
```

> El `docker-compose.yaml` oficial no esta versionado en este repo (ver
> `.gitignore`): cada integrante lo descarga una vez en su maquina con el
> comando de arriba, porque bajarlo automaticamente desde este entorno de
> desarrollo esta bloqueado por politica de red.

## Lo que hay que saber explicar en la defensa

Ver la seccion 6 de la guia de pasos / plan de vuelo: que es una fila, cual
es la columna objetivo, por que hay nulos, que pasa si se corre de nuevo,
donde queda el dato crudo, y el tema de tidy (amenities como texto libre,
mismo caso que el ejemplo de FIFA de la catedra).

## Estado

- [x] Fase 0 - estructura del repo y DAG esqueleto
- [ ] Fase 1 - extraccion real (extract_listings, parse_raw)
- [ ] Fase 2 - transformacion y calidad (transform_clean, quality_check)
- [ ] Fase 3 - correr el DAG completo y dejarlo en verde
- [ ] Fase 4 - documentacion final
- [ ] Fase 5 - ensayo cronometrado
