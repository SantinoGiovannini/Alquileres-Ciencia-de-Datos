# Extiende la imagen oficial de Airflow para poder correr Playwright +
# Chromium -- lo usa `dags/brujula/argenprop.py`, la fuente de respaldo real
# (argenprop.com renderiza precio y superficie por JavaScript del lado del
# cliente, asi que sin navegador esa ficha no se puede leer).
#
# Para usar este Dockerfile con el docker-compose.yaml oficial (Fase 0):
# en el bloque `x-airflow-common` del archivo que bajaste con curl, comenta
# la linea `image: ...` y descomenta `# build: .` (Apache ya deja el
# comentario listo para esto). Despues corre `docker compose build` antes
# de `docker compose up`.
FROM apache/airflow:3.3.1

USER root
# Playwright necesita estas librerias del sistema para correr Chromium
# headless -- `playwright install-deps` las instala solo, pero pide root.
RUN apt-get update && apt-get install -y --no-install-recommends \
        wget \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

USER root
RUN python -m playwright install-deps chromium
USER airflow
RUN python -m playwright install chromium
