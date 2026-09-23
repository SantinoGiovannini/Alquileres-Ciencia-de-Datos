# Imagen reproducible del proyecto sobre la misma version que el compose
# oficial descargado para este repo.
FROM apache/airflow:3.3.0

COPY requirements.txt /requirements.txt

# Las dependencias quedan instaladas en la imagen y no se reinstalan en cada
# arranque de scheduler/worker/web, como ocurriria con _PIP_ADDITIONAL_REQUIREMENTS.
RUN pip install --no-cache-dir -r /requirements.txt
