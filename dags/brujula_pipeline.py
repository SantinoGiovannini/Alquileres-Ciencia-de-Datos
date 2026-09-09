"""
Brujula Inmobiliaria - Pipeline de Entrega 1 (Ingenieria de datos)

Segmento: departamentos en alquiler en Mendoza.
Fuentes: Inmoclick (inmoclick.com) e InmoUP (inmoup.com.ar).

Cinco tareas, capa bronce (data/raw) separada de capa plata (data/processed):

    extract_listings -> parse_raw -> transform_clean -> quality_check -> export_csv

La logica real vive en el paquete dags/brujula/, no aca: asi se puede
probar desde una terminal con `python -m brujula.pipeline` sin levantar
Airflow, y el DAG queda como lo que es, el orden de las tareas.

Parametros de la corrida (se editan al dispararla desde la UI):
  max_paginas -> None baja el listado completo de cada portal
                 (~2.400 avisos, cerca de una hora); un numero chico
                 sirve para una prueba rapida.
  fuentes     -> None usa todos los portales; ["inmoup"] usa solo uno.
  fecha       -> AAAAMMDD, elige la carpeta de la capa bronce. None usa
                 hoy en UTC (el contenedor no corre en hora argentina).
                 Poner una fecha ya scrapeada reprocesa sin volver a
                 pedirle nada a los portales.
"""

from datetime import datetime

from airflow.decorators import dag, task

from brujula import export, extract, parse, quality, transform


@dag(
    dag_id="brujula_inmobiliaria_pipeline",
    schedule=None,  # manual por ahora; no hace falta que corra sola para la Entrega 1
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["brujula-inmobiliaria", "entrega-1"],
    params={"max_paginas": None, "fuentes": None, "fecha": None},
)
def brujula_inmobiliaria_pipeline():

    @task
    def extract_listings(params: dict = None) -> str:
        """
        Capa bronce.
        Baja las paginas de resultados y la ficha de cada aviso de cada
        portal, y guarda el HTML tal como llega en
        data/raw/AAAAMMDD/<fuente>/. No modifica nada: si mas adelante hay
        que corregir la transformacion, no se vuelve a scrapear.
        """
        params = params or {}
        return extract.extract_listings(
            max_paginas=params.get("max_paginas"),
            fuentes=params.get("fuentes"),
            fecha=params.get("fecha"),
        )

    @task
    def parse_raw(run_folder: str, params: dict = None) -> str:
        """
        Recorre el HTML guardado de cada portal y arma un registro por
        aviso. Cada fuente sabe leer lo suyo (tabla HTML en Inmoclick,
        JSON-LD en InmoUP) y devuelve el mismo esquema de columnas.

        Devuelve la ruta del archivo con los registros, no los registros:
        son miles, y pasarlos por XCom cargaria varios MB en la base de
        metadatos de Airflow.
        """
        return parse.parse_raw(run_folder, fuentes=(params or {}).get("fuentes"))

    @task
    def transform_clean(registros_path: str, run_folder: str) -> str:
        """
        Capa plata.
        Tipa las columnas, arma la clave primaria (fuente:usr_id-prp_id),
        marca el mismo inmueble publicado en dos portales y calcula la
        columna objetivo precio_m2.
        """
        return transform.transform_clean(registros_path, run_folder=run_folder)

    @task
    def quality_check(clean_path: str) -> str:
        """
        Corre los seis chequeos de calidad de la catedra y guarda el reporte.
        Si algo falla queda registrado, no oculto.
        """
        return quality.quality_check(clean_path)

    @task
    def export_csv(clean_path: str, report_path: str) -> str:
        """
        Capa plata final: escribe el CSV definitivo en data/processed/.
        """
        return export.export_csv(clean_path, report_path)

    run_folder = extract_listings()
    registros_path = parse_raw(run_folder)
    clean_path = transform_clean(registros_path, run_folder)
    report_path = quality_check(clean_path)
    export_csv(clean_path, report_path)


brujula_inmobiliaria_pipeline()
