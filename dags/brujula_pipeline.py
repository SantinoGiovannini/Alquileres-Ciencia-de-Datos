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
  acumular_historico -> True (por defecto) arma el dataset con todas las
                 fechas que haya en data/raw/, no solo con la de esta
                 corrida. Cada scrapeo es una foto del inventario
                 publicado ese dia: la union suma los avisos que se dieron
                 de baja y los que aparecieron despues. False vuelve al
                 comportamiento de la Entrega 1, una corrida = un dataset.
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task

from brujula import export, extract, parse, quality, transform


@dag(
    dag_id="brujula_inmobiliaria_pipeline",
    # Diario a las 09:00 UTC (06:00 en Argentina). Diario y no por hora
    # porque un alquiler no cambia de precio en horas y cada corrida son
    # ~2.400 pedidos a portales ajenos: es el intervalo mas corto que trae
    # informacion nueva sin abusar del servidor del otro.
    schedule="0 9 * * *",
    start_date=datetime(2026, 9, 1),
    # Sin catchup a proposito. Un backfill no traeria los avisos de esa
    # fecha: los portales muestran solo lo que esta publicado hoy, asi que
    # correr hacia atras scrapearia N veces el inventario actual y lo
    # guardaria bajo N fechas distintas. Seria peor que no tener el dato.
    catchup=False,
    # Una corrida completa tarda ~65 minutos. Sin este limite, una corrida
    # atrasada se solapa con la siguiente y duplican pedidos.
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["brujula-inmobiliaria", "entrega-2"],
    params={
        "max_paginas": None,
        "fuentes": None,
        "fecha": None,
        "acumular_historico": True,
    },
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
        params = params or {}
        return parse.parse_raw(
            run_folder,
            fuentes=params.get("fuentes"),
            acumular=params.get("acumular_historico", True),
        )

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
    def quality_check(clean_path: str, run_folder: str) -> str:
        """
        Corre los seis chequeos de calidad de la catedra y guarda el
        reporte en resultados/<fecha>/. Si algo falla queda registrado,
        no oculto.
        """
        return quality.quality_check(clean_path, run_folder=run_folder)

    @task
    def export_csv(clean_path: str, report_path: str, run_folder: str) -> str:
        """
        Capa plata final: escribe el CSV definitivo en resultados/<fecha>/,
        al lado del reporte de calidad de esa misma corrida.
        """
        return export.export_csv(clean_path, report_path, run_folder=run_folder)

    run_folder = extract_listings()
    registros_path = parse_raw(run_folder)
    clean_path = transform_clean(registros_path, run_folder)
    report_path = quality_check(clean_path, run_folder)
    export_csv(clean_path, report_path, run_folder)


brujula_inmobiliaria_pipeline()
