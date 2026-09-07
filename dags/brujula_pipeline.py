"""
Brujula Inmobiliaria - Pipeline de Entrega 1 (Ingenieria de datos)

Segmento: departamentos en alquiler en Mendoza.
Fuente: Inmoclick (https://inmoclick.com/departamentos-en-alquiler-en-mendoza).

Cinco tareas, capa bronce (data/raw) separada de capa plata (data/processed):

    extract_listings -> parse_raw -> transform_clean -> quality_check -> export_csv

Esqueleto: cada tarea tiene un TODO. No hay logica real todavia -
se completa en las Fases 1 y 2 de la guia de pasos.
"""

from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="brujula_inmobiliaria_pipeline",
    schedule=None,  # manual por ahora; no hace falta que corra sola para la Entrega 1
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["brujula-inmobiliaria", "entrega-1"],
)
def brujula_inmobiliaria_pipeline():

    @task
    def extract_listings() -> str:
        """
        Capa bronce.
        Recorre las paginas de resultados de "departamentos en alquiler en
        Mendoza" en Inmoclick y guarda el HTML crudo de cada aviso, sin
        modificarlo, en data/raw/<fecha>/.

        TODO:
        - Pedir las paginas de resultados (respetar robots.txt, pausa entre pedidos).
        - Guardar cada HTML de aviso en data/raw/AAAAMMDD/<id>.html.
        - Devolver la carpeta de esta corrida para que la use parse_raw.
        """
        run_folder = "data/raw/TODO_fecha"
        return run_folder

    @task
    def parse_raw(run_folder: str) -> list:
        """
        Recorre el HTML guardado por extract_listings y extrae los campos de
        cada aviso a un registro: precio, direccion, localidad, m2 cubiertos
        y totales, ambientes, dormitorios, banos, tipo de propiedad,
        anunciante, descripcion, y el ID de la URL (candidato a clave).

        TODO:
        - Parsear cada archivo HTML de run_folder con BeautifulSoup.
        - Devolver la lista de registros (uno por aviso).
        """
        records = []
        return records

    @task
    def transform_clean(records: list) -> str:
        """
        Capa plata (en construccion).
        Tipa columnas, define la clave primaria, calcula precio_m2.

        TODO:
        - Cargar records en un DataFrame de pandas.
        - Tipar precio a numerico.
        - Definir y verificar la clave primaria (ID de la URL del aviso).
        - Calcular precio_m2 = precio / superficie.
        - Guardar el DataFrame intermedio (o pasarlo a la siguiente tarea).
        """
        clean_path = "data/processed/TODO_clean.parquet"
        return clean_path

    @task
    def quality_check(clean_path: str) -> str:
        """
        Corre los seis chequeos de calidad de la catedra sobre el DataFrame
        final y guarda un reporte. Si algo falla, que quede registrado, no
        oculto.

        TODO (sobre el DataFrame df):
        - df["clave"].is_unique
        - len(df)
        - df.shape
        - df.dtypes.value_counts()
        - df.isna().mean().sort_values(ascending=False)
        - df.columns[df.isna().all()]
        """
        report_path = "data/processed/quality_report.txt"
        return report_path

    @task
    def export_csv(clean_path: str, report_path: str) -> None:
        """
        Capa plata final.
        Escribe el CSV definitivo en data/processed/ - el archivo que se abre
        y se defiende en la Entrega 1.

        TODO:
        - Escribir data/processed/brujula_inmobiliaria_alquiler_mendoza.csv
        """
        pass

    run_folder = extract_listings()
    records = parse_raw(run_folder)
    clean_path = transform_clean(records)
    report_path = quality_check(clean_path)
    export_csv(clean_path, report_path)


brujula_inmobiliaria_pipeline()
