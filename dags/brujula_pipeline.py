"""
Brujula Inmobiliaria -- Pipeline de Entrega 1 (Ingenieria de datos)

Segmento: departamentos en alquiler en Mendoza.
Fuente principal: Inmoclick (https://inmoclick.com/departamentos-en-alquiler-en-mendoza).
Fuente de respaldo real: argenprop.com (no otro motor sobre inmoclick, un
sitio con HTML y estructura completamente distintos). Ultimo recurso: una
semilla congelada versionada en `data/frozen/`.

Modelo medallon, capa bronce (`data/raw/<fecha>/`, HTML crudo tal como
llega) separada de capa plata (`data/processed/`, el CSV final):

    wait_for_source -> check_source ---> extract_listings -> parse_raw ------+
                                     |--> extract_listings_argenprop         |
                                     |        -> parse_raw_argenprop --------+--> transform_clean -> quality_check -> export_csv
                                     `--> use_frozen_snapshot ---------------+

`check_source` es una unica decision de tres vias, a proposito -- ver su
docstring: con dos sensores+branches en cascada, el segundo no puede
distinguir "nunca hizo falta intentarlo" (el primero ya respondio bien) de
"se intento y se agoto el tiempo", porque las dos cosas dejan la misma
tarea previa en `skipped`. Resolverlo en un solo lugar evita la ambiguedad.

Por que argenprop necesita Playwright y ver el `Dockerfile` de este repo:
argenprop renderiza precio y superficie por JavaScript del lado del
cliente -- verificado bajando una ficha con `requests` liso, donde ni el
precio ni la superficie aparecen en el documento. Sin Chromium instalado en
la imagen, la rama de respaldo real (no la del snapshot) no puede correr.
"""
from __future__ import annotations

import gzip
import logging
import sys
from pathlib import Path

import pendulum
from airflow.sdk import Param, PokeReturnValue, dag, task
from airflow.task.trigger_rule import TriggerRule

# Airflow no siempre pone la carpeta de DAGs en sys.path (a diferencia de
# Airflow 2, no esta garantizado con el bundle loader de Airflow 3): sin
# esto, `from brujula import ...` de mas abajo falla con
# ModuleNotFoundError aunque `dags/brujula/` este al lado de este archivo.
sys.path.insert(0, str(Path(__file__).parent))

from brujula import argenprop, schema
from brujula.inmoclick import (fetch, listing_url, n_pages,
                               parse_detail_page, parse_listing_page,
                               total_avisos)
from brujula.transform import to_row, to_row_argenprop

log = logging.getLogger(__name__)

RAW_DIR = Path("/opt/airflow/data/raw")
PROCESSED_DIR = Path("/opt/airflow/data/processed")
FROZEN_DIR = Path("/opt/airflow/data/frozen")
SEMILLA = FROZEN_DIR / "semilla.csv"

CSV_FINAL = PROCESSED_DIR / "brujula_inmobiliaria_alquiler_mendoza.csv"


def _run_folder(fecha: str) -> Path:
    return RAW_DIR / fecha


def _bronze_write(destino: Path, html: str) -> None:
    destino.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(destino, "wt", encoding="utf-8") as f:
        f.write(html)


def _bronze_read(ruta: Path) -> str:
    with gzip.open(ruta, "rt", encoding="utf-8") as f:
        return f.read()


@dag(
    dag_id="brujula_inmobiliaria_pipeline",
    schedule=None,  # manual por ahora; no hace falta que corra sola para la Entrega 1
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Argentina/Buenos_Aires"),
    catchup=False,
    tags=["brujula-inmobiliaria", "entrega-1"],
    params={
        "mode": Param(
            "subset", enum=["subset", "full"],
            title="Modo de corrida",
            description=("subset: la primera pagina del listado (~24 avisos, "
                         "con sus fichas). full: los ~715 departamentos en "
                         "alquiler de toda la provincia."),
        ),
        "engine": Param(
            "auto", enum=["auto", "http", "browser"],
            title="Motor de descarga (inmoclick)",
            description="auto prueba HTTP y cae a navegador si el sitio empieza a filtrar.",
        ),
    },
)
def brujula_inmobiliaria_pipeline():

    @task.sensor(poke_interval=180, timeout=900, mode="reschedule", soft_fail=True)
    def wait_for_source(**context) -> PokeReturnValue:
        """Espera a que Inmoclick responda. Sondea cada 3 min, hasta 15."""
        params = context["params"]
        try:
            html = fetch(listing_url(page=1), engine=params["engine"])
            total = total_avisos(html)
            if not total:
                log.warning("inmoclick respondio pero no se pudo leer el total de avisos")
                return PokeReturnValue(is_done=False)
            log.info("inmoclick responde: %s departamentos en alquiler", total)
            return PokeReturnValue(is_done=True, xcom_value=True)
        except Exception as e:
            log.warning("inmoclick todavia no responde (%s). Reintento en 3 min.", e)
            return PokeReturnValue(is_done=False)

    @task.branch(trigger_rule=TriggerRule.ALL_DONE)
    def check_source(**context) -> str:
        """Decide entre las tres ramas en un solo lugar, a proposito: con
        dos branches en cascada (uno para inmoclick, otro para argenprop) el
        segundo no puede distinguir "nunca hizo falta intentarlo" de "se
        intento y se agoto el tiempo" -- las dos cosas dejan la tarea previa
        en `skipped`. Resolverlo en una sola decision evita la ambiguedad en
        vez de tratar de distinguir dos causas que Airflow deja
        indistinguibles.
        """
        ok = context["ti"].xcom_pull(task_ids="wait_for_source")
        if ok:
            return "extract_listings"
        log.error("inmoclick no respondio en 15 minutos. Se prueba argenprop "
                  "como segunda fuente antes del snapshot congelado.")

        try:
            candidatas = argenprop.discover_urls()
        except Exception as e:
            log.warning("argenprop tampoco responde (%s). Se usa el snapshot "
                       "congelado.", e)
            return "use_frozen_snapshot"

        if not candidatas:
            log.warning("argenprop respondio pero no se encontraron fichas "
                       "candidatas de Mendoza. Se usa el snapshot congelado.")
            return "use_frozen_snapshot"

        log.info("argenprop responde: %s fichas candidatas", len(candidatas))
        context["ti"].xcom_push(key="candidatas_argenprop", value=candidatas)
        return "extract_listings_argenprop"

    @task
    def extract_listings(**context) -> str:
        """Capa bronce, inmoclick. Baja el HTML de cada pagina de resultados
        y de cada ficha individual (dos saltos: la tarjeta del listado trae
        precio/m2/dormitorios, pero antiguedad/ambientes/expensas solo
        estan en la ficha), sin interpretar nada.
        """
        params = context["params"]
        dag_run = context["dag_run"]
        fecha = (dag_run.logical_date or dag_run.run_after).date().isoformat()
        run_folder = _run_folder(fecha) / "inmoclick"

        html = fetch(listing_url(page=1), engine=params["engine"])
        total = total_avisos(html) or 0
        paginas = n_pages(total)
        if params["mode"] == "subset":
            paginas = min(1, paginas)

        cards = {}
        for page in range(1, paginas + 1):
            html = html if page == 1 else fetch(listing_url(page=page), engine=params["engine"])
            destino = run_folder / "listado" / f"pagina_{page:05d}.html.gz"
            _bronze_write(destino, html)
            for c in parse_listing_page(html):
                cards[c["kid"]] = c

        import pandas as pd
        listado_csv = run_folder / "listado.csv"
        listado_csv.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(list(cards.values())).to_csv(listado_csv, index=False)

        detail_dir = run_folder / "detalle"
        for kid, card in cards.items():
            if not card.get("url"):
                continue
            try:
                detalle_html = fetch(card["url"], engine=params["engine"])
            except Exception as e:
                log.warning("no se pudo bajar la ficha %s: %s", kid, e)
                continue
            _bronze_write(detail_dir / f"{kid}.html.gz", detalle_html)

        log.info("inmoclick: %s avisos en %s paginas -> %s", len(cards), paginas, run_folder)
        return str(run_folder)

    @task
    def parse_raw(run_folder: str, **context) -> list:
        """Recorre el bronce de inmoclick (sin red) y arma un registro por
        aviso, combinando tarjeta + ficha.
        """
        import pandas as pd

        dag_run = context["dag_run"]
        fecha_extraccion = (dag_run.logical_date or dag_run.run_after).date().isoformat()

        carpeta = Path(run_folder)
        listado = pd.read_csv(carpeta / "listado.csv", dtype=str, keep_default_na=False)
        detail_dir = carpeta / "detalle"

        registros = []
        for _, raw in listado.iterrows():
            card = {k: (v if v != "" else None) for k, v in raw.to_dict().items()}
            ficha = detail_dir / f"{card['kid']}.html.gz"
            detalle = parse_detail_page(_bronze_read(ficha)) if ficha.exists() else None
            registros.append(to_row(card, detalle, fecha_extraccion))

        log.info("inmoclick: %s registros parseados", len(registros))
        return registros

    @task
    def extract_listings_argenprop(**context) -> list[dict]:
        """Capa bronce, argenprop (respaldo). Usa las candidatas que ya
        encontro `check_source` (no vuelve a tocar la red para descubrir) y
        las baja con un unico navegador Playwright (`argenprop.fetch_many`),
        no uno por ficha.
        """
        params = context["params"]
        dag_run = context["dag_run"]
        fecha = (dag_run.logical_date or dag_run.run_after).date().isoformat()
        run_folder = _run_folder(fecha) / "argenprop"

        candidatas = context["ti"].xcom_pull(task_ids="check_source",
                                             key="candidatas_argenprop") or []
        if params["mode"] == "subset":
            candidatas = candidatas[:12]

        targets = []
        for url in candidatas:
            lid = argenprop.listing_id(url)
            destino = run_folder / f"{lid}.html.gz"
            targets.append({"url": url, "listing_id": lid, "bronce": str(destino)})

        by_url = {t["url"]: t for t in targets}
        pedidas = fallidas = 0
        for url, html, err in argenprop.fetch_many(list(by_url.keys())):
            t = by_url[url]
            if err is not None:
                log.warning("no se pudo bajar la ficha de argenprop %s: %s", url, err)
                fallidas += 1
                t["bronce"] = None
                continue
            _bronze_write(Path(t["bronce"]), html)
            pedidas += 1

        log.info("argenprop: %s fichas (%s pedidas, %s fallidas)",
                 len(targets), pedidas, fallidas)
        return [t for t in targets if t.get("bronce")]

    @task
    def parse_raw_argenprop(targets: list[dict], **context) -> list:
        """Parsea el bronce de argenprop y descarta lo que no sea de
        Mendoza (ver `argenprop.parse_ficha`)."""
        dag_run = context["dag_run"]
        fecha_extraccion = (dag_run.logical_date or dag_run.run_after).date().isoformat()

        registros, descartadas = [], 0
        for t in targets:
            ficha = argenprop.parse_ficha(_bronze_read(Path(t["bronce"])))
            # None: no es Mendoza. Sin precio o sin moneda reconocida
            # ("Consultar precio", un formato de USD no contemplado en
            # argenprop.money): ninguno sirve para el esquema.
            if (ficha is None or ficha.get("precio") is None
                    or ficha.get("moneda") is None):
                descartadas += 1
                continue
            registros.append(to_row_argenprop(ficha, t["listing_id"], t["url"],
                                              fecha_extraccion))

        log.info("argenprop: %s avisos de Mendoza (%s descartados)",
                 len(registros), descartadas)
        return registros

    @task
    def use_frozen_snapshot() -> str:
        """Ultimo recurso: la semilla versionada en el repositorio. Se usa
        tal cual viene (ya es capa plata), asi que `transform_clean` la
        toma como el CSV final y no reconstruye nada desde HTML.
        """
        if not SEMILLA.exists():
            raise FileNotFoundError(
                f"inmoclick y argenprop no responden y no hay semilla en {SEMILLA}. "
                "Falta data/frozen/semilla.csv del repositorio.")
        log.warning("Ni inmoclick ni argenprop respondieron. Se usa la semilla "
                   "congelada -- estos datos NO son de hoy.")
        return str(SEMILLA)

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def transform_clean(desde_inmoclick: list | None, desde_argenprop: list | None,
                        desde_congelado: str | None, **context) -> str:
        """Capa plata. Tipa columnas, define la clave primaria, calcula
        `precio_m2` -- la columna objetivo de la propuesta.
        """
        import pandas as pd

        if desde_congelado:
            df = pd.read_csv(desde_congelado, low_memory=False)
        else:
            registros = (desde_inmoclick or []) + (desde_argenprop or [])
            if not registros:
                raise ValueError("ninguna rama produjo avisos")
            df = pd.DataFrame(registros, columns=schema.COLUMNS)

        antes = len(df)
        df = df.drop_duplicates("listing_id").reset_index(drop=True)
        if len(df) != antes:
            log.info("%s avisos, %s tras deduplicar por listing_id", antes, len(df))

        for c in schema.ENTEROS:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
        for c in schema.BOOLEANOS:
            if c in df.columns:
                df[c] = df[c].map({True: True, False: False, "True": True, "False": False})

        # precio_m2 = precio / superficie cubierta -- columna objetivo de la
        # propuesta. Se recalcula siempre aca, aunque venga de la semilla,
        # para que no dependa de que quien la genero haya usado la misma
        # convencion de redondeo.
        sup = pd.to_numeric(df["superficie_cubierta_m2"], errors="coerce")
        precio = pd.to_numeric(df["precio"], errors="coerce")
        df["precio_m2"] = (precio / sup).where(sup > 0).round(2)

        dag_run = context["dag_run"]
        fecha = (dag_run.logical_date or dag_run.run_after).date().isoformat()
        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        clean_path = PROCESSED_DIR / f"_clean_{fecha}.csv"
        df.to_csv(clean_path, index=False)
        log.info("%s avisos limpios -> %s", len(df), clean_path.name)
        return str(clean_path)

    @task
    def quality_check(clean_path: str) -> str:
        """Los seis chequeos de calidad de la catedra. Si algo falla, que
        quede registrado en el reporte -- no oculto -- salvo que el dataset
        este directamente vacio, ahi si se corta la corrida: no tiene
        sentido exportar un CSV sin filas.
        """
        import pandas as pd

        df = pd.read_csv(clean_path, low_memory=False)
        if len(df) == 0:
            raise ValueError("quality_check: 0 avisos, no hay nada para exportar")

        lineas = [
            f"clave (listing_id) es unica: {df['listing_id'].is_unique}",
            f"len(df): {len(df)}",
            f"df.shape: {df.shape}",
            "df.dtypes.value_counts():",
            df.dtypes.value_counts().to_string(),
            "df.isna().mean().sort_values(ascending=False):",
            df.isna().mean().sort_values(ascending=False).to_string(),
            f"columnas siempre nulas: {list(df.columns[df.isna().all()])}",
        ]
        reporte = "\n\n".join(lineas)

        report_path = Path(clean_path).with_name(
            Path(clean_path).name.replace("_clean_", "quality_report_").replace(".csv", ".txt"))
        report_path.write_text(reporte, encoding="utf-8")
        log.info("Reporte de calidad -> %s", report_path.name)
        return str(report_path)

    @task
    def export_csv(clean_path: str, report_path: str) -> str:
        """Capa plata final. Escribe el CSV definitivo que se abre y se
        defiende en la Entrega 1."""
        import pandas as pd

        df = pd.read_csv(clean_path, low_memory=False)
        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        df.to_csv(CSV_FINAL, index=False)
        log.info("Dataset final -> %s (reporte en %s)", CSV_FINAL, Path(report_path).name)
        return str(CSV_FINAL)

    espera = wait_for_source()
    rama = check_source()

    run_folder = extract_listings()
    registros_inmoclick = parse_raw(run_folder)

    objetivos_ap = extract_listings_argenprop()
    registros_argenprop = parse_raw_argenprop(objetivos_ap)

    congelado = use_frozen_snapshot()

    espera >> rama
    rama >> [run_folder, objetivos_ap, congelado]

    limpio = transform_clean(registros_inmoclick, registros_argenprop, congelado)
    reporte = quality_check(limpio)
    export_csv(limpio, reporte)


brujula_inmobiliaria_pipeline()
