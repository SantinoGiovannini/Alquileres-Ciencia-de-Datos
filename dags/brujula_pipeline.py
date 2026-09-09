"""
Brujula Inmobiliaria -- Pipeline de Entrega 1 (Ingenieria de datos)

Segmento: departamentos y casas en alquiler en Mendoza.
Fuente principal: Inmoclick (https://inmoclick.com/departamentos-en-alquiler-en-mendoza).
Fuentes adicionales reales: argenprop.com e inmoup.com.ar -- no son otro
motor sobre Inmoclick, son sitios con HTML y estructura completamente
distintos. Ultimo recurso: una semilla congelada versionada en `data/frozen/`.

Modelo medallon, capa bronce (`data/raw/<fecha>/`, HTML crudo tal como
llega) separada de capa plata (`data/processed/`, el CSV final):

    wait_for_source -> check_source ---> extract_listings -> parse_raw --------+
                                     `--> use_frozen_snapshot -----------------+
    extract_listings_argenprop -> parse_raw_argenprop -----------------------+--> transform_clean -> quality_check -> export_csv
    extract_listings_inmoup -> parse_raw_inmoup ----------------------------/

**Por que argenprop e inmoup siempre corren, y no solo como respaldo ante
una caida.** El criterio "Volumen suficiente" del Kit de arranque pide mas
de 1.000 filas. Inmoclick solo, incluso sumando departamentos y casas, da
~830 avisos -- por debajo del piso. El kit mismo recomienda la salida para
este caso: "combinar con otra fuente" (en plural, en este caso: dos). Por
eso ninguna de las dos fuentes adicionales esta condicionada a que
Inmoclick falle: corren siempre, en paralelo, y sus avisos se suman a los
de Inmoclick en `transform_clean`. El respaldo ante una fuente caida sigue
existiendo (`check_source` -> `use_frozen_snapshot` si Inmoclick no
responde), pero es una decision aparte de si las otras dos aportan filas o no.

**Por que argenprop necesita Playwright e inmoup no.** argenprop renderiza
precio y superficie por JavaScript del lado del cliente -- verificado
bajando una ficha con `requests` liso, donde ni el precio ni la superficie
aparecen en el documento. inmoup, en cambio, trae toda la ficha en un
bloque JSON-LD (`schema.org RealEstateListing`) ya presente en el HTML sin
JavaScript -- alcanza con `requests`, igual que Inmoclick. Sin Chromium
instalado en la imagen (ver el `Dockerfile` de este repo),
`extract_listings_argenprop` no puede correr; `extract_listings_inmoup` si.
"""
from __future__ import annotations

import gzip
import logging
import sys
import time
from pathlib import Path

import pendulum
from airflow.sdk import Param, PokeReturnValue, dag, task
from airflow.task.trigger_rule import TriggerRule

# Airflow no siempre pone la carpeta de DAGs en sys.path (a diferencia de
# Airflow 2, no esta garantizado con el bundle loader de Airflow 3): sin
# esto, `from brujula import ...` de mas abajo falla con
# ModuleNotFoundError aunque `dags/brujula/` este al lado de este archivo.
sys.path.insert(0, str(Path(__file__).parent))

from brujula import argenprop, inmoup, schema
from brujula.inmoclick import (TIPOS, fetch, listing_url, n_pages,
                               parse_detail_page, parse_listing_page,
                               total_avisos)
from brujula.transform import to_row, to_row_argenprop, to_row_inmoup

log = logging.getLogger(__name__)

RAW_DIR = Path("/opt/airflow/data/raw")
PROCESSED_DIR = Path("/opt/airflow/data/processed")
FROZEN_DIR = Path("/opt/airflow/data/frozen")
SEMILLA = FROZEN_DIR / "semilla.csv"

CSV_FINAL = PROCESSED_DIR / "brujula_inmobiliaria_alquiler_mendoza.csv"

# departamento|casa -> plural que usa la URL de inmoclick
TIPO_SINGULAR = {"departamentos": "departamento", "casas": "casa"}


def _run_folder(fecha: str) -> Path:
    return RAW_DIR / fecha


def _bronze_write(destino: Path, html: str) -> None:
    destino.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(destino, "wt", encoding="utf-8") as f:
        f.write(html)


def _bronze_read(ruta: Path) -> str:
    with gzip.open(ruta, "rt", encoding="utf-8") as f:
        return f.read()


def _bronze_ok(ruta: Path, min_bytes: int = 500) -> bool:
    """No alcanza con que el archivo exista para reusarlo del cache: una
    respuesta 200 pero vacia (ver `extract_listings`) igual queda en disco
    como un .html.gz valido, solo que sin contenido adentro."""
    return ruta.exists() and ruta.stat().st_size > min_bytes // 10


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
            description=("subset: la primera pagina de cada tipo en inmoclick "
                         "(~48 avisos) + ~12 fichas de argenprop. full: "
                         "~980 avisos de inmoclick (departamentos + casas) "
                         "mas lo que aporte argenprop -- para pasar el piso "
                         "de 1.000 filas del Kit de arranque."),
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
            html = fetch(listing_url("departamentos", page=1), engine=params["engine"])
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
        """Solo decide entre Inmoclick vivo y el snapshot congelado.

        Antes esta tarea tambien intentaba argenprop como segunda opcion, y
        eso generaba un problema real: un branch corriendo aguas abajo de un
        sensor con `soft_fail=True` no puede distinguir "nunca hizo falta
        intentar la segunda fuente" de "se intento y se agoto el tiempo" --
        las dos cosas dejan la tarea anterior en `skipped`. Ahora que
        argenprop corre siempre en paralelo (ver el docstring del modulo),
        esa ambiguedad desaparece: esta decision vuelve a ser binaria.
        """
        ok = context["ti"].xcom_pull(task_ids="wait_for_source")
        if ok:
            return "extract_listings"
        log.error("inmoclick no respondio en 15 minutos. Se usa el snapshot "
                  "congelado para esa fuente (argenprop sigue corriendo aparte).")
        return "use_frozen_snapshot"

    @task
    def extract_listings(**context) -> str:
        """Capa bronce, inmoclick. Baja el HTML de cada pagina de resultados
        y de cada ficha individual, para departamentos y casas (dos saltos:
        la tarjeta del listado trae precio/m2/dormitorios, pero
        antiguedad/ambientes/expensas solo estan en la ficha).
        """
        import pandas as pd

        params = context["params"]
        dag_run = context["dag_run"]
        fecha = (dag_run.logical_date or dag_run.run_after).date().isoformat()
        run_folder = _run_folder(fecha) / "inmoclick"

        total_avisos_bajados = 0
        for tipo in TIPOS:
            tipo_folder = run_folder / f"tipo={tipo}"

            # La pagina 1 decide cuantas paginas hay que pedir, asi que si
            # ya esta en bronce de una corrida anterior se usa esa -- no
            # tiene sentido depender de un fetch en vivo para un dato que
            # ya se tiene, y un 200 con una pagina que no trae el texto "N
            # Casas en Alquiler encontradas" (interstitial, respuesta rara
            # puntual) pasa: dos corridas full distintas dieron `total=0`
            # para "casas" en el primer intento en vivo, pisando encima el
            # listado.csv de 197 avisos que ya estaba bien.
            pagina1_destino = tipo_folder / "listado" / "pagina_00001.html.gz"
            if _bronze_ok(pagina1_destino) and not params.get("force"):
                html = _bronze_read(pagina1_destino)
                total = total_avisos(html) or 0
            else:
                total = 0
                for intento in range(3):
                    html = fetch(listing_url(tipo, page=1), engine=params["engine"])
                    total = total_avisos(html) or 0
                    if total:
                        break
                    log.warning("%s: la pagina 1 no trajo el conteo de avisos "
                               "(intento %s/3). Reintento.", tipo, intento + 1)
                    time.sleep(2)

            paginas = n_pages(total)
            if params["mode"] == "subset":
                paginas = min(1, paginas)

            cards = {}
            for page in range(1, paginas + 1):
                destino = tipo_folder / "listado" / f"pagina_{page:05d}.html.gz"
                if _bronze_ok(destino) and not params.get("force"):
                    pagina_html = _bronze_read(destino)
                else:
                    pagina_html = html if page == 1 else fetch(
                        listing_url(tipo, page=page), engine=params["engine"])
                    _bronze_write(destino, pagina_html)
                    time.sleep(0.3)  # no golpear la fuente
                for c in parse_listing_page(pagina_html):
                    cards[c["kid"]] = c

            listado_csv = tipo_folder / "listado.csv"
            if not cards and listado_csv.exists():
                # Red de seguridad: si esta corrida no encontro ni un aviso
                # pero ya habia un listado.csv de una corrida anterior, no
                # se pisa con uno vacio -- mejor un dato de otro dia que
                # perder lo que ya se tenia bien.
                log.warning("%s: 0 avisos esta vez, se conserva el "
                           "listado.csv anterior", tipo)
            else:
                listado_csv.parent.mkdir(parents=True, exist_ok=True)
                pd.DataFrame(list(cards.values())).to_csv(listado_csv, index=False)

            detail_dir = tipo_folder / "detalle"
            vacias = 0
            for kid, card in cards.items():
                if not card.get("url"):
                    continue
                destino = detail_dir / f"{kid}.html.gz"
                if _bronze_ok(destino) and not params.get("force"):
                    continue

                # 0,2s de pausa no alcanzaba: con 635 fichas seguidas,
                # inmoclick empezaba a devolver 200 con cuerpo vacio (mismo
                # patron que el WAF de argenprop, ver argenprop.py). Con
                # mas pausa de base y un reintento con backoff cuando
                # vuelve vacia, se recupera.
                detalle_html = None
                for intento in range(2):
                    try:
                        detalle_html = fetch(card["url"], engine=params["engine"])
                    except Exception as e:
                        log.warning("no se pudo bajar la ficha %s: %s", kid, e)
                        break
                    if detalle_html and len(detalle_html) >= 500:
                        break
                    time.sleep(3)
                time.sleep(0.6)  # no golpear la fuente

                if not detalle_html or len(detalle_html) < 500:
                    # Si sigue vacia tras el reintento, no se guarda -- si
                    # se guardara, `_bronze_ok` la daria por valida y
                    # ningun reintento futuro la volveria a pedir.
                    vacias += 1
                    continue
                _bronze_write(destino, detalle_html)
            if vacias:
                log.warning("%s: %s fichas quedaron vacias tras reintentar", tipo, vacias)

            log.info("inmoclick %s: %s avisos en %s paginas", tipo, len(cards), paginas)
            total_avisos_bajados += len(cards)

        log.info("inmoclick: %s avisos totales -> %s", total_avisos_bajados, run_folder)
        return str(run_folder)

    @task
    def parse_raw(run_folder: str, **context) -> list:
        """Recorre el bronce de inmoclick (sin red) y arma un registro por
        aviso, combinando tarjeta + ficha, para cada tipo.
        """
        import pandas as pd

        dag_run = context["dag_run"]
        fecha_extraccion = (dag_run.logical_date or dag_run.run_after).date().isoformat()

        registros = []
        for tipo in TIPOS:
            tipo_folder = Path(run_folder) / f"tipo={tipo}"
            listado_csv = tipo_folder / "listado.csv"
            if not listado_csv.exists():
                continue
            listado = pd.read_csv(listado_csv, dtype=str, keep_default_na=False)
            detail_dir = tipo_folder / "detalle"

            for _, raw in listado.iterrows():
                card = {k: (v if v != "" else None) for k, v in raw.to_dict().items()}
                ficha = detail_dir / f"{card['kid']}.html.gz"
                detalle = parse_detail_page(_bronze_read(ficha)) if ficha.exists() else None
                registros.append(to_row(card, detalle, TIPO_SINGULAR[tipo], fecha_extraccion))

        log.info("inmoclick: %s registros parseados", len(registros))
        return registros

    @task
    def extract_listings_argenprop(**context) -> list[dict]:
        """Capa bronce, argenprop. Corre siempre (ver docstring del modulo:
        es lo que empuja el dataset por encima de 1.000 filas), descubre las
        candidatas por sitemap y las baja con un unico navegador Playwright
        (`argenprop.fetch_many`), no uno por ficha.
        """
        params = context["params"]
        dag_run = context["dag_run"]
        fecha = (dag_run.logical_date or dag_run.run_after).date().isoformat()
        run_folder = _run_folder(fecha) / "argenprop"

        try:
            candidatas = argenprop.discover_urls()
        except Exception as e:
            log.warning("argenprop no responde (%s). Sigue solo con inmoclick.", e)
            return []

        # Tope tambien en modo full, a proposito: se verifico contra el
        # sitio real que pasado cierto volumen de fichas en poco tiempo, el
        # WAF de argenprop (AWS WAF, pantalla "Human Verification") empieza
        # a bloquear -- de 337 pedidas de una, mas del 90% volvieron
        # bloqueadas. Un tope bajo, con pausa entre pedidos, se mantiene
        # cortes; pedir todo el universo no.
        tope = 12 if params["mode"] == "subset" else 220
        candidatas = candidatas[:tope]

        targets, pendientes = [], []
        for url in candidatas:
            lid = argenprop.listing_id(url)
            tipo = argenprop.tipo_from_url(url)
            destino = run_folder / f"{lid}.html.gz"
            t = {"url": url, "listing_id": lid, "tipo": tipo, "bronce": str(destino)}
            targets.append(t)
            if not (destino.exists() and not params.get("force")):
                pendientes.append(t)

        by_url = {t["url"]: t for t in pendientes}
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

        log.info("argenprop: %s fichas (%s pedidas, %s ya en bronce, %s fallidas)",
                 len(targets), pedidas, len(targets) - len(pendientes), fallidas)
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
                                              t["tipo"], fecha_extraccion))

        log.info("argenprop: %s avisos de Mendoza (%s descartados)",
                 len(registros), descartadas)
        return registros

    @task
    def extract_listings_inmoup(**context) -> list[dict]:
        """Capa bronce, inmoup. Corre siempre, igual que argenprop (ver
        docstring del modulo). No necesita Playwright: la ficha trae el
        JSON-LD completo en el HTML plano, alcanza con `requests` -- por
        eso acá se guarda directamente el HTML con `fetch`, no un navegador.
        """
        params = context["params"]
        dag_run = context["dag_run"]
        fecha = (dag_run.logical_date or dag_run.run_after).date().isoformat()
        run_folder = _run_folder(fecha) / "inmoup"

        try:
            candidatas = inmoup.discover_urls()
        except Exception as e:
            log.warning("inmoup no responde (%s). Sigue con las otras fuentes.", e)
            return []

        tope = 12 if params["mode"] == "subset" else 500
        candidatas = candidatas[:tope]

        targets = []
        for url in candidatas:
            lid = inmoup.listing_id(url)
            tipo = inmoup.tipo_from_url(url)
            if not lid or not tipo:
                continue
            destino = run_folder / f"{lid}.html.gz"
            targets.append({"url": url, "listing_id": lid, "tipo": tipo,
                            "bronce": str(destino)})

        pedidas = fallidas = 0
        for t in targets:
            destino = Path(t["bronce"])
            if _bronze_ok(destino) and not params.get("force"):
                continue
            try:
                html = inmoup.fetch(t["url"])
            except Exception as e:
                log.warning("no se pudo bajar la ficha de inmoup %s: %s", t["url"], e)
                fallidas += 1
                t["bronce"] = None
                continue
            _bronze_write(destino, html)
            pedidas += 1
            time.sleep(0.4)  # no golpear la fuente

        log.info("inmoup: %s fichas (%s pedidas, %s fallidas)",
                 len(targets), pedidas, fallidas)
        return [t for t in targets if t.get("bronce")]

    @task
    def parse_raw_inmoup(targets: list[dict], **context) -> list:
        """Parsea el bronce de inmoup (JSON-LD) y descarta lo que no sea
        de Mendoza (ver `inmoup.parse_ficha`)."""
        dag_run = context["dag_run"]
        fecha_extraccion = (dag_run.logical_date or dag_run.run_after).date().isoformat()

        registros, descartadas = [], 0
        for t in targets:
            ficha = inmoup.parse_ficha(_bronze_read(Path(t["bronce"])), t["url"])
            if ficha is None or ficha.get("precio") is None or ficha.get("moneda") is None:
                descartadas += 1
                continue
            registros.append(to_row_inmoup(ficha, t["listing_id"], t["url"],
                                           t["tipo"], fecha_extraccion))

        log.info("inmoup: %s avisos de Mendoza (%s descartados)",
                 len(registros), descartadas)
        return registros

    @task
    def use_frozen_snapshot() -> list:
        """Ultimo recurso para inmoclick: la semilla versionada en el
        repositorio, leida como registros (mismo formato que `parse_raw`)
        para que `transform_clean` la trate igual que cualquier otra fuente.
        """
        import pandas as pd

        if not SEMILLA.exists():
            raise FileNotFoundError(
                f"inmoclick no responde y no hay semilla en {SEMILLA}. "
                "Falta data/frozen/semilla.csv del repositorio.")
        log.warning("inmoclick no respondio. Se usa la semilla congelada -- "
                   "estos datos NO son de hoy.")
        return pd.read_csv(SEMILLA, low_memory=False).to_dict("records")

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def transform_clean(desde_inmoclick: list | None, desde_congelado: list | None,
                        desde_argenprop: list | None, desde_inmoup: list | None,
                        **context) -> str:
        """Capa plata. Junta las fuentes que corrieron, tipa columnas, dedupe
        por `listing_id` y calcula `precio_m2` -- la columna objetivo de la
        propuesta.
        """
        import pandas as pd

        registros = (list(desde_inmoclick or []) + list(desde_congelado or [])
                    + list(desde_argenprop or []) + list(desde_inmoup or []))
        if not registros:
            raise ValueError("ninguna fuente produjo avisos")
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
        """Los siete criterios del Kit de arranque, los que se verifican con
        un numero sobre el DataFrame ya limpio. Si algo falla, que quede
        registrado en el reporte -- no oculto -- salvo que el dataset este
        directamente vacio, ahi si se corta la corrida: no tiene sentido
        exportar un CSV sin filas.
        """
        import pandas as pd

        df = pd.read_csv(clean_path, low_memory=False)
        if len(df) == 0:
            raise ValueError("quality_check: 0 avisos, no hay nada para exportar")

        lineas = [
            f"1) Clave sin duplicados -- listing_id.is_unique: {df['listing_id'].is_unique}",
            f"2) Volumen suficiente (>1.000) -- len(df): {len(df)}",
            f"3) Ancho suficiente (>=5 cols) -- df.shape: {df.shape}",
            "4) Mezcla de tipos -- df.dtypes.value_counts():",
            df.dtypes.value_counts().to_string(),
            "5) Nulos conocidos -- df.isna().mean().sort_values(ascending=False):",
            df.isna().mean().sort_values(ascending=False).to_string(),
            f"6) Sin columnas 100% vacias -- df.columns[df.isna().all()]: {list(df.columns[df.isna().all()])}",
            "7) Documentacion entendible -- ver dags/brujula/schema.py (cada "
            "columna tiene su origen y su tipo documentados ahi).",
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
    congelado = use_frozen_snapshot()

    espera >> rama
    rama >> [run_folder, congelado]

    objetivos_ap = extract_listings_argenprop()
    registros_argenprop = parse_raw_argenprop(objetivos_ap)

    objetivos_iu = extract_listings_inmoup()
    registros_inmoup = parse_raw_inmoup(objetivos_iu)

    limpio = transform_clean(registros_inmoclick, congelado, registros_argenprop,
                             registros_inmoup)
    reporte = quality_check(limpio)
    export_csv(limpio, reporte)


brujula_inmobiliaria_pipeline()
