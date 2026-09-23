"""Del HTML crudo a un registro por aviso, juntando todas las fuentes.

Cada portal sabe leer lo suyo (ver brujula/fuentes/) y devuelve registros
con el mismo esquema de columnas; aca solo se concatenan.

El resultado se escribe en un archivo y la tarea devuelve la ruta, en vez
de devolver la lista entera: son miles de registros con descripciones
largas, y hacerlos pasar por XCom significaria meter varios MB adentro de
la base de metadatos de Airflow, que no esta para eso.
"""

import json
import logging
from pathlib import Path

from brujula.config import PROCESSED_DIR, RAW_DIR
from brujula.fuentes import elegir

log = logging.getLogger(__name__)

REGISTROS = PROCESSED_DIR / "_registros.json"


def _carpetas_a_leer(run_dir: Path, acumular: bool) -> list:
    """Carpetas de la capa bronce que entran en esta corrida.

    Con `acumular`, entran todas las fechas que haya en data/raw/ y no solo
    la de la corrida. La razon es que cada scrapeo es una *foto* del
    inventario publicado ese dia, no un incremento: el portal muestra lo que
    esta publicado hoy y nada mas. Leyendo una sola fecha, los avisos que se
    dieron de baja la semana pasada no existen para el dataset, aunque su
    HTML este en disco.

    Medido entre corridas consecutivas, aparecen entre 17 y 35 avisos nuevos
    por dia, asi que la union de fechas suma filas reales.
    """
    if not acumular:
        return [run_dir]

    carpetas = sorted(d for d in RAW_DIR.iterdir() if d.is_dir())
    if run_dir not in carpetas and run_dir.exists():
        carpetas.append(run_dir)
    return carpetas


def parse_raw(run_folder: str, fuentes=None, acumular: bool = True) -> str:
    run_dir = Path(run_folder)
    carpetas = _carpetas_a_leer(run_dir, acumular)
    log.info("fechas de la capa bronce que entran: %s", [c.name for c in carpetas])

    registros = []
    for fecha_dir in carpetas:
        for nombre, fuente in elegir(fuentes).items():
            carpeta = fecha_dir / nombre
            if not carpeta.exists():
                log.warning("no hay capa bronce para %s en %s", nombre, fecha_dir)
                continue
            nuevos = fuente.parsear(carpeta)
            # La fecha va por registro y no por corrida: un mismo aviso puede
            # venir de varias fechas, y transform_clean necesita saber de
            # cual es cada copia para quedarse con la mas reciente.
            for r in nuevos:
                r["fecha_scraping"] = fecha_dir.name
            registros.extend(nuevos)
            log.info("%s/%s: %s registros", fecha_dir.name, nombre, len(nuevos))

    if not registros:
        raise ValueError("ninguna fuente devolvio registros: revisar la capa bronce")

    REGISTROS.parent.mkdir(parents=True, exist_ok=True)
    with REGISTROS.open("w", encoding="utf-8") as f:
        json.dump(registros, f, ensure_ascii=False)
    log.info("registros totales de todas las fuentes: %s -> %s", len(registros), REGISTROS)
    return str(REGISTROS)


def leer_registros(registros_path: str) -> list:
    with open(registros_path, encoding="utf-8") as f:
        return json.load(f)
