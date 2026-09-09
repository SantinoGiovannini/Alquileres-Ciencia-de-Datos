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

from brujula.config import PROCESSED_DIR
from brujula.fuentes import elegir

log = logging.getLogger(__name__)

REGISTROS = PROCESSED_DIR / "_registros.json"


def parse_raw(run_folder: str, fuentes=None) -> str:
    run_dir = Path(run_folder)

    registros = []
    for nombre, fuente in elegir(fuentes).items():
        carpeta = run_dir / nombre
        if not carpeta.exists():
            log.warning("no hay capa bronce para %s en %s", nombre, run_folder)
            continue
        registros.extend(fuente.parsear(carpeta))

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
