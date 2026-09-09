"""Capa bronce: bajar y guardar el HTML crudo, sin modificarlo.

Recorre las fuentes configuradas y le pide a cada una que deje su HTML en

    data/raw/<fecha>/<fuente>/listados/pagina_NN.html
    data/raw/<fecha>/<fuente>/avisos/<usr>-<prp>.html

Todo lo que sabe de un portal en particular vive en brujula/fuentes/; aca
solo esta el orden y el manejo de errores.
"""

import logging
from datetime import date
from pathlib import Path

from brujula import http
from brujula.config import RAW_DIR
from brujula.fuentes import elegir

log = logging.getLogger(__name__)


def extract_listings(max_paginas=None, fuentes=None, fecha=None) -> str:
    """Punto de entrada de la tarea. Devuelve la carpeta de la corrida.

    `fecha` (AAAAMMDD) elige la carpeta de la capa bronce. Sin fecha usa
    el dia de hoy *segun el reloj del proceso*, que adentro de Docker es
    UTC y no la hora local: a la noche argentina eso ya es el dia
    siguiente, y sin darse cuenta se scrapea todo de nuevo en una carpeta
    nueva. Para reprocesar una capa bronce que ya esta en disco, se pasa
    su fecha explicita.
    """
    fecha = fecha or date.today().strftime("%Y%m%d")
    run_dir = RAW_DIR / fecha
    run_dir.mkdir(parents=True, exist_ok=True)

    ses = http.sesion()
    fallaron = []
    for nombre, fuente in elegir(fuentes).items():
        log.info("=== fuente: %s ===", nombre)
        try:
            fuente.descargar(run_dir / nombre, ses, max_paginas=max_paginas)
        except Exception as e:
            # Que un portal se caiga no puede dejar sin dataset al resto:
            # se registra y la corrida sigue con las otras fuentes.
            log.exception("la fuente %s fallo y se saltea: %s", nombre, e)
            fallaron.append(nombre)

    if fallaron:
        log.warning("fuentes que fallaron en esta corrida: %s", fallaron)

    total = sum(1 for _ in Path(run_dir).glob("*/avisos/*.html"))
    log.info("fichas en disco para %s: %s", fecha, total)
    if total == 0:
        raise RuntimeError("ninguna fuente dejo avisos en disco")

    return str(run_dir)
