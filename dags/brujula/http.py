"""Pedidos HTTP con buenos modales, compartidos por todas las fuentes."""

import logging
import time
from pathlib import Path

import requests

from brujula.config import (
    PAUSA_SEGUNDOS,
    REINTENTOS,
    TIMEOUT_SEGUNDOS,
    USER_AGENT,
)

log = logging.getLogger(__name__)


def sesion() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT, "Accept-Language": "es-AR,es;q=0.9"})
    return s


def bajar(ses: requests.Session, url: str) -> str:
    """GET con reintentos y espera creciente. Devuelve el HTML tal cual."""
    for intento in range(1, REINTENTOS + 1):
        try:
            r = ses.get(url, timeout=TIMEOUT_SEGUNDOS)
            r.raise_for_status()
            r.encoding = r.encoding or "utf-8"
            return r.text
        except requests.RequestException as e:
            if intento == REINTENTOS:
                raise
            espera = PAUSA_SEGUNDOS * 2 ** intento
            log.warning(
                "fallo %s (intento %s/%s): %s - reintento en %.0fs",
                url, intento, REINTENTOS, e, espera,
            )
            time.sleep(espera)
    raise RuntimeError("inalcanzable")


def guardar(destino: Path, html: str) -> None:
    destino.parent.mkdir(parents=True, exist_ok=True)
    destino.write_text(html, encoding="utf-8")


def descargar_avisos(destino_dir: Path, urls: dict, ses: requests.Session) -> int:
    """Baja la ficha de cada aviso que todavia no este en disco.

    Reanudable a proposito: una corrida cortada a la mitad se retoma sin
    volver a pedirle al servidor lo que ya se tiene.
    """
    destino_dir.mkdir(parents=True, exist_ok=True)

    nuevos = 0
    for i, (clave, url) in enumerate(sorted(urls.items()), start=1):
        archivo = destino_dir / f"{clave}.html"
        if archivo.exists():
            continue
        time.sleep(PAUSA_SEGUNDOS)
        try:
            guardar(archivo, bajar(ses, url))
            nuevos += 1
        except requests.RequestException as e:
            # Un aviso caido no puede voltear la corrida entera; queda
            # registrado y se refleja despues como fila faltante.
            log.warning("no se pudo bajar %s (%s): %s", clave, url, e)
        if i % 100 == 0:
            log.info("avisos procesados: %s/%s", i, len(urls))
    return nuevos
