"""Fuente: Inmoclick (https://inmoclick.com).

Los datos salen de dos lugares:
- la tarjeta del listado (<article class="item">): precio, superficies,
  lat/lng, localidad, direccion y la descripcion limpia;
- la ficha del aviso: la tabla "Datos de la propiedad".

La descripcion se toma del listado a proposito: en la ficha viene partida
en spans con textos "CONTENIDO DE INMOCLICK.COM.AR" intercalados letra
por letra, para que no se copie.
"""

import logging
import re
from pathlib import Path

from bs4 import BeautifulSoup

from brujula import http
from brujula.fuentes import comun

log = logging.getLogger(__name__)

NOMBRE = "inmoclick"
BASE_URL = "https://inmoclick.com"
LISTADO_PATH = "/departamentos-en-alquiler-en-mendoza"

RE_PAGINA = re.compile(rf"{re.escape(LISTADO_PATH)}\?page=(\d+)")


def _sopa(archivo: Path) -> BeautifulSoup:
    return BeautifulSoup(archivo.read_text(encoding="utf-8", errors="replace"), "lxml")


# --------------------------------------------------------------- descarga

def descargar(run_dir: Path, ses, max_paginas=None) -> None:
    listados = run_dir / "listados"

    primera = http.bajar(ses, BASE_URL + LISTADO_PATH)
    http.guardar(listados / "pagina_01.html", primera)

    paginas = [int(n) for n in RE_PAGINA.findall(primera)]
    ultima = max(paginas) if paginas else 1
    if max_paginas:
        ultima = min(ultima, max_paginas)
    log.info("[%s] paginas de resultados: %s", NOMBRE, ultima)

    import time

    from brujula.config import PAUSA_SEGUNDOS

    for n in range(2, ultima + 1):
        archivo = listados / f"pagina_{n:02d}.html"
        if archivo.exists():
            continue
        time.sleep(PAUSA_SEGUNDOS)
        http.guardar(archivo, http.bajar(ses, f"{BASE_URL}{LISTADO_PATH}?page={n}"))

    urls = _urls_de_avisos(run_dir)
    log.info("[%s] avisos unicos detectados: %s", NOMBRE, len(urls))
    nuevos = http.descargar_avisos(run_dir / "avisos", urls, ses)
    log.info("[%s] fichas nuevas bajadas: %s", NOMBRE, nuevos)


def _urls_de_avisos(run_dir: Path) -> dict:
    encontrados = {}
    for archivo in sorted((run_dir / "listados").glob("pagina_*.html")):
        html = archivo.read_text(encoding="utf-8", errors="replace")
        for path, usr_id, prp_id in comun.RE_FICHA.findall(html):
            encontrados[f"{usr_id}-{prp_id}"] = BASE_URL + path
    return encontrados


# ----------------------------------------------------------------- parseo

def _tarjetas(run_dir: Path) -> dict:
    tarjetas = {}
    for archivo in sorted((run_dir / "listados").glob("pagina_*.html")):
        for art in _sopa(archivo).select("article.item"):
            usr_id, prp_id = art.get("usr_id"), art.get("prp_id")
            if not usr_id or not prp_id:
                continue

            enlace = art.select_one("a[href*='/ficha/']")
            url = BASE_URL + enlace["href"] if enlace else None
            marca = art.select_one(".property-brand p")
            etiqueta = comun.normalizar(marca.get_text(strip=True)) if marca else None
            descripcion = art.select_one(".description-hover p")
            localidad = art.select_one("[itemprop=addressLocality]")
            provincia = art.select_one("[itemprop=addressRegion]")
            direccion = art.select_one("[itemprop=streetAddress]")

            precio, moneda = comun.precio_y_moneda(art.get("precio"))

            tarjetas[f"{usr_id}-{prp_id}"] = {
                "fuente": NOMBRE,
                "usr_id": usr_id,
                "prp_id": prp_id,
                "url": url,
                "precio": precio,
                "moneda": moneda,
                "superficie_total_m2": art.get("sup_t"),
                "superficie_cubierta_m2": art.get("sup_c"),
                "latitud": art.get("lat"),
                "longitud": art.get("lng"),
                "localidad": localidad.get_text(strip=True) if localidad else None,
                "provincia": provincia.get_text(strip=True) if provincia else None,
                "direccion": direccion.get_text(strip=True) if direccion else None,
                "descripcion": (
                    descripcion.get_text(" ", strip=True) if descripcion else None
                ),
                "publicador": comun.publicador_de_url(url),
                # El cartelito .property-brand solo aparece en cuentas de
                # particulares y dice "Dueno Directo" o "Inmobiliaria"; si no
                # esta, el que publica es una inmobiliaria con cartera.
                "tipo_anunciante": (
                    "Dueno Directo" if etiqueta == "Dueno Directo" else "Inmobiliaria"
                ),
                "es_dueno_directo": etiqueta == "Dueno Directo",
            }
    return tarjetas


def _ficha(archivo: Path) -> dict:
    sopa = _sopa(archivo)
    titulo = sopa.find("h3", string=re.compile("Datos de la propiedad"))
    if titulo is None:
        log.warning("[%s] ficha sin tabla de datos: %s", NOMBRE, archivo.name)
        return {}

    datos = {}
    for lista in titulo.find_all_next("ul"):
        # Las listas que siguen al h3 son bloques de la misma tabla; se corta
        # en el primer <ul> que no tenga la forma <li><div><div>.
        items = lista.find_all("li", recursive=False)
        pares = [li.find_all("div", recursive=False) for li in items]
        if not pares or not all(len(p) == 2 for p in pares):
            break
        for etiqueta, valor in pares:
            nombre = comun.campo(etiqueta.get_text(strip=True))
            if nombre is None:
                log.debug("[%s] campo no mapeado: %r", NOMBRE, etiqueta.get_text(strip=True))
                continue
            datos[nombre] = valor.get_text(" ", strip=True) or None
    return datos


def parsear(run_dir: Path) -> list:
    tarjetas = _tarjetas(run_dir)
    registros, sin_ficha = [], 0
    for clave, tarjeta in sorted(tarjetas.items()):
        archivo = run_dir / "avisos" / f"{clave}.html"
        if archivo.exists():
            # La ficha pisa a la tarjeta: trae el dato con su etiqueta explicita.
            registros.append({**tarjeta, **_ficha(archivo)})
        else:
            # Visto en el listado pero sin ficha en disco: se conserva la fila
            # con lo que hay, no se descarta en silencio.
            sin_ficha += 1
            registros.append(dict(tarjeta))
    log.info("[%s] registros: %s (sin ficha: %s)", NOMBRE, len(registros), sin_ficha)
    return registros
