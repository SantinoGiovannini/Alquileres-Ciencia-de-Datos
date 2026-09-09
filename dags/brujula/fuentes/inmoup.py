"""Fuente: InmoUP (https://inmoup.com.ar).

Mismo esquema de URL que Inmoclick (<usr_id>-<slug>/inmuebles/<prp_id>/ficha/),
asi que la clave del aviso se arma igual. Lo que cambia es de donde sale el
dato: InmoUP publica un JSON-LD de schema.org en cada ficha, con los
atributos en `additionalProperty` usando las mismas etiquetas que la tabla
de Inmoclick. Se lee de ahi en vez de scrapear HTML: es mas estable y no
depende de nombres de clases CSS.

La paginacion del listado es otra historia. La grilla se completa por
JavaScript y ?page=N devuelve siempre la primera tanda, asi que no se
puede recorrer de a paginas; y los endpoints JSON que usa el sitio estan
prohibidos en su robots.txt (Disallow: /json/, /json-busqueda). Lo que el
robots.txt si permite explicitamente son los sitemaps, y ahi estan
listadas las busquedas ya recortadas por barrio, superficie, dormitorios y
cochera. Cada una de esas busquedas devuelve como mucho 24 avisos, asi que
se piden todas las de Mendoza y se unen los resultados. No garantiza el
100% de cobertura, pero es el camino que el sitio habilita, y el log deja
constancia de cuantos avisos se juntaron.
"""

import json
import logging
import re
import time
from pathlib import Path

from brujula import http
from brujula.config import PAUSA_SEGUNDOS
from brujula.fuentes import comun

log = logging.getLogger(__name__)

NOMBRE = "inmoup"
BASE_URL = "https://inmoup.com.ar"
LISTADO_PATH = "/departamentos-en-alquiler-en-mendoza"
SITEMAP_INDEX = f"{BASE_URL}/sitemap/sitemap-index.xml"

# Sitemaps que traen busquedas de departamentos (el resto son casas,
# lotes, locales o paginas institucionales).
SITEMAPS_UTILES = ("mendoza-zonas", "departamentos")

RE_LOC = re.compile(r"<loc>\s*([^<\s]+)\s*</loc>")
RE_JSONLD = re.compile(
    r'<script type="application/ld\+json"[^>]*>(.*?)</script>', re.S
)


# --------------------------------------------------------------- descarga

def _es_busqueda_util(url: str) -> bool:
    """Busquedas de departamentos en alquiler comun, en Mendoza.

    Se deja afuera el alquiler temporario a proposito: es otro mercado
    (turistico, por dia) y mezclarlo con el alquiler tradicional romperia
    la comparacion de precios, que es justo lo que el proyecto no quiere.
    """
    if "departamentos-en-alquiler-en-" not in url or "temporario" in url:
        return False
    return url.endswith("-en-mendoza") or "-de-mendoza" in url


def _urls_de_busqueda(ses) -> list:
    indice = http.bajar(ses, SITEMAP_INDEX)
    sitemaps = [
        u for u in RE_LOC.findall(indice)
        if any(clave in u for clave in SITEMAPS_UTILES)
    ]
    log.info("[%s] sitemaps de busquedas: %s", NOMBRE, len(sitemaps))

    urls = set()
    for sitemap in sitemaps:
        time.sleep(PAUSA_SEGUNDOS)
        urls.update(u for u in RE_LOC.findall(http.bajar(ses, sitemap))
                    if _es_busqueda_util(u))

    # La busqueda provincial completa siempre va, aunque el sitemap cambie.
    urls.add(BASE_URL + LISTADO_PATH)
    return sorted(urls)


def _nombre_archivo(url: str) -> str:
    slug = url.rsplit("/", 1)[-1] or "index"
    return re.sub(r"[^A-Za-z0-9._-]", "_", slug)[:120] + ".html"


def descargar(run_dir: Path, ses, max_paginas=None) -> None:
    listados = run_dir / "listados"

    busquedas = _urls_de_busqueda(ses)
    if max_paginas:
        # En una prueba corta alcanza con unas pocas busquedas.
        busquedas = busquedas[:max_paginas]
    log.info("[%s] busquedas de Mendoza a recorrer: %s", NOMBRE, len(busquedas))

    for i, url in enumerate(busquedas, start=1):
        archivo = listados / _nombre_archivo(url)
        if archivo.exists():
            continue
        time.sleep(PAUSA_SEGUNDOS)
        try:
            http.guardar(archivo, http.bajar(ses, url))
        except Exception as e:
            log.warning("[%s] no se pudo bajar la busqueda %s: %s", NOMBRE, url, e)
        if i % 100 == 0:
            log.info("[%s] busquedas recorridas: %s/%s", NOMBRE, i, len(busquedas))

    urls = _urls_de_avisos(run_dir)
    log.info("[%s] avisos unicos detectados: %s", NOMBRE, len(urls))
    nuevos = http.descargar_avisos(run_dir / "avisos", urls, ses)
    log.info("[%s] fichas nuevas bajadas: %s", NOMBRE, nuevos)


def _urls_de_avisos(run_dir: Path) -> dict:
    encontrados = {}
    for archivo in sorted((run_dir / "listados").glob("*.html")):
        html = archivo.read_text(encoding="utf-8", errors="replace")
        for path, usr_id, prp_id in comun.RE_FICHA.findall(html):
            encontrados[f"{usr_id}-{prp_id}"] = BASE_URL + path
    return encontrados


# ----------------------------------------------------------------- parseo

def _nodo_aviso(html: str):
    """Devuelve el nodo JSON-LD del aviso, o None si la ficha no lo trae."""
    for bloque in RE_JSONLD.findall(html):
        try:
            datos = json.loads(bloque)
        except json.JSONDecodeError:
            continue
        grafo = datos.get("@graph", datos)
        for nodo in grafo if isinstance(grafo, list) else [grafo]:
            if not isinstance(nodo, dict):
                continue
            tipos = nodo.get("@type", "")
            tipos = tipos if isinstance(tipos, list) else [tipos]
            if "RealEstateListing" in tipos or "Accommodation" in tipos:
                return nodo
    return None


def _parsear_ficha(archivo: Path, clave: str, url: str) -> dict:
    html = archivo.read_text(encoding="utf-8", errors="replace")
    nodo = _nodo_aviso(html)
    if nodo is None:
        log.warning("[%s] ficha sin JSON-LD de aviso: %s", NOMBRE, archivo.name)
        return {}

    usr_id, prp_id = clave.split("-", 1)
    oferta = nodo.get("offers") or {}
    direccion = nodo.get("address") or {}
    geo = nodo.get("geo") or {}
    proveedor = nodo.get("provider") or {}
    tipos_proveedor = proveedor.get("@type", "")
    tipos_proveedor = (
        tipos_proveedor if isinstance(tipos_proveedor, list) else [tipos_proveedor]
    )
    es_inmobiliaria = "RealEstateAgent" in tipos_proveedor

    registro = {
        "fuente": NOMBRE,
        "usr_id": usr_id,
        "prp_id": prp_id,
        "url": url,
        "precio": oferta.get("price"),
        "moneda": oferta.get("priceCurrency"),
        "localidad": direccion.get("addressLocality"),
        "provincia": direccion.get("addressRegion"),
        "direccion": direccion.get("streetAddress"),
        "latitud": geo.get("latitude"),
        "longitud": geo.get("longitude"),
        "descripcion": nodo.get("description"),
        # InmoUP si publica la fecha del aviso; Inmoclick no.
        "fecha_publicacion": nodo.get("datePosted"),
        "publicador": proveedor.get("name") or comun.publicador_de_url(url),
        "tipo_anunciante": "Inmobiliaria" if es_inmobiliaria else "Dueno Directo",
        "es_dueno_directo": not es_inmobiliaria,
        # El segmento scrapeado es, por definicion, alquiler. El tipo fino
        # (Departamento / Duplex / PH) InmoUP no lo publica, asi que queda
        # nulo en vez de inventarle un valor que Inmoclick si distingue.
        "condicion": "Alquiler",
    }

    atributos = nodo.get("additionalProperty") or []
    for atributo in atributos if isinstance(atributos, list) else [atributos]:
        if not isinstance(atributo, dict):
            continue
        nombre = comun.campo(str(atributo.get("name", "")))
        if nombre is None:
            log.debug("[%s] campo no mapeado: %r", NOMBRE, atributo.get("name"))
            continue
        valor = atributo.get("value")
        registro[nombre] = None if valor in ("", None) else str(valor)

    return registro


def parsear(run_dir: Path) -> list:
    urls = _urls_de_avisos(run_dir)
    registros, sin_ficha = [], 0
    for clave, url in sorted(urls.items()):
        archivo = run_dir / "avisos" / f"{clave}.html"
        if not archivo.exists():
            sin_ficha += 1
            continue
        registro = _parsear_ficha(archivo, clave, url)
        if registro:
            registros.append(registro)
    log.info("[%s] registros: %s (sin ficha: %s)", NOMBRE, len(registros), sin_ficha)
    return registros
