"""
Cliente de inmoup.com.ar -- tercera fuente real para Brujula Inmobiliaria,
sumada junto con Inmoclick y argenprop para pasar el piso de 1.000 filas
del criterio "Volumen suficiente" del Kit de arranque.

Ciencia de Datos - UTN FRM - 2026.

A diferencia de argenprop, la ficha de inmoup **si viene renderizada del
lado del servidor**: trae un bloque `<script type="application/ld+json">`
(schema.org `RealEstateListing`) con precio, moneda, direccion, provincia,
coordenadas y hasta `datePosted` -- se verifico bajando una ficha real con
`requests` liso y todo el JSON-LD ya estaba ahi. No hace falta Playwright
para esta fuente (a diferencia de argenprop, donde precio y superficie se
renderizan por JS del lado del cliente).

**Descubrimiento por sitemap.** El `robots.txt` de inmoup permite
`/sitemap/sitemap-*.xml`. El sitemap `sitemap-inmuebles-por-provincias-N.xml`
(N=1..4) tiene ~18.000 fichas de todo el pais -- no hay un sitemap
exclusivo de Mendoza para fichas individuales (si lo hay para paginas de
categoria, pero esas son client-side y no sirven para descubrir fichas).

**Ambiguedad de departamentos con nombre repetido**, misma logica que
argenprop.py: el prefiltro por nombre de departamento en el slug de la URL
(`DEPARTAMENTOS_MENDOZA`) solo reduce que bajar -- "San Martin" y
"Rivadavia" tambien son departamento de San Juan. La confirmacion real es
`addressRegion` del JSON-LD de la ficha ya bajada.

**Clave unica.** La URL tiene la forma
`inmoup.com.ar/<id-agente>-<slug-agente>/inmuebles/<id-interno>/ficha/<slug>`.
El `<id-interno>` solo es unico dentro de ese agente (como `prp_id` en
Inmoclick), asi que la clave real combina los dos: `<id-agente>-<id-interno>`.
"""
import re
import time

import requests
from bs4 import BeautifulSoup

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
BASE = "https://inmoup.com.ar"

SITEMAPS_FICHAS = (
    [f"{BASE}/sitemap/sitemap-inmuebles-por-provincias-{i}.xml" for i in range(1, 5)]
    + [f"{BASE}/sitemap/sitemap-inmuebles.xml",
       f"{BASE}/sitemap/sitemap-inmuebles-destacados.xml"]
)

# Mismos 18 departamentos que argenprop.DEPARTAMENTOS_MENDOZA -- "san-martin"
# y "rivadavia" tambien son departamento de San Juan, a proposito (ver
# docstring del modulo): la confirmacion real es `addressRegion`.
DEPARTAMENTOS_MENDOZA = [
    "mendoza", "capital", "godoy-cruz", "guaymallen", "las-heras",
    "lujan-de-cuyo", "maipu", "junin", "rivadavia", "san-martin",
    "santa-rosa", "la-paz", "san-rafael", "general-alvear", "malargue",
    "tunuyan", "tupungato", "san-carlos", "lavalle",
]


def es_candidata_mendoza(url):
    slug = url.rsplit("/", 1)[-1].lower()
    return any(dep in slug for dep in DEPARTAMENTOS_MENDOZA)


def tipo_from_url(url):
    slug = url.rsplit("/", 1)[-1]
    if slug.startswith("departamento-en-alquiler-"):
        return "departamento"
    if slug.startswith("casa-en-alquiler-"):
        return "casa"
    return None


def listing_id(url):
    """El id interno (`/inmuebles/<id>/`) solo es unico por agente -- se
    combina con el id de agente (el numero al principio del slug de
    usuario) para tener una clave global."""
    m = re.search(r"inmoup\.com\.ar/(\d+)-[^/]+/inmuebles/(\d+)/ficha", url)
    return f"{m.group(1)}-{m.group(2)}" if m else None


# --------------------------------------------------------------- descarga

def fetch(url, timeout=30, retries=3):
    for intento in range(retries):
        try:
            r = requests.get(url, headers={"User-Agent": UA}, timeout=timeout)
            r.raise_for_status()
            return r.text
        except Exception:
            if intento == retries - 1:
                raise
            time.sleep(2 * (intento + 1))


# ------------------------------------------------------- descubrimiento

def discover_urls():
    """Recorre los sitemaps de fichas y devuelve las URLs de alquiler
    (departamento/casa, sin alquiler temporario) ya prefiltradas por
    `es_candidata_mendoza`. No toca ninguna pagina de busqueda -- esas son
    client-side (Next.js) y no traen las fichas en el HTML.
    """
    urls = []
    for sitemap in SITEMAPS_FICHAS:
        xml = fetch(sitemap)
        urls.extend(re.findall(r"<loc>([^<]+)</loc>", xml))

    candidatas = []
    for u in urls:
        slug = u.rsplit("/", 1)[-1]
        if "temporario" in slug:
            continue
        if tipo_from_url(u) is None:
            continue
        if not es_candidata_mendoza(u):
            continue
        candidatas.append(u)
    return candidatas


# ----------------------------------------------------------------- parseo

def parse_ficha(html, url):
    """Lee el JSON-LD `RealEstateListing` de la ficha. Devuelve `None` si
    la provincia real no es Mendoza -- resuelve la ambiguedad de
    departamentos con nombre repetido (ver docstring del modulo).
    """
    import json

    soup = BeautifulSoup(html, "lxml")
    script = soup.find("script", {"type": "application/ld+json"})
    if not script or not script.string:
        return None
    try:
        data = json.loads(script.string)
    except (ValueError, TypeError):
        return None

    grafo = data.get("@graph", [data])
    listado = next((g for g in grafo
                    if "RealEstateListing" in (g.get("@type") or [])), None)
    if not listado:
        return None

    direccion_obj = listado.get("address") or {}
    if direccion_obj.get("addressRegion") != "Mendoza":
        return None

    oferta = listado.get("offers") or {}
    geo = listado.get("geo") or {}

    kv = {}
    for prop in (listado.get("additionalProperty") or []):
        nombre = prop.get("name")
        if nombre:
            kv[nombre] = prop.get("value")

    proveedor = listado.get("provider") or {}

    return {
        "localidad": texto(direccion_obj.get("addressLocality")),
        "provincia": direccion_obj.get("addressRegion"),
        "direccion": texto(direccion_obj.get("streetAddress")),
        "lat": geo.get("latitude"),
        "lng": geo.get("longitude"),
        "moneda": oferta.get("priceCurrency"),
        "precio": oferta.get("price"),
        "fecha_publicacion": (listado.get("datePosted") or "").split(" ")[0] or None,
        "descripcion": texto(listado.get("description")),
        "publicado_por": texto(proveedor.get("name")),
        "kv": kv,
    }


# ------------------------------------------------------------ conversiones

def entero(txt):
    if txt is None:
        return None
    m = re.search(r"\d+", str(txt))
    return int(m.group()) if m else None


def bool_si(txt):
    if txt is None:
        return None
    t = str(txt).strip().lower()
    if t in ("si", "sí", "yes", "true"):
        return True
    if t in ("no", "false"):
        return False
    return None


def texto(txt):
    if txt is None:
        return None
    t = re.sub(r"\s+", " ", str(txt)).strip()
    return t or None
