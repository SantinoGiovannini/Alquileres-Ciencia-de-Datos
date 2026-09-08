"""
Cliente de inmoclick.com para el dataset de Brujula Inmobiliaria.

Ciencia de Datos - UTN FRM - 2026.

Dos rutas de descarga, en orden de preferencia:

  1. HTTP con `requests` (rapida, ~0,3 s por pagina). robots.txt sin
     restricciones y sin bloqueo por huella TLS -- se probo con `curl` liso
     sin User-Agent de navegador y devuelve 200 con el HTML completo.
  2. Playwright + Chromium (respaldo, por si el sitio empieza a filtrar).
     No deberia activarse en la practica: se deja como red de seguridad.

Dos hallazgos de esta fuente que conviene tener presentes:

  * **Filtro por texto libre vs. por localidad.** Buscar `?q=Mendoza` trae
    avisos de otras provincias (la palabra "Mendoza" aparece como nombre de
    calle en Buenos Aires o Rosario). El sitio expone un endpoint
    `/localidades?q=` que devuelve los 18 departamentos de la provincia con
    su id interno; filtrar por `localidades[]=<id>` es lo que da resultados
    limpios. Esos 18 ids estan fijos en `LOCALIDADES_MENDOZA`.

  * **Texto "envenenado" en la descripcion de la ficha individual.** La
    pagina de cada aviso (`/.../ficha/...`) intercala en el parrafo de
    descripcion `<span>` ocultos con `display:none` que dicen "COPIADO DE
    INMOCLICK.COM.AR", "TOMADO DE INMOCLICK.COM.AR", etc. -- invisibles para
    una persona, pero un scraper que lea `.textContent` sin filtrar los
    incorpora en cada oracion. La tarjeta del listado (`.description-hover`)
    NO tiene este problema: por eso `descripcion` sale siempre de la
    tarjeta de listado, nunca de la ficha. La ficha solo se usa para los
    campos estructurados (antiguedad, ambientes, expensas, amenities), que
    no estan envenenados.
"""
import re
import time
import urllib.parse

import requests
from bs4 import BeautifulSoup

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
BASE = "https://www.inmoclick.com"
PAGE_SIZE = 24

# Los 18 departamentos de la provincia de Mendoza, segun
# https://www.inmoclick.com/localidades?q=Mendoza (verificado 07/09/2026).
# Filtrar por estos ids evita los falsos positivos de la busqueda de texto
# libre (avisos de otras provincias donde "Mendoza" aparece en la direccion).
LOCALIDADES_MENDOZA = [1, 2, 3, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 514]

TIPOS = ("departamentos", "casas")
CONDICION = "alquiler"


def listing_url(tipo, page=1, localidades=None):
    """URL de una pagina del listado (tipo: departamentos|casas) en alquiler en Mendoza."""
    localidades = localidades if localidades is not None else LOCALIDADES_MENDOZA
    parts = [f"localidades%5B%5D={i}" for i in localidades]
    if page > 1:
        parts.append(f"page={page}")
    return f"{BASE}/{tipo}-en-{CONDICION}?" + "&".join(parts)


# --------------------------------------------------------------- descarga

def fetch_http(url, timeout=30, retries=3):
    for intento in range(retries):
        try:
            r = requests.get(url, headers={"User-Agent": UA}, timeout=timeout)
            if r.status_code == 403:
                raise PermissionError("inmoclick devolvio 403")
            r.raise_for_status()
            return r.text
        except PermissionError:
            raise
        except Exception:
            if intento == retries - 1:
                raise
            time.sleep(2 * (intento + 1))


def fetch_browser(url, timeout=45000):
    """Ruta de respaldo: navegador real. No deberia hacer falta (ver docstring)."""
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage",
                  "--disable-blink-features=AutomationControlled"])
        ctx = browser.new_context(user_agent=UA,
                                  viewport={"width": 1920, "height": 1080})
        page = ctx.new_page()
        page.route("**/*", lambda r: r.abort()
                   if r.request.resource_type in ("image", "stylesheet", "font", "media")
                   else r.continue_())
        page.goto(url, wait_until="domcontentloaded", timeout=timeout)
        page.wait_for_timeout(1000)
        html = page.content()
        browser.close()
    return html


def fetch(url, engine="auto"):
    """engine: 'http' | 'browser' | 'auto' (http y si da 403, navegador)."""
    if engine == "http":
        return fetch_http(url)
    if engine == "browser":
        return fetch_browser(url)
    try:
        return fetch_http(url)
    except PermissionError:
        return fetch_browser(url)


# ----------------------------------------------------------------- parseo

def total_avisos(html):
    """'715 Departamentos en Alquiler encontrados' -> 715. None si no matchea."""
    m = re.search(r"([\d.]+)\s+[\wáéíóúñ]+\s+en\s+(?:Venta|Alquiler)\s+encontrad[oa]s",
                 html, re.I)
    return int(m.group(1).replace(".", "")) if m else None


def n_pages(total, page_size=PAGE_SIZE):
    return -(-total // page_size) if total else 0


def parse_listing_page(html):
    """Tarjetas del listado: todo lo que trae el <article class="item">.

    Casi todos los campos numericos (dormitorios, banos, superficie, precio)
    viajan como **atributos HTML** del propio `<article>`.
    """
    soup = BeautifulSoup(html, "lxml")
    filas = []
    for item in soup.select("article.item"):
        a = item.attrs
        kid = a.get("kid")
        if not kid:
            continue

        price_el = item.select_one("p.price")
        moneda, precio = money(price_el.get_text(strip=True) if price_el
                               else a.get("precio"))

        link_el = item.select_one('a[itemprop="url"]')
        href = link_el.get("href") if link_el else None
        url_ficha = urllib.parse.urljoin(BASE, href).split("?")[0] if href else None

        desc_el = item.select_one(".description-hover p")
        locality_el = item.select_one('[itemprop="addressLocality"]')
        region_el = item.select_one('[itemprop="addressRegion"]')
        street_el = item.select_one('[itemprop="streetAddress"]')
        brand_p = item.select_one(".property-brand p")
        brand_img = item.select_one(".property-brand img")

        filas.append({
            "kid": kid,
            "prp_id": a.get("prp_id"),
            "usr_id": a.get("usr_id"),
            "moneda": moneda,
            "precio": precio,
            "dormitorios": _sin_disable(a.get("ser_1")),
            "banios": _sin_disable(a.get("ser_2")),
            "cochera": _sin_disable(a.get("ser_3")),
            "sup_total": _sin_disable(a.get("sup_t")),
            "sup_cubierta": _sin_disable(a.get("sup_c")),
            "lat": a.get("lat"),
            "lng": a.get("lng"),
            "localidad": texto(locality_el.get_text() if locality_el else None),
            "provincia": texto(region_el.get_text() if region_el else None),
            "direccion": texto(street_el.get_text() if street_el else None),
            "descripcion": texto(desc_el.get_text() if desc_el else None),
            "publicado_por": (texto(brand_p.get_text()) if brand_p
                              else (brand_img.get("title") if brand_img else None)),
            "url": url_ficha,
        })
    return filas


def parse_detail_page(html):
    """Ficha individual: solo los campos de la lista 'Datos de la propiedad'.

    No toca el parrafo de descripcion: ver el docstring del modulo.
    """
    soup = BeautifulSoup(html, "lxml")
    kv = {}
    for li in soup.select(".cont-sheet ul li, .description ul li"):
        divs = li.find_all("div", recursive=False)
        if len(divs) < 2:
            continue
        k = divs[0].get_text(strip=True)
        v = divs[1].get_text(strip=True)
        if not v and divs[1].select_one(".icon-check-mark"):
            v = "Si"
        if k:
            kv[k] = v

    place_el = soup.select_one("div.cont-names p.place")
    return {"kv": kv, "barrio": texto(place_el.get_text() if place_el else None)}


# ------------------------------------------------------------ conversiones

def _sin_disable(txt):
    return None if txt in (None, "disable") else txt


def money(txt):
    """'$ 700.000' -> ('ARS', 700000) ; 'US$ 800' / 'U$S 800' -> ('USD', 800)."""
    if not txt:
        return (None, None)
    t = txt.strip()
    if re.match(r"^(u\$s|us\$)", t, re.I):
        moneda = "USD"
    elif t.startswith("$"):
        moneda = "ARS"
    else:
        moneda = None
    digitos = re.sub(r"\D", "", t)
    return (moneda, int(digitos) if digitos else None)


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
