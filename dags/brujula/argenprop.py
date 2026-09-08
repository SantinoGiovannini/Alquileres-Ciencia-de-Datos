"""
Cliente de argenprop.com -- fuente de respaldo real (no solo otro motor sobre
el mismo sitio) para Brujula Inmobiliaria: cuando inmoclick no responde, el
DAG prueba esta segunda fuente antes de caer al snapshot congelado.

Ciencia de Datos - UTN FRM - 2026.

Dos hallazgos de esta fuente, verificados contra el sitio real:

  * **Descubrimiento por sitemap, no por paginacion de busqueda.** El
    `robots.txt` de argenprop.com limita a 3 paginas la paginacion de
    resultados de busqueda (`Allow: /*?pagina-1$` ... `pagina-3$`), pero
    declara sitemaps completos sin esa restriccion. La region Cuyo (Mendoza
    + San Juan + San Luis) tiene su propio indice
    (`sitemaps/sitemap-ficha-cuyo.xml.gz`), que apunta a un sitemap hoja por
    operacion (`sitemap-ficha-alquiler-cuyo-01.xml.gz`). De ahi sale la
    lista de fichas a bajar, sin tocar el buscador ni sus 3 paginas.

  * **El HTML sin JavaScript no trae precio.** A diferencia de inmoclick
    (todo en atributos HTML), argenprop renderiza precio, superficie y
    caracteristicas por JavaScript del lado del cliente: se verifico
    bajando una ficha con `requests` liso y ni el precio ni la superficie
    aparecen en ningun lugar del documento -- solo un bloque JSON-LD con
    ambientes/dormitorios/direccion parcial, sin precio. Por eso esta
    fuente **siempre** usa el motor navegador (Playwright); aca no es una
    red de seguridad como en inmoclick, es la unica forma de leerla.

Ambiguedad conocida y como se resuelve: el sitemap de Cuyo mezcla Mendoza,
San Juan y San Luis, y dos departamentos existen con el mismo nombre en mas
de una provincia ("San Martin", "Rivadavia"). `DEPARTAMENTOS_MENDOZA` en el
slug de la URL solo *reduce* el universo a bajar -- la confirmacion real es
la provincia que trae `.location-label` en la ficha ya renderizada: si no
termina en "Mendoza", se descarta ahi, ya pagado el costo de bajarla.
"""
import gzip
import re
import time

import requests
from bs4 import BeautifulSoup

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
BASE = "https://www.argenprop.com"
SITEMAP_INDEX_CUYO = f"{BASE}/sitemaps/sitemap-ficha-cuyo.xml.gz"

# Nombres (en minuscula, con guiones) de los 18 departamentos de Mendoza tal
# como aparecen en el slug de la URL de argenprop. "san-martin" y
# "rivadavia" tambien existen como departamento de San Juan -- es
# intencional que este filtro sea impreciso para esos dos: la confirmacion
# real es la provincia de `.location-label` (ver docstring del modulo).
DEPARTAMENTOS_MENDOZA = [
    "mendoza", "capital", "godoy-cruz", "guaymallen", "las-heras",
    "lujan-de-cuyo", "maipu", "junin", "rivadavia", "san-martin",
    "santa-rosa", "la-paz", "san-rafael", "general-alvear", "malargue",
    "tunuyan", "tupungato", "san-carlos", "lavalle",
]

OPERACION = "alquiler"


def es_candidata_mendoza(url):
    """Prefiltro barato por el slug de la URL. Ver ambiguedad en el docstring."""
    slug = url.rsplit("/", 1)[-1].lower()
    return any(dep in slug for dep in DEPARTAMENTOS_MENDOZA)


def listing_id(url):
    """El id numerico al final del slug, despues del ultimo '--'."""
    m = re.search(r"--(\d+)/?$", url)
    return m.group(1) if m else None


# --------------------------------------------------------------- descarga

def fetch_bytes(url, timeout=30, retries=3):
    for intento in range(retries):
        try:
            r = requests.get(url, headers={"User-Agent": UA}, timeout=timeout)
            r.raise_for_status()
            return r.content
        except Exception:
            if intento == retries - 1:
                raise
            time.sleep(2 * (intento + 1))


def fetch_browser(url, timeout=45000):
    """Unica ruta valida para parsear una ficha: precio y superficie se
    renderizan por JavaScript del lado del cliente."""
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
                   if r.request.resource_type in ("image", "font", "media")
                   else r.continue_())
        page.goto(url, wait_until="networkidle", timeout=timeout)
        html = page.content()
        browser.close()
    return html


def fetch_many(urls, timeout=45000):
    """Baja varias fichas con un unico navegador, en vez de lanzar y cerrar
    Chromium por cada URL. Generador de `(url, html, error)`; si `error` no
    es `None`, `html` es `None` y esa URL se descarta sin cortar el lote.
    """
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage",
                  "--disable-blink-features=AutomationControlled"])
        ctx = browser.new_context(user_agent=UA,
                                  viewport={"width": 1920, "height": 1080})
        ctx.route("**/*", lambda r: r.abort()
                  if r.request.resource_type in ("image", "font", "media")
                  else r.continue_())
        page = ctx.new_page()
        for url in urls:
            try:
                page.goto(url, wait_until="networkidle", timeout=timeout)
                yield url, page.content(), None
            except Exception as e:
                yield url, None, e
        browser.close()


# ------------------------------------------------------- descubrimiento

def discover_urls():
    """Recorre el indice de sitemaps de Cuyo y devuelve las URLs de fichas
    de alquiler, ya prefiltradas por `es_candidata_mendoza`.

    No toca el buscador: todo sale de sitemaps XML, permitidos sin limite
    de paginacion por el propio `robots.txt` de argenprop.com.
    """
    indice = gzip.decompress(fetch_bytes(SITEMAP_INDEX_CUYO)).decode("utf-8")
    hojas = [l for l in re.findall(r"<loc>([^<]+)</loc>", indice)
            if f"ficha-{OPERACION}-cuyo" in l]

    urls = []
    for hoja in hojas:
        xml = gzip.decompress(fetch_bytes(hoja)).decode("utf-8")
        urls.extend(re.findall(r"<loc>([^<]+)</loc>", xml))

    # Solo departamentos: es el segmento de la propuesta (ver README).
    candidatas = [u for u in urls
                 if es_candidata_mendoza(u)
                 and u.rsplit("/", 1)[-1].startswith("departamento-")]
    return candidatas


# ----------------------------------------------------------------- parseo

def parse_ficha(html):
    """Ficha ya renderizada (post-JavaScript). Devuelve `None` si la
    provincia real (`.location-label`) no es Mendoza -- resuelve la
    ambiguedad de departamentos con nombre repetido en Cuyo.
    """
    soup = BeautifulSoup(html, "lxml")

    ubicacion_el = soup.select_one(".location-label")
    ubicacion = texto(ubicacion_el.get_text() if ubicacion_el else None)
    if not ubicacion or not ubicacion.rstrip().endswith("Mendoza"):
        return None

    partes = [p.strip() for p in ubicacion.split(",")]
    provincia = partes[-1] if partes else None
    localidad = partes[-2] if len(partes) >= 2 else None
    direccion = ", ".join(partes[:-2]) if len(partes) > 2 else None

    price_el = soup.select_one(".titlebar__price")
    moneda, precio = money(price_el.get_text() if price_el else None)

    desc_el = soup.select_one(".section-description--content")

    kv = {}
    for li in soup.select("#section-caracteristicas li"):
        t = li.get_text(" ", strip=True)
        if ":" not in t:
            kv[t.strip()] = "Si"          # items sin valor, ej. "Apto Profesional"
            continue
        k, v = t.split(":", 1)
        kv[k.strip()] = v.strip()

    sup_cubierta = None
    sup_total = None
    for li in soup.select(".property-main-features li[title]"):
        titulo = li.get("title")
        val = entero(li.get_text())
        if titulo == "Sup. cubierta":
            sup_cubierta = val
        elif titulo == "Sup. total":
            sup_total = val

    return {
        "localidad": localidad,
        "provincia": provincia,
        "direccion": direccion,
        "moneda": moneda,
        "precio": precio,
        "descripcion": texto(desc_el.get_text() if desc_el else None),
        "sup_cubierta": sup_cubierta,
        "sup_total": sup_total,
        "kv": kv,
    }


# ------------------------------------------------------------ conversiones
# (mismas reglas que inmoclick.py, para que transform.py las trate igual)

def money(txt):
    """'$ 630.000' -> ('ARS', 630000) ; argenprop escribe dolares de varias
    formas ('U$D 1.400', 'USD 1.100', 'U$S 800'), a diferencia de inmoclick
    que siempre usa 'U$S'/'US$' -- verificado contra fichas reales donde el
    primer patron dejaba `moneda=None` con `precio` igual seteado."""
    if not txt:
        return (None, None)
    t = txt.strip()
    if re.match(r"^(u\$s|us\$|u\$d|usd)", t, re.I):
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
