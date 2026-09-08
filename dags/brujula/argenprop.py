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


def _renderizar(page, url, timeout):
    """`networkidle` resulto poco confiable en esta fuente: la pagina tiene
    actividad de red de fondo (probablemente el widget de mapa de
    OpenStreetMap) que a veces nunca deja que la red quede quieta -- contra
    el sitio real, `networkidle` tardaba el timeout completo (45s) en
    algunas fichas y en otras devolvia el HTML antes de que `.location-label`
    se terminara de pintar (94% de las fichas volvian sin ese dato).

    Se espera en cambio el evento `load` (rapido y confiable) y despues, de
    forma explicita, el elemento que sabemos que siempre termina
    apareciendo (`.titlebar__price` -- sin precio la ficha no sirve de
    todas formas). Un pequeno margen extra le da tiempo al resto del DOM
    -- incluida la ubicacion -- a terminar de pintarse.
    """
    page.goto(url, wait_until="load", timeout=timeout)
    try:
        page.wait_for_selector(".titlebar__price", timeout=15000)
    except Exception:
        pass
    page.wait_for_timeout(800)
    return page.content()


def fetch_browser(url, timeout=45000):
    """Unica ruta valida para parsear una ficha: precio, superficie y
    ubicacion se renderizan por JavaScript del lado del cliente."""
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
        html = _renderizar(page, url, timeout)
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
        for i, url in enumerate(urls):
            if i:
                # AWS WAF ("Human Verification") empieza a bloquear despues
                # de varios cientos de pedidos seguidos sin pausa -- se
                # verifico contra el sitio real: sin esta espera, un lote de
                # 337 fichas volvia con >90% de paginas de desafio en vez
                # del contenido real. Con pausa entre pedidos, no.
                time.sleep(1.2)
            try:
                html = _renderizar(page, url, timeout)
                if _es_desafio_waf(html):
                    yield url, None, RuntimeError("bloqueado por WAF (Human Verification)")
                    continue
                yield url, html, None
            except Exception as e:
                yield url, None, e
        browser.close()


def _es_desafio_waf(html):
    """Argenprop referencia awswaf.com como script en TODAS sus paginas
    (integracion normal, no significa bloqueo) -- se verifico que buscar
    ese dominio daba falsos positivos sobre fichas reales de 350KB con
    breadcrumb y precio completos. La pagina de desafio real es chica
    (~10KB) y su `<title>` es literalmente "Human Verification"; se chequea
    eso en vez del script."""
    return len(html) < 20000 and "<title>Human Verification</title>" in html


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

    candidatas = [u for u in urls
                 if es_candidata_mendoza(u)
                 and tipo_from_url(u) is not None]
    return candidatas


def tipo_from_url(url):
    """'departamento-en-alquiler-...' -> 'departamento'; 'casa-en-...' -> 'casa'."""
    slug = url.rsplit("/", 1)[-1]
    if slug.startswith("departamento-"):
        return "departamento"
    if slug.startswith("casa-"):
        return "casa"
    return None


# ----------------------------------------------------------------- parseo

def _ubicacion_por_breadcrumb(soup):
    """Provincia y localidad desde el breadcrumb estructurado (schema.org
    `BreadcrumbList`), no desde `.location-label`.

    Se cambio por esto: `.location-label` se pinta en un paso de render
    tardio (parece atado al widget de mapa de OpenStreetMap) y en la
    practica, contra el sitio real, el 94% de las fichas volvian sin ese
    elemento aunque el resto de la pagina ya estaba lista -- ni esperando
    `networkidle` ni agregando esperas explicitas se resolvia de forma
    confiable, porque a veces la pagina tiene actividad de red de fondo que
    nunca termina de quedar quieta.

    El breadcrumb es HTML estructurado (`itemtype="BreadcrumbList"`,
    `property="position"`), pensado para SEO, asi que argenprop lo rellena
    de forma consistente. El item de provincia es identificable sin
    ambiguedad: es el unico cuyo href termina en "-arg"
    (`/departamentos/alquiler/mendoza-arg`); el item de localidad es el que
    sigue en `position`.
    """
    items = []
    for li in soup.select('ol.breadcrumb li[property="itemListElement"]'):
        span = li.select_one('span[property="name"]')
        a = li.select_one("a")
        meta = li.select_one('meta[property="position"]')
        if not span or not meta or not meta.get("content"):
            continue
        items.append((int(meta["content"]), texto(span.get_text()),
                      a.get("href") if a else None))
    items.sort(key=lambda x: x[0])

    provincia = localidad = None
    for i, (pos, nombre, href) in enumerate(items):
        if href and href.rstrip("/").endswith("-arg"):
            provincia = nombre
            if i + 1 < len(items):
                localidad = items[i + 1][1]
            break
    return provincia, localidad


def parse_ficha(html):
    """Ficha ya renderizada (post-JavaScript). Devuelve `None` si la
    provincia real (breadcrumb) no es Mendoza -- resuelve la ambiguedad de
    departamentos con nombre repetido en Cuyo.
    """
    soup = BeautifulSoup(html, "lxml")

    provincia, localidad = _ubicacion_por_breadcrumb(soup)
    if provincia != "Mendoza":
        return None

    direccion_el = soup.select_one(".titlebar__address")
    direccion = texto(direccion_el.get_text() if direccion_el else None)

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
