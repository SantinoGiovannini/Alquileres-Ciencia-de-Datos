"""Vocabulario compartido entre portales.

Inmoclick y InmoUP usan las mismas etiquetas para los atributos de una
propiedad ("Superficie Cubierta m2", "Cantidad de Ambientes", ...), aunque
uno las publique en una tabla HTML y el otro en un JSON-LD. El mapeo de
esas etiquetas al nombre de columna vive aca, una sola vez.
"""

import re

# Etiqueta publicada -> nombre de columna.
# Lista explicita: si un portal agrega un campo nuevo queda en el log en
# vez de aparecer como una columna sorpresa casi vacia.
CAMPOS = {
    "ID#": "id_publicado",
    "Tipo de construccion": "tipo_construccion",
    "Condicion": "condicion",
    "Superficie Total m2": "superficie_total_m2",
    "Superficie Cubierta m2": "superficie_cubierta_m2",
    "Dormitorios": "dormitorios",
    "Banos": "banos",
    "Cantidad de Ambientes": "ambientes",
    "Cochera": "cochera",
    "Piscina": "piscina",
    "Plantas": "plantas",
    "Amoblado/a": "amoblado",
    "Tiene Expensas": "tiene_expensas",
    "Valor Expensas": "valor_expensas",
    "Valor de Expensas": "valor_expensas",
    "Aceptan Mascotas?": "acepta_mascotas",
    "Estado de Conservacion": "estado_conservacion",
    "Antiguedad": "antiguedad",
    "Ubicacion": "ubicacion",
    "Zona Escolar": "zona_escolar",
    "Orientacion": "orientacion",
    "Apto Credito": "apto_credito",
    "Financiacion": "financiacion",
}

_ACENTOS = str.maketrans("áéíóúüñÁÉÍÓÚÜÑ", "aeiouunAEIOUUN")

# /4335-llopart-inmobiliaria/inmuebles/3059/ficha/departamento-en-alquiler-...
# Misma forma en los dos portales: <usr_id>-<slug>/inmuebles/<prp_id>/ficha/
RE_FICHA = re.compile(r'href="(/(\d+)-[^/"]+/inmuebles/(\d+)/ficha/[^"#?]+)"')
RE_SLUG_URL = re.compile(r"/(\d+)-([^/]+)/inmuebles/")

RE_PRECIO = re.compile(r"^\s*(US\$|U\$S|\$)\s*([\d.]+)\s*$")


def normalizar(texto: str) -> str:
    """Saca acentos y signos de apertura para comparar etiquetas."""
    return texto.translate(_ACENTOS).replace("¿", "").strip()


def campo(etiqueta: str):
    """Etiqueta publicada -> nombre de columna, o None si no la conocemos."""
    return CAMPOS.get(normalizar(etiqueta))


def precio_y_moneda(texto):
    """'$ 720.000' -> (720000.0, 'ARS'); 'US$ 800' -> (800.0, 'USD').

    Lo que no matchea queda en nulo: "Consultar precio" y variantes no se
    convierten en un numero inventado.
    """
    if not isinstance(texto, str):
        return (None, None)
    m = RE_PRECIO.match(texto)
    if not m:
        return (None, None)
    simbolo, numero = m.groups()
    return (float(numero.replace(".", "")), "ARS" if simbolo == "$" else "USD")


def publicador_de_url(url: str):
    """Nombre de quien publica, sacado del slug de la URL del aviso."""
    m = RE_SLUG_URL.search(url or "")
    return m.group(2).replace("-", " ") if m else None
