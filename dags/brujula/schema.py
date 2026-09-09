"""
Esquema canonico de Brujula Inmobiliaria (Entrega 1).

Unidad de observacion: un aviso de alquiler (departamento o casa) publicado
en Inmoclick, argenprop o inmoup para la provincia de Mendoza, en el
momento del scraping. Clave primaria: `listing_id` -- el `kid` interno de
Inmoclick, `argenprop-<id>`, o `inmoup-<id-agente>-<id-interno>` segun la
fuente (el prefijo evita colisiones entre las tres, y dentro de cada una
es unico por aviso).

Por que tres fuentes: Inmoclick solo (aun sumando departamentos y casas) da
~830 avisos, por debajo del piso de 1.000 filas del criterio "Volumen
suficiente". argenprop e inmoup corren siempre, no solo como respaldo ante
una caida, para llegar a ese piso (ver el docstring de
`dags/brujula_pipeline.py`).

`precio_m2` es la columna objetivo de la propuesta (precio / superficie
cubierta): el modelo de U3 es una regresion sobre esta columna, y el
residuo es lo que clasifica un aviso en sobrevalorado / de mercado /
oportunidad.

Sobre `fecha_publicacion`: Inmoclick y argenprop no la exponen en ningun
lado (se busco `datePosted` y variantes, no esta). inmoup si la trae en su
JSON-LD -- se guarda cuando la fuente es inmoup, queda nula para las otras
dos. `fecha_extraccion` (cuando bajo el dato el pipeline) esta siempre,
para las tres fuentes, y no es lo mismo que `fecha_publicacion`.

Por que la descripcion no sale de la ficha en Inmoclick: intercala ahi
spans ocultos con frases anti-scraping ("COPIADO DE INMOCLICK.COM.AR").
`descripcion` sale siempre de la tarjeta del listado para esa fuente, que
no tiene ese problema (ver docstring de inmoclick.py).
"""

IDENTIDAD = [
    "listing_id", "prp_id", "usr_id", "listing_url",
]

UBICACION = [
    "localidad", "provincia", "barrio", "direccion", "lat", "lng",
]

TIPO = [
    "tipo_propiedad", "operacion",
]

ECONOMICO = [
    "moneda", "precio", "tiene_expensas", "valor_expensas", "precio_m2",
]

CARACTERISTICAS = [
    "dormitorios", "banios", "ambientes", "ambientes_texto", "plantas",
    "superficie_total_m2", "superficie_cubierta_m2",
    "antiguedad_anios", "antiguedad_texto", "cochera",
    "amoblado", "pileta", "zona_escolar", "barrio_privado",
    "aire_acondicionado", "calefaccion_central", "telefono", "internet",
    "cable_tv", "apto_credito_hipotecario", "acepta_mascotas",
    "recibe_permuta", "estado_conservacion",
]

CONTENIDO = [
    "descripcion", "publicado_por", "es_dueno_directo",
]

METADATA = [
    "fecha_publicacion", "fecha_extraccion", "fuente",
]

COLUMNS = IDENTIDAD + UBICACION + TIPO + ECONOMICO + CARACTERISTICAS + CONTENIDO + METADATA

# --- tipos, para castear y para los seis chequeos de calidad ---------------

ENTEROS = [
    "prp_id", "usr_id", "precio", "valor_expensas",
    "dormitorios", "banios", "ambientes", "plantas",
    "superficie_total_m2", "superficie_cubierta_m2", "antiguedad_anios",
]

FLOTANTES = ["precio_m2"]

BOOLEANOS = [
    "tiene_expensas", "amoblado", "pileta", "zona_escolar", "barrio_privado",
    "aire_acondicionado", "calefaccion_central", "telefono", "internet",
    "cable_tv", "apto_credito_hipotecario", "acepta_mascotas",
    "recibe_permuta", "es_dueno_directo",
]

# Columnas sin las cuales la fila no sirve para el modelo de U3.
OBLIGATORIAS = ["listing_id", "tipo_propiedad", "operacion", "precio",
                "moneda", "localidad", "listing_url"]
