"""Del HTML crudo (inmoclick, argenprop o inmoup) al esquema canonico.

`precio_m2` no se calcula aca: se hace en `transform_clean` del DAG, sobre
el DataFrame ya consolidado -- es una cuenta vectorizada, no algo que
dependa de como vino cada aviso.
"""
from brujula import schema
from brujula.inmoclick import bool_si, entero, texto

# Etiqueta en espanol de la ficha de inmoclick ("Datos de la propiedad") ->
# columna booleana. Son pares dispersos: un aviso puede no traer ninguna.
AMENITIES_BOOL = {
    "Piscina": "pileta",
    "Zona Escolar": "zona_escolar",
    "Amoblado/a": "amoblado",
    "Tiene Expensas": "tiene_expensas",
    "¿Aceptan Mascotas?": "acepta_mascotas",
    "Barrio Privado": "barrio_privado",
    "Apto Crédito Hipotecario": "apto_credito_hipotecario",
    "Aire Acondicionado": "aire_acondicionado",
    "Calefacción Central": "calefaccion_central",
    "Teléfono": "telefono",
    "Internet": "internet",
    "Cable TV": "cable_tv",
    "¿Recibe Permuta?": "recibe_permuta",
}


def to_row(card, detail, tipo, fecha_extraccion):
    """card: una fila de `inmoclick.parse_listing_page`. tipo: 'departamento' | 'casa'.

    detail: lo que devuelve `inmoclick.parse_detail_page`, o `None` si la
    ficha no se pudo bajar (la fila igual se arma, solo quedan en blanco los
    campos que solo viven ahi: ambientes, antiguedad, amenities).
    """
    r = {c: None for c in schema.COLUMNS}
    kv = (detail or {}).get("kv", {})

    r["listing_id"] = card["kid"]
    r["prp_id"] = entero(card.get("prp_id"))
    r["usr_id"] = entero(card.get("usr_id"))
    r["listing_url"] = card.get("url")

    r["localidad"] = card.get("localidad")
    r["provincia"] = card.get("provincia")
    r["barrio"] = (detail or {}).get("barrio") or card.get("localidad")
    r["direccion"] = card.get("direccion")
    r["lat"] = card.get("lat")
    r["lng"] = card.get("lng")

    r["tipo_propiedad"] = tipo
    r["operacion"] = "alquiler"

    r["moneda"] = card.get("moneda")
    r["precio"] = card.get("precio")
    r["valor_expensas"] = entero(kv.get("Valor de Expensas"))

    r["dormitorios"] = entero(card.get("dormitorios"))
    r["banios"] = entero(card.get("banios"))

    ambientes_texto = kv.get("Cantidad de Ambientes")
    r["ambientes_texto"] = ambientes_texto
    r["ambientes"] = 6 if ambientes_texto == "6 o mas" else entero(ambientes_texto)

    r["plantas"] = entero(kv.get("Plantas"))

    r["superficie_total_m2"] = entero(card.get("sup_total"))
    r["superficie_cubierta_m2"] = entero(card.get("sup_cubierta"))

    antiguedad_texto = kv.get("Antigüedad")
    r["antiguedad_texto"] = antiguedad_texto
    r["antiguedad_anios"] = (
        None if not antiguedad_texto or "indistinto" in antiguedad_texto.lower()
        else entero(antiguedad_texto)
    )

    # La ficha repite Cochera con mas detalle ("Garage Doble", "Sin Cochera");
    # se prefiere sobre la de la tarjeta y se cae a esta ultima si falta.
    r["cochera"] = texto(kv.get("Cochera")) or card.get("cochera")

    for etiqueta, columna in AMENITIES_BOOL.items():
        r[columna] = bool_si(kv.get(etiqueta))

    r["estado_conservacion"] = texto(kv.get("Estado de Conservación"))

    r["descripcion"] = card.get("descripcion")
    r["publicado_por"] = card.get("publicado_por")
    r["es_dueno_directo"] = card.get("publicado_por") == "Dueño Directo"

    r["fecha_extraccion"] = fecha_extraccion
    r["fuente"] = "inmoclick"
    return r


# Etiquetas de "Caracteristicas" de argenprop -> columna booleana. A
# diferencia de AMENITIES_BOOL (verificado contra fichas reales de
# inmoclick), esta lista es best-effort: solo se inspecciono una ficha real
# de argenprop y no traia amenities, asi que las etiquetas se completaron
# por el nombre esperable de la columna, sin confirmar el texto exacto que
# usa el sitio para cada una.
AMENITIES_BOOL_ARGENPROP = {
    "Pileta": "pileta",
    "Zona Escolar": "zona_escolar",
    "Amoblado": "amoblado",
    "Barrio Privado": "barrio_privado",
    "Apto Crédito Hipotecario": "apto_credito_hipotecario",
    "Aire Acondicionado": "aire_acondicionado",
    "Calefacción Central": "calefaccion_central",
    "Teléfono": "telefono",
    "Internet": "internet",
    "Cable TV": "cable_tv",
    "Acepta Mascotas": "acepta_mascotas",
}


def to_row_argenprop(ficha, listing_id, url, tipo, fecha_extraccion):
    """ficha: lo que devuelve `argenprop.parse_ficha` (ya confirmado Mendoza).
    tipo: 'departamento' | 'casa' (de `argenprop.tipo_from_url`).

    A diferencia de `to_row`, aca no hay tarjeta de listado -- todo sale de
    una sola ficha ya renderizada. Lo que falta siempre porque argenprop no
    lo expone tal como lo lee este parser: `prp_id`, `usr_id`, `lat`/`lng`,
    `publicado_por`.
    """
    from brujula.argenprop import bool_si as bool_si_ap
    from brujula.argenprop import entero as entero_ap

    r = {c: None for c in schema.COLUMNS}
    kv = ficha.get("kv", {})

    r["listing_id"] = f"argenprop-{listing_id}"
    r["listing_url"] = url

    r["localidad"] = ficha.get("localidad")
    r["provincia"] = ficha.get("provincia")
    r["direccion"] = ficha.get("direccion")

    r["tipo_propiedad"] = tipo
    r["operacion"] = "alquiler"

    r["moneda"] = ficha.get("moneda")
    r["precio"] = ficha.get("precio")

    r["dormitorios"] = entero_ap(kv.get("Cant. Dormitorios"))
    r["banios"] = entero_ap(kv.get("Cant. Baños"))
    r["ambientes"] = entero_ap(kv.get("Cant. Ambientes"))
    r["cochera"] = kv.get("Cant. Cocheras") if "Cant. Cocheras" in kv else None

    r["superficie_total_m2"] = ficha.get("sup_total")
    r["superficie_cubierta_m2"] = ficha.get("sup_cubierta")

    antiguedad_texto = kv.get("Antiguedad")
    r["antiguedad_texto"] = antiguedad_texto
    r["antiguedad_anios"] = entero_ap(antiguedad_texto)

    for etiqueta, columna in AMENITIES_BOOL_ARGENPROP.items():
        r[columna] = bool_si_ap(kv.get(etiqueta))

    r["estado_conservacion"] = kv.get("Estado")
    r["descripcion"] = ficha.get("descripcion")

    r["fecha_extraccion"] = fecha_extraccion
    r["fuente"] = "argenprop"
    return r


def to_row_inmoup(ficha, listing_id, url, tipo, fecha_extraccion):
    """ficha: lo que devuelve `inmoup.parse_ficha` (ya confirmado Mendoza).
    tipo: 'departamento' | 'casa' (de `inmoup.tipo_from_url`).

    A diferencia de argenprop, las etiquetas de `additionalProperty` en el
    JSON-LD de inmoup coinciden con las que ya se verificaron contra
    Inmoclick ("Piscina", "Zona Escolar", "Amoblado/a", etc.) -- se reusa
    `AMENITIES_BOOL` en vez de armar un diccionario best-effort aparte.
    """
    from brujula.inmoup import bool_si as bool_si_iu
    from brujula.inmoup import entero as entero_iu

    r = {c: None for c in schema.COLUMNS}
    kv = ficha.get("kv", {})

    r["listing_id"] = f"inmoup-{listing_id}"
    r["listing_url"] = url

    r["localidad"] = ficha.get("localidad")
    r["provincia"] = ficha.get("provincia")
    r["direccion"] = ficha.get("direccion")
    r["lat"] = ficha.get("lat")
    r["lng"] = ficha.get("lng")

    r["tipo_propiedad"] = tipo
    r["operacion"] = "alquiler"

    r["moneda"] = ficha.get("moneda")
    r["precio"] = ficha.get("precio")

    r["dormitorios"] = entero_iu(kv.get("Dormitorios"))
    r["banios"] = entero_iu(kv.get("Baños"))

    ambientes_texto = kv.get("Cantidad de Ambientes")
    r["ambientes_texto"] = ambientes_texto
    r["ambientes"] = entero_iu(ambientes_texto)

    r["plantas"] = entero_iu(kv.get("Plantas"))
    r["superficie_total_m2"] = entero_iu(kv.get("Superficie Total m2"))
    r["superficie_cubierta_m2"] = entero_iu(kv.get("Superficie Cubierta m2"))

    antiguedad_texto = kv.get("Antigüedad")
    r["antiguedad_texto"] = antiguedad_texto
    r["antiguedad_anios"] = entero_iu(antiguedad_texto)

    r["cochera"] = kv.get("Cochera")

    for etiqueta, columna in AMENITIES_BOOL.items():
        if etiqueta in kv:
            r[columna] = bool_si_iu(kv.get(etiqueta))

    r["estado_conservacion"] = kv.get("Estado de Conservación")

    r["descripcion"] = ficha.get("descripcion")
    r["publicado_por"] = ficha.get("publicado_por")

    r["fecha_publicacion"] = ficha.get("fecha_publicacion")
    r["fecha_extraccion"] = fecha_extraccion
    r["fuente"] = "inmoup"
    return r
