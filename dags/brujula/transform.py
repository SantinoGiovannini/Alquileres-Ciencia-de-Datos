"""Capa plata: tipar, definir la clave y calcular la columna objetivo."""

import logging
import re
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.neighbors import BallTree

from brujula.config import PROCESSED_DIR
from brujula.parse import leer_registros

log = logging.getLogger(__name__)

INTERMEDIO = PROCESSED_DIR / "_intermedio_clean.csv"

# Plaza Independencia, el centro de la ciudad de Mendoza.
CENTRO_LAT, CENTRO_LON = -32.8908, -68.8458

# Seis zonas: el Gran Mendoza es chico (unos 20 km de punta a punta) y con
# mas grupos quedan clusters de pocas decenas de avisos, que como categoria
# no le sirven a un modelo.
ZONAS = 6

# Radio del aglomerado. Adentro entran Capital, Godoy Cruz, Guaymallen, Las
# Heras, Maipu y Lujan de Cuyo, que son el 99 % de los avisos; afuera quedan
# San Rafael, General Alvear y el resto del interior.
GRAN_MENDOZA_KM = 30

NUMERICAS = (
    "precio", "superficie_total_m2", "superficie_cubierta_m2",
    "dormitorios", "banos", "ambientes", "valor_expensas", "antiguedad",
    "plantas",
)
BOOLEANAS = ("piscina", "amoblado", "tiene_expensas", "acepta_mascotas")
SI_NO = {"si": True, "no": False}

RE_NO_ALFANUM = re.compile(r"[^a-z0-9]+")


def _numero(serie: pd.Series) -> pd.Series:
    """Convierte a numero lo que publica cada portal.

    Tiene que distinguir dos usos del punto, porque conviven en la misma
    columna: en '1.200.000' separa miles (formato argentino) y en '720000.0'
    es el decimal de un float que ya venia convertido. Borrarlo siempre
    multiplicaba los precios por diez.

    Casos que cubre: 61 | '61' | '1.200.000' | '$ 10.000' | '4 Anos' |
    '720000.0' | '1.234,56'.
    """
    if pd.api.types.is_numeric_dtype(serie):
        return pd.to_numeric(serie, errors="coerce")

    limpia = (
        serie.astype("string")
        .str.replace(r"[^\d,.\-]", "", regex=True)
        .replace("", pd.NA)
    )

    # Con coma, la coma es el decimal y el punto separa miles.
    con_coma = limpia.str.contains(",", na=False)
    limpia = limpia.mask(
        con_coma,
        limpia.str.replace(".", "", regex=False).str.replace(",", ".", regex=False),
    )

    # Sin coma, el punto separa miles solo si agrupa de a tres digitos.
    # '1.200.000' si; '720000.0' no, ahi el punto es decimal.
    miles = limpia.str.fullmatch(r"-?\d{1,3}(\.\d{3})+").fillna(False)
    limpia = limpia.mask(miles, limpia.str.replace(".", "", regex=False))

    return pd.to_numeric(limpia, errors="coerce")


def _haversine_km(lat, lon, lat_ref: float, lon_ref: float):
    """Distancia sobre la esfera, en km, contra un punto fijo."""
    radio = 6371.0
    p1, p2 = np.radians(lat), np.radians(lat_ref)
    dp = p2 - p1
    dl = np.radians(lon_ref - lon)
    a = np.sin(dp / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(dl / 2) ** 2
    return 2 * radio * np.arcsin(np.sqrt(a))


def _si_no(serie: pd.Series) -> pd.Series:
    return serie.astype("string").str.strip().str.lower().map(SI_NO).astype("boolean")


def _firma(df: pd.DataFrame) -> pd.Series:
    """Huella de la propiedad, para detectar el mismo inmueble en dos portales.

    No se usa como clave: es solo para marcar. Junta direccion normalizada,
    localidad y superficie cubierta, que es lo que se mantiene igual cuando
    el mismo departamento se publica en Inmoclick y en InmoUP.

    Es deliberadamente aproximada, y **sobreestima**: muchos avisos publican
    la direccion sin numero ("FRENTE AL DALVIAN") o con el numero del
    edificio, asi que dos departamentos distintos de la misma torre y la
    misma tipologia caen en la misma firma. Medido sobre el dataset
    acumulado, de 120 firmas con dos portales salen 250 filas marcadas, y
    algunos grupos tienen 4 y 5 avisos con precios distintos: ahi no es el
    mismo inmueble repetido, es el mismo edificio.

    Por eso la columna se llama `posible_duplicado_cruzado` y no se borra
    nada: sirve para poder filtrar al entrenar, no como verdad.
    """
    texto = (
        df["direccion"].astype("string").fillna("")
        + "|" + df["localidad"].astype("string").fillna("")
    ).str.lower().str.replace(RE_NO_ALFANUM, "", regex=True)
    return texto + "|" + df["superficie_cubierta_m2"].astype("string").fillna("")


def _features_geo(df: pd.DataFrame) -> pd.DataFrame:
    """Convierte latitud y longitud en variables con sentido propio.

    Un par (lat, lon) crudo no le dice nada a un modelo lineal: -32,89 no es
    "mas" ni "menos" que -32,90 en ninguna escala util. Lo que si tiene
    sentido es la distancia al centro, el barrio y que tan densa es la zona.
    """
    if not {"latitud", "longitud"}.issubset(df.columns):
        log.warning("no hay latitud/longitud: se saltean las features geograficas")
        return df

    # Coordenadas imposibles para el segmento. Medido sobre la corrida del
    # 20260908 son 2 de 1559: una en Salta y una en Peru, las dos con
    # localidad "Capital", o sea el que publica marco mal el punto en el
    # mapa. Con esas dos adentro, la asimetria de 'latitud' da 33,5 y es un
    # artefacto, no una propiedad del dato.
    dentro = df["latitud"].between(-35.8, -31.8) & df["longitud"].between(-70.0, -66.0)
    fuera = ~dentro & df["latitud"].notna()
    if fuera.any():
        log.info("coordenadas fuera de Mendoza, se anulan: %s", int(fuera.sum()))
        # np.nan y no pd.NA: las dos columnas son float64 y pd.NA las
        # volveria de tipo object, que rompe el haversine de mas abajo.
        df.loc[fuera, ["latitud", "longitud"]] = np.nan

    # El 6,8 % sin coordenadas no se imputa: se marca. Imputar una posicion
    # inventada arrastra el error a todas las features derivadas.
    df["tiene_geo"] = df["latitud"].notna() & df["longitud"].notna()

    lat = pd.to_numeric(df["latitud"], errors="coerce")
    lon = pd.to_numeric(df["longitud"], errors="coerce")

    df["dist_centro_km"] = _haversine_km(lat, lon, CENTRO_LAT, CENTRO_LON)

    con_geo = df.index[df["tiene_geo"]]
    if len(con_geo) < ZONAS:
        log.warning("muy pocas filas con coordenadas: no se calcula zona_geo")
        return df

    # zona_geo agrupa por cercania real y no por el nombre de la localidad:
    # 'Capital' son 627 avisos de barrios muy distintos, y como categoria
    # unica esconde justamente la variacion que interesa.
    #
    # El clustering corre solo sobre el Gran Mendoza. Dejando entrar al
    # interior provincial (hay avisos hasta a 255 km, San Rafael y General
    # Alvear), KMeans gasta clusters en esos pocos puntos lejanos y mete
    # todo el aglomerado en uno solo: medido con 6 grupos daba 1353 avisos
    # en un cluster y 2 en otro, o sea justo lo contrario de lo que la
    # feature tiene que hacer.
    aglomerado = df["tiene_geo"] & (df["dist_centro_km"] <= GRAN_MENDOZA_KM)
    idx_agl = df.index[aglomerado]

    zona = pd.Series(pd.NA, index=df.index, dtype="Int64")
    if len(idx_agl) >= ZONAS:
        coords_agl = np.c_[lat.loc[idx_agl].to_numpy(), lon.loc[idx_agl].to_numpy()]
        modelo = KMeans(n_clusters=ZONAS, random_state=42, n_init=10).fit(coords_agl)
        zona.loc[idx_agl] = modelo.labels_
    # -1 es el interior provincial: no es un cluster mas, es "esta afuera
    # del aglomerado", y conviene que el modelo lo vea como su propia
    # categoria en vez de mezclarlo con un barrio de la ciudad.
    zona.loc[df.index[df["tiene_geo"] & ~aglomerado]] = -1
    df["zona_geo"] = zona

    coords = np.c_[lat.loc[con_geo].to_numpy(), lon.loc[con_geo].to_numpy()]

    # Vecinos en 1 km. BallTree con metrica haversine trabaja en radianes y
    # devuelve la distancia en radianes de la esfera: 1 km / 6371 km.
    arbol = BallTree(np.radians(coords), metric="haversine")
    vecinos = arbol.query_radius(np.radians(coords), r=1.0 / 6371.0, count_only=True)
    # -1 para no contarse a si mismo.
    df["densidad_1km"] = pd.Series(vecinos - 1, index=con_geo).astype("Int64")

    log.info(
        "features geograficas: %s filas con coordenadas, %s zonas, "
        "distancia al centro mediana %.1f km",
        int(df["tiene_geo"].sum()), ZONAS, df["dist_centro_km"].median(),
    )
    return df


def transform_clean(registros_path: str, run_folder: str = None) -> str:
    registros = leer_registros(registros_path)
    if not registros:
        raise ValueError("parse_raw no devolvio registros: revisar la capa bronce")

    df = pd.DataFrame(registros)

    # --- clave primaria -------------------------------------------------
    # El numero del aviso se repite entre inmobiliarias (hay decenas de
    # avisos con prp_id=1, uno por cada dueno directo), y ademas se repite
    # entre portales. La clave es fuente:usr_id-prp_id.
    df["clave"] = (
        df["fuente"].astype("string")
        + ":" + df["usr_id"].astype("string")
        + "-" + df["prp_id"].astype("string")
    )

    # --- el mismo aviso visto en varias fechas ---------------------------
    # parse_raw puede traer varias fechas de la capa bronce, asi que un
    # aviso que sigue publicado aparece una vez por corrida. No son filas
    # distintas: son la misma, observada varias veces. Antes de quedarnos
    # con una sola, se resume el historial, que es informacion que no
    # existia cuando el dataset era una sola foto.
    if "fecha_scraping" in df:
        df["fecha_scraping"] = pd.to_datetime(
            df["fecha_scraping"], format="%Y%m%d", errors="coerce"
        )
    elif run_folder:
        df["fecha_scraping"] = pd.to_datetime(Path(run_folder).name, format="%Y%m%d")

    por_clave = df.groupby("clave")["fecha_scraping"]
    df["primera_vista"] = por_clave.transform("min")
    df["ultima_vista"] = por_clave.transform("max")
    df["veces_visto"] = por_clave.transform("nunique")
    df["dias_publicado"] = (df["ultima_vista"] - df["primera_vista"]).dt.days

    # Una clave repetida *dentro de la misma corrida* si es una anomalia:
    # significa que un portal publico dos veces el mismo aviso, o que la
    # clave no alcanza para identificarlo. Entre fechas distintas es lo
    # esperado, asi que hay que mirar las dos cosas por separado para que la
    # segunda no tape a la primera.
    repetidos_en_fecha = int(df.duplicated(subset=["clave", "fecha_scraping"]).sum())
    if repetidos_en_fecha:
        log.warning(
            "hay %s avisos con clave repetida dentro de una misma corrida: "
            "revisar si la clave alcanza para identificar el aviso",
            repetidos_en_fecha,
        )

    # keep="last" sobre las fechas ordenadas y no keep="first": de las
    # copias de un aviso nos interesa la mas reciente, porque el precio
    # publicado pudo cambiar entre una corrida y la siguiente.
    antes = len(df)
    df = (
        df.sort_values("fecha_scraping")
        .drop_duplicates(subset="clave", keep="last")
        .reset_index(drop=True)
    )
    if antes != len(df):
        log.info(
            "%s observaciones -> %s avisos unicos (%s repetidos entre fechas)",
            antes, len(df), antes - len(df),
        )

    # --- tipado ---------------------------------------------------------
    # El precio y la moneda ya vienen normalizados por cada fuente: los
    # portales los publican distinto (texto '$ 720.000' vs JSON-LD
    # price/priceCurrency) y esa diferencia se resuelve alla.
    for col in NUMERICAS:
        if col in df:
            df[col] = _numero(df[col])

    for col in BOOLEANAS:
        if col in df:
            df[col] = _si_no(df[col])

    for col in ("latitud", "longitud"):
        if col in df:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "fecha_publicacion" in df:
        # Solo InmoUP la publica; en las filas de Inmoclick queda nula.
        df["fecha_publicacion"] = pd.to_datetime(
            df["fecha_publicacion"], errors="coerce"
        )

    # --- geografia --------------------------------------------------------
    df = _features_geo(df)

    # --- columna objetivo -----------------------------------------------
    # precio_m2 queda expresado en la moneda del aviso: hay avisos en pesos
    # y en dolares, asi que el modelo tiene que segmentar por 'moneda' (o
    # convertir) antes de comparar. Superficie 0 -> nulo, no una division
    # absurda.
    superficie = df["superficie_cubierta_m2"].where(df["superficie_cubierta_m2"] > 0)
    df["precio_m2"] = df["precio"] / superficie

    # --- el mismo inmueble en dos portales ------------------------------
    # No se borran: la unidad de analisis es el aviso publicado, y dos
    # avisos del mismo departamento en portales distintos son dos avisos.
    # Pero para entrenar el modelo hay que poder filtrarlos, porque
    # inflarian el peso de esas propiedades. Por eso se marcan.
    firma = _firma(df)
    portales_por_firma = df.groupby(firma)["fuente"].transform("nunique")
    df["posible_duplicado_cruzado"] = (portales_por_firma > 1) & (firma.str[0] != "|")
    log.info(
        "avisos marcados como posible duplicado entre portales: %s",
        int(df["posible_duplicado_cruzado"].sum()),
    )

    # --- valores que el que publica cargo mal -----------------------------
    # Reglas de sentido comun sobre el dominio, no estadisticas: un
    # departamento no mide 3 m2 ni 150.000, y un alquiler mensual de
    # US$ 850.000 es el precio de venta cargado en un aviso de alquiler.
    # Igual que con los duplicados, se marcan y no se borran: son avisos
    # publicados de verdad, y la Entrega 1 tiene que poder mostrarlos.
    motivos = pd.Series(pd.NA, index=df.index, dtype="string")
    superficie_rara = ~df["superficie_cubierta_m2"].between(10, 1000)
    motivos = motivos.mask(
        superficie_rara & df["superficie_cubierta_m2"].notna(), "superficie fuera de rango"
    )
    precio_de_venta = (df["moneda"] == "USD") & (df["precio"] > 10_000)
    motivos = motivos.mask(precio_de_venta, "precio parece de venta, no de alquiler")
    df["motivo_sospecha"] = motivos
    log.info(
        "avisos con valores sospechosos: %s (%s)",
        int(motivos.notna().sum()),
        motivos.value_counts().to_dict(),
    )

    # Campos que los portales declaran pero que en este segmento nadie
    # completa: se sacan para no dejar columnas 100% nulas en el dataset
    # final. Queda en el log cuales fueron.
    vacias = list(df.columns[df.isna().all()])
    if vacias:
        log.info("columnas sin ningun dato en este segmento, se descartan: %s", vacias)
        df = df.drop(columns=vacias)

    INTERMEDIO.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(INTERMEDIO, index=False, encoding="utf-8")
    log.info("intermedio guardado: %s filas x %s columnas", *df.shape)
    return str(INTERMEDIO)


def leer_intermedio(clean_path: str) -> pd.DataFrame:
    """Relee el intermedio con los tipos que dejo transform_clean."""
    df = pd.read_csv(clean_path, encoding="utf-8")
    for col in BOOLEANAS + ("es_dueno_directo", "posible_duplicado_cruzado"):
        if col in df:
            df[col] = df[col].astype("boolean")
    for col in ("fecha_scraping", "fecha_publicacion", "primera_vista", "ultima_vista"):
        if col in df:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    df["clave"] = df["clave"].astype("string")
    return df
