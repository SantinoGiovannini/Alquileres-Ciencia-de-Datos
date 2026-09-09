"""Capa plata: tipar, definir la clave y calcular la columna objetivo."""

import logging
import re
from pathlib import Path

import pandas as pd

from brujula.config import PROCESSED_DIR
from brujula.parse import leer_registros

log = logging.getLogger(__name__)

INTERMEDIO = PROCESSED_DIR / "_intermedio_clean.csv"

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


def _si_no(serie: pd.Series) -> pd.Series:
    return serie.astype("string").str.strip().str.lower().map(SI_NO).astype("boolean")


def _firma(df: pd.DataFrame) -> pd.Series:
    """Huella de la propiedad, para detectar el mismo inmueble en dos portales.

    No se usa como clave: es solo para marcar. Junta direccion normalizada,
    localidad y superficie cubierta, que es lo que se mantiene igual cuando
    el mismo departamento se publica en Inmoclick y en InmoUP.
    """
    texto = (
        df["direccion"].astype("string").fillna("")
        + "|" + df["localidad"].astype("string").fillna("")
    ).str.lower().str.replace(RE_NO_ALFANUM, "", regex=True)
    return texto + "|" + df["superficie_cubierta_m2"].astype("string").fillna("")


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
    antes = len(df)
    df = df.drop_duplicates(subset="clave", keep="first").reset_index(drop=True)
    if antes != len(df):
        log.warning("se descartaron %s filas con clave repetida", antes - len(df))

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

    if run_folder:
        df["fecha_scraping"] = pd.to_datetime(Path(run_folder).name, format="%Y%m%d")

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
    for col in ("fecha_scraping", "fecha_publicacion"):
        if col in df:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    df["clave"] = df["clave"].astype("string")
    return df
