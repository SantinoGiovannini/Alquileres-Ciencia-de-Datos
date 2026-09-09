"""Los seis chequeos de calidad de la catedra, guardados como reporte.

Si un chequeo falla queda escrito en el reporte y en el log; la tarea no se
cae por eso, porque la idea es poder mostrar y explicar el estado real del
dataset, no esconderlo.
"""

import logging

from brujula.config import PROCESSED_DIR
from brujula.transform import leer_intermedio

log = logging.getLogger(__name__)

REPORTE = PROCESSED_DIR / "quality_report.txt"


def quality_check(clean_path: str) -> str:
    df = leer_intermedio(clean_path)

    clave_unica = bool(df["clave"].is_unique)
    filas = len(df)
    nulos = df.isna().mean().sort_values(ascending=False)
    todo_nulo = list(df.columns[df.isna().all()])

    lineas = [
        "REPORTE DE CALIDAD - Brujula Inmobiliaria",
        f"origen: {clean_path}",
        "",
        "1) clave primaria unica (df['clave'].is_unique)",
        f"   {clave_unica}",
        "",
        "2) cantidad de filas (len(df))",
        f"   {filas}   [objetivo de la catedra: > 1000]",
        "",
        "3) forma del dataset (df.shape)",
        f"   {df.shape}   [objetivo: 5+ columnas utiles]",
        "",
        "4) mezcla de tipos (df.dtypes.value_counts())",
        *[f"   {tipo}: {cant}" for tipo, cant in df.dtypes.value_counts().items()],
        "",
        "5) proporcion de nulos por columna (df.isna().mean())",
        *[f"   {col:<28} {prop:.3f}" for col, prop in nulos.items()],
        "",
        "6) columnas 100% nulas (df.columns[df.isna().all()])",
        f"   {todo_nulo or 'ninguna'}",
        "",
        "OBSERVACIONES",
        "   filas por fuente:",
        *[f"      {f:<12} {n}" for f, n in df["fuente"].value_counts().items()],
        f"   avisos en pesos: {int((df['moneda'] == 'ARS').sum())} | "
        f"en dolares: {int((df['moneda'] == 'USD').sum())}",
        f"   marcados como el mismo inmueble en dos portales: "
        f"{int(df['posible_duplicado_cruzado'].sum())} "
        "(no se borran: la unidad de analisis es el aviso)",
        f"   filas sin precio_m2: {int(df['precio_m2'].isna().sum())} "
        "(sin precio publicado o sin superficie cubierta)",
        f"   avisos con valores sospechosos: {int(df['motivo_sospecha'].notna().sum())} "
        "(se marcan, no se borran)",
        *[f"      {m:<45} {n}"
          for m, n in df["motivo_sospecha"].value_counts().items()],
    ]

    if not clave_unica:
        log.error("hay claves repetidas: la clave primaria no sirve como esta")
    if todo_nulo:
        log.warning("columnas 100 por ciento nulas: %s", todo_nulo)
    if filas <= 1000:
        log.warning("el dataset tiene %s filas, por debajo de las 1000 pedidas", filas)

    REPORTE.parent.mkdir(parents=True, exist_ok=True)
    REPORTE.write_text("\n".join(lineas), encoding="utf-8")
    log.info("reporte de calidad escrito en %s", REPORTE)
    return str(REPORTE)
