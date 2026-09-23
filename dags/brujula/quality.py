"""Los seis chequeos de calidad de la catedra, guardados como reporte.

Si un chequeo falla queda escrito en el reporte y en el log; la tarea no se
cae por eso, porque la idea es poder mostrar y explicar el estado real del
dataset, no esconderlo.
"""

import logging

import pandas as pd

from brujula.config import NOMBRE_REPORTE, carpeta_resultados
from brujula.transform import leer_intermedio

log = logging.getLogger(__name__)


def _ventana_de_observacion(df) -> list:
    """Que fechas de scraping entraron y cuantos avisos aporto cada una.

    Con el dataset acumulado, saber esto no es un detalle: `precio_m2` esta
    en pesos para la mayoria de las filas, y mezclar fechas separadas por
    semanas mete la inflacion adentro de la columna objetivo. El reporte
    tiene que dejar escrito de que ventana se trata.
    """
    if "primera_vista" not in df or "fecha_scraping" not in df:
        return []

    fechas = df["fecha_scraping"].dropna()
    if fechas.empty:
        return []

    nuevos = df["primera_vista"].dt.strftime("%Y-%m-%d").value_counts().sort_index()
    return [
        "   ventana de observacion: "
        f"{fechas.min():%Y-%m-%d} a {fechas.max():%Y-%m-%d} "
        f"({df['fecha_scraping'].nunique()} corridas)",
        "   avisos vistos por primera vez en cada corrida:",
        *[f"      {fecha:<12} {n}" for fecha, n in nuevos.items()],
        f"   avisos vistos en mas de una corrida: "
        f"{int((df['veces_visto'] > 1).sum())}",
    ]


def _cobertura_geografica(df) -> list:
    """Cuantos avisos tienen coordenadas utilizables y como quedan las zonas."""
    if "tiene_geo" not in df:
        return []

    lineas = [
        f"   avisos con coordenadas: {int(df['tiene_geo'].sum())} "
        f"({df['tiene_geo'].mean():.1%})",
    ]
    if "dist_centro_km" in df:
        lineas.append(
            "   distancia al centro (km): "
            f"mediana {df['dist_centro_km'].median():.1f} | "
            f"maxima {df['dist_centro_km'].max():.1f}"
        )
    if "zona_geo" in df:
        reparto = df["zona_geo"].value_counts(dropna=False).sort_index()
        lineas.append("   avisos por zona_geo (-1 = interior provincial):")
        lineas += [f"      zona {z:<8} {n}" for z, n in reparto.items()]
    return lineas


def quality_check(clean_path: str, run_folder: str = None) -> str:
    df = leer_intermedio(clean_path)

    clave_unica = bool(df["clave"].is_unique)
    filas = len(df)
    nulos = df.isna().mean().sort_values(ascending=False)
    todo_nulo = list(df.columns[df.isna().all()])
    duplicados = df.get(
        "posible_duplicado_cruzado",
        pd.Series(False, index=df.index, dtype="boolean"),
    )
    sospechas = df.get(
        "motivo_sospecha",
        pd.Series(pd.NA, index=df.index, dtype="string"),
    )

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
        f"{int(duplicados.sum())} "
        "(no se borran: la unidad de analisis es el aviso)",
        f"   filas sin precio_m2: {int(df['precio_m2'].isna().sum())} "
        "(sin precio publicado o sin superficie cubierta)",
        f"   avisos con valores sospechosos: {int(sospechas.notna().sum())} "
        "(se marcan, no se borran)",
        *[f"      {m:<45} {n}"
          for m, n in sospechas.value_counts().items()],
        *_ventana_de_observacion(df),
        *_cobertura_geografica(df),
    ]

    if not clave_unica:
        log.error("hay claves repetidas: la clave primaria no sirve como esta")
    if todo_nulo:
        log.warning("columnas 100 por ciento nulas: %s", todo_nulo)
    if filas <= 1000:
        log.warning("el dataset tiene %s filas, por debajo de las 1000 pedidas", filas)

    reporte = carpeta_resultados(run_folder) / NOMBRE_REPORTE
    reporte.parent.mkdir(parents=True, exist_ok=True)
    reporte.write_text("\n".join(lineas), encoding="utf-8")
    log.info("reporte de calidad escrito en %s", reporte)
    return str(reporte)
