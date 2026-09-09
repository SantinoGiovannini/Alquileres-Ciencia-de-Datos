"""Capa plata final: el CSV que se abre y se defiende en la Entrega 1."""

import logging

from brujula.config import NOMBRE_CSV, carpeta_resultados
from brujula.transform import leer_intermedio

log = logging.getLogger(__name__)

# Orden pensado para que el CSV se lea de izquierda a derecha: primero la
# identidad del aviso, despues precio y columna objetivo, despues los
# atributos, y al final el texto largo.
ORDEN = [
    "clave", "fuente", "usr_id", "prp_id", "url",
    "fecha_scraping", "fecha_publicacion",
    "precio", "moneda", "precio_m2",
    "superficie_cubierta_m2", "superficie_total_m2",
    "ambientes", "dormitorios", "banos",
    "tipo_construccion", "condicion", "estado_conservacion", "antiguedad",
    "localidad", "provincia", "direccion", "ubicacion", "latitud", "longitud",
    "cochera", "plantas", "piscina", "amoblado",
    "tiene_expensas", "valor_expensas", "acepta_mascotas", "zona_escolar",
    "publicador", "tipo_anunciante", "es_dueno_directo",
    "posible_duplicado_cruzado", "motivo_sospecha",
    "descripcion",
]


def export_csv(clean_path: str, report_path: str, run_folder: str = None) -> str:
    df = leer_intermedio(clean_path)

    columnas = [c for c in ORDEN if c in df.columns]
    sobrantes = [c for c in df.columns if c not in columnas]
    if sobrantes:
        log.info("columnas fuera del orden previsto, van al final: %s", sobrantes)

    df = df[columnas + sobrantes]
    csv_final = carpeta_resultados(run_folder) / NOMBRE_CSV
    csv_final.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_final, index=False, encoding="utf-8")
    log.info(
        "CSV final: %s (%s filas x %s columnas). Calidad en %s",
        csv_final, len(df), df.shape[1], report_path,
    )
    return str(csv_final)
