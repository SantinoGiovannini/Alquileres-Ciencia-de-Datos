"""Fuentes de datos del proyecto, una por portal.

Cada modulo de fuente expone la misma interfaz, asi que agregar un portal
nuevo no toca el resto del pipeline:

    NOMBRE                          nombre corto, va en la columna 'fuente'
    descargar(run_dir, ses, max_paginas)   capa bronce: guarda el HTML crudo
    parsear(run_dir) -> list[dict]         registros con el esquema comun

Los portales que faltan (Argenprop, ZonaProp) no estan aca todavia porque
su robots.txt no permite paginar los resultados de busqueda mas alla de la
pagina 3 y 5 respectivamente: para bajarlos completos hay que ir por sus
sitemaps, que es un camino distinto al de estos dos.
"""

from brujula.fuentes import inmoclick, inmoup

FUENTES = {
    inmoclick.NOMBRE: inmoclick,
    inmoup.NOMBRE: inmoup,
}


def elegir(nombres=None) -> dict:
    """Subconjunto de fuentes a usar en una corrida (por defecto, todas)."""
    if not nombres:
        return dict(FUENTES)
    faltantes = set(nombres) - set(FUENTES)
    if faltantes:
        raise ValueError(f"fuente desconocida: {sorted(faltantes)}")
    return {n: FUENTES[n] for n in nombres}
