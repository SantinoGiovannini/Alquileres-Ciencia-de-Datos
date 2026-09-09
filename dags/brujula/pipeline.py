"""Corre las cinco tareas seguidas, sin Airflow.

Sirve para probar la logica mientras se desarrolla:

    cd dags
    python -m brujula.pipeline --max-paginas 1        # prueba rapida
    python -m brujula.pipeline --fuentes inmoup      # un solo portal
    python -m brujula.pipeline                       # corrida completa

El DAG hace exactamente lo mismo, pero cada paso como tarea separada.
"""

import argparse
import logging

from brujula import export, extract, parse, quality, transform


def main() -> None:
    ap = argparse.ArgumentParser(description="Pipeline Brujula Inmobiliaria")
    ap.add_argument(
        "--max-paginas", type=int, default=None,
        help="cuantas paginas de resultados bajar por fuente (por defecto, todas)",
    )
    ap.add_argument(
        "--fuentes", nargs="*", default=None,
        help="que portales usar (por defecto, todos: inmoclick inmoup)",
    )
    ap.add_argument(
        "--fecha", default=None,
        help="AAAAMMDD de la capa bronce; una ya scrapeada se reprocesa sin "
             "volver a pedirle nada a los portales",
    )
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s | %(message)s"
    )

    run_folder = extract.extract_listings(
        max_paginas=args.max_paginas, fuentes=args.fuentes, fecha=args.fecha
    )
    registros_path = parse.parse_raw(run_folder, fuentes=args.fuentes)
    clean_path = transform.transform_clean(registros_path, run_folder=run_folder)
    report_path = quality.quality_check(clean_path, run_folder=run_folder)
    csv_path = export.export_csv(clean_path, report_path, run_folder=run_folder)

    print("\n" + "=" * 60)
    print("bronce :", run_folder)
    print("calidad:", report_path)
    print("CSV    :", csv_path)


if __name__ == "__main__":
    main()
