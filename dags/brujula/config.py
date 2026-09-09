"""Constantes compartidas por las cinco tareas del pipeline."""

from pathlib import Path

# Raiz del repo: dags/brujula/config.py -> dags/brujula -> dags -> repo
REPO_DIR = Path(__file__).resolve().parents[2]

RAW_DIR = REPO_DIR / "data" / "raw"
PROCESSED_DIR = REPO_DIR / "data" / "processed"

BASE_URL = "https://inmoclick.com"
LISTADO_PATH = "/departamentos-en-alquiler-en-mendoza"

# robots.txt de Inmoclick no tiene reglas de exclusion (verificado), pero
# igual se pide despacio y con user agent identificable.
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "BrujulaInmobiliaria/1.0 (proyecto academico UTN FRM)"
)
PAUSA_SEGUNDOS = 1.0
TIMEOUT_SEGUNDOS = 30
REINTENTOS = 3

CSV_FINAL = PROCESSED_DIR / "brujula_inmobiliaria_alquiler_mendoza.csv"
