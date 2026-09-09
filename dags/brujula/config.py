"""Constantes compartidas por las cinco tareas del pipeline."""

from pathlib import Path

# Raiz del repo: dags/brujula/config.py -> dags/brujula -> dags -> repo
REPO_DIR = Path(__file__).resolve().parents[2]

RAW_DIR = REPO_DIR / "data" / "raw"
PROCESSED_DIR = REPO_DIR / "data" / "processed"

# Resultados de cada scrapeo: el CSV y el reporte de calidad, en una
# carpeta por fecha. A diferencia de data/, esta si se versiona: son los
# entregables, pesan poco y conviene que el equipo los vea sin tener que
# correr el pipeline. El HTML crudo y los intermedios se quedan en data/.
RESULTADOS_DIR = REPO_DIR / "resultados"

# Cada portal permite cosas distintas en su robots.txt, y sus URLs viven
# en brujula/fuentes/. Lo que se comparte es el trato: user agent propio y
# despacio.
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "BrujulaInmobiliaria/1.0 (proyecto academico UTN FRM)"
)
PAUSA_SEGUNDOS = 1.0
TIMEOUT_SEGUNDOS = 30
REINTENTOS = 3

NOMBRE_CSV = "brujula_inmobiliaria_alquiler_mendoza.csv"
NOMBRE_REPORTE = "quality_report.txt"


def carpeta_resultados(run_folder=None) -> Path:
    """resultados/<fecha> de la corrida, o resultados/ si no hay fecha."""
    if not run_folder:
        return RESULTADOS_DIR
    return RESULTADOS_DIR / Path(run_folder).name
