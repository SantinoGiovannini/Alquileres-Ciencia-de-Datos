"""Logica del pipeline Brujula Inmobiliaria.

Vive dentro de dags/ a proposito: Airflow monta esa carpeta y la agrega al
sys.path, asi que los modulos se importan igual desde el DAG y desde una
terminal, sin tocar el docker-compose.yaml oficial.
"""
