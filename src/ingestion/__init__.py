"""
Módulo de Ingesta (src.ingestion).
Responsable de la carga de lotes crudos a la capa Bronze, particionamiento por
fecha de carga y verificación criptográfica SHA-256 para idempotencia.
"""

from src.ingestion.batch_manager import (
    BatchControlManager,
    BatchMetadata,
    calculate_file_sha256,
)
from src.ingestion.raw_ingestor import ingest_raw_file
from src.ingestion.cargar_ira import cargar_ira
from src.ingestion.cargar_malaria import cargar_malaria
from src.ingestion.cargar_senamhi import cargar_senamhi

__all__ = [
    "BatchControlManager",
    "BatchMetadata",
    "calculate_file_sha256",
    "ingest_raw_file",
    "cargar_ira",
    "cargar_malaria",
    "cargar_senamhi",
]
