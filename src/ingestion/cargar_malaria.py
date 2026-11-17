"""
Script de Ingesta para Datos de Malaria (MINSA).
Copia el archivo crudo a la capa Bronze particionado por fecha de carga,
verificando el hash SHA-256 en el registro de control de lotes.
"""

import argparse
from pathlib import Path
import sys
from typing import Optional
from src.common.config import get_config
from src.common.logger import get_logger
from src.ingestion.raw_ingestor import ingest_raw_file
from src.ingestion.batch_manager import BatchMetadata

logger = get_logger("Ingesta_Malaria")


def buscar_archivo_candidato() -> Optional[str]:
    """Busca archivos CSV candidatos disponibles en el repositorio para Malaria."""
    cfg = get_config()
    rutas_candidatas = [
        cfg.root_dir / "data" / "raw" / "malaria" / "dataset_Malaria.csv",
        cfg.root_dir / "data" / "raw" / "malaria" / "malaria_dataset.csv",
        cfg.root_dir / "data" / "processed" / "malaria" / "Malaria_La_Libertad_2015_2024.csv",
    ]
    for ruta in rutas_candidatas:
        if ruta.is_file():
            return str(ruta)
    return None


def cargar_malaria(archivo_origen: Optional[str] = None, force: bool = False) -> Optional[BatchMetadata]:
    """
    Ejecuta el proceso de ingesta del lote de Malaria a la capa Bronze.

    Args:
        archivo_origen: Ruta al archivo CSV a cargar. Si es None, busca candidato local.
        force: Si es True, fuerza la ingesta aunque el hash ya haya sido procesado.

    Returns:
        Optional[BatchMetadata]: Metadatos del lote si fue ingerido, None si fue omitido o falló.
    """
    ruta_archivo = archivo_origen or buscar_archivo_candidato()
    if not ruta_archivo:
        logger.error("No se encontró ningún archivo CSV de Malaria para ingerir.")
        return None

    logger.info("Iniciando ingesta de Malaria desde: %s (force=%s)", ruta_archivo, force)
    exito, metadata, estado = ingest_raw_file(
        file_path=ruta_archivo,
        source="malaria",
        force=force,
    )

    if exito and metadata:
        logger.info("Ingesta de Malaria finalizada exitosamente. Lote: %s", metadata.batch_id)
        return metadata
    elif estado == "SKIPPED_DUPLICATE":
        logger.info("Lote de Malaria ya existente (duplicado). Proceso omitido sin error.")
        return None
    else:
        logger.error("Error al ingerir archivo de Malaria: %s", estado)
        return None


def main():
    parser = argparse.ArgumentParser(description="Ingesta de datos crudos de Malaria (MINSA) a la capa Bronze.")
    parser.add_argument("--archivo", type=str, default=None, help="Ruta al archivo CSV de entrada.")
    parser.add_argument("--force", action="store_true", help="Forzar la recarga ignorando el hash SHA-256.")
    args = parser.parse_args()

    meta = cargar_malaria(archivo_origen=args.archivo, force=args.force)
    sys.exit(0 if meta or not args.force else 1)


if __name__ == "__main__":
    main()
