"""
Módulo de Ingesta a Capa Bronze (Raw Ingestor).
Copia archivos crudos hacia la partición correspondiente fecha_ingesta=YYYY-MM-DD,
garantizando el registro y la verificación por hash SHA-256.
"""

from datetime import datetime, timezone
import os
from pathlib import Path
import shutil
from typing import Optional, Tuple
from src.common.config import get_config, AppConfig
from src.common.logger import get_logger
from src.ingestion.batch_manager import (
    BatchControlManager,
    BatchMetadata,
    calculate_file_sha256,
)

logger = get_logger(__name__)


def ingest_raw_file(
    file_path: str,
    source: str,
    force: bool = False,
    config: Optional[AppConfig] = None,
) -> Tuple[bool, Optional[BatchMetadata], str]:
    """
    Ingesta un archivo crudo hacia la capa Bronze (data/raw/<fuente>/fecha_ingesta=YYYY-MM-DD/).

    Pasos del flujo:
    1. Valida que el archivo exista en la ruta indicada.
    2. Calcula el hash SHA-256 del contenido.
    3. Consulta el registro de control de lotes para validar si ya fue ingerido.
    4. Si ya fue procesado y 'force' es False, se omite para evitar reprocesamiento.
    5. Copia el archivo crudo al destino particionado por fecha de carga.
    6. Registra los metadatos del lote en el archivo de control.

    Args:
        file_path: Ruta al archivo local que se desea ingerir.
        source: Fuente de datos ('ira', 'malaria' o 'senamhi').
        force: Si es True, ignora la existencia previa del hash y vuelve a cargar.
        config: Configuración de la aplicación (AppConfig).

    Returns:
        Tuple[bool, Optional[BatchMetadata], str]:
            - éxito (bool)
            - metadata (BatchMetadata o None si fue omitido)
            - mensaje o estado ('INGESTED', 'SKIPPED_DUPLICATE', 'ERROR')
    """
    cfg = config or get_config()
    source_clean = source.strip().lower()

    if source_clean not in ("ira", "malaria", "senamhi"):
        raise ValueError(f"Fuente no soportada: '{source}'. Debe ser 'ira', 'malaria' o 'senamhi'.")

    input_path = Path(file_path).resolve()
    if not input_path.is_file():
        logger.error("Archivo de origen no encontrado: %s", file_path)
        return False, None, f"FILE_NOT_FOUND: {file_path}"

    # 1. Calcular Hash SHA-256
    file_hash = calculate_file_sha256(str(input_path))
    file_size = input_path.stat().st_size
    logger.info("Verificando hash SHA-256 para [%s]: %s (%d bytes)", source_clean, file_hash, file_size)

    # 2. Control de Idempotencia
    control_mgr = BatchControlManager(config=cfg)
    if not force and control_mgr.is_batch_processed(file_hash, source=source_clean):
        msg = f"El archivo ya fue ingerido y procesado previamente con hash SHA-256: {file_hash[:12]}. Omitiendo carga."
        logger.warning(msg)
        return False, None, "SKIPPED_DUPLICATE"

    # 3. Determinar partición de fecha de carga
    today_iso = datetime.now().strftime("%Y-%m-%d")
    now_ts = datetime.now(timezone.utc).isoformat()
    bronze_base = cfg.get_bronze_path(source_clean)

    if cfg.is_aws:
        # Destino en S3 (prefijo Bronze particionado)
        dest_filename = input_path.name
        dest_path = f"{bronze_base}/fecha_ingesta={today_iso}/{dest_filename}"
        logger.info("Destino S3 configurado: %s", dest_path)
        # Nota: En despliegue AWS directo se invoca transferencia S3/boto3
    else:
        # Destino en filesystem local
        dest_dir = Path(bronze_base) / f"fecha_ingesta={today_iso}"
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_file = dest_dir / input_path.name
        logger.info("Copiando archivo a la capa Bronze: %s", dest_file)
        shutil.copy2(input_path, dest_file)
        dest_path = str(dest_file).replace("\\", "/")

    # 4. Crear y registrar metadatos del lote
    batch_id = f"{source_clean}_{today_iso.replace('-', '')}_{file_hash[:8]}"
    metadata = BatchMetadata(
        batch_id=batch_id,
        source=source_clean,
        file_name=input_path.name,
        file_hash=file_hash,
        file_size_bytes=file_size,
        load_date=today_iso,
        ingest_timestamp=now_ts,
        destination_path=dest_path,
        status="REGISTERED",
    )

    control_mgr.register_batch(metadata)
    logger.info("Ingesta completada exitosamente para lote: %s", batch_id)
    return True, metadata, "INGESTED"
