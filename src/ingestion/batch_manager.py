"""
Módulo de Gestión y Control de Lotes (Batch Control).
Garantiza la idempotencia del pipeline mediante verificación de hash SHA-256,
evitando el reprocesamiento de archivos duplicados o ya ingeridos.
"""

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
from typing import Optional, Dict, List, Any
from src.common.config import get_config, AppConfig
from src.common.logger import get_logger

logger = get_logger(__name__)


def calculate_file_sha256(file_path: str, chunk_size: int = 65536) -> str:
    """
    Calcula el hash criptográfico SHA-256 de un archivo en bloques.

    Args:
        file_path: Ruta al archivo en disco.
        chunk_size: Tamaño del bloque de lectura en bytes (por defecto 64 KB).

    Returns:
        str: Cadena hexadecimal con el hash SHA-256.
    """
    path = Path(file_path)
    if not path.is_file():
        raise FileNotFoundError(f"El archivo para cálculo de hash no existe: {file_path}")

    hasher = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)

    return hasher.hexdigest()


@dataclass
class BatchMetadata:
    """Metadatos representativos de un lote de datos ingerido."""
    batch_id: str
    source: str
    file_name: str
    file_hash: str
    file_size_bytes: int
    load_date: str
    ingest_timestamp: str
    destination_path: str
    status: str  # 'REGISTERED', 'PROCESSED_SILVER', 'PROCESSED_GOLD', 'FAILED'

    def to_dict(self) -> Dict[str, Any]:
        """Convierte los metadatos a diccionario."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BatchMetadata":
        """Instancia metadatos a partir de un diccionario."""
        return cls(**data)


class BatchControlManager:
    """
    Administrador del registro de control de lotes.
    Permite verificar la existencia previa de un hash SHA-256 y persistir
    el estado del procesamiento de cada archivo ingerido.
    """

    def __init__(self, config: Optional[AppConfig] = None):
        self.config = config or get_config()
        self.control_file_path = self.config.batch_control_path
        self._ensure_control_storage()

    def _ensure_control_storage(self) -> None:
        """Asegura que el directorio y archivo de control existan si es local."""
        if not self.config.is_aws:
            p = Path(self.control_file_path)
            p.parent.mkdir(parents=True, exist_ok=True)
            if not p.exists():
                with open(p, "w", encoding="utf-8") as f:
                    json.dump({"batches": []}, f, indent=2)

    def _load_registry(self) -> Dict[str, Any]:
        """Lee el registro completo de lotes desde almacenamiento."""
        if self.config.is_aws:
            # En AWS S3, se puede implementar lectura vía S3 o fallback local
            # Por seguridad y consistencia local-cloud básica se soporta lectura de archivo
            if os.path.exists(self.control_file_path):
                with open(self.control_file_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            return {"batches": []}

        try:
            with open(self.control_file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            return {"batches": []}

    def _save_registry(self, registry: Dict[str, Any]) -> None:
        """Escribe atómicamente el registro completo de lotes."""
        if not self.config.is_aws:
            temp_path = f"{self.control_file_path}.tmp"
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(registry, f, indent=2)
            os.replace(temp_path, self.control_file_path)

    def is_batch_processed(self, file_hash: str, source: Optional[str] = None) -> bool:
        """
        Verifica si un archivo con el hash SHA-256 especificado ya está registrado.

        Args:
            file_hash: Hash SHA-256 a validar.
            source: Nombre opcional de la fuente ('ira', 'malaria', 'senamhi').

        Returns:
            bool: True si el hash ya existe y no falló, False si es un lote nuevo.
        """
        registry = self._load_registry()
        for batch in registry.get("batches", []):
            if batch.get("file_hash") == file_hash:
                if source and batch.get("source", "").lower() != source.lower():
                    continue
                # Se considera procesado si su estado no es 'FAILED'
                if batch.get("status") != "FAILED":
                    return True
        return False

    def register_batch(self, metadata: BatchMetadata) -> None:
        """
        Registra un nuevo lote en el catálogo de control.

        Args:
            metadata: Objeto BatchMetadata a persistir.
        """
        registry = self._load_registry()
        batches = registry.setdefault("batches", [])

        # Evitar duplicar la misma entrada exacta de batch_id
        filtered_batches = [b for b in batches if b.get("batch_id") != metadata.batch_id]
        filtered_batches.append(metadata.to_dict())
        registry["batches"] = filtered_batches

        self._save_registry(registry)
        logger.info(
            "Lote registrado en control: [%s] Fuente: %s | Hash: %s",
            metadata.batch_id,
            metadata.source,
            metadata.file_hash[:12],
        )

    def update_batch_status(self, file_hash: str, new_status: str) -> bool:
        """
        Actualiza el estado de procesamiento de un lote dado su hash SHA-256.

        Args:
            file_hash: Hash SHA-256 del lote.
            new_status: Nuevo estado ('PROCESSED_SILVER', 'PROCESSED_GOLD', 'FAILED').

        Returns:
            bool: True si el lote fue encontrado y actualizado, False en caso contrario.
        """
        registry = self._load_registry()
        updated = False
        for batch in registry.get("batches", []):
            if batch.get("file_hash") == file_hash:
                batch["status"] = new_status
                batch["updated_at"] = datetime.now(timezone.utc).isoformat()
                updated = True

        if updated:
            self._save_registry(registry)
            logger.info("Estado del lote con hash %s actualizado a: %s", file_hash[:12], new_status)
        return updated

    def get_pending_batches(self, source: Optional[str] = None) -> List[BatchMetadata]:
        """
        Retorna la lista de lotes registrados que aún no se procesaron a Silver.

        Args:
            source: Filtro opcional por fuente ('ira', 'malaria', 'senamhi').

        Returns:
            List[BatchMetadata]: Lista de lotes pendientes.
        """
        registry = self._load_registry()
        results = []
        for b in registry.get("batches", []):
            if b.get("status") == "REGISTERED":
                if source and b.get("source", "").lower() != source.lower():
                    continue
                results.append(BatchMetadata.from_dict(b))
        return results
