"""
Módulo de Gestión y Control de Lotes (Batch Control).
Garantiza la idempotencia del pipeline mediante verificación de hash SHA-256.
En modo local persiste en JSON; en modo AWS persiste en la tabla 'lotes' de Amazon RDS PostgreSQL.
"""

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
from urllib.parse import unquote
from typing import Optional, Dict, List, Any
from src.common.config import get_config, AppConfig
from src.common.logger import get_logger
from src.common.postgres import connect_postgres, postgres_cursor
from src.common.s3_artifacts import put_json, validate_run_id

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
    Soporta almacenamiento en archivo local JSON o en tabla 'lotes' de Amazon RDS PostgreSQL.
    """

    def __init__(self, config: Optional[AppConfig] = None):
        self.config = config or get_config()
        if not self.config.is_aws:
            self.control_file_path = self.config.batch_control_path
            path = Path(self.control_file_path)
            try:
                with path.open("x", encoding="utf-8") as file:
                    json.dump({"batches": []}, file, indent=2)
            except FileExistsError:
                pass

    def _get_db_connection(self):
        return connect_postgres(self.config.postgres)

    def _load_registry_local(self) -> Dict[str, Any]:
        """Lee el registro de lotes desde el archivo JSON local."""
        try:
            with open(self.control_file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            return {"batches": []}

    def _save_registry_local(self, registry: Dict[str, Any]) -> None:
        """Escribe atómicamente el registro de lotes en el archivo JSON local."""
        temp_path = f"{self.control_file_path}.tmp"
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(registry, f, indent=2)
        os.replace(temp_path, self.control_file_path)

    def is_batch_processed(self, file_hash: str, source: Optional[str] = None) -> bool:
        if self.config.is_aws:
            with postgres_cursor(self._get_db_connection()) as cur:
                cur.execute(
                    "SELECT 1 FROM public.lotes WHERE file_hash = %s AND status <> 'FAILED' "
                    "AND (%s::text IS NULL OR lower(source) = lower(%s))",
                    (file_hash, source, source),
                )
                return cur.fetchone() is not None
        return self._is_batch_processed_local(file_hash, source)

    def _is_batch_processed_local(self, file_hash: str, source: Optional[str] = None) -> bool:
        registry = self._load_registry_local()
        for batch in registry.get("batches", []):
            if batch.get("file_hash") == file_hash:
                if source and batch.get("source", "").lower() != source.lower():
                    continue
                if batch.get("status") != "FAILED":
                    return True
        return False

    def register_batch(self, metadata: BatchMetadata, *, force: bool = False) -> None:
        if self.config.is_aws:
            with postgres_cursor(self._get_db_connection()) as cur:
                cur.execute("""
                    INSERT INTO public.lotes AS lote (
                        batch_id, source, file_name, file_hash, file_size_bytes,
                        load_date, ingest_timestamp, destination_path, status
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (file_hash) DO UPDATE SET
                        file_name = EXCLUDED.file_name,
                        destination_path = EXCLUDED.destination_path,
                        status = EXCLUDED.status,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE lote.source = EXCLUDED.source
                      AND (lote.status = 'FAILED' OR %s)
                    RETURNING batch_id
                """, (
                    metadata.batch_id, metadata.source, metadata.file_name, metadata.file_hash,
                    metadata.file_size_bytes, metadata.load_date, metadata.ingest_timestamp,
                    metadata.destination_path, metadata.status, force,
                ))
                row = cur.fetchone()
                if row is None:
                    cur.execute("SELECT batch_id, source FROM public.lotes WHERE file_hash = %s",
                                (metadata.file_hash,))
                    batch_id, source = cur.fetchone()
                    if source != metadata.source:
                        raise ValueError("El hash ya está registrado para otra fuente.")
                else:
                    batch_id = row[0]
                metadata.batch_id = batch_id
            logger.info("Lote registrado en public.lotes: %s", metadata.batch_id)
            return

        # Registro en JSON local
        registry = self._load_registry_local()
        batches = registry.setdefault("batches", [])
        filtered_batches = [b for b in batches if b.get("batch_id") != metadata.batch_id]
        filtered_batches.append(metadata.to_dict())
        registry["batches"] = filtered_batches
        self._save_registry_local(registry)
        logger.info(
            "Lote registrado en control local: [%s] Fuente: %s | Hash: %s",
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
        updated = False
        if self.config.is_aws:
            with postgres_cursor(self._get_db_connection()) as cur:
                cur.execute(
                    "UPDATE public.lotes SET status = %s, updated_at = CURRENT_TIMESTAMP "
                    "WHERE file_hash = %s AND (status = 'REGISTERED' OR %s = 'FAILED')",
                    (new_status, file_hash, new_status),
                )
                return cur.rowcount > 0

        registry = self._load_registry_local()
        for batch in registry.get("batches", []):
            if batch.get("file_hash") == file_hash:
                batch["status"] = new_status
                batch["updated_at"] = datetime.now(timezone.utc).isoformat()
                updated = True

        if updated:
            self._save_registry_local(registry)
            logger.info("Estado del lote con hash %s actualizado a: %s en control local", file_hash[:12], new_status)
        return updated

    def mark_silver_batches_as_gold(self) -> None:
        """Avanza a Gold los lotes procesados en Silver."""
        if self.config.is_aws:
            with postgres_cursor(self._get_db_connection()) as cur:
                cur.execute(
                    "UPDATE public.lotes SET status = 'PROCESSED_GOLD', updated_at = CURRENT_TIMESTAMP "
                    "WHERE status = 'PROCESSED_SILVER'"
                )
            return

        registry = self._load_registry_local()
        silver_batches = [
            batch for batch in registry.get("batches", [])
            if batch.get("status") == "PROCESSED_SILVER"
        ]
        if silver_batches:
            updated_at = datetime.now(timezone.utc).isoformat()
            for batch in silver_batches:
                batch.update(status="PROCESSED_GOLD", updated_at=updated_at)
            self._save_registry_local(registry)

    def get_pending_batches(self, source: Optional[str] = None) -> List[BatchMetadata]:
        """
        Retorna la lista de lotes registrados que aún no se procesaron a Silver.

        Args:
            source: Filtro opcional por fuente ('ira', 'malaria', 'senamhi').

        Returns:
            List[BatchMetadata]: Lista de lotes pendientes.
        """
        if self.config.is_aws:
            with postgres_cursor(self._get_db_connection()) as cur:
                cur.execute("""
                    SELECT batch_id, source, file_name, file_hash, file_size_bytes,
                           load_date::text, ingest_timestamp::text, destination_path, status
                    FROM public.lotes
                    WHERE status = 'REGISTERED' AND (%s::text IS NULL OR lower(source) = lower(%s))
                """, (source, source))
                return [BatchMetadata(*row) for row in cur.fetchall()]

        registry = self._load_registry_local()
        results = []
        for b in registry.get("batches", []):
            if b.get("status") == "REGISTERED":
                if source and b.get("source", "").lower() != source.lower():
                    continue
                results.append(BatchMetadata.from_dict(b))
        return results

    def mark_batches_as_silver(self, source: str, input_files: Optional[List[str]] = None) -> None:
        if self.config.is_aws:
            paths = [unquote(path).replace("s3a://", "s3://", 1) for path in input_files or []]
            with postgres_cursor(self._get_db_connection()) as cur:
                cur.execute(
                    "UPDATE public.lotes SET status = 'PROCESSED_SILVER', updated_at = CURRENT_TIMESTAMP "
                    "WHERE status = 'REGISTERED' AND source = %s AND destination_path = ANY(%s::text[])",
                    (source, paths),
                )
            return
        for batch in self.get_pending_batches(source):
            self.update_batch_status(batch.file_hash, "PROCESSED_SILVER")

    def publish_snapshot(self, stage: str) -> str:
        """Publica una lectura consistente del registro de RDS."""
        if not self.config.is_aws or stage not in {"bronze", "silver", "gold", "recovery"}:
            raise ValueError("El snapshot requiere modo AWS y una etapa conocida.")
        run_id = validate_run_id(self.config.run_id)
        with postgres_cursor(self._get_db_connection()) as cur:
            cur.execute("SELECT row_to_json(l) FROM public.lotes l ORDER BY ingest_timestamp, batch_id")
            batches = [row[0] for row in cur.fetchall()]
        uri = put_json(self.config, f"control/runs/{run_id}/{stage}/batch_control.json", {
            "run_id": run_id,
            "stage": stage,
            "timestamp_exportacion": datetime.now(timezone.utc).isoformat(),
            "batches": batches,
        })
        logger.info("Snapshot de lotes guardado en %s", uri)
        return uri
