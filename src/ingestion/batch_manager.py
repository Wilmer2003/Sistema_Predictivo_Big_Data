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
    Soporta almacenamiento en archivo local JSON o en tabla 'lotes' de Amazon RDS PostgreSQL.
    """

    def __init__(self, config: Optional[AppConfig] = None):
        self.config = config or get_config()
        self.control_file_path = self.config.batch_control_path
        self._ensure_control_storage()

    def _ensure_control_storage(self) -> None:
        """Asegura que el directorio y archivo de control existan si es local, o la tabla si es AWS."""
        if not self.config.is_aws:
            p = Path(self.control_file_path)
            p.parent.mkdir(parents=True, exist_ok=True)
            if not p.exists():
                with open(p, "w", encoding="utf-8") as f:
                    json.dump({"batches": []}, f, indent=2)
        else:
            self._ensure_rds_table()

    def _get_db_connection(self):
        """Retorna una conexión a Amazon RDS / PostgreSQL."""
        import psycopg2
        pg = self.config.postgres
        return psycopg2.connect(
            host=pg.host,
            port=pg.port,
            dbname=pg.db,
            user=pg.user,
            password=pg.password,
            connect_timeout=3,
        )

    def _ensure_rds_table(self) -> None:
        """Crea la tabla 'lotes' en Amazon RDS si aún no existe."""
        try:
            conn = self._get_db_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS lotes (
                        batch_id VARCHAR(100) PRIMARY KEY,
                        source VARCHAR(50) NOT NULL,
                        file_name VARCHAR(255) NOT NULL,
                        file_hash VARCHAR(64) NOT NULL UNIQUE,
                        file_size_bytes BIGINT NOT NULL,
                        load_date DATE NOT NULL,
                        ingest_timestamp TIMESTAMP NOT NULL,
                        destination_path TEXT NOT NULL,
                        status VARCHAR(50) NOT NULL,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                    CREATE INDEX IF NOT EXISTS idx_lotes_file_hash ON lotes (file_hash);
                    CREATE INDEX IF NOT EXISTS idx_lotes_source_status ON lotes (source, status);
                """)
            conn.commit()
            conn.close()
        except Exception as e:
            logger.warning("No se pudo verificar/crear la tabla 'lotes' en Amazon RDS: %s", e)

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
        """
        Verifica si un archivo con el hash SHA-256 especificado ya está registrado.

        Args:
            file_hash: Hash SHA-256 a validar.
            source: Nombre opcional de la fuente ('ira', 'malaria', 'senamhi').

        Returns:
            bool: True si el hash ya existe y no falló, False si es un lote nuevo.
        """
        if self.config.is_aws:
            try:
                conn = self._get_db_connection()
                with conn.cursor() as cur:
                    if source:
                        cur.execute(
                            "SELECT status FROM lotes WHERE file_hash = %s AND LOWER(source) = LOWER(%s) AND status != 'FAILED'",
                            (file_hash, source),
                        )
                    else:
                        cur.execute(
                            "SELECT status FROM lotes WHERE file_hash = %s AND status != 'FAILED'",
                            (file_hash,),
                        )
                    row = cur.fetchone()
                conn.close()
                return row is not None
            except Exception as e:
                logger.warning("Fallo al consultar tabla 'lotes' en Amazon RDS (%s). Verificando registro local de respaldo.", e)
                # Fallback al registro local si la base de datos no está disponible
                return self._is_batch_processed_local(file_hash, source)

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

    def register_batch(self, metadata: BatchMetadata) -> None:
        """
        Registra un nuevo lote en el catálogo de control (Amazon RDS o archivo local).

        Args:
            metadata: Objeto BatchMetadata a persistir.
        """
        if self.config.is_aws:
            try:
                conn = self._get_db_connection()
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO lotes (
                            batch_id, source, file_name, file_hash, file_size_bytes,
                            load_date, ingest_timestamp, destination_path, status, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                        ON CONFLICT (batch_id) DO UPDATE SET
                            status = EXCLUDED.status,
                            updated_at = CURRENT_TIMESTAMP
                    """, (
                        metadata.batch_id,
                        metadata.source,
                        metadata.file_name,
                        metadata.file_hash,
                        metadata.file_size_bytes,
                        metadata.load_date,
                        metadata.ingest_timestamp,
                        metadata.destination_path,
                        metadata.status,
                    ))
                conn.commit()
                conn.close()
                logger.info("Lote registrado en Amazon RDS (tabla lotes): [%s]", metadata.batch_id)
                return
            except Exception as e:
                logger.warning("Fallo al registrar lote en Amazon RDS: %s. Respaldando en local.", e)

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
            try:
                conn = self._get_db_connection()
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE lotes SET status = %s, updated_at = CURRENT_TIMESTAMP WHERE file_hash = %s",
                        (new_status, file_hash),
                    )
                    updated = cur.rowcount > 0
                conn.commit()
                conn.close()
                if updated:
                    logger.info("Estado del lote %s actualizado a '%s' en Amazon RDS.", file_hash[:12], new_status)
                    return True
            except Exception as e:
                logger.warning("Fallo al actualizar lote en Amazon RDS: %s. Actualizando registro local.", e)

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

    def get_pending_batches(self, source: Optional[str] = None) -> List[BatchMetadata]:
        """
        Retorna la lista de lotes registrados que aún no se procesaron a Silver.

        Args:
            source: Filtro opcional por fuente ('ira', 'malaria', 'senamhi').

        Returns:
            List[BatchMetadata]: Lista de lotes pendientes.
        """
        if self.config.is_aws:
            try:
                conn = self._get_db_connection()
                with conn.cursor() as cur:
                    if source:
                        cur.execute("""
                            SELECT batch_id, source, file_name, file_hash, file_size_bytes,
                                   load_date::text, ingest_timestamp::text, destination_path, status
                            FROM lotes
                            WHERE status = 'REGISTERED' AND LOWER(source) = LOWER(%s)
                        """, (source,))
                    else:
                        cur.execute("""
                            SELECT batch_id, source, file_name, file_hash, file_size_bytes,
                                   load_date::text, ingest_timestamp::text, destination_path, status
                            FROM lotes
                            WHERE status = 'REGISTERED'
                        """)
                    rows = cur.fetchall()
                conn.close()
                return [
                    BatchMetadata(
                        batch_id=r[0],
                        source=r[1],
                        file_name=r[2],
                        file_hash=r[3],
                        file_size_bytes=int(r[4]),
                        load_date=str(r[5]),
                        ingest_timestamp=str(r[6]),
                        destination_path=r[7],
                        status=r[8],
                    )
                    for r in rows
                ]
            except Exception as e:
                logger.warning("Fallo al leer lotes pendientes de Amazon RDS: %s. Leyendo local.", e)

        registry = self._load_registry_local()
        results = []
        for b in registry.get("batches", []):
            if b.get("status") == "REGISTERED":
                if source and b.get("source", "").lower() != source.lower():
                    continue
                results.append(BatchMetadata.from_dict(b))
        return results
