"""
Módulo de Estandarización y Validación de Ubigeos Geográficos (RENIEC / INEI).
Especializado en la validación y filtrado del departamento de La Libertad (Código 13).
"""

import re
from typing import Any, Optional, TYPE_CHECKING
from src.common.config import (
    CODIGO_DEPARTAMENTO_LA_LIBERTAD,
    NOMBRE_DEPARTAMENTO_LA_LIBERTAD,
    LONGITUD_UBIGEO,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def clean_ubigeo_str(val: Any) -> Optional[str]:
    """
    Limpia y estandariza un valor de Ubigeo a un String de 6 dígitos con ceros a la izquierda.

    Args:
        val: Valor en formato texto, numérico o flotante (ej: 130101, '130101.0', 130101.0).

    Returns:
        Optional[str]: Ubigeo normalizado de 6 dígitos, o None si el valor es nulo/inválido.
    """
    if val is None:
        return None

    s = str(val).strip()
    if not s or s.lower() in ("nan", "null", "none"):
        return None

    # Eliminar posibles decimales introducidos por lecturas de punto flotante
    s = re.sub(r"\.0+$", "", s)

    # Conservar únicamente caracteres numéricos
    s = re.sub(r"\D", "", s)

    if not s:
        return None

    # Rellenar con ceros a la izquierda hasta 6 dígitos si tiene longitud menor
    if len(s) < LONGITUD_UBIGEO:
        s = s.zfill(LONGITUD_UBIGEO)
    elif len(s) > LONGITUD_UBIGEO:
        # Si tiene más de 6 dígitos (ej. centro poblado de 10 dígitos), conservar los 6 primeros (distrito)
        s = s[:LONGITUD_UBIGEO]

    return s


def is_ubigeo_la_libertad(val: Any) -> bool:
    """
    Verifica si un ubigeo pertenece al departamento de La Libertad (código inicia con '13').

    Args:
        val: Valor del ubigeo.

    Returns:
        bool: True si pertenece a La Libertad, False en caso contrario.
    """
    cleaned = clean_ubigeo_str(val)
    if not cleaned:
        return False
    return cleaned.startswith(CODIGO_DEPARTAMENTO_LA_LIBERTAD)


def with_standardized_ubigeo(
    df: "DataFrame",
    ubigeo_col: str = "Codigo_Ubigeo",
    output_col: str = "Codigo_Ubigeo",
) -> "DataFrame":
    """
    Transformación PySpark para estandarizar la columna de Ubigeo a String(6) con lpad.
    """
    from pyspark.sql.functions import col, lpad, regexp_replace, trim

    return df.withColumn(
        output_col,
        lpad(
            regexp_replace(
                trim(col(ubigeo_col).cast("string")),
                r"\.0+$",
                "",
            ),
            LONGITUD_UBIGEO,
            "0",
        ),
    )


def filter_la_libertad(
    df: "DataFrame",
    ubigeo_col: str = "Codigo_Ubigeo",
    dpto_col: Optional[str] = None,
) -> "DataFrame":
    """
    Filtra los registros correspondientes al departamento de La Libertad en PySpark.
    """
    from pyspark.sql.functions import col, upper, trim

    condicion = col(ubigeo_col).startswith(CODIGO_DEPARTAMENTO_LA_LIBERTAD)

    if dpto_col and dpto_col in df.columns:
        cond_dpto = upper(trim(col(dpto_col))) == NOMBRE_DEPARTAMENTO_LA_LIBERTAD
        return df.filter(condicion | cond_dpto)

    return df.filter(condicion)
