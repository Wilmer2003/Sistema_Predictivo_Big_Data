"""
Módulo de Utilidades para el Cálculo y Validación de Semanas Epidemiológicas.
Alineado al estándar de vigilancia epidemiológica MINSA / CDC Perú e ISO-8601.
"""

from datetime import date, datetime
from typing import Union, Tuple, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def is_valid_semana_epidemiologica(semana: Optional[int]) -> bool:
    """
    Valida si un número de semana epidemiológica está en el rango válido [1, 53].

    Args:
        semana: Número de semana epidemiológica.

    Returns:
        bool: True si está entre 1 y 53, False en caso contrario.
    """
    if semana is None:
        return False
    try:
        val = int(semana)
        return 1 <= val <= 53
    except (ValueError, TypeError):
        return False


def calculate_epi_week(dt_value: Union[str, date, datetime]) -> int:
    """
    Calcula el número de semana epidemiológica (1-53) para una fecha dada (ISO-8601 / MINSA).

    Args:
        dt_value: Objeto date, datetime o cadena en formato 'YYYY-MM-DD'.

    Returns:
        int: Semana epidemiológica (1 a 53).
    """
    if isinstance(dt_value, str):
        parsed_dt = datetime.strptime(dt_value.strip()[:10], "%Y-%m-%d").date()
    elif isinstance(dt_value, datetime):
        parsed_dt = dt_value.date()
    elif isinstance(dt_value, date):
        parsed_dt = dt_value
    else:
        raise TypeError(f"Tipo no soportado para fecha: {type(dt_value)}")

    return parsed_dt.isocalendar()[1]


def calculate_epi_year_and_week(dt_value: Union[str, date, datetime]) -> Tuple[int, int]:
    """
    Calcula el año y semana epidemiológica correspondientes.

    Args:
        dt_value: Objeto date, datetime o cadena 'YYYY-MM-DD'.

    Returns:
        Tuple[int, int]: (año_epidemiológico, semana_epidemiológica).
    """
    if isinstance(dt_value, str):
        parsed_dt = datetime.strptime(dt_value.strip()[:10], "%Y-%m-%d").date()
    elif isinstance(dt_value, datetime):
        parsed_dt = dt_value.date()
    elif isinstance(dt_value, date):
        parsed_dt = dt_value
    else:
        raise TypeError(f"Tipo no soportado para fecha: {type(dt_value)}")

    iso_year, iso_week, _ = parsed_dt.isocalendar()
    return iso_year, iso_week


def with_fecha_from_ymd(
    df: "DataFrame",
    anio_col: str = "Ano",
    mes_col: str = "Mes",
    dia_col: str = "Dia",
    output_col: str = "Fecha",
) -> "DataFrame":
    """
    Construye una columna DateType a partir de columnas separadas de Año, Mes y Día en PySpark.
    """
    from pyspark.sql.functions import to_date, concat_ws, lpad, col

    return df.withColumn(
        output_col,
        to_date(
            concat_ws(
                "-",
                col(anio_col).cast("string"),
                lpad(col(mes_col).cast("string"), 2, "0"),
                lpad(col(dia_col).cast("string"), 2, "0"),
            ),
            "yyyy-MM-dd",
        ),
    )


def with_semana_epidemiologica(
    df: "DataFrame",
    fecha_col: str = "Fecha",
    output_col: str = "Semana_Epidemiologica",
) -> "DataFrame":
    """
    Agrega o actualiza la columna de semana epidemiológica basada en la fecha en PySpark.
    """
    from pyspark.sql.functions import col, weekofyear
    from pyspark.sql.types import IntegerType

    return df.withColumn(
        output_col,
        weekofyear(col(fecha_col)).cast(IntegerType()),
    )


def with_anio_epidemiologico(
    df: "DataFrame",
    fecha_col: str = "Fecha",
    output_col: str = "Ano_Epidemiologico",
) -> "DataFrame":
    """
    Calcula el año epidemiológico (ajustando los días limítrofes entre dic y ene según ISO) en PySpark.
    """
    from pyspark.sql.functions import col, weekofyear, month, year, when
    from pyspark.sql.types import IntegerType

    wk = weekofyear(col(fecha_col))
    mo = month(col(fecha_col))
    yr = year(col(fecha_col))

    return df.withColumn(
        output_col,
        when((wk >= 52) & (mo == 1), yr - 1)
        .when((wk == 1) & (mo == 12), yr + 1)
        .otherwise(yr)
        .cast(IntegerType()),
    )
