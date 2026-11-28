"""Publicación transaccional de snapshots Gold de malaria."""
from __future__ import annotations

from collections.abc import Iterable
import re

from psycopg2.extras import execute_values

from src.common.postgres import connect_postgres, postgres_cursor
from src.common.s3_artifacts import validate_run_id


def publicar_malaria(
    config, *, run_id: str, sha256: str, uri_gold: str, filas: Iterable[tuple],
    activa_esperada: str | None, registros_esperados: int,
) -> bool:
    """Publica una revisión completa. Un reintento idéntico no reactiva revisiones."""
    validate_run_id(run_id)
    if activa_esperada is not None:
        validate_run_id(activa_esperada)
    if not re.fullmatch(r"[a-f0-9]{64}", sha256) or registros_esperados <= 0:
        raise ValueError("Se requiere SHA-256 y un total esperado positivo.")

    with postgres_cursor(connect_postgres(config)) as cur:
        cur.execute("""
            CREATE TEMP TABLE malaria_staging (
                ubigeo varchar(6) NOT NULL CHECK (ubigeo ~ '^13[0-9]{4}$'),
                ano integer NOT NULL CHECK (ano BETWEEN 2015 AND 2024),
                semana integer NOT NULL CHECK (semana BETWEEN 1 AND 53),
                especie varchar(3) NOT NULL CHECK (especie IN ('B50', 'B51')),
                tiempo_id integer NOT NULL CHECK (tiempo_id = ano * 100 + semana),
                departamento text, provincia text, distrito text,
                registros bigint NOT NULL CHECK (registros > 0),
                PRIMARY KEY (ubigeo, tiempo_id, especie)
            ) ON COMMIT DROP
        """)
        execute_values(cur, "INSERT INTO pg_temp.malaria_staging VALUES %s", filas, page_size=1000)
        cur.execute("SELECT COALESCE(sum(registros), 0) FROM pg_temp.malaria_staging")
        total = cur.fetchone()[0]
        if total != registros_esperados:
            raise ValueError(f"Total Gold inesperado: {total}; esperado: {registros_esperados}.")
        cur.execute("SELECT run_id FROM public.malaria_publicacion_activa WHERE unica FOR UPDATE")
        vigente = cur.fetchone()[0]
        cur.execute("""
            SELECT sha256_bronze, uri_gold, total_registros
            FROM public.malaria_publicaciones WHERE run_id = %s
        """, (run_id,))
        previa = cur.fetchone()
        if previa is not None:
            cur.execute("""
                WITH anterior AS (
                    SELECT f.ubigeo, t.ano, t.semana_epidemiologica, f.especie_codigo,
                           f.tiempo_id, g.departamento, g.provincia, g.distrito, f.registros_fuente
                    FROM public.fact_vigilancia_malaria f
                    JOIN public.dim_ubigeo_malaria g USING (run_id, ubigeo)
                    JOIN public.dim_tiempo_malaria t USING (run_id, tiempo_id)
                    WHERE f.run_id = %s
                )
                SELECT EXISTS (
                    (SELECT * FROM pg_temp.malaria_staging EXCEPT SELECT * FROM anterior)
                    UNION ALL
                    (SELECT * FROM anterior EXCEPT SELECT * FROM pg_temp.malaria_staging)
                )
            """, (run_id,))
            diferente, = cur.fetchone()
            if diferente or previa != (sha256, uri_gold, total):
                raise ValueError("El run_id ya existe con contenido o linaje distinto.")
            return False
        if vigente != activa_esperada:
            raise ValueError("La publicación activa no coincide con la esperada.")
        cur.execute("""
            INSERT INTO public.malaria_publicaciones
                (run_id, sha256_bronze, uri_gold, total_registros)
            VALUES (%s, %s, %s, %s)
        """, (run_id, sha256, uri_gold, total))
        cur.execute("""
            INSERT INTO public.dim_ubigeo_malaria
            SELECT DISTINCT %s, ubigeo, departamento, provincia, distrito
            FROM pg_temp.malaria_staging
        """, (run_id,))
        cur.execute("""
            INSERT INTO public.dim_tiempo_malaria
            SELECT DISTINCT %s, tiempo_id, ano, semana FROM pg_temp.malaria_staging
        """, (run_id,))
        cur.execute("""
            INSERT INTO public.fact_vigilancia_malaria
            SELECT %s, ubigeo, tiempo_id, especie, registros FROM pg_temp.malaria_staging
        """, (run_id,))
        cur.execute("UPDATE public.malaria_publicacion_activa SET run_id = %s WHERE unica", (run_id,))
        cur.execute("""
            INSERT INTO public.malaria_cambios_publicacion (anterior, siguiente, operacion)
            VALUES (%s, %s, 'publicar')
        """, (vigente, run_id))
        return True
