"""Integración en una instancia PostgreSQL local y descartable."""
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

psycopg2 = pytest.importorskip("psycopg2")

from src.common.config import PostgresConfig
from src.common.postgres import connect_postgres, postgres_cursor
from src.serving.publicar_malaria import publicar_malaria

MIGRACION = Path(__file__).resolve().parents[1] / "src/serving/migrations/001_vigilancia_malaria.sql"
SHA = "b" * 64


@pytest.fixture
def pg():
    puerto = os.getenv("MALARIA_TEST_PG_PORT")
    if not puerto:
        pytest.skip("Requiere PostgreSQL descartable en loopback.")
    cfg = PostgresConfig(
        host="127.0.0.1", port=int(puerto), db="postgres", user="malaria_test",
        password=os.environ["MALARIA_TEST_PG_PASSWORD"], sslmode="disable", sslrootcert=None,
    )
    with postgres_cursor(connect_postgres(cfg)) as cur:
        cur.execute("SELECT to_regclass('public.malaria_publicaciones')")
        assert cur.fetchone() == (None,), "La instancia de prueba debe estar vacía."
        cur.execute("CREATE TABLE public.fact_vigilancia_clima (valor integer)")
        cur.execute("INSERT INTO public.fact_vigilancia_clima VALUES (470)")
        sql = MIGRACION.read_text(encoding="utf-8")
        cur.execute(sql)
        cur.execute(sql)
    return cfg


def test_publicacion_reintento_correccion_fallo_y_rollback(pg):
    f1 = ("130301", 2018, 19, "B51", 201819, "LA LIBERTAD", "BOLIVAR", "BOLIVAR", 2)
    f2 = ("130306", 2018, 24, "B50", 201824, "LA LIBERTAD", "BOLIVAR", "UCUNCHA", 1)

    def publicar(run, filas, activa=None, total=None):
        return publicar_malaria(
            pg, run_id=run, sha256=SHA, uri_gold=f"mem://gold/{run}", filas=filas,
            activa_esperada=activa, registros_esperados=total or sum(f[-1] for f in filas),
        )

    assert publicar("primera", [f1, f2])
    assert not publicar("primera", [f2, f1])
    with pytest.raises(ValueError, match="contenido o linaje"):
        publicar("primera", [(*f1[:-1], 1), (*f2[:-1], 2)])
    with pytest.raises(ValueError, match="Total Gold"):
        publicar("total_invalido", [f1], "primera", total=3)
    with pytest.raises(psycopg2.errors.UniqueViolation):
        publicar("duplicada", [f1, f1], "primera")
    with pytest.raises(psycopg2.errors.UniqueViolation):
        publicar("conflicto_dim", [f1, (*f1[:3], "B50", *f1[4:7], "OTRO", 1)], "primera")
    with pytest.raises(ValueError, match="activa no coincide"):
        publicar("fuera_de_orden", [f1], "otra")
    assert publicar("corregida", [(*f1[:-1], 5)], "primera")
    assert not publicar("primera", [f1, f2])
    with postgres_cursor(connect_postgres(pg)) as cur:
        cur.execute("SELECT run_id, registros_fuente FROM public.vw_vigilancia_malaria")
        assert cur.fetchall() == [("corregida", 5)]
        cur.execute("SELECT public.restaurar_publicacion_malaria('corregida', 'primera')")
        cur.execute("SELECT sum(registros_fuente) FROM public.vw_vigilancia_malaria")
        assert cur.fetchone() == (3,)
        cur.execute("SELECT array_agg(run_id ORDER BY run_id) FROM public.malaria_publicaciones")
        assert cur.fetchone() == (["corregida", "primera"],)
        cur.execute("SELECT valor FROM public.fact_vigilancia_clima")
        assert cur.fetchone() == (470,)
        cur.execute("SELECT count(*) FROM public.malaria_cambios_publicacion")
        assert cur.fetchone() == (3,)

    with pytest.raises(psycopg2.errors.RaiseException, match="activa cambió"):
        with postgres_cursor(connect_postgres(pg)) as cur:
            cur.execute("SELECT public.restaurar_publicacion_malaria('corregida', NULL)")

    def competir(run):
        try:
            return publicar(run, [f1], "primera")
        except ValueError as exc:
            assert "activa no coincide" in str(exc)
            return False

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(competir, ("concurrente_a", "concurrente_b"))) == [False, True]
    with postgres_cursor(connect_postgres(pg)) as cur:
        cur.execute("SELECT count(*) FROM public.malaria_publicaciones")
        assert cur.fetchone() == (3,)
