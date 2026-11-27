"""Transactional access to the configured PostgreSQL database."""

from contextlib import contextmanager, closing

from src.common.config import PostgresConfig


def connect_postgres(config: PostgresConfig):
    import psycopg2

    return psycopg2.connect(
        host=config.host,
        port=config.port,
        dbname=config.db,
        user=config.user,
        password=config.password,
        connect_timeout=5,
        **config.tls_options,
    )


@contextmanager
def postgres_cursor(connection):
    with closing(connection) as conn, conn, conn.cursor() as cursor:
        yield cursor
