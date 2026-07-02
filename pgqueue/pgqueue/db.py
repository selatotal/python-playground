"""Conexão com o Postgres e garantia (idempotente) de que a fila existe."""
import logging

import psycopg

from . import settings

logger = logging.getLogger("pgqueue.db")


def connect() -> psycopg.Connection:
    """Abre uma conexão em autocommit (cada statement é confirmado na hora)."""
    return psycopg.connect(settings.conninfo(), autocommit=True)


def ensure_queue(conn: psycopg.Connection, queue_name: str) -> None:
    """Garante extensão + fila, criando a fila apenas se ainda não existir.

    Complementa o script de init do Docker: útil quando o banco já existia
    (data dir persistido) ou a fila foi removida.
    """
    conn.execute("CREATE EXTENSION IF NOT EXISTS pgmq CASCADE;")
    exists = conn.execute(
        "SELECT 1 FROM pgmq.list_queues() WHERE queue_name = %s",
        (queue_name,),
    ).fetchone()
    if exists is None:
        conn.execute("SELECT pgmq.create(%s)", (queue_name,))
        logger.info("Fila '%s' criada.", queue_name)
    else:
        logger.info("Fila '%s' já existe.", queue_name)
