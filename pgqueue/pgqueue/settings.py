"""Configuração via variáveis de ambiente (com defaults do docker-compose)."""
import os


PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres")
PG_DATABASE = os.getenv("PG_DATABASE", "pgqueue")

# Nome da fila pgmq usada pelo produtor e consumidor.
QUEUE_NAME = os.getenv("PGQUEUE_NAME", "demo_queue")

# Tempo (segundos) que uma mensagem lida fica invisível para outros
# consumidores antes de reaparecer, caso não seja removida (ack).
VISIBILITY_TIMEOUT = int(os.getenv("PGQUEUE_VT", "30"))

# Intervalo (segundos) entre tentativas de leitura quando a fila está vazia.
POLL_INTERVAL = float(os.getenv("PGQUEUE_POLL_INTERVAL", "1.0"))


def conninfo() -> str:
    """String de conexão libpq usada pelo psycopg."""
    return (
        f"host={PG_HOST} port={PG_PORT} user={PG_USER} "
        f"password={PG_PASSWORD} dbname={PG_DATABASE}"
    )
