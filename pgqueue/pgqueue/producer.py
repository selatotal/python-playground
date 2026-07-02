"""Produtor: enfileira uma mensagem JSON na fila pgmq (para testar o consumidor)."""
import argparse
import json
import logging

from psycopg.types.json import Jsonb

from . import settings
from .db import connect, ensure_queue
from .logging_config import configure_logging

logger = logging.getLogger("pgqueue.producer")


def send(payload: dict) -> int:
    with connect() as conn:
        ensure_queue(conn, settings.QUEUE_NAME)
        # Jsonb adapta o dict para o parâmetro jsonb esperado por pgmq.send.
        row = conn.execute(
            "SELECT pgmq.send(%s, %s)",
            (settings.QUEUE_NAME, Jsonb(payload)),
        ).fetchone()
        msg_id = row[0]
        logger.info("Mensagem enfileirada msg_id=%s payload=%s", msg_id, payload)
        return msg_id


def main() -> None:
    configure_logging()
    parser = argparse.ArgumentParser(
        description="Enfileira uma mensagem JSON na fila pgmq."
    )
    parser.add_argument(
        "payload",
        nargs="?",
        help='Payload JSON, ex.: \'{"evento": "teste"}\'. '
        "Se omitido, envia uma mensagem de exemplo.",
    )
    args = parser.parse_args()

    if args.payload:
        payload = json.loads(args.payload)
    else:
        payload = {"evento": "exemplo", "mensagem": "olá pgmq", "n": 1}

    send(payload)


if __name__ == "__main__":
    main()
