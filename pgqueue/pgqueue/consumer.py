"""Consumidor da fila pgmq: lê mensagens, imprime o payload JSON e faz o ack."""
import json
import logging
import time

from . import settings
from .db import connect, ensure_queue
from .logging_config import configure_logging

logger = logging.getLogger("pgqueue.consumer")


def consume() -> None:
    with connect() as conn:
        ensure_queue(conn, settings.QUEUE_NAME)
        logger.info(
            "Consumidor iniciado na fila '%s' (vt=%ss, poll=%ss). Ctrl+C para sair.",
            settings.QUEUE_NAME,
            settings.VISIBILITY_TIMEOUT,
            settings.POLL_INTERVAL,
        )

        while True:
            # pgmq.read torna a mensagem invisível por VISIBILITY_TIMEOUT
            # segundos; se o consumidor cair antes do ack, ela reaparece.
            row = conn.execute(
                "SELECT msg_id, read_ct, enqueued_at, message "
                "FROM pgmq.read(%s, %s, %s)",
                (settings.QUEUE_NAME, settings.VISIBILITY_TIMEOUT, 1),
            ).fetchone()

            if row is None:
                time.sleep(settings.POLL_INTERVAL)
                continue

            msg_id, read_ct, enqueued_at, message = row
            logger.info(
                "Mensagem recebida msg_id=%s read_ct=%s enqueued_at=%s",
                msg_id,
                read_ct,
                enqueued_at,
            )
            # `message` já vem como dict (psycopg desserializa jsonb).
            logger.info(
                "Payload JSON:\n%s",
                json.dumps(message, indent=2, ensure_ascii=False),
            )

            # ack: remove a mensagem da fila após o processamento.
            conn.execute("SELECT pgmq.delete(%s, %s)", (settings.QUEUE_NAME, msg_id))
            logger.info("Mensagem msg_id=%s processada e removida da fila.", msg_id)


def main() -> None:
    configure_logging()
    try:
        consume()
    except KeyboardInterrupt:
        logger.info("Consumidor encerrado pelo usuário.")


if __name__ == "__main__":
    main()
