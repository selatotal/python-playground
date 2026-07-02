# pgqueue — estudo do pgmq (Postgres Message Queue)

Projeto simples para entender o funcionamento do [pgmq](https://pgmq.github.io/pgmq/), uma extensão que habilita filas de mensagens no PostgreSQL.

## Requisitos

- Docker
- [uv](https://docs.astral.sh/uv/) e Python 3.13

## Como testar

```bash
# 1. Sobe o Postgres + pgmq. Na primeira subida, o script em db/init/
#    cria a extensão pgmq e a fila 'demo_queue' automaticamente.
docker-compose up -d

# 2. Instala as dependências Python
uv sync

# 3. Em um terminal, inicia o consumidor (fica em loop)
uv run python -m pgqueue.consumer

# 4. Em outro terminal, enfileira uma mensagem
uv run python -m pgqueue.producer '{"evento": "pedido_criado", "id": 42}'
```

O consumidor imprime algo como:

```
2026-07-02 17:25:35 | INFO | pgqueue.consumer | Mensagem recebida msg_id=1 read_ct=1 ...
2026-07-02 17:25:35 | INFO | pgqueue.consumer | Payload JSON:
{
  "evento": "pedido_criado",
  "id": 42
}
2026-07-02 17:25:35 | INFO | pgqueue.consumer | Mensagem msg_id=1 processada e removida da fila.
```

Sem argumento, o produtor envia uma mensagem de exemplo.

## Como funciona

- **Criação automática das tabelas**: `db/init/01_init_pgmq.sql` roda pelo entrypoint do Postgres na primeira subida do container e executa `CREATE EXTENSION IF NOT EXISTS pgmq pgmq.create('demo_queue')`. O código Python (`ensure_queue`) também garante a fila caso ela não exista.
- **Consumo**: o consumidor usa `pgmq.read` (com *visibility timeout*), imprime  o payload e faz o *ack* com `pgmq.delete`. Se o consumidor cair antes do ack, a mensagem reaparece após o timeout — entrega *at-least-once*.

## Configuração

Variáveis de ambiente (veja `.env.example`); os defaults já batem com o `docker-compose.yml`. Principais: `PGQUEUE_NAME`, `PGQUEUE_VT` (visibility timeout, s), `PGQUEUE_POLL_INTERVAL` (intervalo de polling quando a fila está vazia).

## Parar

```bash
docker-compose down        # para o container (mantém os dados)
docker-compose down -v     # para e apaga o volume de dados
```
