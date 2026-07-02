-- Executado automaticamente pelo entrypoint do Postgres na primeira subida
-- do container (quando o diretório de dados está vazio).

-- Habilita a extensão pgmq (Postgres Message Queue).
CREATE EXTENSION IF NOT EXISTS pgmq CASCADE;

-- Cria a fila de demonstração. pgmq.create() gera as tabelas de apoio no
-- schema pgmq: pgmq.q_demo_queue (mensagens ativas) e pgmq.a_demo_queue
-- (arquivo). É seguro rodar em um banco recém-criado.
SELECT pgmq.create('demo_queue');
