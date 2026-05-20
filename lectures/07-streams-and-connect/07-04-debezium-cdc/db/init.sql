-- Две таблицы: одна — обычная users (классический CDC: видим INSERT/UPDATE/DELETE
-- как операции op=c/u/d), вторая — outbox (CDC + event router SMT превращает
-- каждую строку в доменное событие в отдельный топик).
--
-- REPLICA IDENTITY FULL нужно для UPDATE/DELETE, чтобы в WAL писалась полная
-- старая версия строки (поле "before" в Debezium event'е). Без этого Debezium
-- увидит только PK в before, а остальные поля будут null — для лекции про CDC
-- это слишком урезано.

CREATE TABLE IF NOT EXISTS users (
    id         BIGINT       PRIMARY KEY,
    email      TEXT         NOT NULL,
    full_name  TEXT         NOT NULL,
    status     TEXT         NOT NULL DEFAULT 'active',
    updated_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

ALTER TABLE users REPLICA IDENTITY FULL;

-- Outbox — таблица, в которую сервис в одной транзакции с бизнес-данными
-- кладёт описание доменного события. Debezium читает её WAL и через SMT
-- EventRouter раскладывает строки по топикам по полю aggregate_type.
-- Структура колонок согласована с дефолтами io.debezium.transforms.outbox.EventRouter.
CREATE TABLE IF NOT EXISTS outbox (
    id              UUID         PRIMARY KEY,
    aggregate_type  TEXT         NOT NULL,   -- → имя топика (route.topic.replacement)
    aggregate_id    TEXT         NOT NULL,   -- → ключ сообщения
    type            TEXT         NOT NULL,   -- → header eventType
    payload         JSONB        NOT NULL,   -- → value сообщения
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

ALTER TABLE outbox REPLICA IDENTITY FULL;

-- Publication — это объект Postgres'а, через который logical replication
-- решает, какие изменения отдавать подписчикам. Debezium в connector-конфиге
-- выставляет publication.name=dbz_publication и publication.autocreate.mode=
-- disabled. С disabled Debezium ничего сам не создаёт и требует, чтобы
-- publication уже существовала — поэтому создаём её руками тут. Это и
-- контроль, и наглядность: видно, какие таблицы реально стримятся.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'dbz_publication') THEN
        CREATE PUBLICATION dbz_publication FOR TABLE users, outbox;
    END IF;
END $$;
