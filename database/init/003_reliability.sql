-- ЛР 3: история обработки и идемпотентность потребителей. Применяется после 001–002 и к уже развернутой БД.
SET search_path TO transit;

-- processed_messages – реестр обработанных сообщений: строка вставляется первой операцией транзакции
-- обработки (claim), конфликт ключа (consumer, message_id) означает повторную доставку уже обработанного
-- сообщения. result заполняется последней операцией той же транзакции.
ALTER TABLE processed_messages ADD COLUMN IF NOT EXISTS attempt smallint NOT NULL DEFAULT 1;
COMMENT ON TABLE processed_messages IS 'Обработанные сообщения: идемпотентность потребителей и итог обработки';

CREATE TABLE IF NOT EXISTS processing_log (   -- каждая попытка обработки каждой доставки
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    consumer    varchar(20) NOT NULL,
    instance_id varchar(40) NOT NULL,
    queue       varchar(40) NOT NULL,
    message_id  uuid,                         -- NULL, если идентификатор не удалось извлечь (битое сообщение)
    msg_type    varchar(20),
    attempt     smallint    NOT NULL,         -- 1 – первая доставка, далее – номер повтора + 1
    redelivered boolean     NOT NULL,         -- повторная доставка брокером (не было ack)
    received_at timestamptz NOT NULL,
    finished_at timestamptz NOT NULL,
    outcome     varchar(10) NOT NULL CHECK (outcome IN ('success', 'duplicate', 'retry', 'dlq')),
    result      varchar(32),                  -- итог бизнес-обработки (как в processed_messages)
    error       text                          -- причина ошибки: класс исключения и сообщение
);
CREATE INDEX IF NOT EXISTS processing_log_message ON processing_log (message_id, consumer);
CREATE INDEX IF NOT EXISTS processing_log_outcome ON processing_log (consumer, outcome) WHERE outcome <> 'success';
