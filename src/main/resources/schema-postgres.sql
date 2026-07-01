CREATE TABLE IF NOT EXISTS queues (
    name        TEXT PRIMARY KEY,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS messages (
    id              BIGSERIAL PRIMARY KEY,
    queue_name      TEXT NOT NULL REFERENCES queues(name),
    body            BYTEA NOT NULL,
    properties      JSONB,
    application_properties JSONB,
    state           TEXT NOT NULL DEFAULT 'ready',
    locked_by       TEXT,
    locked_at       TIMESTAMPTZ,
    delivery_count  INT NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_messages_ready
    ON messages (queue_name, id)
    WHERE state = 'ready';

CREATE INDEX IF NOT EXISTS idx_messages_locked_stale
    ON messages (locked_at)
    WHERE state = 'locked';

CREATE TABLE IF NOT EXISTS subscriptions (
    queue_name  TEXT PRIMARY KEY REFERENCES queues(name),
    topic_name  TEXT NOT NULL,
    durable     BOOLEAN NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_subscriptions_topic
    ON subscriptions (topic_name);
