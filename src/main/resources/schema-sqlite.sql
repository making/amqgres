CREATE TABLE IF NOT EXISTS queues (
    name        TEXT PRIMARY KEY,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS messages (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_name      TEXT NOT NULL REFERENCES queues(name),
    body            BLOB NOT NULL,
    properties      TEXT,
    application_properties TEXT,
    state           TEXT NOT NULL DEFAULT 'ready',
    locked_by       TEXT,
    locked_at       TEXT,
    delivery_count  INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
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
    durable     INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_subscriptions_topic
    ON subscriptions (topic_name);
