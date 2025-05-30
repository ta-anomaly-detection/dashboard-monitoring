CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    password TEXT NOT NULL,
    name TEXT NOT NULL,
    token TEXT,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
);