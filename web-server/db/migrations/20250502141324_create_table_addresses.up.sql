CREATE TABLE IF NOT EXISTS addresses (
    id TEXT PRIMARY KEY,
    contact_id TEXT NOT NULL,
    street TEXT,
    city TEXT,
    province TEXT,
    postal_code TEXT,
    country TEXT,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    CONSTRAINT fk_contact FOREIGN KEY(contact_id) REFERENCES contacts(id) ON DELETE CASCADE
);