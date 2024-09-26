CREATE TABLE IF NOT EXISTS items (
    id TEXT PRIMARY KEY,  -- Because we need to save leading zeros
    name TEXT NOT NULL,
    description TEXT
)