-- Create users table
CREATE TABLE users (
    id UInt64,
    email String,
    name String,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY id;
