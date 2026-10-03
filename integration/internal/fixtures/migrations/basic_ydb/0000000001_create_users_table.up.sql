-- Create users table. YQL takes the key as a table-level clause and an index
-- inside CREATE TABLE; a unique index can be added only while the table is
-- created.
CREATE TABLE users (
    id Serial NOT NULL,
    email Utf8 NOT NULL,
    name Utf8 NOT NULL,
    created_at Timestamp,
    updated_at Timestamp,
    PRIMARY KEY (id),
    INDEX idx_users_email GLOBAL UNIQUE SYNC ON (email)
);

-- A data query, which commits with its checkpoint in one transaction. A dry
-- run executes it no more than it creates the table above.
INSERT INTO users (email, name) VALUES ('admin@example.com'u, 'Admin'u);
