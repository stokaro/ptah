-- A curated pile of production hazards; -- semicolons in comments must not split statements.
DROP TABLE audit_log;

ALTER TABLE users DROP COLUMN legacy_flags;

ALTER TABLE users RENAME COLUMN email TO email_address;

ALTER TABLE users MODIFY COLUMN email VARCHAR(64) NOT NULL;

CREATE INDEX idx_users_email ON users (email);

ALTER TYPE mood ADD VALUE 'ambivalent';
