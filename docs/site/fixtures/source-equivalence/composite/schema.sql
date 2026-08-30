CREATE TABLE authors (
    id INTEGER NOT NULL PRIMARY KEY,
    name VARCHAR(120) NOT NULL
);

CREATE UNIQUE INDEX idx_authors_name ON authors (name);
