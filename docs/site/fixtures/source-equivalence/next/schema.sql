CREATE TABLE authors (
    id INTEGER NOT NULL PRIMARY KEY,
    name VARCHAR(120) NOT NULL
);

CREATE UNIQUE INDEX idx_authors_name ON authors (name);

CREATE TABLE books (
    id INTEGER NOT NULL PRIMARY KEY,
    author_id INTEGER NOT NULL REFERENCES authors(id),
    title VARCHAR(240) NOT NULL,
    summary TEXT,
    published_at TEXT
);

CREATE TABLE tags (
    id INTEGER NOT NULL PRIMARY KEY,
    name VARCHAR(80) NOT NULL
);

CREATE TABLE book_tags (
    id INTEGER NOT NULL PRIMARY KEY,
    book_id INTEGER NOT NULL REFERENCES books(id),
    tag_id INTEGER NOT NULL REFERENCES tags(id)
);

CREATE UNIQUE INDEX idx_book_tags_pair ON book_tags (book_id, tag_id);
