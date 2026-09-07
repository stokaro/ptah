-- The desired state of the diff ERD fixture; see erd-from.sql for why the pair
-- is shaped this way. books.title carries a default because SQLite rebuilds a
-- table to add a column, and the copy would violate a bare NOT NULL.
CREATE TABLE authors (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL
);

CREATE TABLE books (
  id INTEGER PRIMARY KEY,
  author_id INTEGER NOT NULL REFERENCES authors(id),
  title TEXT NOT NULL DEFAULT ''
);

CREATE TABLE reviews (
  id INTEGER PRIMARY KEY,
  book_id INTEGER NOT NULL REFERENCES books(id),
  rating INTEGER NOT NULL
);
