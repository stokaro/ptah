-- The earlier state of the diff ERD fixture. Paired with erd-to.sql so one
-- comparison exercises every mark the diagram can draw: authors is untouched,
-- books changes, legacy_notes leaves and reviews arrives. The foreign keys are
-- there so the picture has edges to lay out, which is what makes a dependency
-- diagram worth looking at.
CREATE TABLE authors (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL
);

CREATE TABLE books (
  id INTEGER PRIMARY KEY,
  author_id INTEGER NOT NULL REFERENCES authors(id)
);

CREATE TABLE legacy_notes (
  id INTEGER PRIMARY KEY,
  body TEXT
);
