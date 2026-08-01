-- migrate:up
CREATE TABLE widgets (id INTEGER PRIMARY KEY);
-- migrate:down
DROP TABLE widgets;
