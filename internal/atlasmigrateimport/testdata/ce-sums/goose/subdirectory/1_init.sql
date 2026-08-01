-- +goose Up
CREATE TABLE widgets (id INTEGER PRIMARY KEY);
-- +goose Down
DROP TABLE widgets;
