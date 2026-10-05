package entities

// Warehouse declares the systems YDB reads from: a PostgreSQL database whose
// password the secret ext/pg_password holds, and a bucket of event files.
//
//ptah:schema:externaldatasource name="warehouse" schema="ext" source_type="PostgreSQL" location="pg.example.test:5432" auth_method="BASIC" options="DATABASE_NAME=app;LOGIN=reader;PASSWORD_SECRET_PATH=ext/pg_password"
//ptah:schema:externaldatasource name="events_bucket" schema="ext" source_type="ObjectStorage" location="https://storage.example.test/events/" auth_method="NONE"
type Warehouse struct{}

// Event is a row of the newline-delimited JSON files in the bucket.
//
//ptah:schema:externaltable name="events" schema="ext" data_source="ext/events_bucket" location="2026/" columns="id Int64 NOT NULL, kind Utf8, amount Decimal(22,9)" options="FORMAT=json_each_row;COMPRESSION=gzip"
type Event struct{}
