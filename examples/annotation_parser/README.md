# Annotation parser example

## What this example demonstrates

This program reads one directory of annotated Go structs, resolves embedded
types, and renders the resulting schema for PostgreSQL, MySQL, and MariaDB. It
shows table, field, index, enum, and embedded-field annotations without needing
a database.

## Prerequisites

- The Go toolchain declared in the repository's `go.mod`.
- A Ptah checkout with its module dependencies available.

## Run

From the repository root:

```bash
go run ./examples/annotation_parser ./examples/annotation_parser/models/example_entities.go
```

## Expected result

The command reports four tables and prints a section for each dialect. Stable
output includes:

```text
Found 4 tables
=== POSTGRES ===
CREATE TABLE "users"
=== MYSQL ===
=== MARIADB ===
```

## Verify

The repository example gate runs the command and checks those stable fragments.
For the embedded-field inventory as well, run:

```bash
SHOW_DETAILS=1 go run ./examples/annotation_parser ./examples/annotation_parser/models/embedded_example.go
```

## Cleanup

The example writes only to stdout and creates no database or generated file.

## Learn more

Use the [Go annotations guide](https://stokaro.github.io/ptah/edge/schema/go-annotations/)
for the authoring workflow and the
[annotation reference](https://stokaro.github.io/ptah/edge/reference/go-annotations/)
for every accepted directive and attribute.
