---
title: Go schema workflow
description: Use annotated Go structs as the desired database schema.
---

Use this workflow when your Go application owns the schema and the database should follow the annotated model types.

## Model the schema

Ptah scans Go packages for table annotations and field tags:

```go
package models

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int

	//migrator:schema:field name="email" type="TEXT" unique="true" not_null="true"
	Email string

	//migrator:schema:field name="first_name" type="TEXT"
	FirstName string

	//migrator:schema:field name="last_name" type="TEXT"
	LastName string
}
```

Render the desired SQL before connecting to a database:

```bash
ptah schema render --root-dir ./models --dialect postgres
```

## Compare before changing data

For an existing database, inspect and compare first:

```bash
ptah db read --db-url "$DATABASE_URL"
ptah schema compare --root-dir ./models --db-url "$DATABASE_URL"
ptah migrations plan --root-dir ./models --db-url "$DATABASE_URL"
```

Review the plan output before generating files. Destructive changes should be explicit and gated in CI.

## Generate and apply

```bash
ptah migrations generate \
  --root-dir ./models \
  --db-url "$DATABASE_URL" \
  --migrations-dir ./migrations

ptah migrations hash --dir ./migrations
ptah migrations validate --dir ./migrations
ptah migrations up --db-url "$DATABASE_URL" --migrations-dir ./migrations --verify-sum
```

## Keep references close

- Full native command tree: [Commands](../reference/commands/).
- Annotation comparison with Atlas HCL: [Go annotations vs Atlas HCL](https://github.com/stokaro/ptah/blob/master/docs/go_annotations_vs_atlas_hcl.md).
- Programmatic parser usage: [Public API](https://github.com/stokaro/ptah/blob/master/docs/public_api.md).
