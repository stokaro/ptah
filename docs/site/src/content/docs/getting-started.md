---
title: Quick start
description: Build Ptah and run a complete local schema, migration, hash, Atlas-compatible, and cleanup flow.
---

This guide is a copy-pasteable path from a fresh checkout to a working local SQLite migration run. You annotate Go structs; Ptah generates the migrations from those annotations — you do not hand-write the SQL. Run commands from the Ptah repository root.

## Build the CLI

```bash
GOWORK=off go build -o ./bin/ptah ./cmd/ptah
./bin/ptah version
```

Expected shape:

```text
ptah version ...
```

## Create a tiny Go model

The `//migrator:schema:*` annotations are your source of truth: they describe the desired schema, and every later step is driven from them.

```bash
rm -rf /tmp/ptah-quickstart
mkdir -p /tmp/ptah-quickstart/models /tmp/ptah-quickstart/migrations

cat > /tmp/ptah-quickstart/models/user.go <<'EOF'
package models

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="INTEGER" primary="true" auto_increment="true" not_null="true"
	ID int

	//migrator:schema:field name="email" type="TEXT" unique="true" not_null="true"
	Email string

	//migrator:schema:field name="name" type="TEXT"
	Name string
}
EOF
```

Prefer not to annotate Go structs? Ptah also reads the desired schema from an **Atlas HCL** file (or YAML and plain SQL) — see [Atlas HCL schema](../workflows/schema-files/#atlas-hcl-schema), where `ptah schema render --schema-file …` takes a file instead of `--root-dir`.

## Preview the SQL

Before generating a migration, you can see the SQL your annotations produce:

```bash
./bin/ptah schema render \
  --root-dir /tmp/ptah-quickstart/models \
  --dialect sqlite
```

Expected output includes:

```sql
CREATE TABLE "users"
"email" TEXT NOT NULL
```

## Generate the first migration

Let Ptah write the migration for you. Point it at a throwaway SQLite database: because the database is empty, the difference between it and your annotated schema is the whole schema, so Ptah generates the full `CREATE TABLE` — and the matching rollback — automatically.

```bash
DB_URL=sqlite:////tmp/ptah-quickstart/app.db

./bin/ptah migrations generate \
  --root-dir /tmp/ptah-quickstart/models \
  --db-url "$DB_URL" \
  --migrations-dir /tmp/ptah-quickstart/migrations \
  --name init

ls /tmp/ptah-quickstart/migrations
```

Ptah writes both the up and the down file:

```text
Generated migration files for sqlite:////tmp/ptah-quickstart/app.db:
UP:   .../<timestamp>_init.up.sql
DOWN: .../<timestamp>_init.down.sql
```

The generated `*.up.sql` is derived from your annotations, not hand-written:

```sql
CREATE TABLE "users" (
  "id" INTEGER PRIMARY KEY AUTOINCREMENT,
  "email" TEXT NOT NULL UNIQUE,
  "name" TEXT
);
```

Prefer to author a migration by hand? `ptah migrations create <name> --migrations-dir /tmp/ptah-quickstart/migrations` scaffolds empty `*.up.sql` / `*.down.sql` files for you to fill in. This quick start uses generation so the annotations stay the single source of truth.

## Hash and validate the directory

```bash
./bin/ptah migrations hash --dir /tmp/ptah-quickstart/migrations
./bin/ptah migrations validate --dir /tmp/ptah-quickstart/migrations
```

Expected output includes:

```text
OK: migrations directory matches ptah.sum
```

## Apply and inspect

```bash
./bin/ptah migrations up \
  --db-url "$DB_URL" \
  --migrations-dir /tmp/ptah-quickstart/migrations \
  --verify-sum

./bin/ptah db read --db-url "$DB_URL"

./bin/ptah migrations status \
  --db-url "$DB_URL" \
  --migrations-dir /tmp/ptah-quickstart/migrations
```

Expected output includes the applied version and the introspected `users` table:

```text
✅ Migrations completed successfully!
Database is now at version: ...
```

## Change the schema and regenerate

This is the loop Ptah is built for. Change your annotations and generate again: Ptah diffs your desired schema against the live database and writes a migration for **just the delta** — here, a new `posts` table — instead of a full rebuild.

```bash
cat > /tmp/ptah-quickstart/models/post.go <<'EOF'
package models

//migrator:schema:table name="posts"
type Post struct {
	//migrator:schema:field name="id" type="INTEGER" primary="true" auto_increment="true" not_null="true"
	ID int

	//migrator:schema:field name="title" type="TEXT" not_null="true"
	Title string
}
EOF

./bin/ptah migrations generate \
  --root-dir /tmp/ptah-quickstart/models \
  --db-url "$DB_URL" \
  --migrations-dir /tmp/ptah-quickstart/migrations \
  --name add_posts
```

The new `*.up.sql` contains only the new table — `users` is left untouched because the database already has it:

```sql
CREATE TABLE "posts" (
  "id" INTEGER PRIMARY KEY AUTOINCREMENT,
  "title" TEXT NOT NULL
);
```

Hash, apply, and confirm both tables now exist:

```bash
./bin/ptah migrations hash --dir /tmp/ptah-quickstart/migrations

./bin/ptah migrations up \
  --db-url "$DB_URL" \
  --migrations-dir /tmp/ptah-quickstart/migrations \
  --verify-sum

./bin/ptah db read --db-url "$DB_URL"
```

Expected `db read` output now includes both `CREATE TABLE "posts"` and `CREATE TABLE "users"`.

## Roll back

```bash
./bin/ptah migrations down \
  --db-url "$DB_URL" \
  --migrations-dir /tmp/ptah-quickstart/migrations \
  --target 0 \
  --confirm
```

`--target 0` rolls back every applied migration, returning the database to an empty application schema.

## Try the Atlas-compatible path

Atlas-compatible commands live under `ptah atlas <command> ...`.

```bash
./bin/ptah atlas migrate hash \
  --dir /tmp/ptah-quickstart/migrations

./bin/ptah atlas migrate validate \
  --dir /tmp/ptah-quickstart/migrations
```

These commands delegate to Ptah's native migration hash and validation behavior while accepting Atlas-style paths and flags where implemented.

## Clean up

```bash
rm -rf /tmp/ptah-quickstart
rm -f ./bin/ptah
```

## Next steps

- Use [Go schema workflow](../workflows/go-schema/) when the application owns schema definitions in Go code.
- Use [Schema files](../workflows/schema-files/) for YAML, HCL, or SQL sources.
- Use [Migrations](../workflows/migrations/) before applying Ptah to shared databases.
- Use [Comparison](../reference/comparison/) to understand where Ptah currently matches Atlas and where conformance gaps remain.
