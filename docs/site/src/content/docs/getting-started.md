---
title: Quick start
description: Build Ptah and run a complete local schema, migration, hash, Atlas-compatible, and cleanup flow.
---

This guide is a copy-pasteable path from a fresh checkout to a working local SQLite migration run. Run commands from the Ptah repository root.

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

## Render SQL

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

## Create and edit a migration

```bash
./bin/ptah migrations create init \
  --migrations-dir /tmp/ptah-quickstart/migrations

ls /tmp/ptah-quickstart/migrations
```

Use the rendered SQL as a guide, write the `*.up.sql` body, and add the rollback:

```bash
up_file=$(ls /tmp/ptah-quickstart/migrations/*.up.sql)
down_file=${up_file%.up.sql}.down.sql

cat > "$up_file" <<'EOF'
CREATE TABLE "users" (
  "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  "email" TEXT NOT NULL,
  "name" TEXT,
  CONSTRAINT "uni_users_email" UNIQUE ("email")
);
EOF

cat > "$down_file" <<'EOF'
DROP TABLE "users";
EOF
```

## Hash and validate the directory

```bash
./bin/ptah migrations hash --dir /tmp/ptah-quickstart/migrations
./bin/ptah migrations validate --dir /tmp/ptah-quickstart/migrations
```

Expected output includes:

```text
OK: migrations directory matches ptah.sum
```

## Apply, inspect, status, and roll back

```bash
DB_URL=sqlite:////tmp/ptah-quickstart/app.db

./bin/ptah migrations up \
  --db-url "$DB_URL" \
  --migrations-dir /tmp/ptah-quickstart/migrations \
  --verify-sum

./bin/ptah db read --db-url "$DB_URL"

./bin/ptah migrations status \
  --db-url "$DB_URL" \
  --migrations-dir /tmp/ptah-quickstart/migrations

./bin/ptah migrations down \
  --db-url "$DB_URL" \
  --migrations-dir /tmp/ptah-quickstart/migrations \
  --target 0 \
  --confirm
```

Expected status after `up` includes the initial migration as applied. The final `down` returns the database to an empty application schema.

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
