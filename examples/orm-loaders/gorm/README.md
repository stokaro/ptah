# GORM external schema loader

This example keeps the application module small: its only direct dependency is
GORM. `load-schema.sh` runs the official standalone provider command in an
ephemeral copy of the model module and pins `atlas-provider-gorm` to `v0.6.1`.
The provider and its transitive dependencies therefore do not become part of
the application module's dependency graph. The model module also overrides
GORM's older `golang.org/x/text` requirement with the current fixed `v0.40.0`
release.

The first loader run needs network access to download the pinned provider and
its transitive modules into the Go cache. It does not modify this example's
`go.mod` or `go.sum`.

From this directory:

```bash
go mod download
ptah schema render \
  --config ptah.yaml \
  --allow-external-schema \
  --dialect postgres
```

`--allow-external-schema` is required because the configuration executes a
local program. Review both `ptah.yaml` and `load-schema.sh` before opting in.

The rendered schema contains:

- `users` and `pets` tables.
- A primary key on each table.
- A unique index on `users.email`.
- An index on `pets.user_id`.
- A `pets.user_id` foreign key that references `users.id` with
  `ON DELETE CASCADE`.
