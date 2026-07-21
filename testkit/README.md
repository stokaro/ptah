# Ptah Testkit

`github.com/stokaro/ptah/testkit` is an opt-in helper module for tests that
need real databases. It keeps `testcontainers-go` out of Ptah's main module
graph while giving users a small public API for migration and schema tests.

```go
package example_test

import (
	"embed"
	"testing"

	"github.com/stokaro/ptah/testkit"
)

//go:embed migrations/*.sql
var migrations embed.FS

func TestMigrations(t *testing.T) {
	db := testkit.StartPostgres(t, testkit.WithReuseByName("my-package-postgres"))

	testkit.ApplyMigrationsFromFS(t, db, migrations)
	testkit.Seed(t, db, []byte(`INSERT INTO users (email) VALUES ('a@example.com');`))

	got := testkit.Snapshot(t, db)
	t.Log(got)
}
```

Available helpers:

- `StartPostgres(t, opts...)`
- `StartMySQL(t, opts...)`
- `StartMariaDB(t, opts...)`
- `StartSQLite(t, opts...)`
- `ApplyMigrationsFromFS(t, db, fsys, opts...)`
- `Seed(t, db, sqlBytes)`
- `Snapshot(t, db)`

Use `WithReuseByName` to keep a package-level container around while still
getting a fresh random database per test. SQLite does not use containers and
defaults to a temporary file. `Snapshot` returns deterministic schema JSON and
omits Ptah/Atlas migration metadata tables from the output.

Because Ptah has not published a first release tag yet, repository-local
development uses:

```go
replace github.com/stokaro/ptah => ..
```

Consumers should pin `github.com/stokaro/ptah` and
`github.com/stokaro/ptah/testkit` to matching commits until Ptah starts
publishing release tags.
