//go:build integration

package postgres_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5/pgconn"

	"ptah.run/core/platform"
	"ptah.run/feature/pgextension"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/schemafile"
	"ptah.run/internal/sqlschema"
)

// extensionCreatedTwice is a file's statements: one extension created twice,
// the second time without IF NOT EXISTS.
var extensionCreatedTwice = []string{"CREATE EXTENSION pgcrypto", "CREATE EXTENSION pgcrypto"}

// runExtensionCreatedTwice runs extensionCreatedTwice on a throwaway database
// of engine, a statement at a time as a schema file is run, and returns the
// extensions the database then holds and the error of the second statement.
func runExtensionCreatedTwice(c *qt.C, engine dbtarget.Engine) (installed []string, second error) {
	c.Helper()
	ctx, cancel := context.WithTimeout(c.Context(), 2*time.Minute)
	defer cancel()
	live := newPostgresWriterLiveDatabase(c, ctx, requirePostgresWriterFamilyLiveURL(c, engine))
	defer live.cleanup()
	_, err := live.db.ExecContext(ctx, extensionCreatedTwice[0])
	c.Assert(err, qt.IsNil)
	_, second = live.db.ExecContext(ctx, extensionCreatedTwice[1])
	return postgresWriterLiveExtensionNames(c, ctx, live.db), second
}

// loadExtensionCreatedTwice reads extensionCreatedTwice as a schema file for
// dialect.
func loadExtensionCreatedTwice(c *qt.C, dialect string) (names []string, err error) {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(strings.Join(extensionCreatedTwice, ";\n")+";\n"), 0o600), qt.IsNil)
	runtime := builtintest.Runtime()
	desired, err := schemafile.LoadAll(c.Context(), []string{path},
		schemafile.Options{YAML: runtime.YAML(), SQL: runtime.SQL(), HCL: runtime.HCL(), Dialect: dialect})
	if desired == nil {
		return nil, err
	}
	return pgextension.Names(desired.FeatureObjects), err
}

// TestExtensionCreatedTwice_LiveRefusedWhereTheServerRefusesIt_FailurePath
// runs a file that creates one extension twice on a server that refuses the
// second statement, and reads the same file for that server: the read is
// refused too, rather than accepted as one extension the file never gets to
// declare.
func TestExtensionCreatedTwice_LiveRefusedWhereTheServerRefusesIt_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		engine  dbtarget.Engine
		dialect string
	}{
		{name: "postgres", engine: dbtarget.PostgreSQL, dialect: platform.Postgres},
		{name: "yugabytedb", engine: dbtarget.YugabyteDB, dialect: platform.YugabyteDB},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			installed, second := runExtensionCreatedTwice(c, test.engine)
			names, err := loadExtensionCreatedTwice(c, test.dialect)

			var refusal *pgconn.PgError
			c.Assert(second, qt.ErrorAs, &refusal)
			c.Assert(refusal.Code, qt.Equals, "42710")
			c.Assert(refusal.Message, qt.Equals, `extension "pgcrypto" already exists`)
			c.Assert(installed, qt.Contains, "pgcrypto")
			c.Assert(err, qt.ErrorIs, sqlschema.ErrNameInUse)
			c.Assert(err, qt.ErrorMatches, `(?s).*extension "pgcrypto" is created again without IF NOT EXISTS.*`)
			c.Assert(names, qt.IsNil)
		})
	}
}

// TestExtensionCreatedTwice_LiveReadWhereTheServerRunsIt_HappyPath is the
// control: CockroachDB runs the second statement, so the read of the same
// file for it declares the extension once.
func TestExtensionCreatedTwice_LiveReadWhereTheServerRunsIt_HappyPath(t *testing.T) {
	c := qt.New(t)

	_, second := runExtensionCreatedTwice(c, dbtarget.CockroachDB)
	names, err := loadExtensionCreatedTwice(c, platform.CockroachDB)

	c.Assert(second, qt.IsNil)
	c.Assert(err, qt.IsNil)
	c.Assert(names, qt.DeepEquals, []string{"pgcrypto"})
}
