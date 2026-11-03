//go:build integration

package postgres_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5/pgconn"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/engine/builtin"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/schemafile"
)

// uniqueMethodTable is the table every unique index below is declared on.
const uniqueMethodTable = "CREATE TABLE t (id int PRIMARY KEY, a int, arr int[])"

// renderUniqueMethodFile reads index, a CREATE INDEX on uniqueMethodTable's
// table, from a schema file for dialect and renders it, as `ptah schema
// render` does.
func renderUniqueMethodFile(c *qt.C, dialect, index string) ([]string, error) {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(uniqueMethodTable+";\n"+index+";\n"), 0o600), qt.IsNil)
	runtime := builtintest.Runtime()
	desired, err := schemafile.LoadAll(c.Context(), []string{path},
		schemafile.Options{YAML: runtime.YAML(), SQL: runtime.SQL(), HCL: runtime.HCL(), Dialect: dialect})
	c.Assert(err, qt.IsNil)
	return builtin.GetOrderedCreateStatements(desired, dialect)
}

// TestUniqueIndexMethod_LiveRefusedWhereTheServerRefusesIt_FailurePath runs a
// unique index over an access method the server builds no unique index with,
// and renders the same declaration for that server: Ptah refuses it, naming
// the index, before anything is written, where the server would refuse it at
// apply.
func TestUniqueIndexMethod_LiveRefusedWhereTheServerRefusesIt_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		engine  dbtarget.Engine
		dialect string
		index   string
		wantErr string
	}{
		{
			name: "postgres hash", engine: dbtarget.PostgreSQL, dialect: platform.Postgres,
			index:   "CREATE UNIQUE INDEX t_a ON t USING hash (a)",
			wantErr: `access method "hash" does not support unique indexes`,
		},
		{
			name: "yugabytedb gin", engine: dbtarget.YugabyteDB, dialect: platform.YugabyteDB,
			index:   "CREATE UNIQUE INDEX t_a ON t USING gin (arr)",
			wantErr: `access method "ybgin" does not support unique indexes`,
		},
		{
			name: "cockroachdb gin", engine: dbtarget.CockroachDB, dialect: platform.CockroachDB,
			index:   "CREATE UNIQUE INDEX t_a ON t USING gin (arr)",
			wantErr: `inverted indexes can't be unique`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(c.Context(), 2*time.Minute)
			defer cancel()
			live := newPostgresWriterLiveDatabase(c, ctx, requirePostgresWriterFamilyLiveURL(c, test.engine))
			defer live.cleanup()
			_, err := live.db.ExecContext(ctx, uniqueMethodTable)
			c.Assert(err, qt.IsNil)

			_, serverErr := live.db.ExecContext(ctx, test.index)
			statements, renderErr := renderUniqueMethodFile(c, test.dialect, test.index)

			var refusal *pgconn.PgError
			c.Assert(serverErr, qt.ErrorAs, &refusal)
			c.Assert(refusal.Message, qt.Equals, test.wantErr)
			c.Assert(renderErr, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(renderErr, qt.ErrorMatches, `(?s).*unique index "t_a" uses access method .*`)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// TestUniqueIndexMethod_LiveAppliedWhereTheServerBuildsIt_HappyPath is the
// control: YugabyteDB builds a unique hash index as lsm, so Ptah renders the
// declaration for it, and the statements it renders build a unique index.
func TestUniqueIndexMethod_LiveAppliedWhereTheServerBuildsIt_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(c.Context(), 2*time.Minute)
	defer cancel()
	live := newPostgresWriterLiveDatabase(c, ctx, requirePostgresWriterFamilyLiveURL(c, dbtarget.YugabyteDB))
	defer live.cleanup()

	statements, err := renderUniqueMethodFile(c, platform.YugabyteDB, "CREATE UNIQUE INDEX t_a ON t USING hash (a)")
	c.Assert(err, qt.IsNil)
	for _, statement := range statements {
		_, err := live.db.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
	var unique bool
	var method string
	err = live.db.QueryRowContext(ctx, `SELECT i.indisunique, am.amname FROM pg_index i
		JOIN pg_class c ON c.oid = i.indexrelid JOIN pg_am am ON am.oid = c.relam WHERE c.relname = 't_a'`).Scan(&unique, &method)

	c.Assert(err, qt.IsNil)
	c.Assert(unique, qt.IsTrue)
	c.Assert(method, qt.Equals, "lsm")
}
