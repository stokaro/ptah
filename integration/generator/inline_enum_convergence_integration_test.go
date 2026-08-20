//go:build integration

package generator_test

import (
	"context"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// inlineEnumEntities parses a Go-annotation source declaring one table whose
// enum column carries the given values.
//
// The description is built by the parser rather than by hand: an enum field
// only renders its values once the schema has been finalized, so a
// hand-assembled goschema.Database emits a bare ENUM and measures the fixture
// instead of the product.
//
// In memory, because ParseFS takes an fs.FS and needs nothing else. The
// neighbouring drift tests write real files for a reason this one does not
// share: they hand a DIRECTORY PATH to generator.GenerateMigration, which
// cannot take a filesystem.
func inlineEnumEntities(c *qt.C, values string) *goschema.Database {
	c.Helper()
	source := `package entities

//ptah:schema:table name="ptah_inline_enum"
type Order struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int

	//ptah:schema:field name="status" type="ENUM" enum="` + values + `"
	Status string
}
`
	description, err := goschema.ParseFS(fstest.MapFS{
		"entities/schema.go": &fstest.MapFile{Data: []byte(source)},
	}, "entities")
	c.Assert(err, qt.IsNil)
	return description
}

// TestInlineEnumConvergence_Integration is the convergence test
// stokaro/ptah#1716 asks for: apply, read back, diff, and require the diff to
// be empty, on every dialect that spells an enum into the column.
//
// It is the test that cannot be written offline. SQL Server does not keep
// `col IN ('a','b')` -- it stores the disjunction it compiles the list into,
// in an order of its own -- so a fixture written by the same hand as the
// declaration cannot show the mismatch. Compared as text, the two never
// matched, and every apply planned the same DROP and ADD of the same
// constraint: a change applied, never converging and never failing.
//
// The second half is what keeps this from being a silenced diff: after a real
// value change the diff must be NON-empty, then empty again once applied.
func TestInlineEnumConvergence_Integration(t *testing.T) {
	cases := []struct {
		dialect string
		connect func(t *testing.T) *dbschema.DatabaseConnection
	}{
		{"mysql", func(t *testing.T) *dbschema.DatabaseConnection {
			return requireGeneratorDatabaseConnection(t, dbtarget.MySQL)
		}},
		{"mariadb", func(t *testing.T) *dbschema.DatabaseConnection {
			return requireGeneratorDatabaseConnection(t, dbtarget.MariaDB)
		}},
		{"sqlserver", func(t *testing.T) *dbschema.DatabaseConnection {
			return requireGeneratorDatabaseConnection(t, dbtarget.SQLServer)
		}},
		// SQLite is the fourth inline-enum dialect stokaro/ptah#1716 names, and
		// the only one that needs no server: it takes a file rather than a URL
		// out of the environment, so it has no dbtarget entry and is connected
		// directly. Leaving it out left the dialect whose enum change costs a
		// TABLE REBUILD as the one with no convergence assertion.
		{"sqlite", requireInlineEnumSQLiteConnection},
	}

	for _, tc := range cases {
		t.Run(tc.dialect, func(t *testing.T) {
			c := qt.New(t)
			ctx := context.Background()
			conn := tc.connect(t)
			dialect := conn.Info().Dialect
			cleanupInlineEnumTable(conn, dialect)
			t.Cleanup(func() { cleanupInlineEnumTable(conn, dialect) })

			// 1. Apply the declaration, then read the catalog back and require
			// the comparison to have nothing left to do.
			base := inlineEnumEntities(c, "new,paid")
			applyInlineEnum(c, ctx, conn, dialect, base)
			c.Assert(inlineEnumDiffCount(c, conn, dialect, base), qt.Equals, 0)

			// 2. A real change is still seen.
			changed := inlineEnumEntities(c, "new,paid,shipped")
			c.Assert(inlineEnumDiffCount(c, conn, dialect, changed) > 0, qt.IsTrue,
				qt.Commentf("adding an enum value must produce a change"))

			// 3. Applied, it converges again.
			applyInlineEnum(c, ctx, conn, dialect, changed)
			c.Assert(inlineEnumDiffCount(c, conn, dialect, changed), qt.Equals, 0)

			// 4. And a removal is seen and converges too, which is what a
			// comparison that merely stopped reporting would fail.
			shrunk := inlineEnumEntities(c, "new,shipped")
			c.Assert(inlineEnumDiffCount(c, conn, dialect, shrunk) > 0, qt.IsTrue,
				qt.Commentf("removing an enum value must produce a change"))
			applyInlineEnum(c, ctx, conn, dialect, shrunk)
			c.Assert(inlineEnumDiffCount(c, conn, dialect, shrunk), qt.Equals, 0)
		})
	}
}

// inlineEnumDiff compares through the connection, the way `schema apply` does.
//
// CompareWithDialect is not enough on SQL Server: identifier identity there is
// resolved from the live catalog's collation, and without it the comparison
// fails closed with "may have the same catalog identity" rather than answering
// about the enum at all.
func inlineEnumDiff(
	c *qt.C,
	conn *dbschema.DatabaseConnection,
	description *goschema.Database,
) *difftypes.SchemaDiff {
	c.Helper()
	live, err := conn.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	diff, err := schemadiff.CompareWithDatabase(
		context.Background(), conn, description, live, config.DefaultCompareOptions())
	c.Assert(err, qt.IsNil)
	return diff
}

// inlineEnumDiffCount reads the live schema and counts what the comparison
// still wants to do to it.
func inlineEnumDiffCount(
	c *qt.C,
	conn *dbschema.DatabaseConnection,
	dialect string,
	description *goschema.Database,
) int {
	c.Helper()
	diff := inlineEnumDiff(c, conn, description)
	_ = dialect
	return len(diff.TablesAdded) + len(diff.TablesModified) +
		len(diff.ConstraintsAdded) + len(diff.ConstraintsRemoved)
}

// applyInlineEnum renders the comparison's plan and executes it.
func applyInlineEnum(
	c *qt.C,
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	dialect string,
	description *goschema.Database,
) {
	c.Helper()
	_ = dialect
	diff := inlineEnumDiff(c, conn, description)
	info := conn.Info()
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithCapabilities(
		diff, description, info.Dialect, info.Capabilities)
	c.Assert(err, qt.IsNil)
	for _, statement := range statements {
		_, execErr := conn.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}
}

func cleanupInlineEnumTable(conn *dbschema.DatabaseConnection, dialect string) {
	_ = dialect
	_, _ = conn.ExecContext(context.Background(), "DROP TABLE IF EXISTS ptah_inline_enum")
}

// requireInlineEnumSQLiteConnection opens a throwaway SQLite database.
//
// Every other engine here comes out of the environment because it needs a
// server; SQLite needs a path, so a temp file is the whole configuration and
// the case never skips.
func requireInlineEnumSQLiteConnection(t *testing.T) *dbschema.DatabaseConnection {
	t.Helper()
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(
		t.Context(), "sqlite://"+filepath.Join(t.TempDir(), "inline_enum.db"))
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	return conn
}
