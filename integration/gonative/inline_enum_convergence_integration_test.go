//go:build integration

package gonative_test

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/dbschema/mysql"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// inlineEnumSchema declares one enum-typed column carrying the given values.
//
// It is PARSED rather than constructed, from the annotations an author writes,
// so the round trip below measures the shape the CLI produces. A hand-built
// goschema.Database is not that shape: the enum catalog a column reference
// resolves against is derived during parsing, and a struct literal that sets
// Field.Enum without it renders as a bare `ENUM`, which MySQL answers with a
// syntax error.
func inlineEnumSchema(c *qt.C, values ...string) *goschema.Database {
	c.Helper()

	source := fmt.Sprintf(`package models

//ptah:schema:table name="ptah_enum_conv"
type User struct {
	//ptah:schema:field name="id" type="INT" primary="true"
	ID int64
	//ptah:schema:field name="state" type="ENUM" enum=%q not_null="true"
	State string
}
`, strings.Join(values, ","))

	database, err := goschema.ParseFS(fstest.MapFS{
		"models/models.go": &fstest.MapFile{Data: []byte(source)},
	}, "models")
	c.Assert(err, qt.IsNil)
	return database
}

// TestInlineEnumChangeConverges_Integration is stokaro/ptah#1716's convergence
// requirement on the two inline-enum engines with live coverage here.
//
// The issue recorded that an enum value change on these engines produced a
// warning comment and never converged: the apply exited 0 with the enum
// unchanged, and the next run did the same. The assertion that separates a plan
// from a comment is this one -- apply, read the catalog back, compare, and
// require nothing left to do -- made twice, so the second round measures a
// CHANGE to an enum the database already holds rather than its creation.
func TestInlineEnumChangeConverges_Integration(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		dsn     func(t *testing.T) string
	}{
		{name: "mysql", dialect: platform.MySQL, dsn: skipIfNoMySQL},
		{name: "mariadb", dialect: platform.MariaDB, dsn: skipIfNoMariaDB},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dsn := test.dsn(t)
			c := qt.New(t)

			db, err := sql.Open("mysql", dsn)
			c.Assert(err, qt.IsNil)
			defer db.Close()
			dropInlineEnumTable(db)
			defer dropInlineEnumTable(db)

			applyInlineEnumSchema(c, db, test.dialect, inlineEnumSchema(c, "active", "inactive"))
			c.Assert(inlineEnumPending(c, db, test.dialect, inlineEnumSchema(c, "active", "inactive")),
				qt.HasLen, 0)

			// The change the issue is about: a value added to an enum the
			// database already holds.
			applyInlineEnumSchema(c, db, test.dialect, inlineEnumSchema(c, "active", "inactive", "archived"))
			c.Assert(inlineEnumPending(c, db, test.dialect, inlineEnumSchema(c, "active", "inactive", "archived")),
				qt.HasLen, 0)
		})
	}
}

// applyInlineEnumSchema plans desired against the live catalog and executes
// every statement, failing on the first one the engine refuses.
func applyInlineEnumSchema(c *qt.C, db *sql.DB, dialect string, desired *goschema.Database) {
	c.Helper()

	statements := inlineEnumPending(c, db, dialect, desired)
	c.Assert(len(statements) > 0, qt.IsTrue, qt.Commentf("nothing planned for %s", dialect))
	for _, statement := range statements {
		_, execErr := db.Exec(statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
}

// inlineEnumPending returns the statements that would still run. Empty is
// convergence.
func inlineEnumPending(c *qt.C, db *sql.DB, dialect string, desired *goschema.Database) []string {
	c.Helper()

	reader := mysql.NewMySQLReader(db, "")
	live, err := reader.ReadSchema()
	c.Assert(err, qt.IsNil)

	// CompareWithDialect rather than Compare: the inline-enum fold is what turns
	// a declared enum into the column type this engine stores, and it is keyed
	// on the dialect. Comparing dialect-neutrally would set a named enum type
	// against a catalog that has none and report a difference on every run --
	// which is the shape the issue describes.
	diff := schemadiff.CompareWithDialect(desired, live, dialect)
	statements, err := planner.GenerateSchemaDiffSQLStatements(diff, desired, dialect)
	c.Assert(err, qt.IsNil)
	return statements
}

func dropInlineEnumTable(db *sql.DB) {
	_, _ = db.Exec("DROP TABLE IF EXISTS ptah_enum_conv")
}
