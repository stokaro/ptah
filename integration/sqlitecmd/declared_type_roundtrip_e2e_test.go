//go:build integration

package sqlitecmd_test

import (
	"context"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// declaredTypeDDL is a table written the way a person writes SQLite by hand:
// none of the spellings is the one Ptah's renderer would choose.
const declaredTypeDDL = `CREATE TABLE probe (
	id INTEGER PRIMARY KEY,
	c_vc VARCHAR(80),
	c_char CHARACTER(4),
	c_clob CLOB,
	c_custom MY_OWN_TYPE,
	c_dbl DOUBLE PRECISION,
	c_bool BOOLEAN,
	c_none
);`

// TestSQLiteDeclaredTypesSurviveAReadE2E pins that a description of a SQLite
// database says what the database says.
//
// SQLite stores the declaration verbatim and derives an affinity from it at use
// time, so the declared text IS the type. A description that rewrote
// `VARCHAR(80)` to `TEXT` replayed as a different table, and the comparison
// then planned a rebuild -- drop, recreate, copy every row -- to change nothing
// an application can observe (stokaro/ptah#2040).
func TestSQLiteDeclaredTypesSurviveAReadE2E(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "declared-types.db")
	seedSQLite(c, dbPath, declaredTypeDDL)

	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	read, err := conn.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)

	described := dbschematogo.ConvertDBSchemaToGoSchema(read)

	c.Assert(describedColumnTypes(described), qt.DeepEquals, map[string]string{
		"id":       "INTEGER",
		"c_vc":     "VARCHAR(80)",
		"c_char":   "CHARACTER(4)",
		"c_clob":   "CLOB",
		"c_custom": "MY_OWN_TYPE",
		"c_dbl":    "DOUBLE PRECISION",
		// The row that needs the description to say the type was STORED
		// rather than declared. Ptah's renderer turns a declared BOOLEAN into
		// INTEGER, which changes the affinity from NUMERIC to INTEGER, so a
		// comparison that canonicalized this side would report a change for
		// the column it had just described.
		"c_bool": "BOOLEAN",
		// The reader answers BLOB for a column with no declared type, which is
		// the affinity SQLite gives it.
		"c_none": "BLOB",
	})

	// And the description of the database compares clean against the database
	// it describes.
	diff, err := schemadiff.CompareWithDatabase(
		context.Background(), conn, described, read, config.DefaultCompareOptions())

	c.Assert(err, qt.IsNil)
	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("diff: %+v", diff))
}

// TestSQLiteEquivalentSpellingsDoNotRebuildE2E is the comparison half, against
// a live database rather than a hand-built read.
//
// Every row is a declaration whose spelling differs from the catalog's and
// whose AFFINITY does not, which is what SQLite means by "the same type".
func TestSQLiteEquivalentSpellingsDoNotRebuildE2E(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		stored   string
	}{
		{name: "text against a hand-made varchar", declared: "TEXT", stored: "VARCHAR(80)"},
		{name: "text against a hand-made clob", declared: "TEXT", stored: "CLOB"},
		{name: "integer against a hand-made bigint", declared: "INTEGER", stored: "BIGINT"},
		{name: "real against a hand-made double precision", declared: "REAL", stored: "DOUBLE PRECISION"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dbPath := filepath.Join(t.TempDir(), "affinity.db")
			seedSQLite(c, dbPath, "CREATE TABLE probe (id INTEGER, v "+test.stored+");")

			conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)
			read, err := conn.Reader().ReadSchema()
			c.Assert(err, qt.IsNil)

			diff, err := schemadiff.CompareWithDatabase(context.Background(), conn,
				sqliteColumnDeclaration(test.declared), read, config.DefaultCompareOptions())

			c.Assert(err, qt.IsNil)
			c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("diff: %+v", diff))
		})
	}
}

// TestSQLiteDifferentAffinitiesStillRebuildE2E is the control the two above
// need: a spelling difference the engine DOES have is still a change.
func TestSQLiteDifferentAffinitiesStillRebuildE2E(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		stored   string
	}{
		{name: "text is not integer", declared: "TEXT", stored: "INTEGER"},
		{name: "integer is not blob", declared: "INTEGER", stored: "BLOB"},
		{name: "real is not text", declared: "REAL", stored: "VARCHAR(20)"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dbPath := filepath.Join(t.TempDir(), "affinity.db")
			seedSQLite(c, dbPath, "CREATE TABLE probe (id INTEGER, v "+test.stored+");")

			conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)
			read, err := conn.Reader().ReadSchema()
			c.Assert(err, qt.IsNil)

			diff, err := schemadiff.CompareWithDatabase(context.Background(), conn,
				sqliteColumnDeclaration(test.declared), read, config.DefaultCompareOptions())

			c.Assert(err, qt.IsNil)
			c.Assert(diff.HasChanges(), qt.IsTrue, qt.Commentf("diff: %+v", diff))
		})
	}
}

// describedColumnTypes names the type each column of the described table
// carries, so a failure shows the whole table rather than one assertion.
func describedColumnTypes(described *goschema.Database) map[string]string {
	types := make(map[string]string, len(described.Fields))
	for _, field := range described.Fields {
		types[field.Name] = field.Type
	}
	return types
}

// sqliteColumnDeclaration is a description of the probe table with one column
// declared as the caller spells it.
func sqliteColumnDeclaration(declaredType string) *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{StructName: "P", Name: "probe"}},
		Fields: []goschema.Field{
			// Not a primary key: SQLite reports an INTEGER PRIMARY KEY column
			// as nullable, and a declaration that made it a key would report a
			// nullability change this test is not about.
			{StructName: "P", Name: "id", Type: "INTEGER", Nullable: true},
			{StructName: "P", Name: "v", Type: declaredType, Nullable: true},
		},
	}
}
