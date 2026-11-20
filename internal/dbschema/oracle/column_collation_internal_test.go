package oracle

// White-box testing required: the column read is package-local and its
// projection is not reachable through an exported API without a live Oracle
// server. The live read is covered by integration/oracle_column_collation_live_test.go.

import (
	"database/sql/driver"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbschema/dbtest"
)

// A column's collation is read from the catalog. The read has to ask for the
// column's COLLATION and for the table's DEFAULT_COLLATION, since a column
// without a COLLATE of its own reports its table's, and the two together tell
// a declared collation from an inherited one.
func TestReadColumns_CarriesAColumnCollation(t *testing.T) {
	c := qt.New(t)

	db := dbtest.Open(t, answeringCollationCatalog)
	reader := &Reader{db: db.SQL, schema: "APP"}

	columns, err := reader.readColumns(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(columns["NAMES"], qt.HasLen, 5)
	for i, want := range []struct{ declared, effective string }{
		{"", ""},                   // ID: NUMBER reads no collation
		{"", ""},                   // PLAIN: USING_NLS_COMP
		{"BINARY_CI", "BINARY_CI"}, // CI: declared
		{"", "BINARY_AI"},          // INHERITED: the table's default
		{"BINARY", "BINARY"},       // OVERRIDE: another than the table's default
	} {
		c.Assert(columns["NAMES"][i].Collate, qt.Equals, want.declared, qt.Commentf("column %d", i))
		c.Assert(columns["NAMES"][i].EffectiveCollate, qt.Equals, want.effective, qt.Commentf("column %d", i))
	}
}

// answeringCollationCatalog answers the column read, and refuses a projection
// that asks for neither the column's collation nor its table's default.
//
// A fake that answers the columns whatever the query selects cannot tell a
// reader that stopped asking from one that still does.
func answeringCollationCatalog(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	if !contains(query, "c.collation") || !contains(query, "t.default_collation") || !contains(query, "all_tables t") {
		return dbtest.QueryResult{}, errUnaskedCollation
	}
	row := func(name, typ string, id int64, collation, tableDefault driver.Value) []driver.Value {
		return []driver.Value{
			"NAMES", name, typ, nil, nil, nil, nil, nil, "Y", id, "NO", "NO", nil, nil, nil, collation, tableDefault,
		}
	}
	return dbtest.QueryResult{
		Columns: []string{
			"TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "VECTOR_INFO", "CHAR_LENGTH", "CHAR_USED",
			"DATA_PRECISION", "DATA_SCALE", "NULLABLE", "COLUMN_ID",
			"IDENTITY_COLUMN", "VIRTUAL_COLUMN", "DATA_DEFAULT", "COMMENTS", "GENERATION_TYPE",
			"COLLATION", "DEFAULT_COLLATION",
		},
		Rows: [][]driver.Value{
			row("ID", "NUMBER", 1, nil, "BINARY_AI"),
			row("PLAIN", "VARCHAR2", 2, "USING_NLS_COMP", "BINARY_AI"),
			row("CI", "VARCHAR2", 3, "BINARY_CI", "BINARY_AI"),
			row("INHERITED", "VARCHAR2", 4, "BINARY_AI", "BINARY_AI"),
			row("OVERRIDE", "VARCHAR2", 5, "BINARY", "BINARY_AI"),
		},
	}, nil
}

var errUnaskedCollation = errors.New("column query does not read the column's collation and its table's default")
