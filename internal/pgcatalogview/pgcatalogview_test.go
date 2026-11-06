package pgcatalogview_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/pgcatalogview"
)

// TestStatementsCarryNoPrivilegeCondition reads each statement for what makes
// information_schema depend on the connecting role. Whether the statements answer
// what the views answer takes a server: see
// integration/pgcatalogview_live_test.go.
func TestStatementsCarryNoPrivilegeCondition(t *testing.T) {
	tests := []struct {
		name      string
		statement string
	}{
		{name: "tables", statement: pgcatalogview.Tables},
		{name: "columns", statement: pgcatalogview.Columns},
		{name: "columns without generated columns", statement: pgcatalogview.ColumnsWithoutGenerated},
		{name: "table constraints", statement: pgcatalogview.TableConstraints},
		{name: "table grants", statement: pgcatalogview.TableGrants},
		{name: "view check option", statement: pgcatalogview.ViewCheckOption("c")},
		{name: "owned sequence", statement: pgcatalogview.OwnedSequence("c.oid", "a.attnum")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			for _, word := range []string{"information_schema", "has_table_privilege", "has_column_privilege", "has_any_column_privilege", "enabled_roles"} {
				c.Assert(test.statement, qt.Not(qt.Contains), word)
			}
			// pg_has_role with two arguments asks about the connecting role, which
			// is the condition left out. The three-argument form names the role.
			c.Assert(test.statement, qt.Not(qt.Matches), `(?s).*pg_has_role\([^,()]+,\s*'.*`)
		})
	}
}

// TestColumns_AsksForAttgeneratedOnlyWhereItExists pins the one variation. On a
// server without pg_attribute.attgenerated the column's name fails the whole
// statement, and on a server with it an unconditional default would report a
// generated column's expression as its default too.
func TestColumns_AsksForAttgeneratedOnlyWhereItExists(t *testing.T) {
	c := qt.New(t)

	c.Assert(pgcatalogview.Columns, qt.Contains, "CASE WHEN a.attgenerated = '' THEN pg_get_expr(ad.adbin, ad.adrelid) END AS column_default")
	c.Assert(pgcatalogview.ColumnsWithoutGenerated, qt.Not(qt.Contains), "attgenerated")
	c.Assert(pgcatalogview.ColumnsWithoutGenerated, qt.Contains, "pg_get_expr(ad.adbin, ad.adrelid) AS column_default")
}
