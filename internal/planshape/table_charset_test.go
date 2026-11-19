package planshape_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/internal/planshape"
)

// tableCollationPlan is the change of table t's default collation from current
// to desired, as the planner writes it with the character set it changes, and
// the diff that carries it.
func tableCollationPlan(current, desired, charset string) ([]ast.Node, *difftypes.SchemaDiff) {
	nodes := []ast.Node{&ast.AlterTableNode{Name: "t", Operations: []ast.AlterOperation{
		&ast.SetTableCollationOperation{Charset: charset, Collation: desired},
	}}}
	diff := &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
		TableName:       "t",
		CollationChange: &difftypes.CollationChange{Current: current, Desired: desired, Charset: &difftypes.CharsetChange{Desired: charset}},
	}}}
	return nodes, diff
}

// TestApply_WritesTheCharsetOfATableCollationAsTheCatalogReadsIt changes the
// default collation of a table together with its character set. MariaDB lists
// the UCA 14.0.0 collations with no character set, so a catalog read names the
// character set of neither side of utf8mb3_uca1400_ai_ci to
// utf8mb4_uca1400_ai_ci, and the change is written as the collation alone,
// also where a mysql URL names the MariaDB server. A side the read names keeps
// the character set.
func TestApply_WritesTheCharsetOfATableCollationAsTheCatalogReadsIt(t *testing.T) {
	for _, test := range []struct {
		name, dialect, current, desired string
		charset, wantCharset            string
	}{
		{"both sides unread on MariaDB", "mariadb", "utf8mb3_uca1400_ai_ci", "utf8mb4_uca1400_ai_ci", "utf8mb4", ""},
		{"the current side read on MariaDB", "mariadb", "latin1_swedish_ci", "utf8mb4_uca1400_ai_ci", "utf8mb4", "utf8mb4"},
		{"the desired side read on MariaDB", "mariadb", "utf8mb4_uca1400_ai_ci", "latin1_swedish_ci", "latin1", "latin1"},
		{"a collation named without its character set", "mariadb", "utf8mb3_uca1400_ai_ci", "uca1400_as_cs", "utf8mb4", ""},
		{"both sides of a mysql URL that names MariaDB", "mysql", "utf8mb3_uca1400_ai_ci", "utf8mb4_uca1400_ai_ci", "utf8mb4", ""},
		{"both sides read on MySQL", "mysql", "utf8mb3_general_ci", "utf8mb4_0900_ai_ci", "utf8mb4", "utf8mb4"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			nodes, diff := tableCollationPlan(test.current, test.desired, test.charset)

			shaped := planshape.Apply(test.dialect, nodes, diff)

			c.Assert(shaped, qt.HasLen, 1)
			alter := shaped[0].(*ast.AlterTableNode)
			c.Assert(alter.Operations, qt.DeepEquals, []ast.AlterOperation{
				&ast.SetTableCollationOperation{Charset: test.wantCharset, Collation: test.desired},
			})
		})
	}
}
