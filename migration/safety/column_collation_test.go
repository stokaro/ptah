package safety_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/engine/builtin"
	"ptah.run/migration/safety"
)

// collationChangeReason is the reason a change of a column's collation alone
// carries, on the node and on the statement a plan rates by its text.
const collationChangeReason = "the collation change rebuilds every index on the column, changes how its values " +
	"sort and compare, and fails where a unique key holds values the new collation finds equal"

// setsCollationReason is the reason a statement that writes a column's
// collation carries where no node says whether the collation changes.
const setsCollationReason = "the statement sets a column's collation: a change of it rebuilds every index on " +
	"the column and fails where a unique key holds values the new collation finds equal"

// TestAssess_ACollationChange pins the reason a change of a column's collation
// alone carries, and that one made with another change is judged by the
// other change.
func TestAssess_ACollationChange(t *testing.T) {
	tests := []struct {
		name    string
		changed ast.ColumnProperties
		want    string
	}{
		{name: "the collation alone", changed: ast.ColumnProperties{Collation: true}, want: collationChangeReason},
		{name: "the collation and the default", changed: ast.ColumnProperties{Collation: true, Default: true},
			want: "DROP DEFAULT can break writers that leave the column out"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			column := ast.NewColumn("c", "text").SetCollate("C")
			column.Nullable = true

			got := safety.Assess([]ast.Node{modifyColumn(&ast.ModifyColumnOperation{
				Column: column, Changed: test.changed, HasChanged: true,
			})})

			c.Assert(got, qt.HasLen, 1)
			c.Assert(got[0].Severity, qt.Equals, safety.Warning)
			c.Assert(got[0].Reason, qt.Equals, test.want)
		})
	}
}

// TestAssessSQL_AStatementThatSetsACollation rates a statement by its words
// where it writes the collation of a column that holds rows, on each spelling
// a target writes. The collation reason wins over a NOT NULL the statement
// restates on the way, which says nothing about the collation.
func TestAssessSQL_AStatementThatSetsACollation(t *testing.T) {
	tests := []struct {
		name      string
		target    string
		statement string
	}{
		{name: "a PostgreSQL TYPE with COLLATE", target: "postgres", statement: `ALTER TABLE "t" ALTER COLUMN "n" TYPE text COLLATE "C";`},
		{name: "SET DATA TYPE", target: "postgres", statement: `ALTER TABLE t ALTER COLUMN n SET DATA TYPE text COLLATE "POSIX";`},
		{name: "a MySQL MODIFY", target: "mysql", statement: "ALTER TABLE `t` MODIFY COLUMN `n` varchar(20) COLLATE utf8mb4_general_ci NOT NULL;"},
		{name: "MODIFY without COLUMN", target: "mariadb", statement: "ALTER TABLE t MODIFY n varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;"},
		{name: "CHANGE", target: "mysql", statement: "ALTER TABLE t CHANGE COLUMN n n varchar(20) COLLATE utf8mb4_bin;"},
		{name: "CONVERT TO", target: "mysql", statement: "ALTER TABLE t CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := safety.AssessSQL(c.Context(), must.Must(builtin.New()), test.target, []string{test.statement})

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.HasLen, 1)
			c.Assert(got[0].Severity, qt.Equals, safety.Warning)
			c.Assert(got[0].Reason, qt.Equals, setsCollationReason)
		})
	}
}

// TestAssessSQL_ACollateThatChangesNoColumn is the control: a COLLATE in a new
// table, a new column or a table's default options changes no column that
// holds rows, and a TYPE without one says nothing of the collation.
func TestAssessSQL_ACollateThatChangesNoColumn(t *testing.T) {
	tests := []struct {
		name      string
		target    string
		statement string
	}{
		{name: "a new table", target: "postgres", statement: `CREATE TABLE t (n text COLLATE "C");`},
		{name: "a new column", target: "postgres", statement: `ALTER TABLE t ADD COLUMN n text COLLATE "C";`},
		{name: "a table default", target: "mysql", statement: "ALTER TABLE t COLLATE utf8mb4_bin;"},
		{name: "a type without one", target: "postgres", statement: `ALTER TABLE t ALTER COLUMN n TYPE varchar(40);`},
		{name: "a MODIFY without one", target: "mysql", statement: "ALTER TABLE t MODIFY COLUMN n varchar(40);"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := safety.AssessSQL(c.Context(), must.Must(builtin.New()), test.target, []string{test.statement})

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.HasLen, 1)
			c.Assert(got[0].Severity, qt.Equals, safety.Safe)
		})
	}
}

// TestColumnVerdicts_ACollationChange carries the node's reading to a planned
// statement: the words of a TYPE or MODIFY clause cannot tell a changed
// collation from one the column keeps, and the node can. A collation that
// changes with the type is left to the type's reading.
func TestColumnVerdicts_ACollationChange(t *testing.T) {
	column := ast.NewColumn("n", "text").SetCollate("C")
	column.Nullable = true

	t.Run("the collation alone", func(t *testing.T) {
		c := qt.New(t)

		verdicts, err := safety.ColumnVerdicts(
			[]ast.Node{modifyColumn(&ast.ModifyColumnOperation{Column: column, Changed: ast.ColumnProperties{Collation: true}, HasChanged: true})},
			[]int{0}, []string{`ALTER TABLE "t" ALTER COLUMN "n" TYPE text COLLATE "C"`},
		)

		c.Assert(err, qt.IsNil)
		c.Assert(verdicts, qt.DeepEquals, []safety.StatementAssessment{{Severity: safety.Warning, Reason: collationChangeReason}})
	})
	t.Run("with the type", func(t *testing.T) {
		c := qt.New(t)

		verdicts, err := safety.ColumnVerdicts(
			[]ast.Node{modifyColumn(&ast.ModifyColumnOperation{
				Column: column, PreviousType: "varchar(20)", Changed: ast.ColumnProperties{Collation: true, Type: true}, HasChanged: true,
			})},
			[]int{0}, []string{`ALTER TABLE "t" ALTER COLUMN "n" TYPE text COLLATE "C"`},
		)

		c.Assert(err, qt.IsNil)
		c.Assert(verdicts, qt.DeepEquals, []safety.StatementAssessment{{}})
	})
}
