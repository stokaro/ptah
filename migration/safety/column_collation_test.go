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
const setsCollationReason = "the statement sets a column's collation, which may change it: a changed collation " +
	"rebuilds every index on the column and fails where a unique key holds values the new collation finds equal"

// keptCollationReason is the reason such a statement carries where the node
// that rendered it keeps the collation.
const keptCollationReason = "the statement restates the collation the column holds"

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
// a target writes. The words cannot say whether the collation changes, so the
// reason says it may.
func TestAssessSQL_AStatementThatSetsACollation(t *testing.T) {
	tests := []struct {
		name      string
		target    string
		statement string
	}{
		{name: "a PostgreSQL TYPE with COLLATE", target: "postgres", statement: `ALTER TABLE "t" ALTER COLUMN "n" TYPE text COLLATE "C";`},
		{name: "SET DATA TYPE", target: "postgres", statement: `ALTER TABLE t ALTER COLUMN n SET DATA TYPE text COLLATE "POSIX";`},
		{name: "a MySQL MODIFY", target: "mysql", statement: "ALTER TABLE `t` MODIFY COLUMN `n` varchar(20) COLLATE utf8mb4_general_ci;"},
		{name: "ALTER without COLUMN", target: "postgres", statement: `ALTER TABLE t ALTER n TYPE text COLLATE "C";`},
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
// holds rows, a TYPE without one says nothing of the collation, and the word
// inside a string literal is not a COLLATE clause.
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
		{name: "a COLLATE in a COMMENT", target: "mysql", statement: "ALTER TABLE `t` MODIFY COLUMN `a` varchar(30) COMMENT 'sort COLLATE x';"},
		{name: "a COLLATE in a DEFAULT", target: "mysql", statement: "ALTER TABLE `t` MODIFY COLUMN `a` varchar(30) DEFAULT 'p COLLATE q';"},
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

// restatedCollations are column modifications that write the COLLATE the
// column holds: a MySQL MODIFY that widens the column or adds NOT NULL, and a
// PostgreSQL TYPE clause that widens a column of collation "C". Each node
// states what it changes, and the collation is not among it.
func restatedCollations() []struct {
	name, target string
	node         *ast.AlterTableNode
	statement    string
} {
	widened := ast.NewColumn("a", "varchar(30)").SetCollate("utf8mb4_bin")
	required := ast.NewColumn("a", "varchar(20)").SetCollate("utf8mb4_bin")
	required.Nullable = false
	sku := ast.NewColumn("sku", "varchar(40)").SetCollate("C")
	return []struct {
		name, target string
		node         *ast.AlterTableNode
		statement    string
	}{
		{
			name: "a MySQL MODIFY that widens", target: "mysql",
			node: modifyColumn(&ast.ModifyColumnOperation{
				Column: widened, PreviousType: "varchar(20)", Changed: ast.ColumnProperties{Type: true}, HasChanged: true,
			}),
			statement: "ALTER TABLE `t` MODIFY COLUMN `a` varchar(30) COLLATE utf8mb4_bin",
		},
		{
			name: "a MySQL MODIFY that adds NOT NULL", target: "mysql",
			node: modifyColumn(&ast.ModifyColumnOperation{
				Column: required, PreviousNullable: true, HasPreviousNullable: true,
				Changed: ast.ColumnProperties{Nullability: true}, HasChanged: true,
			}),
			statement: "ALTER TABLE `t` MODIFY COLUMN `a` varchar(20) COLLATE utf8mb4_bin NOT NULL",
		},
		{
			name: "a PostgreSQL TYPE that widens", target: "postgres",
			node: modifyColumn(&ast.ModifyColumnOperation{
				Column: sku, PreviousType: "varchar(20)", Changed: ast.ColumnProperties{Type: true}, HasChanged: true,
			}),
			statement: `ALTER TABLE "t" ALTER COLUMN "sku" TYPE varchar(40) COLLATE "C"`,
		},
	}
}

// TestAssessRendered_ARestatedCollation rates a planned column modification
// that writes the collation the column holds by what its node changes: the
// type or the nullability, and never the collation.
func TestAssessRendered_ARestatedCollation(t *testing.T) {
	wants := map[string]string{
		"a MySQL MODIFY that widens": "column type changes from varchar(20) to varchar(30)",
		"a MySQL MODIFY that adds NOT NULL": "MODIFY or CHANGE ... NOT NULL fails when a row holds NULL; outside strict SQL mode " +
			"MySQL and MariaDB rewrite the NULL to the type's zero value instead",
		"a PostgreSQL TYPE that widens": "column type changes from varchar(20) to varchar(40)",
	}
	for _, test := range restatedCollations() {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := safety.AssessRendered(c.Context(), must.Must(builtin.New()), []ast.Node{test.node}, test.target)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.HasLen, 1)
			c.Assert(got[0].Statement, qt.Equals, test.statement)
			c.Assert(got[0].Reason, qt.Equals, wants[test.name])
		})
	}
}

// TestColumnVerdicts_ARestatedCollation folds the node's reading into the
// rating of the planned statement's words, as plan and apply do: the words
// alone rate the COLLATE as a collation that may change, and the node says it
// is the one the column holds. A NOT NULL the words rate on their own keeps
// its reason.
func TestColumnVerdicts_ARestatedCollation(t *testing.T) {
	wants := map[string]safety.StatementAssessment{
		"a MySQL MODIFY that widens": {Severity: safety.Safe, Reason: keptCollationReason},
		"a MySQL MODIFY that adds NOT NULL": {Severity: safety.Warning,
			Reason: "MODIFY or CHANGE ... NOT NULL fails when a row holds NULL; outside strict SQL mode " +
				"MySQL and MariaDB rewrite the NULL to the type's zero value instead"},
		"a PostgreSQL TYPE that widens": {Severity: safety.Safe, Reason: keptCollationReason},
	}
	for _, test := range restatedCollations() {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			assessed, err := safety.AssessSQL(c.Context(), must.Must(builtin.New()), test.target, []string{test.statement})
			c.Assert(err, qt.IsNil)
			c.Assert(assessed, qt.HasLen, 1)

			verdicts, err := safety.ColumnVerdicts([]ast.Node{test.node}, []int{0}, []string{test.statement})
			c.Assert(err, qt.IsNil)
			got := assessed[0]
			safety.Fold(&got, verdicts[0])

			c.Assert(safety.StatementAssessment{Severity: got.Severity, Reason: got.Reason}, qt.DeepEquals, wants[test.name])
		})
	}
}

// TestColumnVerdicts_AChangedCollationTakesTheNodesReason is the inverse: a
// statement whose node changes the collation alone keeps the warning, with
// the node's reason in place of the one the words give.
func TestColumnVerdicts_AChangedCollationTakesTheNodesReason(t *testing.T) {
	c := qt.New(t)
	column := ast.NewColumn("a", "varchar(20)").SetCollate("utf8mb4_general_ci")
	node := modifyColumn(&ast.ModifyColumnOperation{Column: column, Changed: ast.ColumnProperties{Collation: true}, HasChanged: true})
	statement := "ALTER TABLE `t` MODIFY COLUMN `a` varchar(20) COLLATE utf8mb4_general_ci"
	assessed, err := safety.AssessSQL(c.Context(), must.Must(builtin.New()), "mysql", []string{statement})
	c.Assert(err, qt.IsNil)
	c.Assert(assessed, qt.HasLen, 1)
	c.Assert(assessed[0].Reason, qt.Equals, setsCollationReason)

	verdicts, err := safety.ColumnVerdicts([]ast.Node{node}, []int{0}, []string{statement})
	c.Assert(err, qt.IsNil)
	got := assessed[0]
	safety.Fold(&got, verdicts[0])

	c.Assert(safety.StatementAssessment{Severity: got.Severity, Reason: got.Reason}, qt.DeepEquals,
		safety.StatementAssessment{Severity: safety.Warning, Reason: collationChangeReason})
}
