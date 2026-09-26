package safety_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/planner"
	"ptah.run/migration/safety"
	"ptah.run/migration/schemadiff/difftypes"
)

const (
	modifyNotNull      = "MODIFY or CHANGE ... NOT NULL fails when a row holds NULL; outside strict SQL mode MySQL and MariaDB rewrite the NULL to the type's zero value instead"
	alterColumnNotNull = "ALTER COLUMN ... NOT NULL fails when a row holds NULL"
	novalidateNotNull  = "NOT NULL ENABLE NOVALIDATE keeps the NULL rows the table holds and refuses new ones"
	safeReason         = "does not remove data or tighten constraints"
)

// A statement that restates a column NOT NULL is a warning in each dialect's
// spelling, with the reason its engine gives for it. Measured over a NULL row:
// MySQL 26.7 answers error 1138 and MariaDB 12.3 error 1265 under strict SQL
// mode, and both rewrite the NULL to 0 or the empty string without it; SQL
// Server 2022 answers Msg 515; Oracle 23.26 answers ORA-02296 for every form
// but NOVALIDATE, which keeps the NULL. Read by its words alone, each of these
// reported "does not remove data or tighten constraints" (stokaro/ptah#3669).
func TestAssessSQL_RestatedNotNull(t *testing.T) {
	tests := []struct {
		name       string
		statement  string
		wantReason string
	}{
		{name: "MySQL MODIFY COLUMN", statement: "ALTER TABLE `t` MODIFY COLUMN `c` INT NOT NULL", wantReason: modifyNotNull},
		{name: "MySQL MODIFY with a default", statement: "ALTER TABLE t MODIFY c INT NOT NULL DEFAULT 9", wantReason: modifyNotNull},
		{name: "MySQL CHANGE COLUMN", statement: "ALTER TABLE t CHANGE COLUMN c c INT NOT NULL", wantReason: modifyNotNull},
		{name: "a MODIFY beside another clause", statement: "ALTER TABLE t MODIFY c INT NOT NULL, ADD COLUMN d INT", wantReason: modifyNotNull},
		{name: "SQL Server ALTER COLUMN", statement: "ALTER TABLE [t] ALTER COLUMN [c] INT NOT NULL", wantReason: alterColumnNotNull},
		{
			name:       "SQL Server ALTER COLUMN after a plan comment",
			statement:  "-- Modify column t.c: nullable: true -> false\nALTER TABLE [t] ALTER COLUMN [c] INT NOT NULL",
			wantReason: alterColumnNotNull,
		},
		{name: "Oracle MODIFY with a type", statement: "ALTER TABLE t MODIFY (c NUMBER(10) NOT NULL)", wantReason: modifyNotNull},
		{name: "Oracle MODIFY without one", statement: "ALTER TABLE t MODIFY c NOT NULL", wantReason: modifyNotNull},
		{name: "Oracle MODIFY of two columns", statement: "ALTER TABLE t MODIFY (a NUMBER(10), b NUMBER(10) NOT NULL)", wantReason: modifyNotNull},
		{name: "Oracle DEFAULT ON NULL", statement: "ALTER TABLE t MODIFY (f DEFAULT ON NULL 9)", wantReason: modifyNotNull},
		{name: "Oracle NOVALIDATE", statement: "ALTER TABLE t MODIFY (e NOT NULL ENABLE NOVALIDATE)", wantReason: novalidateNotNull},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := safety.AssessSQL(test.statement)

			c.Assert(got.Severity, qt.Equals, safety.Warning)
			c.Assert(got.Reason, qt.Equals, test.wantReason)
		})
	}
}

// The words NOT NULL outside a restated column definition make nothing NOT
// NULL: in a CHECK, a string literal, a comment, a clause that adds a column,
// or a statement that is not an ALTER TABLE. SQL Server's ALTER COLUMN with
// NULL, and PostgreSQL's ALTER COLUMN ... TYPE, restate no NOT NULL either.
func TestAssessSQL_NotNullWordsThatRestateNothing(t *testing.T) {
	tests := []struct {
		name      string
		statement string
	}{
		{name: "a CHECK testing IS NOT NULL", statement: "ALTER TABLE t MODIFY c INT CHECK (c IS NOT NULL)"},
		{name: "a string literal", statement: "ALTER TABLE t MODIFY c VARCHAR(10) COMMENT 'may be NOT NULL later'"},
		{name: "an unclosed string literal", statement: "ALTER TABLE t MODIFY c VARCHAR(10) COMMENT 'may be NOT NULL later"},
		{name: "a comment", statement: "-- NOT NULL\nALTER TABLE t MODIFY c INT"},
		{name: "NOT NULL in the clause that adds a column", statement: "ALTER TABLE t ADD COLUMN d INT NOT NULL, MODIFY c INT"},
		{name: "SQL Server ALTER COLUMN to NULL", statement: "ALTER TABLE [t] ALTER COLUMN [c] INT NULL"},
		{name: "MySQL ALTER COLUMN SET DEFAULT", statement: "ALTER TABLE t ALTER COLUMN c SET DEFAULT 5"},
		{name: "PostgreSQL ALTER COLUMN TYPE", statement: `ALTER TABLE "t" ALTER COLUMN "c" TYPE bigint`},
		{name: "CREATE TABLE", statement: "CREATE TABLE t (c INT NOT NULL)"},
		{name: "CREATE TABLE of a table named modify", statement: "CREATE TABLE modify (c INT NOT NULL)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := safety.AssessSQL(test.statement)

			c.Assert(got.Severity, qt.Equals, safety.Safe)
			c.Assert(got.Reason, qt.Equals, safeReason)
		})
	}
}

// singleColumnChange is a diff that changes column c of "flags" once.
func singleColumnChange(desired schemamodel.Field, changes map[string]string) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
		TableName:       "flags",
		ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "c", Desired: desired, Changes: changes}},
	}}}
}

// judgedStatements answers the report on the plan for diff, without the
// comment nodes the plan carries.
func judgedStatements(c *qt.C, diff *difftypes.SchemaDiff, dialect string) []judgedStatement {
	c.Helper()
	nodes, err := planner.GenerateSchemaDiffASTWithOptions(diff, dialect, planner.Options{})
	c.Assert(err, qt.IsNil)
	assessments, err := safety.AssessRendered(nodes, dialect)
	c.Assert(err, qt.IsNil)
	out := make([]judgedStatement, 0, len(assessments))
	for _, assessment := range assessments {
		out = append(out, judgedStatement{Severity: assessment.Severity, Reason: assessment.Reason, NodeType: assessment.NodeType})
	}
	return slices.DeleteFunc(out, func(judged judgedStatement) bool { return judged.NodeType == "*ast.CommentNode" })
}

type judgedStatement struct {
	Severity safety.Severity
	Reason   string
	NodeType string
}

// The plan the MySQL-family planner writes for a column change restates the
// whole column, and repeats NOT NULL on a column that already had it. The
// report reads which properties changed from the operation, so a restated NOT
// NULL is judged only when nullability is what changed: a type change reports
// the type, and a default change on a NOT NULL column is safe, where reading
// the restated NOT NULL called it a statement that can fail on a NULL row.
func TestAssessRendered_RestatedNotNullIsJudgedByWhatChanged(t *testing.T) {
	notNull := schemamodel.Field{Name: "c", Type: "INTEGER", StructName: "Flag"}
	widened := schemamodel.Field{Name: "c", Type: "BIGINT", StructName: "Flag"}
	defaulted := schemamodel.Field{Name: "c", Type: "INTEGER", StructName: "Flag", Default: "9"}
	nullable := schemamodel.Field{Name: "c", Type: "INTEGER", StructName: "Flag", Nullable: true}
	const typeReason = "column type changes from int to BIGINT"
	const dropNotNull = "DROP NOT NULL removes a column-level data protection"
	tests := []struct {
		name     string
		dialect  string
		desired  schemamodel.Field
		changes  map[string]string
		severity safety.Severity
		reason   string
	}{
		{name: "MySQL, NOT NULL gained", dialect: platform.MySQL, desired: notNull, changes: map[string]string{"nullable": "true -> false"}, severity: safety.Warning, reason: modifyNotNull},
		{name: "MySQL, a type changed", dialect: platform.MySQL, desired: widened, changes: map[string]string{"type": "int -> bigint"}, severity: safety.Warning, reason: typeReason},
		{name: "MySQL, a default changed", dialect: platform.MySQL, desired: defaulted, changes: map[string]string{"default_expr": " -> 9"}, severity: safety.Safe, reason: safeReason},
		{name: "MySQL, NOT NULL dropped", dialect: platform.MySQL, desired: nullable, changes: map[string]string{"nullable": "false -> true"}, severity: safety.Destructive, reason: dropNotNull},
		{name: "MariaDB, NOT NULL gained", dialect: platform.MariaDB, desired: notNull, changes: map[string]string{"nullable": "true -> false"}, severity: safety.Warning, reason: modifyNotNull},
		{name: "SQL Server, NOT NULL gained", dialect: platform.SQLServer, desired: notNull, changes: map[string]string{"nullable": "true -> false"}, severity: safety.Warning, reason: alterColumnNotNull},
		{name: "SQL Server, a type changed", dialect: platform.SQLServer, desired: widened, changes: map[string]string{"type": "int -> bigint"}, severity: safety.Warning, reason: typeReason},
		{name: "SQL Server, NOT NULL dropped", dialect: platform.SQLServer, desired: nullable, changes: map[string]string{"nullable": "false -> true"}, severity: safety.Destructive, reason: dropNotNull},
		{name: "Oracle, NOT NULL gained", dialect: platform.Oracle, desired: notNull, changes: map[string]string{"nullable": "true -> false"}, severity: safety.Warning, reason: modifyNotNull},
		{name: "Oracle, a default changed", dialect: platform.Oracle, desired: defaulted, changes: map[string]string{"default_expr": " -> 9"}, severity: safety.Safe, reason: safeReason},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := judgedStatements(c, singleColumnChange(test.desired, test.changes), test.dialect)

			c.Assert(got, qt.DeepEquals, []judgedStatement{{Severity: test.severity, Reason: test.reason, NodeType: "*ast.AlterTableNode"}})
		})
	}
}

// In one ALTER TABLE carrying several column restatements, as SQL Server
// spells them, each restatement takes the node's verdict as a MySQL MODIFY
// does. Read by its words alone, the ALTER COLUMN that drops NOT NULL reported
// safe. The node's verdict is the most severe of its operations, so a sibling
// statement is reported at least as severe; Ptah's planners write one
// modification per ALTER TABLE, which keeps that to hand-built nodes.
func TestAssessRendered_SQLServerRestatementsTakeTheNodeVerdict(t *testing.T) {
	c := qt.New(t)
	node := &ast.AlterTableNode{Name: "flags", Operations: []ast.AlterOperation{
		&ast.ModifyColumnOperation{Column: ast.NewColumn("a", "INT").SetNotNull(), PreviousNullable: true, HasPreviousNullable: true},
		&ast.ModifyColumnOperation{Column: ast.NewColumn("b", "INT"), PreviousNullable: false, HasPreviousNullable: true},
	}}

	assessments, err := safety.AssessRendered([]ast.Node{node}, platform.SQLServer)

	c.Assert(err, qt.IsNil)
	c.Assert(assessments, qt.HasLen, 2)
	c.Assert(assessments[0].Statement, qt.Equals, "ALTER TABLE [flags] ALTER COLUMN [a] INT NOT NULL")
	c.Assert(assessments[1].Statement, qt.Equals, "ALTER TABLE [flags] ALTER COLUMN [b] INT NULL")
	c.Assert(assessments[1].Severity, qt.Equals, safety.Destructive)
	c.Assert(assessments[1].Reason, qt.Equals, "DROP NOT NULL removes a column-level data protection")
	c.Assert(assessments[0].Severity, qt.Equals, safety.Destructive)
}

// A PostgreSQL ALTER COLUMN that changes one property restates nothing, so it
// keeps the verdict its own words give and does not take the node's: in a type
// and default change the TYPE clause reports the type, and the SET DEFAULT
// after it stays safe.
func TestAssessRendered_PostgreSQLSingleClausesAreNotRestatements(t *testing.T) {
	c := qt.New(t)
	node := &ast.AlterTableNode{Name: "flags", Operations: []ast.AlterOperation{&ast.ModifyColumnOperation{
		Column:       ast.NewColumn("c", "BIGINT").SetDefault("9"),
		PreviousType: "int",
		Changed:      ast.ColumnProperties{Type: true, Default: true},
		HasChanged:   true,
	}}}

	assessments, err := safety.AssessRendered([]ast.Node{node}, platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(assessments, qt.HasLen, 2)
	c.Assert(assessments[0].Reason, qt.Equals, "column type changes from int to BIGINT")
	c.Assert(assessments[1].Statement, qt.Equals, `ALTER TABLE "flags" ALTER COLUMN "c" SET DEFAULT 9`)
	c.Assert(assessments[1].Severity, qt.Equals, safety.Safe)
	c.Assert(assessments[1].Reason, qt.Equals, safeReason)
}
