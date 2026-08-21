package oracle_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/renderer"
)

// render is the whole path a caller takes, so a defect in the dispatch is a
// failure here rather than a test that passes against a renderer nothing
// reaches.
func render(c *qt.C, caps capability.Capabilities, nodes ...ast.Node) string {
	c.Helper()
	sql, err := renderer.RenderSQLWithCapabilities(platform.Oracle, caps, nodes...)
	c.Assert(err, qt.IsNil)
	return sql
}

func renderErr(c *qt.C, caps capability.Capabilities, nodes ...ast.Node) error {
	c.Helper()
	_, err := renderer.RenderSQLWithCapabilities(platform.Oracle, caps, nodes...)
	return err
}

// TestCreateTable_IsAcceptedByBothMeasuredLines pins the exact text both live
// servers accepted.
//
// The statement below is byte-for-byte what `ptah schema render --dialect
// oracle` produced for this table and what Oracle 23.26.2.0.0 and 21.3.0.0.0
// each created without error, with user_tab_columns reading the columns back
// afterwards (stokaro/ptah#1875).
func TestCreateTable_IsAcceptedByBothMeasuredLines(t *testing.T) {
	c := qt.New(t)

	table := &ast.CreateTableNode{
		Name: "ora_authors",
		Columns: []*ast.ColumnNode{
			{Name: "id", Type: "INT", Primary: true},
			{Name: "name", Type: "VARCHAR(200)"},
			{Name: "email", Type: "VARCHAR(255)", Unique: true},
			{Name: "bio", Type: "TEXT", Nullable: true},
			{Name: "is_active", Type: "BOOLEAN", Default: &ast.DefaultValue{Expression: "1"}},
			{Name: "rating", Type: "DECIMAL(5,2)", Nullable: true},
		},
	}

	c.Assert(render(c, capability.Oracle23(), table), qt.Equals, `CREATE TABLE ora_authors (
  id NUMBER(10) NOT NULL PRIMARY KEY,
  name VARCHAR2(200) NOT NULL,
  email VARCHAR2(255) NOT NULL UNIQUE,
  bio CLOB,
  is_active NUMBER(1) DEFAULT 1 NOT NULL,
  rating NUMBER(5,2)
);
`)
}

// TestObjectGuards_FollowTheMeasuredVersionStep is the difference between the
// two presets, and it is load-bearing rather than cosmetic.
//
// Measured: the guarded render applied to 21.3 answers ORA-00969, missing ON
// keyword, on both index statements, while the unguarded render of the same
// schema is accepted whole on the same server.
func TestObjectGuards_FollowTheMeasuredVersionStep(t *testing.T) {
	c := qt.New(t)

	index := &ast.IndexNode{Name: "idx_ora_posts_title", Table: "ora_posts", Unique: true, IfNotExists: true,
		Columns: []string{"title"}}
	drop := &ast.DropTableNode{Name: "ora_posts", IfExists: true}

	c.Assert(render(c, capability.Oracle23(), index), qt.Equals,
		"CREATE UNIQUE INDEX IF NOT EXISTS idx_ora_posts_title ON ora_posts (title);\n")
	c.Assert(render(c, capability.Oracle21(), index), qt.Equals,
		"CREATE UNIQUE INDEX idx_ora_posts_title ON ora_posts (title);\n")

	c.Assert(render(c, capability.Oracle23(), drop), qt.Equals, "DROP TABLE IF EXISTS ora_posts;\n")
	c.Assert(render(c, capability.Oracle21(), drop), qt.Equals, "DROP TABLE ora_posts;\n")
}

// TestIdentifiers_AreBareUntilOracleRefusesThemBare holds the decision that
// separates this renderer from every other one here.
//
// A plain name is written without quotes so that a CHECK or a generated
// expression naming it bare refers to the same column; a reserved word is
// quoted because there is no other way to express it. Measured on 23.26, the
// mixed form is refused: a quoted column with a bare reference in its own CHECK
// answers ORA-02438, and at table level ORA-00904.
func TestIdentifiers_AreBareUntilOracleRefusesThemBare(t *testing.T) {
	c := qt.New(t)

	table := &ast.CreateTableNode{
		Name: "ora_events",
		Columns: []*ast.ColumnNode{
			{Name: "kind", Type: "VARCHAR(40)"},
			{Name: "size", Type: "INT"},
			{Name: "mixedCase", Type: "INT"},
			{Name: "with-dash", Type: "INT"},
		},
	}

	c.Assert(render(c, capability.Oracle23(), table), qt.Equals, `CREATE TABLE ora_events (
  kind VARCHAR2(40) NOT NULL,
  "size" NUMBER(10) NOT NULL,
  mixedCase NUMBER(10) NOT NULL,
  "with-dash" NUMBER(10) NOT NULL
);
`)
}

// TestQuotedColumn_RefusesABareReferenceInItsOwnExpression converts the
// server's least useful error into one that names the column.
//
// Measured on 23.26: `"size" NUMBER(10), doubled NUMBER(10) GENERATED ALWAYS AS
// (size * 2) VIRTUAL` answers ORA-00936, missing expression -- which names
// neither the column nor the reason.
func TestQuotedColumn_RefusesABareReferenceInItsOwnExpression(t *testing.T) {
	c := qt.New(t)

	bare := &ast.CreateTableNode{
		Name: "ora_events",
		Columns: []*ast.ColumnNode{
			{Name: "size", Type: "INT"},
			{Name: "doubled", Type: "INT", Nullable: true, GeneratedExpression: "size * 2"},
		},
	}
	err := renderErr(c, capability.Oracle23(), bare)
	c.Assert(err, qt.ErrorMatches, `.*column "size" of table "ora_events" needs quoting in Oracle.*write "size" in the expression.*`)

	// The quoted form is what 23.26 accepted, so the refusal above is about
	// the spelling rather than about the feature.
	quoted := &ast.CreateTableNode{
		Name: "ora_events",
		Columns: []*ast.ColumnNode{
			{Name: "size", Type: "INT"},
			{Name: "doubled", Type: "INT", Nullable: true, GeneratedExpression: `"size" * 2`},
		},
	}
	c.Assert(render(c, capability.Oracle23(), quoted), qt.Contains, `GENERATED ALWAYS AS ("size" * 2) VIRTUAL`)

	// A plain column is not affected: nothing is quoted, so nothing disagrees.
	plain := &ast.CreateTableNode{
		Name: "ora_posts",
		Columns: []*ast.ColumnNode{
			{Name: "view_count", Type: "INT", Check: "view_count >= 0"},
		},
	}
	c.Assert(render(c, capability.Oracle23(), plain), qt.Contains, "CHECK (view_count >= 0)")
}

// TestSerial_BecomesAnIdentityColumn holds the half of SERIAL that is a clause
// rather than a width.
//
// Rendering the width alone produced `id NUMBER(10) PRIMARY KEY` for a column
// PostgreSQL fills by itself: the table is created, the migration reports
// success, and the first INSERT that omits the key fails instead.
func TestSerial_BecomesAnIdentityColumn(t *testing.T) {
	c := qt.New(t)

	for _, declared := range []string{"SERIAL", "BIGSERIAL", "SMALLSERIAL"} {
		table := &ast.CreateTableNode{
			Name:    "ora_events",
			Columns: []*ast.ColumnNode{{Name: "id", Type: declared, Primary: true}},
		}
		c.Assert(render(c, capability.Oracle23(), table), qt.Contains, "GENERATED BY DEFAULT AS IDENTITY",
			qt.Commentf("declared type %q", declared))
	}
}

// TestIdentity_RefusesASecondGeneratedColumn measures the same rule the
// renderer's guard states, and it exists because the guard and the emitter once
// asked different questions: the guard read AutoInc while the emitter also
// treated SERIAL as generated, so this table passed the guard and answered
// ORA-30669 from the server.
func TestIdentity_RefusesASecondGeneratedColumn(t *testing.T) {
	c := qt.New(t)

	mixed := &ast.CreateTableNode{
		Name: "ora_two",
		Columns: []*ast.ColumnNode{
			{Name: "id", Type: "SERIAL", Primary: true},
			{Name: "seq", Type: "INT", AutoInc: true},
		},
	}
	err := renderErr(c, capability.Oracle23(), mixed)
	c.Assert(err, qt.ErrorMatches, `.*table "ora_two" declares 2 auto-increment columns \(id, seq\) and Oracle allows one per table.*`)

	single := &ast.CreateTableNode{
		Name:    "ora_one",
		Columns: []*ast.ColumnNode{{Name: "id", Type: "SERIAL", Primary: true}, {Name: "seq", Type: "INT"}},
	}
	c.Assert(renderErr(c, capability.Oracle23(), single), qt.IsNil)
}

// TestAlterTable_UsesOraclesOwnClauseNames pins the three spellings the server
// refuses in the shapes every other dialect here writes: ADD COLUMN is
// ORA-03050, ALTER COLUMN ... TYPE is ORA-01735, and DROP CONSTRAINT IF EXISTS
// is ORA-01735.
func TestAlterTable_UsesOraclesOwnClauseNames(t *testing.T) {
	c := qt.New(t)

	alter := &ast.AlterTableNode{
		Name: "ora_posts",
		Operations: []ast.AlterOperation{
			&ast.AddColumnOperation{Column: &ast.ColumnNode{Name: "slug", Type: "VARCHAR(80)", Nullable: true}},
			&ast.ModifyColumnOperation{Column: &ast.ColumnNode{Name: "title", Type: "VARCHAR(300)"}},
			&ast.RenameColumnOperation{OldName: "body", NewName: "content"},
			&ast.DropColumnOperation{ColumnName: "payload"},
			&ast.DropConstraintOperation{ConstraintName: "fk_post_author", IfExists: true},
		},
	}

	c.Assert(render(c, capability.Oracle23(), alter), qt.Equals, `ALTER TABLE ora_posts ADD (slug VARCHAR2(80));
ALTER TABLE ora_posts MODIFY (title VARCHAR2(300) NOT NULL);
ALTER TABLE ora_posts RENAME COLUMN body TO content;
ALTER TABLE ora_posts DROP COLUMN payload;
ALTER TABLE ora_posts DROP CONSTRAINT fk_post_author;
`)
}

// TestBooleanDefaultLiteralIsNumeric pins the default beside a BOOLEAN column,
// which maps to NUMBER(1) here.
//
// It uses a declared literal rather than an Expression, because the two take
// different arms and only the literal arm quotes. A test that sets Expression
// exercises the pass-through and says nothing about the arm that renders a
// declared default.
//
// Measured on 23.26 -- Oracle converts a quoted number implicitly, so a quoted
// numeric default reaches a created table, and only the boolean spelling is
// refused:
//
//	qty NUMBER(10) DEFAULT '5'      accepted
//	flag NUMBER(1) DEFAULT 'false'  ORA-01722, unable to convert string value containing 'f'
func TestBooleanDefaultLiteralIsNumeric(t *testing.T) {
	c := qt.New(t)

	table := &ast.CreateTableNode{
		Name: "ora_flags",
		Columns: []*ast.ColumnNode{
			{Name: "off_flag", Type: "BOOLEAN", Default: &ast.DefaultValue{Value: "false"}, Nullable: true},
			{Name: "on_flag", Type: "BOOL", Default: &ast.DefaultValue{Value: "true"}, Nullable: true},
			// Not a boolean: the literal stays a literal, because Oracle takes
			// it and rewriting defaults that already work is not this fix.
			{Name: "note", Type: "TEXT", Default: &ast.DefaultValue{Value: "none"}, Nullable: true},
		},
	}

	c.Assert(render(c, capability.Oracle23(), table), qt.Equals, `CREATE TABLE ora_flags (
  off_flag NUMBER(1) DEFAULT 0,
  on_flag NUMBER(1) DEFAULT 1,
  note CLOB DEFAULT 'none'
);
`)
}

// TestModifyColumn_WritesNullabilityOnlyWhenItChanges pins when MODIFY states
// nullability at all.
//
// Oracle's nullability change is not idempotent, and a bare MODIFY is not a
// no-op that fails -- it is a no-op that succeeds. Measured on 23.26 against a
// VARCHAR2 column declared NOT NULL:
//
//	start                              nullable = N
//	MODIFY (note VARCHAR2(200))        accepted, nullable = N   <- unchanged
//	MODIFY (note VARCHAR2(200) NULL)   accepted, nullable = Y
//
// so the relaxing direction has to spell the clause, and the unchanged
// direction must not: MODIFY (n NOT NULL) on a column already NOT NULL answers
// ORA-01442 and MODIFY (n NULL) on one already nullable answers ORA-01451.
//
// Worth knowing beside this: a CLOB column refuses both forms with ORA-22859,
// invalid modification of columns, so a TEXT column -- which maps to CLOB --
// cannot have its nullability changed by MODIFY at all. That is an engine limit
// rather than a rendering choice, and it is why this test uses VARCHAR2.
func TestModifyColumn_WritesNullabilityOnlyWhenItChanges(t *testing.T) {
	tests := []struct {
		name     string
		op       *ast.ModifyColumnOperation
		contains string
		absent   string
	}{
		{
			name: "relaxing states NULL",
			op: &ast.ModifyColumnOperation{
				Column:              &ast.ColumnNode{Name: "note", Type: "VARCHAR(200)", Nullable: true},
				HasPreviousNullable: true, PreviousNullable: false,
			},
			contains: "NULL", absent: "NOT NULL",
		},
		{
			name: "tightening states NOT NULL",
			op: &ast.ModifyColumnOperation{
				Column:              &ast.ColumnNode{Name: "note", Type: "VARCHAR(200)"},
				HasPreviousNullable: true, PreviousNullable: true,
			},
			contains: "NOT NULL", absent: "",
		},
		{
			name: "unchanged NOT NULL states neither, because restating it is ORA-01442",
			op: &ast.ModifyColumnOperation{
				Column:              &ast.ColumnNode{Name: "note", Type: "VARCHAR(200)"},
				HasPreviousNullable: true, PreviousNullable: false,
			},
			contains: "VARCHAR2(200)", absent: "NULL",
		},
		{
			name: "unchanged nullable states neither, because restating it is ORA-01451",
			op: &ast.ModifyColumnOperation{
				Column:              &ast.ColumnNode{Name: "note", Type: "VARCHAR(200)", Nullable: true},
				HasPreviousNullable: true, PreviousNullable: true,
			},
			contains: "VARCHAR2(200)", absent: "NULL",
		},
		{
			name: "an unknown previous states the target, which is what a fresh plan needs",
			op: &ast.ModifyColumnOperation{
				Column: &ast.ColumnNode{Name: "note", Type: "VARCHAR(200)"},
			},
			contains: "NOT NULL", absent: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			alter := &ast.AlterTableNode{Name: "t", Operations: []ast.AlterOperation{tt.op}}

			rendered := render(c, capability.Oracle23(), alter)

			c.Assert(rendered, qt.Contains, tt.contains)
			c.Assert(strings.Contains(rendered, tt.absent) && tt.absent != "", qt.IsFalse,
				qt.Commentf("rendered %q must not carry %q", rendered, tt.absent))
		})
	}
}
