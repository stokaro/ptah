package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff/internal/compare"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// TestConstraintsWithSemantics_ANameThatDiffersOnlyInCase pins which engines
// resolve a constraint name case-insensitively, and keeps the one that does not
// as the control.
//
// Measured live on MySQL 8.4.11 and MariaDB 11.4.12. A constraint created as
// `UQ_A`:
//
//	information_schema.table_constraints            -> UQ_A
//	ALTER TABLE u DROP INDEX uq_a                   -> accepted
//	ALTER TABLE u ADD CONSTRAINT uq_a UNIQUE (b)    -> ERROR 1061: Duplicate key name 'uq_a'
//
// The third line is the decisive one: the two spellings cannot coexist, so they
// are one namespace entry. The comparator keyed the name with the rule for a
// COLUMN, which on those two dialects preserves case, so a declaration writing
// the name in lower case and a catalog reporting it in upper case were two
// objects -- a drop and an add planned for a constraint nobody had touched, on
// every run (stokaro/ptah#2028).
//
// Measured on PostgreSQL 17.11, the control goes the other way: `"UQ_A"` and
// `uq_a` coexist on one table, because an upper-case name in that catalog was
// created quoted. There the two really are two objects and the removal must
// stay.
func TestConstraintsWithSemantics_ANameThatDiffersOnlyInCase(t *testing.T) {
	tests := []struct {
		name        string
		dialect     string
		wantAdded   []string
		wantRemoved []string
	}{
		{
			name: "mysql resolves it case-insensitively", dialect: platform.MySQL,
			wantAdded: noConstraints(), wantRemoved: noConstraints(),
		},
		{
			name: "mariadb does too", dialect: platform.MariaDB,
			wantAdded: noConstraints(), wantRemoved: noConstraints(),
		},
		{
			name:        "postgres keeps both, because the upper-case one was quoted",
			dialect:     platform.Postgres,
			wantAdded:   []string{"uq_widget_scope"},
			wantRemoved: []string{"UQ_WIDGET_SCOPE"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}

			compare.ConstraintsWithSemantics(
				caseFixtureDeclaration(),
				caseFixtureDatabase(),
				diff,
				nil,
				caseFixtureSemantics(test.dialect),
			)

			c.Assert(addedConstraintNames(diff), qt.DeepEquals, test.wantAdded)
			c.Assert(removedConstraintNames(diff), qt.DeepEquals, test.wantRemoved)
		})
	}
}

// caseFixtureDeclaration writes the constraint name in lower case, which is
// what an annotation or an HCL document normally carries.
func caseFixtureDeclaration() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{StructName: "Widget", Name: "widget"}},
		Fields: []goschema.Field{
			{StructName: "Widget", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Widget", Name: "scope", Type: "VARCHAR(50)"},
		},
		Constraints: []goschema.Constraint{{
			StructName: "Widget", Name: "uq_widget_scope", Table: "widget",
			Type: "UNIQUE", Columns: []string{"scope"},
		}},
	}
}

// caseFixtureDatabase reports the same constraint the way a server that was
// given an upper-case name reports it.
func caseFixtureDatabase() *types.DBSchema {
	return &types.DBSchema{
		Tables: []types.DBTable{{Name: "widget", Schema: "testdb"}},
		Constraints: []types.DBConstraint{{
			Name: "UQ_WIDGET_SCOPE", TableName: "widget", Schema: "testdb",
			Type: "UNIQUE", ColumnName: "scope", ColumnNames: []string{"scope"},
		}},
	}
}

// caseFixtureSemantics is the rule set a live connection supplies. The default
// schema is what a MySQL-family connection carries -- a schema there IS the
// database -- and PostgreSQL's own is the one its reads report.
func caseFixtureSemantics(dialect string) identifier.Semantics {
	semantics := identifier.ForDialect(dialect)
	semantics.DefaultSchema = "testdb"
	return semantics
}

// noConstraints is the empty answer, spelled as a value rather than as nil so
// a row saying "nothing changed" compares against what the helpers below build.
func noConstraints() []string {
	return make([]string, 0)
}

// addedConstraintNames and removedConstraintNames name what a comparison
// reported, in a form that says "nothing" as an empty list rather than as a nil
// one -- the two are the same fact and only one of them is what a diff happens
// to build.
func addedConstraintNames(diff *difftypes.SchemaDiff) []string {
	return append(make([]string, 0, len(diff.ConstraintsAdded)), diff.ConstraintsAdded...)
}

func removedConstraintNames(diff *difftypes.SchemaDiff) []string {
	return append(make([]string, 0, len(diff.ConstraintsRemoved)), diff.ConstraintsRemoved...)
}
