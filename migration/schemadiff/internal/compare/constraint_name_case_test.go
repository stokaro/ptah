package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
	"go.5x5.cz/ptah/migration/schemadiff/internal/compare"
)

// TestConstraints_NameCaseFollowsTheEngineThatResolvesIt pins which spellings of
// one constraint name are one object.
//
// A constraint name is neither a column nor an index, and the two rules that
// might fold it disagree on exactly two targets. Measured live: on `mysql:8.4`
// and `mariadb:11.4` a constraint created as `UQ_A` is dropped by
// `ALTER TABLE u DROP INDEX uq_a`, and one created as `FK_A` by
// `ALTER TABLE child DROP FOREIGN KEY fk_a`. The server resolves the name
// case-insensitively, so the two spellings name one constraint and there is
// nothing to add or remove.
//
// Keyed as a COLUMN they were two, and the comparison reported the declaration
// added and the catalog's removed -- a rebuild planned for a constraint nobody
// changed, and drift reported against a database that matches, on every run
// (stokaro/ptah#2028).
//
// PostgreSQL is the control, and it is not an oversight that it disagrees: an
// unquoted name is folded to lower case there, so an upper-case name in a
// catalog was created quoted and really is a different object.
func TestConstraints_NameCaseFollowsTheEngineThatResolvesIt(t *testing.T) {
	tests := []struct {
		name        string
		dialect     string
		wantAdded   []string
		wantRemoved []string
	}{
		{
			name:    "MySQL resolves the name case-insensitively",
			dialect: "mysql",
		},
		{
			name:    "MariaDB resolves the name case-insensitively",
			dialect: "mariadb",
		},
		{
			// The quoted upper-case name is its own object here.
			name:        "PostgreSQL keeps a quoted name apart",
			dialect:     "postgres",
			wantAdded:   []string{"uq_widget_scope"},
			wantRemoved: []string{"UQ_WIDGET_SCOPE"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}

			compare.ConstraintsWithSemantics(
				widgetDeclaringLowerCaseConstraint(),
				widgetReportingUpperCaseConstraint(),
				diff, nil, identifier.ForDialect(test.dialect),
			)

			c.Assert(diff.ConstraintsAdded, qt.DeepEquals, test.wantAdded)
			c.Assert(diff.ConstraintsRemoved, qt.DeepEquals, test.wantRemoved)
		})
	}
}

// widgetDeclaringLowerCaseConstraint names the constraint the way an annotation
// does.
func widgetDeclaringLowerCaseConstraint() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Widget", Name: "widget"}},
		Fields: []schemamodel.Field{
			{StructName: "Widget", Name: "id", Type: "int", Primary: true},
			{StructName: "Widget", Name: "tenant", Type: "text"},
		},
		Constraints: []schemamodel.Constraint{{
			StructName: "Widget", Name: "uq_widget_scope", Table: "widget",
			Type: "UNIQUE", Columns: []string{"tenant"},
		}},
	}
}

// widgetReportingUpperCaseConstraint is the same table as a catalog holding the
// constraint under the other spelling reports it.
func widgetReportingUpperCaseConstraint() *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{{Name: "widget", Columns: []catalog.Column{
			{Name: "id", DataType: "integer", IsPrimaryKey: true, IsNullable: "NO"},
			{Name: "tenant", DataType: "text", IsNullable: "NO"},
		}}},
		Constraints: []catalog.Constraint{{
			TableName: "widget", Name: "UQ_WIDGET_SCOPE",
			Type: "UNIQUE", ColumnNames: []string{"tenant"},
		}},
	}
}

// TestConstraints_ASynthesizedForeignKeyMatchesTheCatalogsSpelling keeps the
// two halves of one lookup on one rule.
//
// A field-level `Foreign:` declaration is synthesized into the generated set
// under a generated name -- `fk_<table>_<column>`, always lower case -- and the
// database side is then asked whether the catalog's foreign key is that
// synthesized one. The set is built in ConstraintsWithSemantics and read in
// isFieldLevelConstraint, so the two must fold the name the same way.
//
// Built under one rule and read under the other, a catalog reporting
// `FK_WIDGET_PARENT` does not answer to the synthesized `fk_widget_parent`: the
// row is taken for a field-level constraint, leaves the comparison, and the
// declaration is reported as an addition -- CREATE for a foreign key the
// database already has.
func TestConstraints_ASynthesizedForeignKeyMatchesTheCatalogsSpelling(t *testing.T) {
	tests := []struct {
		name            string
		catalogSpelling string
	}{
		{
			name:            "the catalog agrees with the generated name",
			catalogSpelling: "fk_widget_parent",
		},
		{
			name:            "the catalog holds it in another case",
			catalogSpelling: "FK_WIDGET_PARENT",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}

			compare.ConstraintsWithSemantics(
				widgetReferencingParent(),
				parentAndWidgetHoldingForeignKey(test.catalogSpelling),
				diff, nil, identifier.ForDialect("mysql"),
			)

			c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
			c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
		})
	}
}

// widgetReferencingParent declares the foreign key on the field, which is what
// the synthesizer turns into a named constraint.
func widgetReferencingParent() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Parent", Name: "parent"},
			{StructName: "Widget", Name: "widget"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Parent", Name: "id", Type: "int", Primary: true},
			{StructName: "Widget", Name: "id", Type: "int", Primary: true},
			{StructName: "Widget", Name: "parent", Type: "int", Foreign: "parent(id)"},
		},
	}
}

// parentAndWidgetHoldingForeignKey is the pair as a catalog reports it, with the
// foreign key under the given spelling and its body otherwise identical.
func parentAndWidgetHoldingForeignKey(name string) *catalog.Database {
	parent, id := "parent", "id"
	return &catalog.Database{
		Tables: []catalog.Table{
			{Name: "parent", Columns: []catalog.Column{
				{Name: "id", DataType: "integer", IsPrimaryKey: true, IsNullable: "NO"},
			}},
			{Name: "widget", Columns: []catalog.Column{
				{Name: "id", DataType: "integer", IsPrimaryKey: true, IsNullable: "NO"},
				{Name: "parent", DataType: "integer", IsNullable: "NO"},
			}},
		},
		Constraints: []catalog.Constraint{{
			TableName: "widget", Name: name, Type: "FOREIGN KEY",
			ColumnName: "parent", ColumnNames: []string{"parent"},
			ForeignTable: &parent, ForeignColumn: &id, ForeignColumns: []string{"id"},
		}},
	}
}
