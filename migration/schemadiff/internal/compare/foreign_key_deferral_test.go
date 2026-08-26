package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
	"go.5x5.cz/ptah/migration/schemadiff/internal/compare"
)

// deferrableDeclaration is a description whose child table carries a
// single-column foreign key with the given deferral.
//
// Single-column matters: a declaration carries such a key on the FIELD rather
// than as a constraint, so the comparator has to synthesize one for it, and the
// synthesis is what this is about.
func deferrableDeclaration(deferrable bool, initially string) *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Parent", Name: "parent"},
			{StructName: "Child", Name: "child"},
		},
		Fields: []goschema.Field{
			{StructName: "Parent", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Child", Name: "id", Type: "INTEGER", Primary: true},
			{
				StructName: "Child", Name: "pid", Type: "INTEGER",
				Foreign: "parent(id)", ForeignKeyName: "fk_child_pid",
				Deferrable: deferrable, Initially: initially,
			},
		},
	}
}

// deferrableCatalog is the same pair of tables as the server reports them, with
// the given deferral on the foreign key.
func deferrableCatalog(deferrable bool, initially string) *catalog.Database {
	parent := "parent"
	return &catalog.Database{
		Tables: []catalog.Table{
			{Name: "parent", Type: "BASE TABLE", Columns: []catalog.Column{
				{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
			}},
			{Name: "child", Type: "BASE TABLE", Columns: []catalog.Column{
				{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "pid", DataType: "integer", IsNullable: "NO"},
			}},
		},
		Constraints: []catalog.Constraint{{
			TableName: "child", Name: "fk_child_pid", Type: "FOREIGN KEY",
			ColumnNames: []string{"pid"}, ForeignTable: &parent,
			ForeignColumns: []string{"id"},
			Deferrable:     deferrable, Initially: initially,
		}},
	}
}

// deferralDiff compares one description against one catalog.
func deferralDiff(c *qt.C, generated *goschema.Database, database *catalog.Database) *difftypes.SchemaDiff {
	c.Helper()
	diff := &difftypes.SchemaDiff{}
	compare.ConstraintsWithSemantics(generated, database, diff, nil, identifier.ForDialect("postgres"))
	return diff
}

// TestConstraints_ADeferralThatAlreadyMatchesIsNotPlanned pins that a declared
// deferral equal to the live one is no difference.
//
// A single-column foreign key has no constraint row in a description -- it is
// carried on the field -- so the comparator synthesizes one to hold against the
// catalog's. That synthesis copied the referential actions and stopped, while
// foreignKeyConstraintChanged compares the deferral as well. Every deferrable
// key was therefore unequal to itself, which planned a drop and an add on every
// run (stokaro/ptah#2202).
func TestConstraints_ADeferralThatAlreadyMatchesIsNotPlanned(t *testing.T) {
	tests := []struct {
		name       string
		deferrable bool
		initially  string
	}{
		{name: "deferred", deferrable: true, initially: "DEFERRED"},
		{name: "immediate", deferrable: true, initially: "IMMEDIATE"},
		{name: "deferrable with no timing", deferrable: true, initially: ""},
		{name: "not deferrable at all", deferrable: false, initially: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			diff := deferralDiff(c,
				deferrableDeclaration(tt.deferrable, tt.initially),
				deferrableCatalog(tt.deferrable, tt.initially))

			c.Assert(diff.ConstraintsAddedWithTables, qt.HasLen, 0)
			c.Assert(diff.ConstraintsRemovedWithTables, qt.HasLen, 0)
		})
	}
}

// TestConstraints_AChangedDeferralIsStillPlanned is the control that separates a
// fix from a silencing.
//
// Carrying the deferral into the synthesis makes the equal case equal. It must
// not make every case equal: a declaration asking for a deferral the live key
// does not have is a real difference, and the plan is the only thing that would
// ever apply it.
func TestConstraints_AChangedDeferralIsStillPlanned(t *testing.T) {
	tests := []struct {
		name               string
		declared, live     bool
		declaredAt, liveAt string
	}{
		{name: "the declaration adds a deferral", declared: true, declaredAt: "DEFERRED", live: false},
		{name: "the declaration drops a deferral", declared: false, live: true, liveAt: "DEFERRED"},
		{name: "only the timing differs", declared: true, declaredAt: "DEFERRED", live: true, liveAt: "IMMEDIATE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			diff := deferralDiff(c,
				deferrableDeclaration(tt.declared, tt.declaredAt),
				deferrableCatalog(tt.live, tt.liveAt))

			c.Assert(diff.ConstraintsAddedWithTables, qt.HasLen, 1)
			c.Assert(diff.ConstraintsAddedWithTables[0].Type, qt.Equals, "FOREIGN KEY")
		})
	}
}
