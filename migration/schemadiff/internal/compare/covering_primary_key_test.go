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

// coveringDeclaration is a description whose table declares a primary key with
// the given INCLUDE payload.
func coveringDeclaration(include []string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{
			StructName:        "Covering",
			Name:              "covering",
			PrimaryKey:        []string{"a", "b"},
			PrimaryKeyInclude: include,
		}},
		Fields: []schemamodel.Field{
			{StructName: "Covering", Name: "a", Type: "INTEGER"},
			{StructName: "Covering", Name: "b", Type: "INTEGER"},
			{StructName: "Covering", Name: "payload", Type: "TEXT", Nullable: true},
		},
	}
}

// coveringCatalog is the same table as the server reports it, with the given
// payload on its primary key.
func coveringCatalog(include []string) *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{{
			Name: "covering", Schema: "public", Type: "BASE TABLE",
			Columns: []catalog.Column{
				{Name: "a", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "b", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "payload", DataType: "text", IsNullable: "YES"},
			},
		}},
		Constraints: []catalog.Constraint{{
			Schema: "public", TableName: "covering", Name: "covering_pkey",
			Type: "PRIMARY KEY", ColumnNames: []string{"a", "b"}, IncludeColumns: include,
		}},
	}
}

// constraintDiff compares one description against one catalog.
func constraintDiff(c *qt.C, desired *schemamodel.Database, current *catalog.Database) *difftypes.SchemaDiff {
	c.Helper()
	diff := &difftypes.SchemaDiff{}
	compare.ConstraintsWithSemantics(desired, current, diff, nil, identifier.ForDialect("postgres"))
	return diff
}

// TestConstraints_ACoveringPrimaryKeyThatAlreadyMatchesIsNotPlanned pins that a
// declared payload equal to the live one is no difference.
//
// A table-level primary key has no constraint row in a description -- it is
// `Table.PrimaryKey`, a bare column list -- so the comparator synthesizes one to
// hold against the catalog's. That synthesis carried the columns and not the
// payload, while primaryKeyConstraintChanged compares both. Every covering
// primary key was therefore unequal to itself: the plan dropped it and added it
// back WITHOUT the payload, the live index lost its INCLUDE, and the next run
// reported the schema as synced (stokaro/ptah#2199).
func TestConstraints_ACoveringPrimaryKeyThatAlreadyMatchesIsNotPlanned(t *testing.T) {
	c := qt.New(t)

	diff := constraintDiff(c, coveringDeclaration([]string{"payload"}), coveringCatalog([]string{"payload"}))

	c.Assert(diff.ConstraintsAddedWithTables, qt.HasLen, 0)
	c.Assert(diff.ConstraintsRemovedWithTables, qt.HasLen, 0)
}

// TestConstraints_AChangedCoveringPayloadIsStillPlanned is the control that
// separates a fix from a silencing.
//
// Carrying the payload into the synthesis makes the equal case equal. It must
// not make every case equal: a declaration that asks for a payload the live key
// does not have is a real difference, and the plan is the only thing that would
// ever add it.
func TestConstraints_AChangedCoveringPayloadIsStillPlanned(t *testing.T) {
	tests := []struct {
		name            string
		declaredInclude []string
		liveInclude     []string
	}{
		{name: "the declaration adds a payload", declaredInclude: []string{"payload"}, liveInclude: nil},
		{name: "the declaration drops a payload", declaredInclude: nil, liveInclude: []string{"payload"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			diff := constraintDiff(c, coveringDeclaration(tt.declaredInclude), coveringCatalog(tt.liveInclude))

			c.Assert(diff.ConstraintsAddedWithTables, qt.HasLen, 1)
			c.Assert(diff.ConstraintsAddedWithTables[0].Type, qt.Equals, "PRIMARY KEY")
			// The addition has to carry the payload the declaration asked for,
			// or the statement it becomes rebuilds a plain key.
			c.Assert(diff.ConstraintsAddedWithTables[0].IncludeColumns, qt.DeepEquals, tt.declaredInclude)
		})
	}
}
