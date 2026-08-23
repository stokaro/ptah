package schemachange_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/schemachange"
)

// A foreign key is legal when its referenced column LIST is a key, and the
// prototype asked whether each column was a key on its own. That answered no to
// a reference against a composite key and yes to a single-column reference
// whose only guarantee is a composite key. Wrong in both directions,
// conservative in one of them (stokaro/ptah#1662).
//
// Measured on PostgreSQL 18.4, in a rolled-back transaction, against a parent
// keyed on (tenant, id):
//
//	FOREIGN KEY (pt, pi) REFERENCES p (tenant, id)   CREATE TABLE
//	FOREIGN KEY (pt)     REFERENCES p (tenant)       ERROR: there is no unique
//	                                                 constraint matching given
//	                                                 keys for referenced table

func TestAReferenceAgainstACompositeKeyIsPlanned(t *testing.T) {
	c := qt.New(t)

	changes := changesFor(c, tenantScopedSchema([]string{"tenant", "id"}), tenantScopedCatalog())

	c.Assert(changes, qt.HasLen, 1)
	c.Assert(changes[0].Operation, qt.Equals, schemachange.Add)
	c.Assert(changes[0].Status, qt.Equals, schemachange.Planned)
	c.Assert(changes[0].Diagnostic, qt.Equals, "")
}

// TestAReferenceAgainstHalfACompositeKeyIsBlocked is the control, and it is the
// half a rule that simply answered "yes" would get wrong. A unique constraint
// on (tenant, id) makes the PAIR unique and says nothing about either column,
// so a foreign key against tenant alone is one PostgreSQL refuses.
func TestAReferenceAgainstHalfACompositeKeyIsBlocked(t *testing.T) {
	c := qt.New(t)

	changes := changesFor(c, tenantScopedSchema([]string{"tenant"}), tenantScopedCatalog())

	c.Assert(changes, qt.HasLen, 1)
	c.Assert(changes[0].Status, qt.Equals, schemachange.Blocked)
	c.Assert(changes[0].Diagnostic, qt.Contains, "which no unique constraint covers")
}

// tenantScopedSchema is a parent keyed on (tenant, id) and a child whose
// foreign key references the given columns of it.
func tenantScopedSchema(references []string) *goschema.Database {
	local := make([]string, 0, len(references))
	for _, column := range references {
		local = append(local, "parent_"+column)
	}
	// The child carries both columns whichever the key references, so the two
	// rows differ in the REFERENCE and in nothing else. A fixture that also
	// varied the column list would report a column removal beside the
	// constraint, and the assertion about the constraint would be reading a
	// list it did not mean to change.
	fields := []goschema.Field{
		{StructName: "Parent", Name: "tenant", Type: "int"},
		{StructName: "Parent", Name: "id", Type: "int"},
		{StructName: "Child", Name: "parent_tenant", Type: "int", Nullable: true},
		{StructName: "Child", Name: "parent_id", Type: "int", Nullable: true},
	}
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Parent", Name: "parent", PrimaryKey: []string{"tenant", "id"}},
			{StructName: "Child", Name: "child"},
		},
		Fields: fields,
		Constraints: []goschema.Constraint{{
			StructName:     "Child",
			Name:           "fk_child_parent",
			Type:           "FOREIGN KEY",
			Columns:        local,
			ForeignTable:   "parent",
			ForeignColumns: references,
		}},
	}
}

// tenantScopedCatalog holds both tables and the composite key, and no foreign
// key, so the reference is the only change.
func tenantScopedCatalog() *dbschematypes.DBSchema {
	return &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{
			{Name: "parent", Schema: "public", Columns: []dbschematypes.DBColumn{
				{Name: "tenant", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
			}},
			{Name: "child", Schema: "public", Columns: []dbschematypes.DBColumn{
				{Name: "parent_tenant", DataType: "integer", IsNullable: "YES"},
				{Name: "parent_id", DataType: "integer", IsNullable: "YES"},
			}},
		},
		Constraints: []dbschematypes.DBConstraint{{
			Name: "parent_pkey", TableName: "parent", Schema: "public",
			Type: "PRIMARY KEY", ColumnNames: []string{"tenant", "id"},
		}},
	}
}
