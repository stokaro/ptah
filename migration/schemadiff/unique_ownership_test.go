package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

func TestCompareWithDialect_UniqueIndexOwnsColumnUniqueness(t *testing.T) {
	c := qt.New(t)

	desired := uniqueIndexGeneratedSchema()
	database := uniqueIndexDatabaseSchema()

	diff := schemadiff.CompareWithDialect(desired, database, "sqlite")

	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("round-trip diff: %+v", diff))
}

func TestCompareWithDialect_ResolvedUniqueIndexOwnerOverridesImportedStructName(t *testing.T) {
	c := qt.New(t)

	desired := uniqueIndexGeneratedSchema()
	desired.Indexes[0].StructName = "users"
	database := uniqueIndexDatabaseSchema()

	diff := schemadiff.CompareWithDialect(desired, database, "sqlite")

	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("round-trip diff: %+v", diff))
}

func TestCompareWithDialect_UniqueIndexAdditionDoesNotModifyColumn(t *testing.T) {
	c := qt.New(t)

	desired := uniqueIndexGeneratedSchema()
	database := uniqueIndexDatabaseSchema()
	database.Indexes = nil
	database.Tables[0].Columns[0].IsUnique = false

	diff := schemadiff.CompareWithDialect(desired, database, "sqlite")

	c.Assert(diff.TablesModified, qt.HasLen, 0)
	c.Assert(diff.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{{
		Name:      "idx_users_email",
		TableName: "users",
	}})
	c.Assert(diff.IndexRemovals(), qt.HasLen, 0)
}

func TestCompareWithDialect_UniqueIndexRemovalKeepsColumnDifferenceVisible(t *testing.T) {
	c := qt.New(t)

	desired := uniqueIndexGeneratedSchema()
	desired.Indexes = nil
	database := uniqueIndexDatabaseSchema()

	diff := schemadiff.CompareWithDialect(desired, database, "sqlite")

	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].ColumnsModified, qt.DeepEquals, []difftypes.ColumnDiff{{
		ColumnName: "email",
		// The operand is the column the declaration wrote, taken from the
		// declaration this comparison was given rather than restated.
		Desired: desired.Fields[0],
		Changes: map[string]string{"unique": "true -> false"},
	}})
	c.Assert(diff.IndexAdditions(), qt.HasLen, 0)
	c.Assert(diff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{{
		Name:      "idx_users_email",
		TableName: "users",
	}})
}

func TestCompareWithDialect_FieldUniqueStillOwnsMissingUniqueness(t *testing.T) {
	c := qt.New(t)

	desired := uniqueIndexGeneratedSchema()
	desired.Indexes = nil
	desired.Fields[0].Unique = true
	database := uniqueIndexDatabaseSchema()
	database.Indexes = nil
	database.Tables[0].Columns[0].IsUnique = false

	diff := schemadiff.CompareWithDialect(desired, database, "sqlite")

	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].ColumnsModified, qt.DeepEquals, []difftypes.ColumnDiff{{
		ColumnName: "email",
		// The operand is the column the declaration wrote, taken from the
		// declaration this comparison was given rather than restated.
		Desired: desired.Fields[0],
		Changes: map[string]string{"unique": "false -> true"},
	}})
}

func TestCompareWithDialect_SingleColumnUniqueConstraintOwnsUniqueness(t *testing.T) {
	c := qt.New(t)

	desired := uniqueIndexGeneratedSchema()
	desired.Indexes = nil
	desired.Constraints = []schemamodel.Constraint{{
		StructName: "User",
		Name:       "users_email_key",
		Type:       "UNIQUE",
		Table:      "users",
		Columns:    []string{"email"},
	}}
	database := uniqueIndexDatabaseSchema()
	database.Indexes = []catalog.Index{{
		Name:       "sqlite_autoindex_users_1",
		TableName:  "users",
		Columns:    []string{"email"},
		IsUnique:   true,
		Definition: "",
	}}
	database.Constraints = []catalog.Constraint{{
		Name:        "users_email_key",
		TableName:   "users",
		Type:        "UNIQUE",
		ColumnName:  "email",
		ColumnNames: []string{"email"},
	}}

	diff := schemadiff.CompareWithDialect(desired, database, "sqlite")

	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("round-trip diff: %+v", diff))
}

func TestCompareWithDialect_StructOwnedUniqueConstraintOwnsUniqueness(t *testing.T) {
	c := qt.New(t)

	desired := uniqueIndexGeneratedSchema()
	desired.Indexes = nil
	desired.Constraints = []schemamodel.Constraint{{
		StructName: "User",
		Name:       "users_email_key",
		Type:       "UNIQUE",
		Columns:    []string{"email"},
	}}
	database := uniqueIndexDatabaseSchema()
	database.Indexes = []catalog.Index{{
		Name:       "sqlite_autoindex_users_1",
		TableName:  "users",
		Columns:    []string{"email"},
		IsUnique:   true,
		Definition: "",
	}}
	database.Constraints = []catalog.Constraint{{
		Name:        "users_email_key",
		TableName:   "users",
		Type:        "UNIQUE",
		ColumnName:  "email",
		ColumnNames: []string{"email"},
	}}

	diff := schemadiff.CompareWithDialect(desired, database, "sqlite")

	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("round-trip diff: %+v", diff))
}

func TestCompareWithDialect_SchemaQualifiedStructOwnedUniqueConstraintOwnsUniqueness(t *testing.T) {
	c := qt.New(t)

	desired := uniqueIndexGeneratedSchema()
	desired.Tables[0].Schema = "audit"
	desired.Indexes = nil
	desired.Constraints = []schemamodel.Constraint{{
		StructName: "User",
		Name:       "users_email_key",
		Type:       "UNIQUE",
		Columns:    []string{"email"},
	}}
	database := uniqueIndexDatabaseSchema()
	database.Tables[0].Schema = "audit"
	database.Indexes = []catalog.Index{{
		Name:       "users_email_key",
		Schema:     "audit",
		TableName:  "users",
		Columns:    []string{"email"},
		IsUnique:   true,
		Definition: "",
	}}
	database.Constraints = []catalog.Constraint{{
		Name:        "users_email_key",
		Schema:      "audit",
		TableName:   "users",
		Type:        "UNIQUE",
		ColumnName:  "email",
		ColumnNames: []string{"email"},
	}}

	diff := schemadiff.CompareWithDialect(desired, database, "postgres")

	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("round-trip diff: %+v", diff))
}

func TestCompareWithDialect_LiteralDotUsesStructuralTableIdentity(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{
			StructName: "TenantData",
			Name:       "tenant.data",
		}},
		Fields: []schemamodel.Field{{
			StructName: "TenantData",
			Name:       "event.id",
			Type:       "INTEGER",
			Nullable:   true,
		}},
	}
	database := &catalog.Database{Tables: []catalog.Table{{
		Name: "tenant.data",
		Type: "TABLE",
		Columns: []catalog.Column{{
			Name:          "event.id",
			DataType:      "INTEGER",
			ColumnType:    "INTEGER",
			IsNullable:    "YES",
			ColumnDefault: nil,
		}},
	}}}

	diff := schemadiff.CompareWithDialect(desired, database, "postgres")

	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("round-trip diff: %+v", diff))
}

func TestCompareWithDialect_LiteralDotDoesNotMatchSchemaQualification(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{Tables: []schemamodel.Table{{
		Name: "tenant.data",
	}}}
	database := &catalog.Database{Tables: []catalog.Table{{
		Schema: "tenant",
		Name:   "data",
		Type:   "TABLE",
	}}}

	diff := schemadiff.CompareWithDialect(desired, database, "postgres")

	c.Assert(diff.TablesAdded.Names(), qt.DeepEquals, []string{`"tenant.data"`})
	c.Assert(diff.TablesRemoved, qt.DeepEquals, []string{"tenant.data"})
	c.Assert(diff.TablesModified, qt.HasLen, 0)
}

func TestCompareWithDialect_LiteralDotAndQualifiedTablesRemainDistinct(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{Tables: []schemamodel.Table{
		{StructName: "Literal", Name: "tenant.data"},
		{StructName: "Qualified", Schema: "tenant", Name: "data"},
	}}

	diff := schemadiff.CompareWithDialect(desired, &catalog.Database{}, "postgres")

	c.Assert(diff.TablesAdded.Names(), qt.DeepEquals, []string{`"tenant.data"`, "tenant.data"})
}

func TestCompareWithDialect_ConstraintMembersPreserveStructuralIdentity(t *testing.T) {
	c := qt.New(t)
	checkClause := "id > 0"
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Literal", Name: "tenant.data"},
			{StructName: "Qualified", Schema: "tenant.data", Name: "payload"},
		},
		Constraints: []schemamodel.Constraint{{
			StructName:      "Qualified",
			Name:            "guard",
			Type:            "CHECK",
			Table:           `"tenant.data".payload`,
			CheckExpression: checkClause,
		}},
	}
	database := &catalog.Database{
		Tables: []catalog.Table{
			{Name: "tenant.data", Type: "TABLE"},
			{Schema: "tenant.data", Name: "payload", Type: "TABLE"},
		},
		Constraints: []catalog.Constraint{{
			Name:        "payload.guard",
			TableName:   "tenant.data",
			Type:        "CHECK",
			CheckClause: &checkClause,
		}},
	}

	diff := schemadiff.CompareWithDialect(desired, database, "postgres")

	c.Assert(diff.ConstraintsAdded.Names(), qt.DeepEquals, []string{"guard"})
	c.Assert(diff.ConstraintsRemoved.Names(), qt.DeepEquals, []string{"payload.guard"})
	c.Assert(diff.ConstraintsAdded, qt.DeepEquals, difftypes.ConstraintAdditions{{
		Name:            "guard",
		TableName:       `"tenant.data".payload`,
		Type:            "CHECK",
		CheckExpression: checkClause,
		// The quoted half is the SCHEMA here, dot and all, and the table is
		// payload -- which is the whole point of the row: the two sides name
		// different objects, and the identity says so in parts rather than in
		// one string a reader would have to take apart again.
		Identity: difftypes.ConstraintIdentity{
			Schema: "tenant.data", Table: "payload", Name: "guard",
		},
	}})
	c.Assert(diff.ConstraintsRemoved, qt.DeepEquals, difftypes.ConstraintRemovals{{
		Name:      "payload.guard",
		TableName: `"tenant.data"`,
		Type:      "CHECK",
		// And here the quoted name is the TABLE, so the schema is the
		// target's default and the dot belongs to the constraint's own name.
		Identity: difftypes.ConstraintIdentity{
			Schema: "public", Table: "tenant.data", Name: "payload.guard",
		},
	}})
}

func TestCompareWithDialect_LiteralDotUniqueIndexOwnsColumnUniqueness(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{
			StructName: "TenantData",
			Name:       "tenant.data",
		}},
		Fields: []schemamodel.Field{{
			StructName: "TenantData",
			Name:       "event.id",
			Type:       "TEXT",
			Nullable:   false,
		}},
		Indexes: []schemamodel.Index{{
			StructName: "TenantData",
			Name:       "event.lookup",
			TableName:  `"tenant.data"`,
			Fields:     []string{"event.id"},
			Unique:     true,
		}},
	}
	database := &catalog.Database{
		Tables: []catalog.Table{{
			Name: "tenant.data",
			Type: "TABLE",
			Columns: []catalog.Column{{
				Name:       "event.id",
				DataType:   "TEXT",
				ColumnType: "TEXT",
				IsNullable: "NO",
				IsUnique:   true,
			}},
		}},
		Indexes: []catalog.Index{{
			Name:       "event.lookup",
			TableName:  "tenant.data",
			Columns:    []string{"event.id"},
			IsUnique:   true,
			Definition: `CREATE UNIQUE INDEX "event.lookup" ON "tenant.data" ("event.id")`,
		}},
	}

	diff := schemadiff.CompareWithDialect(desired, database, "postgres")

	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("round-trip diff: %+v", diff))
}

func TestCompareWithDialect_FilteredDatabaseIndexCannotHideUniqueRemoval(t *testing.T) {
	c := qt.New(t)

	desired := uniqueIndexGeneratedSchema()
	desired.Indexes = nil
	database := uniqueIndexDatabaseSchema()
	database.Indexes[0].Name = "users_email_key"
	// The constraint is what makes the index the constraint's object. It used
	// to be the index's NAME, which hid a user's own index from comparison and
	// never planned the removal they asked for (stokaro/ptah#2615); a catalog
	// reporting a backing index without its constraint is a shape a fixture can
	// have and a reader does not produce.
	database.Constraints = []catalog.Constraint{{
		Name: "users_email_key", TableName: "users", Type: "UNIQUE", ColumnNames: []string{"email"},
	}}

	diff := schemadiff.CompareWithDialect(desired, database, "postgres")

	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].ColumnsModified, qt.DeepEquals, []difftypes.ColumnDiff{{
		ColumnName: "email",
		// The operand is the column the declaration wrote, taken from the
		// declaration this comparison was given rather than restated.
		Desired: desired.Fields[0],
		Changes: map[string]string{"unique": "true -> false"},
	}})
	c.Assert(diff.IndexRemovals(), qt.HasLen, 0)
}

func TestCompareWithDialect_PartialUniqueIndexDoesNotOwnColumnUniqueness(t *testing.T) {
	c := qt.New(t)

	desired := uniqueIndexGeneratedSchema()
	desired.Indexes = nil
	database := uniqueIndexDatabaseSchema()
	database.Indexes[0].Condition = "email IS NOT NULL"

	diff := schemadiff.CompareWithDialect(desired, database, "sqlite")

	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].ColumnsModified, qt.DeepEquals, []difftypes.ColumnDiff{{
		ColumnName: "email",
		// The operand is the column the declaration wrote, taken from the
		// declaration this comparison was given rather than restated.
		Desired: desired.Fields[0],
		Changes: map[string]string{"unique": "true -> false"},
	}})
}

func uniqueIndexGeneratedSchema() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{
			Name:       "users",
			StructName: "User",
		}},
		Fields: []schemamodel.Field{{
			StructName: "User",
			Name:       "email",
			Type:       "TEXT",
			Nullable:   false,
		}},
		Indexes: []schemamodel.Index{{
			StructName: "User",
			Name:       "idx_users_email",
			TableName:  "users",
			Fields:     []string{"email"},
			Unique:     true,
		}},
	}
}

func uniqueIndexDatabaseSchema() *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{{
			Name: "users",
			Type: "TABLE",
			Columns: []catalog.Column{{
				Name:       "email",
				DataType:   "TEXT",
				ColumnType: "TEXT",
				IsNullable: "NO",
				IsUnique:   true,
			}},
		}},
		Indexes: []catalog.Index{{
			Name:       "idx_users_email",
			TableName:  "users",
			Columns:    []string{"email"},
			IsUnique:   true,
			Definition: `CREATE UNIQUE INDEX "idx_users_email" ON "users" ("email")`,
		}},
	}
}

// TestCompareWithDialect_AUniqueIndexIsNotTheConstraintsBecauseOfItsName is
// stokaro/ptah#2615.
//
// A user's own unique index was hidden from comparison when its name resembled
// one the engine would have generated, so the removal the author asked for was
// never planned and `--dry-run` reported the database in sync. Measured on
// PostgreSQL 18 over one table, one column and a desired schema declaring no
// index, changing only the index's name:
//
//	slug               No schema differences detected
//	tenants_slug_key   No schema differences detected
//	tenants_slug       DROP INDEX IF EXISTS "tenants_slug"
//	uk_tenants_slug    DROP INDEX IF EXISTS "uk_tenants_slug"
//	idx_tenants_slug   DROP INDEX IF EXISTS "idx_tenants_slug"
//
// The rows here are the two names the scan recognized. Both are ordinary names
// for an index somebody wrote, and neither is evidence about the object.
func TestCompareWithDialect_AUniqueIndexIsNotTheConstraintsBecauseOfItsName(t *testing.T) {
	tests := []struct {
		name  string
		index string
	}{
		{name: "the engine's own convention for a backing index", index: "users_email_key"},
		{name: "an index named after the column it covers", index: "email"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			desired := uniqueIndexGeneratedSchema()
			desired.Indexes = nil
			desired.Fields[0].Unique = false
			database := uniqueIndexDatabaseSchema()
			database.Indexes[0].Name = test.index
			database.Tables[0].Columns[0].IsUnique = false

			diff := schemadiff.CompareWithDialect(desired, database, "postgres")

			c.Assert(diff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{{
				Name:      test.index,
				TableName: "users",
			}})
		})
	}
}

// TestCompareWithDialect_AConstraintsBackingIndexIsStillTheConstraints is the
// control for the test above, and the reason the scan existed at all.
//
// The same two names, with the constraint the catalog reports beside the index.
// The object is the constraint's, the removal is spelled through it, and the
// index pool stays out — which is the answer the name was approximating and the
// catalog gives directly.
func TestCompareWithDialect_AConstraintsBackingIndexIsStillTheConstraints(t *testing.T) {
	tests := []struct {
		name  string
		index string
	}{
		{name: "the engine's own convention for a backing index", index: "users_email_key"},
		{name: "an index named after the column it covers", index: "email"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			desired := uniqueIndexGeneratedSchema()
			desired.Indexes = nil
			desired.Fields[0].Unique = false
			database := uniqueIndexDatabaseSchema()
			database.Indexes[0].Name = test.index
			database.Tables[0].Columns[0].IsUnique = false
			database.Constraints = []catalog.Constraint{{
				Name: test.index, TableName: "users", Type: "UNIQUE", ColumnNames: []string{"email"},
			}}

			diff := schemadiff.CompareWithDialect(desired, database, "postgres")

			c.Assert(diff.IndexRemovals(), qt.HasLen, 0)
		})
	}
}
