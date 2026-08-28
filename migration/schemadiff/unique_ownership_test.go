package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/schemadiff"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
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
		Changes:    map[string]string{"unique": "true -> false"},
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
		Changes:    map[string]string{"unique": "false -> true"},
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

	c.Assert(diff.ConstraintsAdded, qt.DeepEquals, []string{"guard"})
	c.Assert(diff.ConstraintsRemoved, qt.DeepEquals, []string{"payload.guard"})
	c.Assert(diff.ConstraintsAddedWithTables, qt.DeepEquals, []difftypes.ConstraintAdditionInfo{{
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
	c.Assert(diff.ConstraintsRemovedWithTables, qt.DeepEquals, []difftypes.ConstraintRemovalInfo{{
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

	diff := schemadiff.CompareWithDialect(desired, database, "postgres")

	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].ColumnsModified, qt.DeepEquals, []difftypes.ColumnDiff{{
		ColumnName: "email",
		Changes:    map[string]string{"unique": "true -> false"},
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
		Changes:    map[string]string{"unique": "true -> false"},
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
