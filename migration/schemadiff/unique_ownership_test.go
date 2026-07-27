package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/schemadiff"
	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
)

func TestCompareWithDialect_UniqueIndexOwnsColumnUniqueness(t *testing.T) {
	c := qt.New(t)

	generated := uniqueIndexGeneratedSchema()
	database := uniqueIndexDatabaseSchema()

	diff := schemadiff.CompareWithDialect(generated, database, "sqlite")

	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("round-trip diff: %+v", diff))
}

func TestCompareWithDialect_ResolvedUniqueIndexOwnerOverridesImportedStructName(t *testing.T) {
	c := qt.New(t)

	generated := uniqueIndexGeneratedSchema()
	generated.Indexes[0].StructName = "users"
	database := uniqueIndexDatabaseSchema()

	diff := schemadiff.CompareWithDialect(generated, database, "sqlite")

	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("round-trip diff: %+v", diff))
}

func TestCompareWithDialect_UniqueIndexAdditionDoesNotModifyColumn(t *testing.T) {
	c := qt.New(t)

	generated := uniqueIndexGeneratedSchema()
	database := uniqueIndexDatabaseSchema()
	database.Indexes = nil
	database.Tables[0].Columns[0].IsUnique = false

	diff := schemadiff.CompareWithDialect(generated, database, "sqlite")

	c.Assert(diff.TablesModified, qt.HasLen, 0)
	c.Assert(diff.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{{
		Name:      "idx_users_email",
		TableName: "users",
	}})
	c.Assert(diff.IndexRemovals(), qt.HasLen, 0)
}

func TestCompareWithDialect_UniqueIndexRemovalKeepsColumnDifferenceVisible(t *testing.T) {
	c := qt.New(t)

	generated := uniqueIndexGeneratedSchema()
	generated.Indexes = nil
	database := uniqueIndexDatabaseSchema()

	diff := schemadiff.CompareWithDialect(generated, database, "sqlite")

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

	generated := uniqueIndexGeneratedSchema()
	generated.Indexes = nil
	generated.Fields[0].Unique = true
	database := uniqueIndexDatabaseSchema()
	database.Indexes = nil
	database.Tables[0].Columns[0].IsUnique = false

	diff := schemadiff.CompareWithDialect(generated, database, "sqlite")

	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].ColumnsModified, qt.DeepEquals, []difftypes.ColumnDiff{{
		ColumnName: "email",
		Changes:    map[string]string{"unique": "false -> true"},
	}})
}

func TestCompareWithDialect_SingleColumnUniqueConstraintOwnsUniqueness(t *testing.T) {
	c := qt.New(t)

	generated := uniqueIndexGeneratedSchema()
	generated.Indexes = nil
	generated.Constraints = []goschema.Constraint{{
		StructName: "User",
		Name:       "users_email_key",
		Type:       "UNIQUE",
		Table:      "users",
		Columns:    []string{"email"},
	}}
	database := uniqueIndexDatabaseSchema()
	database.Indexes = []types.DBIndex{{
		Name:       "sqlite_autoindex_users_1",
		TableName:  "users",
		Columns:    []string{"email"},
		IsUnique:   true,
		Definition: "",
	}}
	database.Constraints = []types.DBConstraint{{
		Name:        "users_email_key",
		TableName:   "users",
		Type:        "UNIQUE",
		ColumnName:  "email",
		ColumnNames: []string{"email"},
	}}

	diff := schemadiff.CompareWithDialect(generated, database, "sqlite")

	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("round-trip diff: %+v", diff))
}

func TestCompareWithDialect_StructOwnedUniqueConstraintOwnsUniqueness(t *testing.T) {
	c := qt.New(t)

	generated := uniqueIndexGeneratedSchema()
	generated.Indexes = nil
	generated.Constraints = []goschema.Constraint{{
		StructName: "User",
		Name:       "users_email_key",
		Type:       "UNIQUE",
		Columns:    []string{"email"},
	}}
	database := uniqueIndexDatabaseSchema()
	database.Indexes = []types.DBIndex{{
		Name:       "sqlite_autoindex_users_1",
		TableName:  "users",
		Columns:    []string{"email"},
		IsUnique:   true,
		Definition: "",
	}}
	database.Constraints = []types.DBConstraint{{
		Name:        "users_email_key",
		TableName:   "users",
		Type:        "UNIQUE",
		ColumnName:  "email",
		ColumnNames: []string{"email"},
	}}

	diff := schemadiff.CompareWithDialect(generated, database, "sqlite")

	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("round-trip diff: %+v", diff))
}

func TestCompareWithDialect_SchemaQualifiedStructOwnedUniqueConstraintOwnsUniqueness(t *testing.T) {
	c := qt.New(t)

	generated := uniqueIndexGeneratedSchema()
	generated.Tables[0].Schema = "audit"
	generated.Indexes = nil
	generated.Constraints = []goschema.Constraint{{
		StructName: "User",
		Name:       "users_email_key",
		Type:       "UNIQUE",
		Columns:    []string{"email"},
	}}
	database := uniqueIndexDatabaseSchema()
	database.Tables[0].Schema = "audit"
	database.Indexes = []types.DBIndex{{
		Name:       "users_email_key",
		Schema:     "audit",
		TableName:  "users",
		Columns:    []string{"email"},
		IsUnique:   true,
		Definition: "",
	}}
	database.Constraints = []types.DBConstraint{{
		Name:        "users_email_key",
		Schema:      "audit",
		TableName:   "users",
		Type:        "UNIQUE",
		ColumnName:  "email",
		ColumnNames: []string{"email"},
	}}

	diff := schemadiff.CompareWithDialect(generated, database, "postgres")

	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("round-trip diff: %+v", diff))
}

func TestCompareWithDialect_LiteralDotUsesStructuralTableIdentity(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{
		Tables: []goschema.Table{{
			StructName: "TenantData",
			Name:       "tenant.data",
		}},
		Fields: []goschema.Field{{
			StructName: "TenantData",
			Name:       "event.id",
			Type:       "INTEGER",
			Nullable:   true,
		}},
	}
	database := &types.DBSchema{Tables: []types.DBTable{{
		Name: "tenant.data",
		Type: "TABLE",
		Columns: []types.DBColumn{{
			Name:          "event.id",
			DataType:      "INTEGER",
			ColumnType:    "INTEGER",
			IsNullable:    "YES",
			ColumnDefault: nil,
		}},
	}}}

	diff := schemadiff.CompareWithDialect(generated, database, "postgres")

	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("round-trip diff: %+v", diff))
}

func TestCompareWithDialect_LiteralDotDoesNotMatchSchemaQualification(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{Tables: []goschema.Table{{
		Name: "tenant.data",
	}}}
	database := &types.DBSchema{Tables: []types.DBTable{{
		Schema: "tenant",
		Name:   "data",
		Type:   "TABLE",
	}}}

	diff := schemadiff.CompareWithDialect(generated, database, "postgres")

	c.Assert(diff.TablesAdded, qt.DeepEquals, []string{`"tenant.data"`})
	c.Assert(diff.TablesRemoved, qt.DeepEquals, []string{"tenant.data"})
	c.Assert(diff.TablesModified, qt.HasLen, 0)
}

func TestCompareWithDialect_LiteralDotAndQualifiedTablesRemainDistinct(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{Tables: []goschema.Table{
		{StructName: "Literal", Name: "tenant.data"},
		{StructName: "Qualified", Schema: "tenant", Name: "data"},
	}}

	diff := schemadiff.CompareWithDialect(generated, &types.DBSchema{}, "postgres")

	c.Assert(diff.TablesAdded, qt.DeepEquals, []string{`"tenant.data"`, "tenant.data"})
}

func TestCompareWithDialect_ConstraintMembersPreserveStructuralIdentity(t *testing.T) {
	c := qt.New(t)
	checkClause := "id > 0"
	generated := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Literal", Name: "tenant.data"},
			{StructName: "Qualified", Schema: "tenant.data", Name: "payload"},
		},
		Constraints: []goschema.Constraint{{
			StructName:      "Qualified",
			Name:            "guard",
			Type:            "CHECK",
			Table:           `"tenant.data".payload`,
			CheckExpression: checkClause,
		}},
	}
	database := &types.DBSchema{
		Tables: []types.DBTable{
			{Name: "tenant.data", Type: "TABLE"},
			{Schema: "tenant.data", Name: "payload", Type: "TABLE"},
		},
		Constraints: []types.DBConstraint{{
			Name:        "payload.guard",
			TableName:   "tenant.data",
			Type:        "CHECK",
			CheckClause: &checkClause,
		}},
	}

	diff := schemadiff.CompareWithDialect(generated, database, "postgres")

	c.Assert(diff.ConstraintsAdded, qt.DeepEquals, []string{"guard"})
	c.Assert(diff.ConstraintsRemoved, qt.DeepEquals, []string{"payload.guard"})
	c.Assert(diff.ConstraintsAddedWithTables, qt.DeepEquals, []difftypes.ConstraintAdditionInfo{{
		Name:            "guard",
		TableName:       `"tenant.data".payload`,
		Type:            "CHECK",
		CheckExpression: checkClause,
	}})
	c.Assert(diff.ConstraintsRemovedWithTables, qt.DeepEquals, []difftypes.ConstraintRemovalInfo{{
		Name:      "payload.guard",
		TableName: `"tenant.data"`,
		Type:      "CHECK",
	}})
}

func TestCompareWithDialect_LiteralDotUniqueIndexOwnsColumnUniqueness(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{
		Tables: []goschema.Table{{
			StructName: "TenantData",
			Name:       "tenant.data",
		}},
		Fields: []goschema.Field{{
			StructName: "TenantData",
			Name:       "event.id",
			Type:       "TEXT",
			Nullable:   false,
		}},
		Indexes: []goschema.Index{{
			StructName: "TenantData",
			Name:       "event.lookup",
			TableName:  `"tenant.data"`,
			Fields:     []string{"event.id"},
			Unique:     true,
		}},
	}
	database := &types.DBSchema{
		Tables: []types.DBTable{{
			Name: "tenant.data",
			Type: "TABLE",
			Columns: []types.DBColumn{{
				Name:       "event.id",
				DataType:   "TEXT",
				ColumnType: "TEXT",
				IsNullable: "NO",
				IsUnique:   true,
			}},
		}},
		Indexes: []types.DBIndex{{
			Name:       "event.lookup",
			TableName:  "tenant.data",
			Columns:    []string{"event.id"},
			IsUnique:   true,
			Definition: `CREATE UNIQUE INDEX "event.lookup" ON "tenant.data" ("event.id")`,
		}},
	}

	diff := schemadiff.CompareWithDialect(generated, database, "postgres")

	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("round-trip diff: %+v", diff))
}

func TestCompareWithDialect_FilteredDatabaseIndexCannotHideUniqueRemoval(t *testing.T) {
	c := qt.New(t)

	generated := uniqueIndexGeneratedSchema()
	generated.Indexes = nil
	database := uniqueIndexDatabaseSchema()
	database.Indexes[0].Name = "users_email_key"

	diff := schemadiff.CompareWithDialect(generated, database, "postgres")

	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].ColumnsModified, qt.DeepEquals, []difftypes.ColumnDiff{{
		ColumnName: "email",
		Changes:    map[string]string{"unique": "true -> false"},
	}})
	c.Assert(diff.IndexRemovals(), qt.HasLen, 0)
}

func TestCompareWithDialect_PartialUniqueIndexDoesNotOwnColumnUniqueness(t *testing.T) {
	c := qt.New(t)

	generated := uniqueIndexGeneratedSchema()
	generated.Indexes = nil
	database := uniqueIndexDatabaseSchema()
	database.Indexes[0].Condition = "email IS NOT NULL"

	diff := schemadiff.CompareWithDialect(generated, database, "sqlite")

	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].ColumnsModified, qt.DeepEquals, []difftypes.ColumnDiff{{
		ColumnName: "email",
		Changes:    map[string]string{"unique": "true -> false"},
	}})
}

func uniqueIndexGeneratedSchema() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{
			Name:       "users",
			StructName: "User",
		}},
		Fields: []goschema.Field{{
			StructName: "User",
			Name:       "email",
			Type:       "TEXT",
			Nullable:   false,
		}},
		Indexes: []goschema.Index{{
			StructName: "User",
			Name:       "idx_users_email",
			TableName:  "users",
			Fields:     []string{"email"},
			Unique:     true,
		}},
	}
}

func uniqueIndexDatabaseSchema() *types.DBSchema {
	return &types.DBSchema{
		Tables: []types.DBTable{{
			Name: "users",
			Type: "TABLE",
			Columns: []types.DBColumn{{
				Name:       "email",
				DataType:   "TEXT",
				ColumnType: "TEXT",
				IsNullable: "NO",
				IsUnique:   true,
			}},
		}},
		Indexes: []types.DBIndex{{
			Name:       "idx_users_email",
			TableName:  "users",
			Columns:    []string{"email"},
			IsUnique:   true,
			Definition: `CREATE UNIQUE INDEX "idx_users_email" ON "users" ("email")`,
		}},
	}
}
