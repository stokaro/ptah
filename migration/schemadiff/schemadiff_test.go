package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/config"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/schemadiff"
)

func TestCompare_DefaultBehavior(t *testing.T) {
	c := qt.New(t)

	// Setup test data with plpgsql in database but not in generated schema
	generated := &goschema.Database{
		Extensions: []goschema.Extension{
			{Name: "pg_trgm", IfNotExists: true},
		},
	}
	database := &types.DBSchema{
		Extensions: []types.DBExtension{
			{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},
		},
	}

	// Test default behavior (should ignore plpgsql)
	diff := schemadiff.Compare(generated, database)

	// plpgsql should be ignored by default, so no extensions should be removed
	c.Assert(diff.ExtensionsAdded, qt.DeepEquals, []string{"pg_trgm"})
	c.Assert(diff.ExtensionsRemoved, qt.DeepEquals, []string{})
}

func TestCompareWithDialect_MySQLFamilyInlineEnumsMatchGeneratedEnumFields(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			generated := &goschema.Database{
				Tables: []goschema.Table{{
					Name:       "products",
					StructName: "Product",
				}},
				Fields: []goschema.Field{
					{StructName: "Product", Name: "id", Type: "int", Primary: true},
					{
						StructName: "Product",
						Name:       "status",
						Type:       "enum_product_status",
						Enum:       []string{"draft", "active"},
						Nullable:   false,
					},
				},
				Enums: []goschema.Enum{{
					Name:   "enum_product_status",
					Values: []string{"draft", "active"},
				}},
			}
			database := &types.DBSchema{
				Tables: []types.DBTable{{
					Name: "products",
					Type: "TABLE",
					Columns: []types.DBColumn{
						{Name: "id", DataType: "int", IsNullable: "NO", IsPrimaryKey: true},
						{Name: "status", DataType: "enum('draft','active')", IsNullable: "NO"},
					},
				}},
				Enums: []types.DBEnum{{
					Name:   "enum_draft_active",
					Values: []string{"draft", "active"},
				}},
			}

			diff := schemadiff.CompareWithDialect(generated, database, dialect)
			c.Assert(diff.EnumsAdded, qt.HasLen, 0)
			c.Assert(diff.EnumsRemoved, qt.HasLen, 0)
			c.Assert(diff.TablesModified, qt.HasLen, 0)
		})
	}
}

func TestCompareWithDialect_GeneratedColumnDefaultKindMatchesDialect(t *testing.T) {
	tests := []struct {
		name         string
		dialect      string
		databaseKind string
	}{
		{name: "postgres", dialect: "postgres", databaseKind: "STORED"},
		{name: "mysql", dialect: "mysql", databaseKind: "VIRTUAL"},
		{name: "mariadb", dialect: "mariadb", databaseKind: "VIRTUAL"},
		{name: "sqlite", dialect: "sqlite", databaseKind: "VIRTUAL"},
		{name: "sqlserver", dialect: "sqlserver", databaseKind: "PERSISTED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			expression := "lower(email)"
			generated := &goschema.Database{
				Tables: []goschema.Table{{Name: "users", StructName: "User"}},
				Fields: []goschema.Field{
					{StructName: "User", Name: "email_lc", Type: "TEXT", Nullable: true, GeneratedExpression: expression},
				},
			}
			database := &types.DBSchema{
				Tables: []types.DBTable{{
					Name: "users",
					Type: "TABLE",
					Columns: []types.DBColumn{{
						Name:                "email_lc",
						DataType:            "TEXT",
						IsNullable:          "YES",
						GeneratedExpression: &expression,
						GeneratedKind:       tt.databaseKind,
					}},
				}},
			}

			diff := schemadiff.CompareWithDialect(generated, database, tt.dialect)
			c.Assert(diff.TablesModified, qt.HasLen, 0)
		})
	}
}

func TestCompareWithDialect_GeneratedColumnCatalogExpressionsMatch(t *testing.T) {
	tests := []struct {
		name               string
		dialect            string
		generatedType      string
		generatedExpr      string
		generatedKind      string
		databaseType       string
		databaseColumnType string
		databaseExpr       string
		databaseKind       string
	}{
		{
			name:               "postgres lower varchar cast",
			dialect:            "postgres",
			generatedType:      "VARCHAR(255)",
			generatedExpr:      "lower(email)",
			generatedKind:      "stored",
			databaseType:       "varchar",
			databaseColumnType: "varchar(255)",
			databaseExpr:       "lower((email)::text)",
			databaseKind:       "STORED",
		},
		{
			name:               "mysql backtick identifier",
			dialect:            "mysql",
			generatedType:      "VARCHAR(255)",
			generatedExpr:      "lower(email)",
			generatedKind:      "stored",
			databaseType:       "varchar",
			databaseColumnType: "varchar(255)",
			databaseExpr:       "lower(`email`)",
			databaseKind:       "STORED",
		},
		{
			name:               "postgres numeric cast parameters",
			dialect:            "postgres",
			generatedType:      "DECIMAL(10,2)",
			generatedExpr:      "round(amount)",
			generatedKind:      "stored",
			databaseType:       "decimal",
			databaseColumnType: "decimal(10,2)",
			databaseExpr:       "round((amount)::numeric(10,2))",
			databaseKind:       "STORED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			generated := &goschema.Database{
				Tables: []goschema.Table{{Name: "contacts", StructName: "Contact"}},
				Fields: []goschema.Field{
					{
						StructName:          "Contact",
						Name:                "email_normalized",
						Type:                tt.generatedType,
						Nullable:            true,
						GeneratedExpression: tt.generatedExpr,
						GeneratedKind:       tt.generatedKind,
					},
				},
			}
			database := &types.DBSchema{
				Tables: []types.DBTable{{
					Name: "contacts",
					Type: "TABLE",
					Columns: []types.DBColumn{{
						Name:                "email_normalized",
						DataType:            tt.databaseType,
						ColumnType:          tt.databaseColumnType,
						IsNullable:          "YES",
						GeneratedExpression: &tt.databaseExpr,
						GeneratedKind:       tt.databaseKind,
					}},
				}},
			}

			diff := schemadiff.CompareWithDialect(generated, database, tt.dialect)
			c.Assert(diff.TablesModified, qt.HasLen, 0)
		})
	}
}

func TestCompareWithDialect_GeneratedColumnStringLiteralMismatchIsAGap(t *testing.T) {
	c := qt.New(t)
	generated := &goschema.Database{
		Tables: []goschema.Table{{Name: "contacts", StructName: "Contact"}},
		Fields: []goschema.Field{
			{
				StructName:          "Contact",
				Name:                "email_normalized",
				Type:                "TEXT",
				Nullable:            true,
				GeneratedExpression: "concat(email, 'ACTIVE')",
				GeneratedKind:       "stored",
			},
		},
	}
	databaseExpression := "concat(`email`, 'active')"
	database := &types.DBSchema{
		Tables: []types.DBTable{{
			Name: "contacts",
			Type: "TABLE",
			Columns: []types.DBColumn{{
				Name:                "email_normalized",
				DataType:            "text",
				IsNullable:          "YES",
				GeneratedExpression: &databaseExpression,
				GeneratedKind:       "STORED",
			}},
		}},
	}

	diff := schemadiff.CompareWithDialect(generated, database, "mysql")
	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].ColumnsModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].ColumnsModified[0].Changes["generated"], qt.Contains, "'ACTIVE'")
}

func TestCompareWithDialect_SQLServerGeneratedExpressionNormalizesCatalogDefinition(t *testing.T) {
	for _, tt := range []struct {
		name                string
		generatedExpression string
		databaseExpression  string
	}{
		{
			name:                "bracketed identifier",
			generatedExpression: "lower(email)",
			databaseExpression:  "((LOWER([email])))",
		},
		{
			name:                "escaped closing bracket in identifier",
			generatedExpression: "lower(odd]Name)",
			databaseExpression:  "((LOWER([odd]]Name])))",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			generatedExpression := tt.generatedExpression
			databaseExpression := tt.databaseExpression
			generated := &goschema.Database{
				Tables: []goschema.Table{{Name: "users", Schema: "dbo", StructName: "User"}},
				Fields: []goschema.Field{{
					StructName:          "User",
					Name:                "email_lc",
					Type:                "NVARCHAR(320)",
					Nullable:            true,
					GeneratedExpression: generatedExpression,
					GeneratedKind:       "",
				}},
			}
			database := &types.DBSchema{
				Tables: []types.DBTable{{
					Name:   "users",
					Schema: "dbo",
					Type:   "TABLE",
					Columns: []types.DBColumn{{
						Name:                "email_lc",
						DataType:            "NVARCHAR",
						ColumnType:          "NVARCHAR(320)",
						IsNullable:          "YES",
						GeneratedExpression: &databaseExpression,
						GeneratedKind:       "PERSISTED",
					}},
				}},
			}

			diff := schemadiff.CompareWithDialect(generated, database, "sqlserver")
			c.Assert(diff.TablesModified, qt.HasLen, 0)
		})
	}
}

func TestCompareWithDialect_MySQLDefaultsTypesFixtureMatchesCatalogReadback(t *testing.T) {
	c := qt.New(t)
	subtotalDefault := "0.00"
	taxRateDefault := "0.0000"
	issuedAtDefault := "current_timestamp()"
	paidDefault := "0"

	generated := &goschema.Database{
		Tables: []goschema.Table{{Name: "invoices", StructName: "Invoice"}},
		Fields: []goschema.Field{
			{StructName: "Invoice", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "Invoice", Name: "invoice_number", Type: "VARCHAR(32)", Nullable: false, Unique: true},
			{
				StructName: "Invoice",
				Name:       "subtotal",
				Type:       "DECIMAL(12,2)",
				Nullable:   false,
				Default:    "0.00",
				DefaultSet: true,
			},
			{
				StructName:  "Invoice",
				Name:        "tax_rate",
				Type:        "DECIMAL(5,4)",
				Nullable:    false,
				DefaultExpr: "0",
			},
			{
				StructName:  "Invoice",
				Name:        "issued_at",
				Type:        "TIMESTAMP",
				Nullable:    false,
				DefaultExpr: "CURRENT_TIMESTAMP",
			},
			{
				StructName: "Invoice",
				Name:       "paid",
				Type:       "BOOLEAN",
				Nullable:   false,
				Default:    "false",
				DefaultSet: true,
			},
		},
	}
	database := &types.DBSchema{
		Tables: []types.DBTable{{
			Name: "invoices",
			Type: "TABLE",
			Columns: []types.DBColumn{
				{
					Name:            "id",
					DataType:        "int",
					ColumnType:      "int",
					IsNullable:      "NO",
					IsPrimaryKey:    true,
					IsAutoIncrement: true,
				},
				{
					Name:       "invoice_number",
					DataType:   "varchar(32)",
					ColumnType: "varchar(32)",
					IsNullable: "NO",
					IsUnique:   true,
				},
				{
					Name:          "subtotal",
					DataType:      "decimal(12,2)",
					ColumnType:    "decimal(12,2)",
					IsNullable:    "NO",
					ColumnDefault: &subtotalDefault,
				},
				{
					Name:          "tax_rate",
					DataType:      "decimal(5,4)",
					ColumnType:    "decimal(5,4)",
					IsNullable:    "NO",
					ColumnDefault: &taxRateDefault,
				},
				{
					Name:          "issued_at",
					DataType:      "timestamp",
					ColumnType:    "timestamp",
					IsNullable:    "NO",
					ColumnDefault: &issuedAtDefault,
				},
				{
					Name:          "paid",
					DataType:      "tinyint(1)",
					ColumnType:    "tinyint(1)",
					IsNullable:    "NO",
					ColumnDefault: &paidDefault,
				},
			},
		}},
	}

	diff := schemadiff.CompareWithDialect(generated, database, "mysql")
	c.Assert(diff.TablesModified, qt.HasLen, 0)
}

func TestCompareWithDialect_MySQLConstraintsActionsFixtureMatchesCatalogReadback(t *testing.T) {
	c := qt.New(t)
	statusDefault := "'active'"
	budgetDefault := "0"
	budgetCheck := "(`budget_cents` >= 0)"
	statusCheck := "(`status` in (_latin1\\'active\\',_latin1\\'archived\\'))"
	deleteRule := "CASCADE"
	updateRule := "RESTRICT"

	generated := &goschema.Database{
		Tables: []goschema.Table{
			{Name: "organizations", StructName: "Organization"},
			{Name: "projects", StructName: "Project"},
		},
		Fields: []goschema.Field{
			{StructName: "Organization", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "Organization", Name: "slug", Type: "VARCHAR(64)", Nullable: false, Unique: true},
			{StructName: "Project", Name: "id", Type: "SERIAL", Primary: true},
			{
				StructName:     "Project",
				Name:           "organization_id",
				Type:           "INTEGER",
				Nullable:       false,
				Foreign:        "organizations(id)",
				ForeignKeyName: "fk_projects_organization",
				OnDelete:       "CASCADE",
				OnUpdate:       "RESTRICT",
			},
			{StructName: "Project", Name: "slug", Type: "VARCHAR(64)", Nullable: false},
			{
				StructName: "Project",
				Name:       "status",
				Type:       "VARCHAR(16)",
				Nullable:   false,
				Default:    "active",
				DefaultSet: true,
			},
			{
				StructName:  "Project",
				Name:        "budget_cents",
				Type:        "INTEGER",
				Nullable:    false,
				DefaultExpr: "0",
				Check:       "budget_cents >= 0",
				CheckName:   "projects_budget_nonnegative",
			},
		},
		Constraints: []goschema.Constraint{
			{
				StructName: "Project",
				Name:       "projects_org_slug_unique",
				Type:       "UNIQUE",
				Table:      "projects",
				Columns:    []string{"organization_id", "slug"},
			},
			{
				StructName:      "Project",
				Name:            "projects_status_check",
				Type:            "CHECK",
				Table:           "projects",
				CheckExpression: "status IN ('active', 'archived')",
			},
		},
	}
	database := &types.DBSchema{
		Tables: []types.DBTable{
			{
				Name: "organizations",
				Type: "TABLE",
				Columns: []types.DBColumn{
					{Name: "id", DataType: "int", ColumnType: "int", IsNullable: "NO", IsPrimaryKey: true, IsAutoIncrement: true},
					{Name: "slug", DataType: "varchar(64)", ColumnType: "varchar(64)", IsNullable: "NO", IsUnique: true},
				},
			},
			{
				Name: "projects",
				Type: "TABLE",
				Columns: []types.DBColumn{
					{Name: "id", DataType: "int", ColumnType: "int", IsNullable: "NO", IsPrimaryKey: true, IsAutoIncrement: true},
					{Name: "organization_id", DataType: "int", ColumnType: "int", IsNullable: "NO"},
					{Name: "slug", DataType: "varchar(64)", ColumnType: "varchar(64)", IsNullable: "NO"},
					{Name: "status", DataType: "varchar(16)", ColumnType: "varchar(16)", IsNullable: "NO", ColumnDefault: &statusDefault},
					{Name: "budget_cents", DataType: "int", ColumnType: "int", IsNullable: "NO", ColumnDefault: &budgetDefault},
				},
			},
		},
		Constraints: []types.DBConstraint{
			{Name: "PRIMARY", TableName: "organizations", Type: "PRIMARY KEY", ColumnName: "id", ColumnNames: []string{"id"}},
			{Name: "slug", TableName: "organizations", Type: "UNIQUE", ColumnName: "slug", ColumnNames: []string{"slug"}},
			{Name: "PRIMARY", TableName: "projects", Type: "PRIMARY KEY", ColumnName: "id", ColumnNames: []string{"id"}},
			{
				Name:           "fk_projects_organization",
				TableName:      "projects",
				Type:           "FOREIGN KEY",
				ColumnName:     "organization_id",
				ColumnNames:    []string{"organization_id"},
				ForeignTable:   new("organizations"),
				ForeignColumn:  new("id"),
				ForeignColumns: []string{"id"},
				DeleteRule:     &deleteRule,
				UpdateRule:     &updateRule,
			},
			{
				Name:        "projects_budget_nonnegative",
				TableName:   "projects",
				Type:        "CHECK",
				CheckClause: &budgetCheck,
			},
			{
				Name:        "projects_org_slug_unique",
				TableName:   "projects",
				Type:        "UNIQUE",
				ColumnName:  "organization_id",
				ColumnNames: []string{"organization_id", "slug"},
			},
			{
				Name:        "projects_status_check",
				TableName:   "projects",
				Type:        "CHECK",
				CheckClause: &statusCheck,
			},
		},
	}

	diff := schemadiff.CompareWithDialect(generated, database, "mysql")
	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("round-trip diff: %+v", diff))
}

func TestCompareWithDialect_MySQLCharsetEscapedStringLiteralMatchesGeneratedEscapes(t *testing.T) {
	c := qt.New(t)
	checkClause := "(`name` <> _latin1\\'owner\\'s\\')"

	generated := &goschema.Database{
		Tables: []goschema.Table{{Name: "projects", StructName: "Project"}},
		Constraints: []goschema.Constraint{{
			StructName:      "Project",
			Name:            "projects_name_check",
			Type:            "CHECK",
			Table:           "projects",
			CheckExpression: "name <> 'owner''s'",
		}},
	}
	database := &types.DBSchema{
		Tables: []types.DBTable{{Name: "projects", Type: "TABLE"}},
		Constraints: []types.DBConstraint{{
			Name:        "projects_name_check",
			TableName:   "projects",
			Type:        "CHECK",
			CheckClause: &checkClause,
		}},
	}

	diff := schemadiff.CompareWithDialect(generated, database, "mysql")
	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("round-trip diff: %+v", diff))
}

func TestCompareWithDialect_SQLiteInlineEnumsMatchGeneratedEnumFields(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{
		Tables: []goschema.Table{{
			Name:       "products",
			StructName: "Product",
		}},
		Fields: []goschema.Field{
			{StructName: "Product", Name: "id", Type: "INTEGER", Primary: true},
			{
				StructName: "Product",
				Name:       "status",
				Type:       "enum_product_status",
				Enum:       []string{"draft", "active"},
				Nullable:   false,
			},
		},
		Enums: []goschema.Enum{{
			Name:   "enum_product_status",
			Values: []string{"draft", "active"},
		}},
	}
	check := "status IN ('draft', 'active')"
	database := &types.DBSchema{
		Tables: []types.DBTable{{
			Name: "products",
			Type: "TABLE",
			Columns: []types.DBColumn{
				{Name: "id", DataType: "INTEGER", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "status", DataType: "TEXT", IsNullable: "NO"},
			},
		}},
		Constraints: []types.DBConstraint{{
			Name:        "products_status_check",
			TableName:   "products",
			Type:        "CHECK",
			CheckClause: &check,
		}},
	}

	diff := schemadiff.CompareWithDialect(generated, database, "sqlite")
	c.Assert(diff.EnumsAdded, qt.HasLen, 0)
	c.Assert(diff.EnumsRemoved, qt.HasLen, 0)
	c.Assert(diff.TablesModified, qt.HasLen, 0)
	c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
	c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
}

func TestCompareWithDialect_SQLiteRenderedColumnTypesMatchCatalogReadback(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name          string
		generatedType string
		databaseType  string
	}{
		{name: "varchar renders as text", generatedType: "VARCHAR(255)", databaseType: "TEXT"},
		{name: "char renders as text", generatedType: "CHAR(2)", databaseType: "TEXT"},
		{name: "boolean renders as integer", generatedType: "BOOLEAN", databaseType: "INTEGER"},
		{name: "serial renders as integer", generatedType: "SERIAL", databaseType: "INTEGER"},
		{name: "bytea renders as blob", generatedType: "BYTEA", databaseType: "BLOB"},
		{name: "double precision renders as real", generatedType: "DOUBLE PRECISION", databaseType: "REAL"},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			diff := schemadiff.CompareWithDialect(
				sqliteColumnGeneratedSchema(tt.generatedType),
				sqliteColumnDatabaseSchema(tt.databaseType),
				"sqlite",
			)
			c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("round-trip diff: %+v", diff))
		})
	}
}

func TestCompareWithDialect_SQLiteDistinctColumnTypesStillDiff(t *testing.T) {
	c := qt.New(t)

	diff := schemadiff.CompareWithDialect(
		sqliteColumnGeneratedSchema("INTEGER"),
		sqliteColumnDatabaseSchema("TEXT"),
		"sqlite",
	)

	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].ColumnsModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].ColumnsModified[0].Changes["type"], qt.Equals, "text -> integer")
}

func TestCompareWithDialect_SQLiteDeclaredTypeDriftStillDiffs(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name          string
		generatedType string
		databaseType  string
		wantChange    string
	}{
		{name: "database boolean is not rendered integer", generatedType: "INTEGER", databaseType: "BOOLEAN", wantChange: "boolean -> integer"},
		{name: "database varchar is not rendered text", generatedType: "TEXT", databaseType: "VARCHAR(255)", wantChange: "varchar -> text"},
		{name: "database empty type is not rendered blob", generatedType: "BLOB", databaseType: "", wantChange: " -> blob"},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			diff := schemadiff.CompareWithDialect(
				sqliteColumnGeneratedSchema(tt.generatedType),
				sqliteColumnDatabaseSchema(tt.databaseType),
				"sqlite",
			)
			c.Assert(diff.TablesModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].ColumnsModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].ColumnsModified[0].Changes["type"], qt.Equals, tt.wantChange)
		})
	}
}

func TestCompareWithDialect_SQLiteUniqueConstraintAutoindexIsIgnored(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{
		Tables: []goschema.Table{{
			Name:       "projects",
			StructName: "Project",
		}},
		Fields: []goschema.Field{
			{StructName: "Project", Name: "organization_id", Type: "INTEGER", Nullable: false},
			{StructName: "Project", Name: "slug", Type: "VARCHAR(64)", Nullable: false},
		},
		Constraints: []goschema.Constraint{{
			Name:       "projects_org_slug_unique",
			Type:       "UNIQUE",
			Table:      "projects",
			StructName: "Project",
			Columns:    []string{"organization_id", "slug"},
		}},
	}
	database := &types.DBSchema{
		Tables: []types.DBTable{{
			Name: "projects",
			Type: "TABLE",
			Columns: []types.DBColumn{
				{Name: "organization_id", DataType: "INTEGER", ColumnType: "INTEGER", IsNullable: "NO"},
				{Name: "slug", DataType: "TEXT", ColumnType: "TEXT", IsNullable: "NO"},
			},
		}},
		Indexes: []types.DBIndex{{
			Name:      "sqlite_autoindex_projects_1",
			TableName: "projects",
			Columns:   []string{"organization_id", "slug"},
			IsUnique:  true,
		}},
		Constraints: []types.DBConstraint{{
			Name:        "projects_org_slug_unique",
			TableName:   "projects",
			Type:        "UNIQUE",
			ColumnName:  "organization_id",
			ColumnNames: []string{"organization_id", "slug"},
		}},
	}

	diff := schemadiff.CompareWithDialect(generated, database, "sqlite")

	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("round-trip diff: %+v", diff))
}

func TestCompareWithDialect_NonSQLiteAutoindexNameIsCompared(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{
		Tables: []goschema.Table{{Name: "projects", StructName: "Project"}},
	}
	database := &types.DBSchema{
		Tables: []types.DBTable{{Name: "projects", Type: "TABLE"}},
		Indexes: []types.DBIndex{{
			Name:      "sqlite_autoindex_projects_1",
			TableName: "projects",
			Columns:   []string{"slug"},
			IsUnique:  true,
		}},
	}

	diff := schemadiff.CompareWithDialect(generated, database, "postgres")

	c.Assert(diff.IndexesRemoved, qt.DeepEquals, []string{"sqlite_autoindex_projects_1"})
	c.Assert(diff.IndexesRemovedWithTables, qt.HasLen, 1)
	c.Assert(diff.IndexesRemovedWithTables[0].TableName, qt.Equals, "projects")
}

func TestCompareWithDialect_SQLServerInlineEnumsMatchGeneratedEnumFields(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{
		Tables: []goschema.Table{{
			Name:       "products",
			Schema:     "dbo",
			StructName: "Product",
		}},
		Fields: []goschema.Field{
			{StructName: "Product", Name: "id", Type: "INT", Primary: true},
			{
				StructName: "Product",
				Name:       "status",
				Type:       "enum_product_status",
				Enum:       []string{"draft", "active"},
				Nullable:   false,
				CheckName:  "products_status_check",
			},
		},
		Enums: []goschema.Enum{{
			Name:   "enum_product_status",
			Values: []string{"draft", "active"},
		}},
	}
	check := "[status] IN ('draft', 'active')"
	database := &types.DBSchema{
		Tables: []types.DBTable{{
			Name:   "products",
			Schema: "dbo",
			Type:   "TABLE",
			Columns: []types.DBColumn{
				{Name: "id", DataType: "INT", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "status", DataType: "NVARCHAR", ColumnType: "NVARCHAR(255)", IsNullable: "NO"},
			},
		}},
		Constraints: []types.DBConstraint{{
			Name:        "products_status_check",
			Schema:      "dbo",
			TableName:   "products",
			Type:        "CHECK",
			CheckClause: &check,
		}},
	}

	diff := schemadiff.CompareWithDialect(generated, database, "sqlserver")
	c.Assert(diff.EnumsAdded, qt.HasLen, 0)
	c.Assert(diff.EnumsRemoved, qt.HasLen, 0)
	c.Assert(diff.TablesModified, qt.HasLen, 0)
	c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
	c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
}

func sqliteColumnGeneratedSchema(columnType string) *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{
			Name:       "users",
			StructName: "User",
		}},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "User", Name: "value", Type: columnType, Nullable: false},
		},
	}
}

func sqliteColumnDatabaseSchema(columnType string) *types.DBSchema {
	return &types.DBSchema{
		Tables: []types.DBTable{{
			Name: "users",
			Type: "TABLE",
			Columns: []types.DBColumn{
				{Name: "id", DataType: "INTEGER", ColumnType: "INTEGER", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "value", DataType: columnType, ColumnType: columnType, IsNullable: "NO"},
			},
		}},
	}
}

func TestCompareWithOptions_CustomIgnoreList(t *testing.T) {
	c := qt.New(t)

	// Setup test data
	generated := &goschema.Database{
		Extensions: []goschema.Extension{
			{Name: "pg_trgm", IfNotExists: true},
		},
	}
	database := &types.DBSchema{
		Extensions: []types.DBExtension{
			{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},
			{Name: "adminpack", Version: "2.1", Schema: "public"},
		},
	}

	// Test with custom ignore list (ignore adminpack but not plpgsql)
	opts := config.WithIgnoredExtensions("adminpack")
	diff := schemadiff.CompareWithOptions(generated, database, opts)

	// adminpack should be ignored, plpgsql should be marked for removal
	c.Assert(diff.ExtensionsAdded, qt.DeepEquals, []string{"pg_trgm"})
	c.Assert(diff.ExtensionsRemoved, qt.DeepEquals, []string{"plpgsql"})
}

func TestCompareWithOptions_NoIgnoredExtensions(t *testing.T) {
	c := qt.New(t)

	// Setup test data
	generated := &goschema.Database{
		Extensions: []goschema.Extension{
			{Name: "pg_trgm", IfNotExists: true},
		},
	}
	database := &types.DBSchema{
		Extensions: []types.DBExtension{
			{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},
			{Name: "adminpack", Version: "2.1", Schema: "public"},
		},
	}

	// Test with no ignored extensions (manage all extensions)
	opts := config.WithIgnoredExtensions() // Empty list
	diff := schemadiff.CompareWithOptions(generated, database, opts)

	// All database extensions should be marked for removal
	c.Assert(diff.ExtensionsAdded, qt.DeepEquals, []string{"pg_trgm"})
	c.Assert(diff.ExtensionsRemoved, qt.DeepEquals, []string{"adminpack", "plpgsql"})
}

func TestCompareWithOptions_AdditionalIgnoredExtensions(t *testing.T) {
	c := qt.New(t)

	// Setup test data
	generated := &goschema.Database{
		Extensions: []goschema.Extension{
			{Name: "pg_trgm", IfNotExists: true},
		},
	}
	database := &types.DBSchema{
		Extensions: []types.DBExtension{
			{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},
			{Name: "adminpack", Version: "2.1", Schema: "public"},
			{Name: "pg_stat_statements", Version: "1.9", Schema: "public"},
		},
	}

	// Test with additional ignored extensions (default + adminpack)
	opts := config.WithAdditionalIgnoredExtensions("adminpack")
	diff := schemadiff.CompareWithOptions(generated, database, opts)

	// plpgsql and adminpack should be ignored, only pg_stat_statements should be removed
	c.Assert(diff.ExtensionsAdded, qt.DeepEquals, []string{"pg_trgm"})
	c.Assert(diff.ExtensionsRemoved, qt.DeepEquals, []string{"pg_stat_statements"})
}

func TestCompareWithOptions_NilOptions(t *testing.T) {
	c := qt.New(t)

	// Setup test data
	generated := &goschema.Database{
		Extensions: []goschema.Extension{
			{Name: "pg_trgm", IfNotExists: true},
		},
	}
	database := &types.DBSchema{
		Extensions: []types.DBExtension{
			{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},
		},
	}

	// Test with nil options (should use defaults)
	diff := schemadiff.CompareWithOptions(generated, database, nil)

	// Should behave the same as Compare() - ignore plpgsql by default
	c.Assert(diff.ExtensionsAdded, qt.DeepEquals, []string{"pg_trgm"})
	c.Assert(diff.ExtensionsRemoved, qt.DeepEquals, []string{})
}

func TestLibraryUsageExamples(t *testing.T) {
	c := qt.New(t)

	// Example data
	generated := &goschema.Database{
		Extensions: []goschema.Extension{
			{Name: "pg_trgm", IfNotExists: true},
			{Name: "btree_gin", IfNotExists: true},
		},
	}
	database := &types.DBSchema{
		Extensions: []types.DBExtension{
			{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},
			{Name: "pg_trgm", Version: "1.6", Schema: "public"},
		},
	}

	t.Run("simple usage with defaults", func(t *testing.T) {
		// Most common usage - just compare with defaults
		diff := schemadiff.Compare(generated, database)

		c.Assert(diff.ExtensionsAdded, qt.DeepEquals, []string{"btree_gin"})
		c.Assert(diff.ExtensionsRemoved, qt.DeepEquals, []string{}) // plpgsql ignored
	})

	t.Run("custom ignore list", func(t *testing.T) {
		// User wants to ignore specific extensions
		opts := config.WithIgnoredExtensions("plpgsql", "adminpack")
		diff := schemadiff.CompareWithOptions(generated, database, opts)

		c.Assert(diff.ExtensionsAdded, qt.DeepEquals, []string{"btree_gin"})
		c.Assert(diff.ExtensionsRemoved, qt.DeepEquals, []string{})
	})

	t.Run("manage all extensions", func(t *testing.T) {
		// User wants to manage all extensions (no ignoring)
		opts := config.WithIgnoredExtensions()
		diff := schemadiff.CompareWithOptions(generated, database, opts)

		c.Assert(diff.ExtensionsAdded, qt.DeepEquals, []string{"btree_gin"})
		c.Assert(diff.ExtensionsRemoved, qt.DeepEquals, []string{"plpgsql"})
	})

	t.Run("add to default ignore list", func(t *testing.T) {
		// User wants defaults plus additional ignored extensions
		opts := config.WithAdditionalIgnoredExtensions("uuid-ossp")
		diff := schemadiff.CompareWithOptions(generated, database, opts)

		c.Assert(diff.ExtensionsAdded, qt.DeepEquals, []string{"btree_gin"})
		c.Assert(diff.ExtensionsRemoved, qt.DeepEquals, []string{}) // plpgsql still ignored
	})
}
