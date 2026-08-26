package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/schemadiff"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

func TestCompare_DefaultBehavior(t *testing.T) {
	c := qt.New(t)

	// Setup test data with plpgsql in database but not in generated schema
	desired := &schemamodel.Database{
		Extensions: []schemamodel.Extension{
			{Name: "pg_trgm", IfNotExists: true},
		},
	}
	current := &catalog.Database{
		Extensions: []catalog.Extension{
			{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},
		},
	}

	// Test default behavior (should ignore plpgsql)
	diff := schemadiff.Compare(desired, current)

	// plpgsql should be ignored by default, so no extensions should be removed
	c.Assert(diff.ExtensionsAdded, qt.DeepEquals, []string{"pg_trgm"})
	c.Assert(diff.ExtensionsRemoved, qt.DeepEquals, make([]string, 0))
}

func TestCompareWithDialect_MySQLFamilyInlineEnumsMatchGeneratedEnumFields(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			desired := &schemamodel.Database{
				Tables: []schemamodel.Table{{
					Name:       "products",
					StructName: "Product",
				}},
				Fields: []schemamodel.Field{
					{StructName: "Product", Name: "id", Type: "int", Primary: true},
					{
						StructName: "Product",
						Name:       "status",
						Type:       "enum_product_status",
						Enum:       []string{"draft", "active"},
						Nullable:   false,
					},
				},
				Enums: []schemamodel.Enum{{
					Name:   "enum_product_status",
					Values: []string{"draft", "active"},
				}},
			}
			current := &catalog.Database{
				Tables: []catalog.Table{{
					Name: "products",
					Type: "TABLE",
					Columns: []catalog.Column{
						{Name: "id", DataType: "int", IsNullable: "NO", IsPrimaryKey: true},
						{Name: "status", DataType: "enum('draft','active')", IsNullable: "NO"},
					},
				}},
				Enums: []catalog.Enum{{
					Name:   "enum_draft_active",
					Values: []string{"draft", "active"},
				}},
			}

			diff := schemadiff.CompareWithDialect(desired, current, dialect)
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
			desired := &schemamodel.Database{
				Tables: []schemamodel.Table{{Name: "users", StructName: "User"}},
				Fields: []schemamodel.Field{
					{StructName: "User", Name: "email_lc", Type: "TEXT", Nullable: true, GeneratedExpression: expression},
				},
			}
			current := &catalog.Database{
				Tables: []catalog.Table{{
					Name: "users",
					Type: "TABLE",
					Columns: []catalog.Column{{
						Name:                "email_lc",
						DataType:            "TEXT",
						IsNullable:          "YES",
						GeneratedExpression: &expression,
						GeneratedKind:       tt.databaseKind,
					}},
				}},
			}

			diff := schemadiff.CompareWithDialect(desired, current, tt.dialect)
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
			name:               "mariadb lower alias",
			dialect:            "mariadb",
			generatedType:      "VARCHAR(255)",
			generatedExpr:      "lower(email)",
			generatedKind:      "stored",
			databaseType:       "varchar",
			databaseColumnType: "varchar(255)",
			databaseExpr:       "lcase(`email`)",
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
			desired := &schemamodel.Database{
				Tables: []schemamodel.Table{{Name: "contacts", StructName: "Contact"}},
				Fields: []schemamodel.Field{
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
			current := &catalog.Database{
				Tables: []catalog.Table{{
					Name: "contacts",
					Type: "TABLE",
					Columns: []catalog.Column{{
						Name:                "email_normalized",
						DataType:            tt.databaseType,
						ColumnType:          tt.databaseColumnType,
						IsNullable:          "YES",
						GeneratedExpression: &tt.databaseExpr,
						GeneratedKind:       tt.databaseKind,
					}},
				}},
			}

			diff := schemadiff.CompareWithDialect(desired, current, tt.dialect)
			c.Assert(diff.TablesModified, qt.HasLen, 0)
		})
	}
}

func TestCompareWithDialect_GeneratedColumnStringLiteralMismatchIsAGap(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "contacts", StructName: "Contact"}},
		Fields: []schemamodel.Field{
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
	current := &catalog.Database{
		Tables: []catalog.Table{{
			Name: "contacts",
			Type: "TABLE",
			Columns: []catalog.Column{{
				Name:                "email_normalized",
				DataType:            "text",
				IsNullable:          "YES",
				GeneratedExpression: &databaseExpression,
				GeneratedKind:       "STORED",
			}},
		}},
	}

	diff := schemadiff.CompareWithDialect(desired, current, "mysql")
	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].ColumnsModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].ColumnsModified[0].Changes["generated"], qt.Contains, "'ACTIVE'")
}

func TestCompareWithDialect_GeneratedColumnEscapedStringLiteralMismatchIsAGap(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "contacts", StructName: "Contact"}},
		Fields: []schemamodel.Field{
			{
				StructName:          "Contact",
				Name:                "email_normalized",
				Type:                "TEXT",
				Nullable:            true,
				GeneratedExpression: `concat(email, 'can\'t lcase(email)')`,
				GeneratedKind:       "stored",
			},
		},
	}
	databaseExpression := `concat(` + "`email`" + `, 'can\'t lower(email)')`
	current := &catalog.Database{
		Tables: []catalog.Table{{
			Name: "contacts",
			Type: "TABLE",
			Columns: []catalog.Column{{
				Name:                "email_normalized",
				DataType:            "text",
				IsNullable:          "YES",
				GeneratedExpression: &databaseExpression,
				GeneratedKind:       "STORED",
			}},
		}},
	}

	diff := schemadiff.CompareWithDialect(desired, current, "mariadb")

	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].ColumnsModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].ColumnsModified[0].Changes["generated"], qt.Contains, `lcase`)
}

func TestCompareWithDialect_GeneratedColumnDoubleQuotedStringLiteralMismatchIsAGap(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "contacts", StructName: "Contact"}},
		Fields: []schemamodel.Field{
			{
				StructName:          "Contact",
				Name:                "email_normalized",
				Type:                "TEXT",
				Nullable:            true,
				GeneratedExpression: `concat(email, "lcase(email)")`,
				GeneratedKind:       "stored",
			},
		},
	}
	databaseExpression := `concat(` + "`email`" + `, "lower(email)")`
	current := &catalog.Database{
		Tables: []catalog.Table{{
			Name: "contacts",
			Type: "TABLE",
			Columns: []catalog.Column{{
				Name:                "email_normalized",
				DataType:            "text",
				IsNullable:          "YES",
				GeneratedExpression: &databaseExpression,
				GeneratedKind:       "STORED",
			}},
		}},
	}

	diff := schemadiff.CompareWithDialect(desired, current, "mariadb")

	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].ColumnsModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].ColumnsModified[0].Changes["generated"], qt.Contains, `lcase`)
}

func TestCompareWithDialect_MariaDBViewBodyMatchesCatalogReadback(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Views: []schemamodel.View{{
			Name: "live_products",
			Body: "SELECT id, name FROM products WHERE archived = false",
		}},
	}
	current := &catalog.Database{
		Views: []catalog.View{{
			Name:   "live_products",
			Schema: "conf",
			Body: "select `conf`.`products`.`id` AS `id`,`conf`.`products`.`name` AS `name` " +
				"from `conf`.`products` where `conf`.`products`.`archived` = 0",
		}},
	}

	diff := schemadiff.CompareWithDialect(desired, current, "mariadb")

	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("round-trip diff: %+v", diff))
}

func TestCompareWithDialect_MariaDBViewPredicateDriftStillDiffs(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Views: []schemamodel.View{{
			Name: "live_products",
			Body: "SELECT id, name FROM products WHERE archived = false",
		}},
	}
	current := &catalog.Database{
		Views: []catalog.View{{
			Name:   "live_products",
			Schema: "conf",
			Body: "select `conf`.`products`.`id` AS `id`,`conf`.`products`.`name` AS `name` " +
				"from `conf`.`products` where `conf`.`products`.`archived` = 1",
		}},
	}

	diff := schemadiff.CompareWithDialect(desired, current, "mariadb")

	c.Assert(diff.ViewsModified, qt.HasLen, 1)
	c.Assert(diff.ViewsModified[0].Changes["body"], qt.Not(qt.Equals), "")
}

func TestCompareWithDialect_MariaDBViewWrongSchemaQualifierStillDiffs(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Views: []schemamodel.View{{
			Name: "live_products",
			Body: "SELECT id, name FROM products WHERE archived = false",
		}},
	}
	current := &catalog.Database{
		Views: []catalog.View{{
			Name:   "live_products",
			Schema: "conf",
			Body: "select `otherdb`.`products`.`id` AS `id`,`otherdb`.`products`.`name` AS `name` " +
				"from `otherdb`.`products` where `otherdb`.`products`.`archived` = 0",
		}},
	}

	diff := schemadiff.CompareWithDialect(desired, current, "mariadb")

	c.Assert(diff.ViewsModified, qt.HasLen, 1)
	c.Assert(diff.ViewsModified[0].Changes["body"], qt.Not(qt.Equals), "")
}

func TestCompareWithDialect_MariaDBViewWrongSchemaRelationStillDiffs(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Views: []schemamodel.View{{
			Name: "live_products",
			Body: "SELECT id, name FROM products WHERE archived = false",
		}},
	}
	current := &catalog.Database{
		Views: []catalog.View{{
			Name:   "live_products",
			Schema: "conf",
			Body: "select `products`.`id` AS `id`,`products`.`name` AS `name` " +
				"from `otherdb`.`products` where `products`.`archived` = 0",
		}},
	}

	diff := schemadiff.CompareWithDialect(desired, current, "mariadb")

	c.Assert(diff.ViewsModified, qt.HasLen, 1)
	c.Assert(diff.ViewsModified[0].Changes["body"], qt.Not(qt.Equals), "")
}

func TestCompareWithDialect_MariaDBViewStringLiteralDriftStillDiffs(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Views: []schemamodel.View{{
			Name: "view_notes",
			Body: "SELECT 'archived = false' AS note",
		}},
	}
	current := &catalog.Database{
		Views: []catalog.View{{
			Name: "view_notes",
			Body: "select 'archived = 0' AS `note`",
		}},
	}

	diff := schemadiff.CompareWithDialect(desired, current, "mariadb")

	c.Assert(diff.ViewsModified, qt.HasLen, 1)
	c.Assert(diff.ViewsModified[0].Changes["body"], qt.Not(qt.Equals), "")
}

func TestCompareWithDialect_MariaDBViewEscapedStringLiteralDriftStillDiffs(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Views: []schemamodel.View{{
			Name: "view_notes",
			Body: `SELECT 'can\'t = false' AS note`,
		}},
	}
	current := &catalog.Database{
		Views: []catalog.View{{
			Name: "view_notes",
			Body: `select 'can\'t = 0' AS ` + "`note`",
		}},
	}

	diff := schemadiff.CompareWithDialect(desired, current, "mariadb")

	c.Assert(diff.ViewsModified, qt.HasLen, 1)
	c.Assert(diff.ViewsModified[0].Changes["body"], qt.Not(qt.Equals), "")
}

func TestCompareWithDialect_MariaDBViewDoubleQuotedStringLiteralDriftStillDiffs(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Views: []schemamodel.View{{
			Name: "view_notes",
			Body: `SELECT "archived = false" AS note`,
		}},
	}
	current := &catalog.Database{
		Views: []catalog.View{{
			Name: "view_notes",
			Body: `select "archived = 0" AS ` + "`note`",
		}},
	}

	diff := schemadiff.CompareWithDialect(desired, current, "mariadb")

	c.Assert(diff.ViewsModified, qt.HasLen, 1)
	c.Assert(diff.ViewsModified[0].Changes["body"], qt.Not(qt.Equals), "")
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
			desired := &schemamodel.Database{
				Tables: []schemamodel.Table{{Name: "users", Schema: "dbo", StructName: "User"}},
				Fields: []schemamodel.Field{{
					StructName:          "User",
					Name:                "email_lc",
					Type:                "NVARCHAR(320)",
					Nullable:            true,
					GeneratedExpression: generatedExpression,
					GeneratedKind:       "",
				}},
			}
			current := &catalog.Database{
				Tables: []catalog.Table{{
					Name:   "users",
					Schema: "dbo",
					Type:   "TABLE",
					Columns: []catalog.Column{{
						Name:                "email_lc",
						DataType:            "NVARCHAR",
						ColumnType:          "NVARCHAR(320)",
						IsNullable:          "YES",
						GeneratedExpression: &databaseExpression,
						GeneratedKind:       "PERSISTED",
					}},
				}},
			}

			diff := schemadiff.CompareWithDialect(desired, current, "sqlserver")
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

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "invoices", StructName: "Invoice"}},
		Fields: []schemamodel.Field{
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
	current := &catalog.Database{
		Tables: []catalog.Table{{
			Name: "invoices",
			Type: "TABLE",
			Columns: []catalog.Column{
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

	diff := schemadiff.CompareWithDialect(desired, current, "mysql")
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

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{Name: "organizations", StructName: "Organization"},
			{Name: "projects", StructName: "Project"},
		},
		Fields: []schemamodel.Field{
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
		Constraints: []schemamodel.Constraint{
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
	current := &catalog.Database{
		Tables: []catalog.Table{
			{
				Name: "organizations",
				Type: "TABLE",
				Columns: []catalog.Column{
					{Name: "id", DataType: "int", ColumnType: "int", IsNullable: "NO", IsPrimaryKey: true, IsAutoIncrement: true},
					{Name: "slug", DataType: "varchar(64)", ColumnType: "varchar(64)", IsNullable: "NO", IsUnique: true},
				},
			},
			{
				Name: "projects",
				Type: "TABLE",
				Columns: []catalog.Column{
					{Name: "id", DataType: "int", ColumnType: "int", IsNullable: "NO", IsPrimaryKey: true, IsAutoIncrement: true},
					{Name: "organization_id", DataType: "int", ColumnType: "int", IsNullable: "NO"},
					{Name: "slug", DataType: "varchar(64)", ColumnType: "varchar(64)", IsNullable: "NO"},
					{Name: "status", DataType: "varchar(16)", ColumnType: "varchar(16)", IsNullable: "NO", ColumnDefault: &statusDefault},
					{Name: "budget_cents", DataType: "int", ColumnType: "int", IsNullable: "NO", ColumnDefault: &budgetDefault},
				},
			},
		},
		Constraints: []catalog.Constraint{
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

	diff := schemadiff.CompareWithDialect(desired, current, "mysql")
	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("round-trip diff: %+v", diff))
}

func TestCompareWithDialect_MySQLCharsetEscapedStringLiteralMatchesGeneratedEscapes(t *testing.T) {
	c := qt.New(t)
	checkClause := "(`name` <> _latin1\\'owner\\'s\\')"

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "projects", StructName: "Project"}},
		Constraints: []schemamodel.Constraint{{
			StructName:      "Project",
			Name:            "projects_name_check",
			Type:            "CHECK",
			Table:           "projects",
			CheckExpression: "name <> 'owner''s'",
		}},
	}
	current := &catalog.Database{
		Tables: []catalog.Table{{Name: "projects", Type: "TABLE"}},
		Constraints: []catalog.Constraint{{
			Name:        "projects_name_check",
			TableName:   "projects",
			Type:        "CHECK",
			CheckClause: &checkClause,
		}},
	}

	diff := schemadiff.CompareWithDialect(desired, current, "mysql")
	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("round-trip diff: %+v", diff))
}

func TestCompareWithDialect_SQLiteInlineEnumsMatchGeneratedEnumFields(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{
			Name:       "products",
			StructName: "Product",
		}},
		Fields: []schemamodel.Field{
			{StructName: "Product", Name: "id", Type: "INTEGER", Primary: true},
			{
				StructName: "Product",
				Name:       "status",
				Type:       "enum_product_status",
				Enum:       []string{"draft", "active"},
				Nullable:   false,
			},
		},
		Enums: []schemamodel.Enum{{
			Name:   "enum_product_status",
			Values: []string{"draft", "active"},
		}},
	}
	check := "status IN ('draft', 'active')"
	current := &catalog.Database{
		Tables: []catalog.Table{{
			Name: "products",
			Type: "TABLE",
			Columns: []catalog.Column{
				{Name: "id", DataType: "INTEGER", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "status", DataType: "TEXT", IsNullable: "NO"},
			},
		}},
		Constraints: []catalog.Constraint{{
			Name:        "products_status_check",
			TableName:   "products",
			Type:        "CHECK",
			CheckClause: &check,
		}},
	}

	diff := schemadiff.CompareWithDialect(desired, current, "sqlite")
	c.Assert(diff.EnumsAdded, qt.HasLen, 0)
	c.Assert(diff.EnumsRemoved, qt.HasLen, 0)
	c.Assert(diff.TablesModified, qt.HasLen, 0)
	c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
	c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
}

func TestCompareWithDialect_SQLiteRenderedColumnTypesMatchCatalogReadback(t *testing.T) {
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
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			diff := schemadiff.CompareWithDialect(
				sqliteColumnGeneratedSchema(tt.generatedType),
				sqliteColumnDatabaseSchema(tt.databaseType),
				"sqlite",
			)
			c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("round-trip diff: %+v", diff))
		})
	}
}

// TestCompareWithDialect_SQLiteEquivalentDeclarationsDoNotRebuild pins that a
// spelling difference SQLite does not have is not a schema change.
//
// SQLite stores the declaration and never resolves it, deriving an affinity
// from the text at use time. Two declarations with one affinity are one type,
// and the plan for a difference between them is a table REBUILD -- drop,
// recreate, copy every row -- to change nothing an application can observe
// (stokaro/ptah#2040).
//
// The rows are the ones a hand-made database produces: nobody writing SQL by
// hand writes the canonical spelling Ptah's renderer would.
func TestCompareWithDialect_SQLiteEquivalentDeclarationsDoNotRebuild(t *testing.T) {
	tests := []struct {
		name          string
		generatedType string
		databaseType  string
	}{
		{name: "text against a hand-made varchar", generatedType: "TEXT", databaseType: "VARCHAR(255)"},
		{name: "text against a hand-made char", generatedType: "TEXT", databaseType: "CHARACTER(4)"},
		{name: "text against a hand-made clob", generatedType: "TEXT", databaseType: "CLOB"},
		{name: "blob against a column with no declared type", generatedType: "BLOB", databaseType: ""},
		{name: "integer against a hand-made bigint", generatedType: "INTEGER", databaseType: "BIGINT"},
		{name: "real against a hand-made double", generatedType: "REAL", databaseType: "DOUBLE PRECISION"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			diff := schemadiff.CompareWithDialect(
				sqliteColumnGeneratedSchema(tt.generatedType),
				sqliteColumnDatabaseSchema(tt.databaseType),
				"sqlite",
			)
			c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("diff: %+v", diff))
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
	// The raw spellings, not the affinities the comparison decided on: an
	// operator told `TEXT -> INTEGER` can find the column, and one told
	// `NUMERIC -> INTEGER` has to work out which of their columns that was
	// (stokaro/ptah#2040).
	c.Assert(diff.TablesModified[0].ColumnsModified[0].Changes["type"], qt.Equals, "TEXT -> INTEGER")
}

// TestCompareWithDialect_SQLiteDeclaredTypeDriftStillDiffs keeps the half of
// the SQLite comparison that must still report a change.
//
// Two rows moved out of it and into
// TestCompareWithDialect_SQLiteEquivalentDeclarationsDoNotRebuild, because
// they compared spellings SQLite gives the same meaning: `VARCHAR(255)` and
// `TEXT` are both TEXT affinity, and a column with no declared type and one
// declared `BLOB` are both BLOB. Reporting those planned a table rebuild that
// copied every row to change nothing an application can observe
// (stokaro/ptah#2040).
//
// This row stays because the affinities really do differ: `BOOLEAN` is
// NUMERIC and `INTEGER` is INTEGER, which is the pair that looks
// interchangeable and is not.
func TestCompareWithDialect_SQLiteDeclaredTypeDriftStillDiffs(t *testing.T) {
	tests := []struct {
		name          string
		generatedType string
		databaseType  string
		wantChange    string
	}{
		{
			name: "database boolean is not rendered integer", generatedType: "INTEGER",
			databaseType: "BOOLEAN", wantChange: "BOOLEAN -> INTEGER",
		},
		{
			name: "a text column is not a blob one", generatedType: "BLOB",
			databaseType: "TEXT", wantChange: "TEXT -> BLOB",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
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

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{
			Name:       "projects",
			StructName: "Project",
		}},
		Fields: []schemamodel.Field{
			{StructName: "Project", Name: "organization_id", Type: "INTEGER", Nullable: false},
			{StructName: "Project", Name: "slug", Type: "VARCHAR(64)", Nullable: false},
		},
		Constraints: []schemamodel.Constraint{{
			Name:       "projects_org_slug_unique",
			Type:       "UNIQUE",
			Table:      "projects",
			StructName: "Project",
			Columns:    []string{"organization_id", "slug"},
		}},
	}
	current := &catalog.Database{
		Tables: []catalog.Table{{
			Name: "projects",
			Type: "TABLE",
			Columns: []catalog.Column{
				{Name: "organization_id", DataType: "INTEGER", ColumnType: "INTEGER", IsNullable: "NO"},
				{Name: "slug", DataType: "TEXT", ColumnType: "TEXT", IsNullable: "NO"},
			},
		}},
		Indexes: []catalog.Index{{
			Name:      "sqlite_autoindex_projects_1",
			TableName: "projects",
			Columns:   []string{"organization_id", "slug"},
			IsUnique:  true,
		}},
		Constraints: []catalog.Constraint{{
			Name:        "projects_org_slug_unique",
			TableName:   "projects",
			Type:        "UNIQUE",
			ColumnName:  "organization_id",
			ColumnNames: []string{"organization_id", "slug"},
		}},
	}

	diff := schemadiff.CompareWithDialect(desired, current, "sqlite")

	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("round-trip diff: %+v", diff))
}

func TestCompareWithDialect_NonSQLiteAutoindexNameIsCompared(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "projects", StructName: "Project"}},
	}
	current := &catalog.Database{
		Tables: []catalog.Table{{Name: "projects", Type: "TABLE"}},
		Indexes: []catalog.Index{{
			Name:      "sqlite_autoindex_projects_1",
			TableName: "projects",
			Columns:   []string{"slug"},
			IsUnique:  true,
		}},
	}

	diff := schemadiff.CompareWithDialect(desired, current, "postgres")

	c.Assert(diff.IndexesRemoved, qt.DeepEquals, []difftypes.IndexRef{
		{Name: "sqlite_autoindex_projects_1", TableName: "projects"},
	})
}

func TestCompareWithDialect_SQLServerInlineEnumsMatchGeneratedEnumFields(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{
			Name:       "products",
			Schema:     "dbo",
			StructName: "Product",
		}},
		Fields: []schemamodel.Field{
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
		Enums: []schemamodel.Enum{{
			Name:   "enum_product_status",
			Values: []string{"draft", "active"},
		}},
	}
	check := "[status] IN ('draft', 'active')"
	current := &catalog.Database{
		Tables: []catalog.Table{{
			Name:   "products",
			Schema: "dbo",
			Type:   "TABLE",
			Columns: []catalog.Column{
				{Name: "id", DataType: "INT", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "status", DataType: "NVARCHAR", ColumnType: "NVARCHAR(255)", IsNullable: "NO"},
			},
		}},
		Constraints: []catalog.Constraint{{
			Name:        "products_status_check",
			Schema:      "dbo",
			TableName:   "products",
			Type:        "CHECK",
			CheckClause: &check,
		}},
	}

	diff := schemadiff.CompareWithDialect(desired, current, "sqlserver")
	c.Assert(diff.EnumsAdded, qt.HasLen, 0)
	c.Assert(diff.EnumsRemoved, qt.HasLen, 0)
	c.Assert(diff.TablesModified, qt.HasLen, 0)
	c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
	c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
}

func sqliteColumnGeneratedSchema(columnType string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{
			Name:       "users",
			StructName: "User",
		}},
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "User", Name: "value", Type: columnType, Nullable: false},
		},
	}
}

func sqliteColumnDatabaseSchema(columnType string) *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{{
			Name: "users",
			Type: "TABLE",
			Columns: []catalog.Column{
				{Name: "id", DataType: "INTEGER", ColumnType: "INTEGER", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "value", DataType: columnType, ColumnType: columnType, IsNullable: "NO"},
			},
		}},
	}
}

func TestCompareWithOptions_CustomIgnoreList(t *testing.T) {
	c := qt.New(t)

	// Setup test data
	desired := &schemamodel.Database{
		Extensions: []schemamodel.Extension{
			{Name: "pg_trgm", IfNotExists: true},
		},
	}
	current := &catalog.Database{
		Extensions: []catalog.Extension{
			{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},
			{Name: "adminpack", Version: "2.1", Schema: "public"},
		},
	}

	// Test with custom ignore list (ignore adminpack but not plpgsql)
	opts := config.WithIgnoredExtensions("adminpack")
	diff := schemadiff.CompareWithOptions(desired, current, opts)

	// adminpack should be ignored, plpgsql should be marked for removal
	c.Assert(diff.ExtensionsAdded, qt.DeepEquals, []string{"pg_trgm"})
	c.Assert(diff.ExtensionsRemoved, qt.DeepEquals, []string{"plpgsql"})
}

func TestCompareWithOptions_NoIgnoredExtensions(t *testing.T) {
	c := qt.New(t)

	// Setup test data
	desired := &schemamodel.Database{
		Extensions: []schemamodel.Extension{
			{Name: "pg_trgm", IfNotExists: true},
		},
	}
	current := &catalog.Database{
		Extensions: []catalog.Extension{
			{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},
			{Name: "adminpack", Version: "2.1", Schema: "public"},
		},
	}

	// Test with no ignored extensions (manage all extensions)
	opts := config.WithIgnoredExtensions() // Empty list
	diff := schemadiff.CompareWithOptions(desired, current, opts)

	// All database extensions should be marked for removal
	c.Assert(diff.ExtensionsAdded, qt.DeepEquals, []string{"pg_trgm"})
	c.Assert(diff.ExtensionsRemoved, qt.DeepEquals, []string{"adminpack", "plpgsql"})
}

func TestCompareWithOptions_AdditionalIgnoredExtensions(t *testing.T) {
	c := qt.New(t)

	// Setup test data
	desired := &schemamodel.Database{
		Extensions: []schemamodel.Extension{
			{Name: "pg_trgm", IfNotExists: true},
		},
	}
	current := &catalog.Database{
		Extensions: []catalog.Extension{
			{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},
			{Name: "adminpack", Version: "2.1", Schema: "public"},
			{Name: "pg_stat_statements", Version: "1.9", Schema: "public"},
		},
	}

	// Test with additional ignored extensions (default + adminpack)
	opts := config.WithAdditionalIgnoredExtensions("adminpack")
	diff := schemadiff.CompareWithOptions(desired, current, opts)

	// plpgsql and adminpack should be ignored, only pg_stat_statements should be removed
	c.Assert(diff.ExtensionsAdded, qt.DeepEquals, []string{"pg_trgm"})
	c.Assert(diff.ExtensionsRemoved, qt.DeepEquals, []string{"pg_stat_statements"})
}

func TestCompareWithOptions_NilOptions(t *testing.T) {
	c := qt.New(t)

	// Setup test data
	desired := &schemamodel.Database{
		Extensions: []schemamodel.Extension{
			{Name: "pg_trgm", IfNotExists: true},
		},
	}
	current := &catalog.Database{
		Extensions: []catalog.Extension{
			{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},
		},
	}

	// Test with nil options (should use defaults)
	diff := schemadiff.CompareWithOptions(desired, current, nil)

	// Should behave the same as Compare() - ignore plpgsql by default
	c.Assert(diff.ExtensionsAdded, qt.DeepEquals, []string{"pg_trgm"})
	c.Assert(diff.ExtensionsRemoved, qt.DeepEquals, make([]string, 0))
}

func TestLibraryUsageExamples(t *testing.T) {
	// Example data
	desired := &schemamodel.Database{
		Extensions: []schemamodel.Extension{
			{Name: "pg_trgm", IfNotExists: true},
			{Name: "btree_gin", IfNotExists: true},
		},
	}
	current := &catalog.Database{
		Extensions: []catalog.Extension{
			{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},
			{Name: "pg_trgm", Version: "1.6", Schema: "public"},
		},
	}

	t.Run("simple usage with defaults", func(t *testing.T) {
		// Most common usage - just compare with defaults
		c := qt.New(t)
		diff := schemadiff.Compare(desired, current)

		c.Assert(diff.ExtensionsAdded, qt.DeepEquals, []string{"btree_gin"})
		c.Assert(diff.ExtensionsRemoved, qt.DeepEquals, make([]string, 0)) // plpgsql ignored
	})

	t.Run("custom ignore list", func(t *testing.T) {
		// User wants to ignore specific extensions
		c := qt.New(t)
		opts := config.WithIgnoredExtensions("plpgsql", "adminpack")
		diff := schemadiff.CompareWithOptions(desired, current, opts)

		c.Assert(diff.ExtensionsAdded, qt.DeepEquals, []string{"btree_gin"})
		c.Assert(diff.ExtensionsRemoved, qt.DeepEquals, make([]string, 0))
	})

	t.Run("manage all extensions", func(t *testing.T) {
		// User wants to manage all extensions (no ignoring)
		c := qt.New(t)
		opts := config.WithIgnoredExtensions()
		diff := schemadiff.CompareWithOptions(desired, current, opts)

		c.Assert(diff.ExtensionsAdded, qt.DeepEquals, []string{"btree_gin"})
		c.Assert(diff.ExtensionsRemoved, qt.DeepEquals, []string{"plpgsql"})
	})

	t.Run("add to default ignore list", func(t *testing.T) {
		// User wants defaults plus additional ignored extensions
		c := qt.New(t)
		opts := config.WithAdditionalIgnoredExtensions("uuid-ossp")
		diff := schemadiff.CompareWithOptions(desired, current, opts)

		c.Assert(diff.ExtensionsAdded, qt.DeepEquals, []string{"btree_gin"})
		c.Assert(diff.ExtensionsRemoved, qt.DeepEquals, make([]string, 0)) // plpgsql still ignored
	})
}

// TestCompareWithDialect_SpannerStringSpellingsAreOneType pins that Spanner's
// two spellings of its single string type are not a schema change.
//
// Spanner's PostgreSQL interface has one string type. A `text` column and an
// unbounded `character varying` are the same STRING(MAX), and the catalog
// reports the second whichever was declared -- measured on the PGAdapter
// emulator v0.55.2, a column applied as `text` reads back as
// `character varying`. Comparing the spellings planned
// `ALTER COLUMN ... TYPE text` on every run of a document the database already
// matched, and the emulator answers that ALTER with a GOOGLESQL_RET_CHECK
// failure, so the drift could never be cleared (stokaro/ptah#2074).
func TestCompareWithDialect_SpannerStringSpellingsAreOneType(t *testing.T) {
	tests := []struct {
		name          string
		generatedType string
		databaseType  string
	}{
		{name: "declared text against the catalog's spelling", generatedType: "text", databaseType: "character varying"},
		{name: "declared varchar against the catalog's spelling", generatedType: "varchar", databaseType: "character varying"},
		{name: "declared text against varchar", generatedType: "text", databaseType: "varchar"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			diff := schemadiff.CompareWithDialect(
				sqliteColumnGeneratedSchema(tt.generatedType),
				sqliteColumnDatabaseSchema(tt.databaseType),
				"spanner",
			)
			c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("diff: %+v", diff))
		})
	}
}

// TestCompareWithDialect_SpannerWidthIsStillATypeChange is the control the fold
// above needs.
//
// A width is a real distinction on Spanner -- STRING(200) is not STRING(MAX) --
// so folding the unbounded spellings together must not fold a bounded one in
// with them, in either direction.
func TestCompareWithDialect_SpannerWidthIsStillATypeChange(t *testing.T) {
	tests := []struct {
		name          string
		generatedType string
		databaseType  string
	}{
		{name: "a bounded declaration against an unbounded column", generatedType: "varchar(200)", databaseType: "character varying"},
		{name: "an unbounded declaration against a bounded column", generatedType: "text", databaseType: "character varying(200)"},
		{name: "two different widths", generatedType: "varchar(200)", databaseType: "character varying(400)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			diff := schemadiff.CompareWithDialect(
				sqliteColumnGeneratedSchema(tt.generatedType),
				sqliteColumnDatabaseSchema(tt.databaseType),
				"spanner",
			)
			c.Assert(diff.HasChanges(), qt.IsTrue, qt.Commentf("diff: %+v", diff))
		})
	}
}

// TestCompareWithDialect_SpannerFoldDoesNotReachOtherTargets keeps the fold
// where it belongs: PostgreSQL has both types and they are not the same one.
func TestCompareWithDialect_SpannerFoldDoesNotReachOtherTargets(t *testing.T) {
	c := qt.New(t)

	diff := schemadiff.CompareWithDialect(
		sqliteColumnGeneratedSchema("text"),
		sqliteColumnDatabaseSchema("character varying"),
		"postgres",
	)

	c.Assert(diff.HasChanges(), qt.IsTrue, qt.Commentf("diff: %+v", diff))
}

// spannerForeignKeySchemas is the pair a Spanner foreign key produces: the
// constraint, and the index Spanner built to enforce it under the name Spanner
// chose.
func spannerForeignKeySchemas(indexName string) (*schemamodel.Database, *catalog.Database) {
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{Name: "parents", StructName: "Parent"},
			{Name: "children", StructName: "Child"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Parent", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Child", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Child", Name: "parent_id", Type: "BIGINT", Foreign: "parents(id)",
				ForeignKeyName: "children_parent_fk"},
		},
	}
	current := &catalog.Database{
		Tables: []catalog.Table{
			{Name: "parents", Type: "TABLE", Columns: []catalog.Column{
				{Name: "id", DataType: "bigint", ColumnType: "bigint", IsNullable: "NO", IsPrimaryKey: true},
			}},
			{Name: "children", Type: "TABLE", Columns: []catalog.Column{
				{Name: "id", DataType: "bigint", ColumnType: "bigint", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "parent_id", DataType: "bigint", ColumnType: "bigint", IsNullable: "NO"},
			}},
		},
		Constraints: []catalog.Constraint{{
			Name:           "children_parent_fk",
			TableName:      "children",
			Type:           "FOREIGN KEY",
			ColumnName:     "parent_id",
			ColumnNames:    []string{"parent_id"},
			ForeignTable:   new("parents"),
			ForeignColumn:  new("id"),
			ForeignColumns: []string{"id"},
		}},
		Indexes: []catalog.Index{{
			Name:      indexName,
			TableName: "children",
			Columns:   []string{"parent_id"},
		}},
	}
	return desired, current
}

// TestCompareWithDialect_SpannerForeignKeyBackingIndexIsNotDrift pins that the
// index Spanner builds for a foreign key is not reported as an index to drop.
//
// Spanner creates it and names it itself, so no desired state mentions it, and
// the ordinary "in the database, not in the desired state" arm planned a DROP.
// The server refuses that drop -- measured on the PGAdapter emulator v0.55.2:
//
//	Cannot drop index `IDX_children_parent_id_FBF4366D73F2084A`.
//	It is in use by foreign keys: `children_parent_fk`.
//
// so a document with a relationship applied once and failed on every run
// afterwards, with a plan that never changed (stokaro/ptah#2076).
func TestCompareWithDialect_SpannerForeignKeyBackingIndexIsNotDrift(t *testing.T) {
	c := qt.New(t)
	desired, current := spannerForeignKeySchemas("IDX_children_parent_id_FBF4366D73F2084A")

	diff := schemadiff.CompareWithDialect(desired, current, "spanner")

	c.Assert(diff.IndexesRemoved, qt.HasLen, 0, qt.Commentf("diff: %+v", diff))
}

// TestCompareWithDialect_SpannerStillDropsAnIndexAPersonWrote is the control.
//
// A user's own index over the foreign key's columns is a different object --
// measured on the emulator, declaring one leaves Spanner's beside it rather
// than reusing it -- so removing it from the document still drops it. Without
// the name test the columns alone would have claimed it, and an index a person
// wrote would have become impossible to remove through Ptah.
func TestCompareWithDialect_SpannerStillDropsAnIndexAPersonWrote(t *testing.T) {
	c := qt.New(t)
	desired, current := spannerForeignKeySchemas("children_parent_idx")

	diff := schemadiff.CompareWithDialect(desired, current, "spanner")

	c.Assert(diff.IndexesRemoved, qt.DeepEquals, []difftypes.IndexRef{
		{Name: "children_parent_idx", TableName: "children"},
	})
}

// TestCompareWithDialect_SpannerBackingIndexRuleStaysOnSpanner keeps the rule
// where it was measured. PostgreSQL builds no index for a foreign key, so a
// PostgreSQL index carrying that name is a person's and is dropped.
func TestCompareWithDialect_SpannerBackingIndexRuleStaysOnSpanner(t *testing.T) {
	c := qt.New(t)
	desired, current := spannerForeignKeySchemas("IDX_children_parent_id_FBF4366D73F2084A")

	diff := schemadiff.CompareWithDialect(desired, current, "postgres")

	c.Assert(diff.IndexesRemoved, qt.DeepEquals, []difftypes.IndexRef{
		{Name: "IDX_children_parent_id_FBF4366D73F2084A", TableName: "children"},
	})
}

// TestCompareWithDialect_SQLiteBooleanDefaultMatchesWhatTheRendererWrites is
// the comparator half of the renderer's affinity rule.
//
// SQLite has no boolean, so a declared `true` is written as `1` and read back
// as `1`. Comparing the declared word against the catalog's number reported a
// default change on a column that matched -- and on SQLite that plan is a table
// REBUILD, which copies every row to change nothing (stokaro/ptah#2092).
func TestCompareWithDialect_SQLiteBooleanDefaultMatchesWhatTheRendererWrites(t *testing.T) {
	tests := []struct {
		name          string
		columnType    string
		declared      string
		live          string
		wantNoChanges bool
	}{
		{name: "true against the number it renders as", columnType: "INTEGER", declared: "true", live: "1", wantNoChanges: true},
		{name: "false against the number it renders as", columnType: "INTEGER", declared: "false", live: "0", wantNoChanges: true},
		{
			// The control: TEXT affinity keeps the word, so the word is what
			// the column holds and a number there is a real difference.
			name: "a text column keeps the word", columnType: "TEXT", declared: "true", live: "1", wantNoChanges: false,
		},
		{
			name: "a real difference is still reported", columnType: "INTEGER", declared: "true", live: "0", wantNoChanges: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			desired := sqliteColumnGeneratedSchema(tt.columnType)
			desired.Fields[1].Default = tt.declared
			current := sqliteColumnDatabaseSchema(tt.columnType)
			current.Tables[0].Columns[1].ColumnDefault = &tt.live

			diff := schemadiff.CompareWithDialect(desired, current, "sqlite")

			c.Assert(!diff.HasChanges(), qt.Equals, tt.wantNoChanges, qt.Commentf("diff: %+v", diff))
		})
	}
}
