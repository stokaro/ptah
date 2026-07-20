package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/migration/planner"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

func TestGenerateSchemaDiffSQL_SQLServerCreatesTSQL(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		TablesAdded:  []string{"users"},
		IndexesAdded: []string{"idx_users_email"},
	}
	generated := &goschema.Database{
		Tables: []goschema.Table{{
			StructName: "User",
			Schema:     "dbo",
			Name:       "users",
		}},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "SERIAL", Primary: true, AutoInc: true},
			{StructName: "User", Name: "email", Type: "VARCHAR(320)", Nullable: false},
			{StructName: "User", Name: "status", Type: "enum_user_status", Nullable: false},
		},
		Enums: []goschema.Enum{{
			Name:   "enum_user_status",
			Values: []string{"active", "blocked"},
		}},
		Indexes: []goschema.Index{{
			StructName: "User",
			Name:       "idx_users_email",
			Fields:     []string{"email"},
		}},
	}

	sql, err := planner.GenerateSchemaDiffSQL(diff, generated, platform.SQLServer)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "CREATE TABLE [dbo].[users] (")
	c.Assert(sql, qt.Contains, "[id] INT IDENTITY(1,1) PRIMARY KEY")
	c.Assert(sql, qt.Contains, "[email] NVARCHAR(320) NOT NULL")
	c.Assert(sql, qt.Contains, "[status] NVARCHAR(255) NOT NULL CHECK ([status] IN ('active', 'blocked'))")
	c.Assert(sql, qt.Contains, "CREATE INDEX [idx_users_email] ON [dbo].[users] ([email]);")
	c.Assert(sql, qt.Not(qt.Contains), "MySQL")
}

func TestGetPlanner_SQLServerAlias(t *testing.T) {
	c := qt.New(t)

	p, err := planner.GetPlanner("mssql")

	c.Assert(err, qt.IsNil)
	c.Assert(p, qt.IsNotNil)
}

func TestGenerateSchemaDiffSQL_SQLServerRejectsUnsupportedColumnDrift(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		TablesModified: []types.TableDiff{{
			TableName: "users",
			ColumnsModified: []types.ColumnDiff{{
				ColumnName: "status",
				Changes: map[string]string{
					"default": "'inactive' -> 'active'",
				},
			}},
		}},
	}
	generated := &goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Name: "users"}},
		Fields: []goschema.Field{
			{StructName: "User", Name: "status", Type: "NVARCHAR(255)", Default: "active"},
		},
	}

	_, err := planner.GenerateSchemaDiffSQL(diff, generated, platform.SQLServer)

	c.Assert(err, qt.ErrorMatches, `.*SQL Server planner only supports ALTER COLUMN for type/nullability changes on users\.status; unsupported changes: default.*`)
}

func TestGenerateSchemaDiffSQL_SQLServerAddsColumnToQualifiedTable(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		TablesModified: []types.TableDiff{{
			TableName:    "dbo.users",
			ColumnsAdded: []string{"nickname"},
		}},
	}
	generated := &goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Schema: "dbo", Name: "users"}},
		Fields: []goschema.Field{
			{StructName: "User", Name: "nickname", Type: "VARCHAR(64)", Nullable: true},
		},
	}

	sql, err := planner.GenerateSchemaDiffSQL(diff, generated, platform.SQLServer)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "ALTER TABLE [dbo].[users] ADD [nickname] NVARCHAR(64);")
}

func TestGenerateSchemaDiffSQL_SQLServerModifiesColumnOnQualifiedTable(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		TablesModified: []types.TableDiff{{
			TableName: "dbo.users",
			ColumnsModified: []types.ColumnDiff{{
				ColumnName: "email",
				Changes: map[string]string{
					"type":     "NVARCHAR(100) -> NVARCHAR(320)",
					"nullable": "true -> false",
				},
			}},
		}},
	}
	generated := &goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Schema: "dbo", Name: "users"}},
		Fields: []goschema.Field{
			{StructName: "User", Name: "email", Type: "NVARCHAR(320)", Nullable: false},
		},
	}

	sql, err := planner.GenerateSchemaDiffSQL(diff, generated, platform.SQLServer)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "ALTER TABLE [dbo].[users] ALTER COLUMN [email] NVARCHAR(320) NOT NULL;")
	c.Assert(sql, qt.Contains, "-- Modify column dbo.users.email: nullable: true -> false, type: NVARCHAR(100) -> NVARCHAR(320)")
}

func TestGenerateSchemaDiffSQL_SQLServerRejectsColumnRemoval(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		TablesModified: []types.TableDiff{{
			TableName:      "users",
			ColumnsRemoved: []string{"legacy_id"},
		}},
	}
	generated := &goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Name: "users"}},
	}

	_, err := planner.GenerateSchemaDiffSQL(diff, generated, platform.SQLServer)

	c.Assert(err, qt.ErrorMatches, `.*SQL Server planner does not support automatic DROP COLUMN for users; write an explicit migration that drops dependent constraints and indexes first.*`)
}
