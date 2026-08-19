package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

func TestGenerateSchemaDiffSQL_SQLServerCreatesTSQL(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		TablesAdded: []string{"users"},
		IndexesAdded: []types.IndexRef{
			{Name: "idx_users_email", TableName: "dbo.users"},
		},
	}
	semantics := identifier.ForSQLServerCatalog("SQL_Latin1_General_CP1_CI_AS").
		WithResolvedNames([]identifier.ResolvedName{
			{Name: "dbo", Key: "dbo"},
			{Name: "email", Key: "email"},
			{Name: "id", Key: "id"},
			{Name: "idx_users_email", Key: "idx_users_email"},
			{Name: "status", Key: "status"},
			{Name: "users", Key: "users"},
		})
	diff.IdentifierSemantics = &semantics
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
	// The check carries the name the comparison looks for. Left unnamed, SQL
	// Server assigns its own -- CK__users__status__<hash>, is_system_named = 1
	// -- which no declaration can predict, so the first read-back disagreed and
	// the next apply renamed it (stokaro/ptah#1716).
	c.Assert(sql, qt.Contains,
		"[status] NVARCHAR(255) NOT NULL CONSTRAINT [users_status_check] CHECK ([status] IN ('active', 'blocked'))")
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

// TestGenerateSchemaDiffSQL_SQLServerFilteredIndexPredicateChange proves that
// a changed filtered-index predicate plans as DROP INDEX ... ON ... followed
// by CREATE INDEX ... WHERE ... carrying the target predicate (#781).
func TestGenerateSchemaDiffSQL_SQLServerFilteredIndexPredicateChange(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		IndexesAdded: []types.IndexRef{
			{Name: "idx_active_users", TableName: "dbo.users"},
		},
		IndexesRemoved: []types.IndexRef{
			{Name: "idx_active_users", TableName: "dbo.users"},
		},
	}
	semantics := identifier.ForSQLServerCatalog("SQL_Latin1_General_CP1_CI_AS").
		WithResolvedNames([]identifier.ResolvedName{
			{Name: "dbo", Key: "dbo"},
			{Name: "id", Key: "id"},
			{Name: "idx_active_users", Key: "idx_active_users"},
			{Name: "status", Key: "status"},
			{Name: "users", Key: "users"},
		})
	diff.IdentifierSemantics = &semantics
	generated := &goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Schema: "dbo", Name: "users"}},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "INT", Primary: true},
			{StructName: "User", Name: "status", Type: "INT", Nullable: false},
		},
		Indexes: []goschema.Index{{
			StructName: "User",
			Name:       "idx_active_users",
			Fields:     []string{"status"},
			Condition:  "[status] = (2)",
		}},
	}

	statements, err := planner.GenerateSchemaDiffSQLStatements(diff, generated, platform.SQLServer)

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.HasLen, 2)
	c.Assert(statements[0], qt.Equals, "DROP INDEX IF EXISTS [idx_active_users] ON [dbo].[users]")
	c.Assert(statements[1], qt.Equals, "CREATE INDEX [idx_active_users] ON [dbo].[users] ([status]) WHERE [status] = (2)")
}

// TestGenerateSchemaDiffSQL_SQLServerUnfilteredIndexStaysWithoutWhere guards
// that ordinary index additions keep rendering without a WHERE clause.
func TestGenerateSchemaDiffSQL_SQLServerUnfilteredIndexStaysWithoutWhere(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		IndexesAdded: []types.IndexRef{
			{Name: "idx_users_status", TableName: "dbo.users"},
		},
	}
	semantics := identifier.ForSQLServerCatalog("SQL_Latin1_General_CP1_CI_AS").
		WithResolvedNames([]identifier.ResolvedName{
			{Name: "dbo", Key: "dbo"},
			{Name: "idx_users_status", Key: "idx_users_status"},
			{Name: "status", Key: "status"},
			{Name: "users", Key: "users"},
		})
	diff.IdentifierSemantics = &semantics
	generated := &goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Schema: "dbo", Name: "users"}},
		Fields: []goschema.Field{
			{StructName: "User", Name: "status", Type: "INT", Nullable: false},
		},
		Indexes: []goschema.Index{{
			StructName: "User",
			Name:       "idx_users_status",
			Fields:     []string{"status"},
		}},
	}

	statements, err := planner.GenerateSchemaDiffSQLStatements(diff, generated, platform.SQLServer)

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.HasLen, 1)
	c.Assert(statements[0], qt.Equals, "CREATE INDEX [idx_users_status] ON [dbo].[users] ([status])")
}
