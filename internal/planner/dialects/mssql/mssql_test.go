package mssql

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

func TestNewWithCapabilitiesUsesSQLServerDialect(t *testing.T) {
	c := qt.New(t)

	plan := New()
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

	_, err := plan.GenerateMigrationASTChecked(diff, generated)

	c.Assert(err, qt.ErrorMatches, `.*SQL Server planner only supports ALTER COLUMN for type/nullability changes on users\.status; unsupported changes: default.*`)
}

func TestNewWithCapabilitiesRejectsSQLServerColumnRemoval(t *testing.T) {
	c := qt.New(t)

	plan := New()
	diff := &types.SchemaDiff{
		TablesModified: []types.TableDiff{{
			TableName:      "users",
			ColumnsRemoved: []string{"legacy_id"},
		}},
	}
	generated := &goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Name: "users"}},
	}

	_, err := plan.GenerateMigrationASTChecked(diff, generated)

	c.Assert(err, qt.ErrorMatches, `.*SQL Server planner does not support automatic DROP COLUMN for users; write an explicit migration that drops dependent constraints and indexes first.*`)
}
