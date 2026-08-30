package mssql

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

func TestNewWithCapabilitiesUsesSQLServerDialect(t *testing.T) {
	c := qt.New(t)

	plan := New()
	diff := &difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{{
			TableName: "users",
			ColumnsModified: []difftypes.ColumnDiff{{
				ColumnName: "status",
				Changes: map[string]string{
					"default": "'inactive' -> 'active'",
				},
			}},
		}},
	}

	_, err := plan.GenerateMigrationAST(diff)

	c.Assert(err, qt.ErrorMatches, `.*SQL Server planner only supports ALTER COLUMN for type/nullability changes on users\.status; unsupported changes: default.*`)
}

func TestNewWithCapabilitiesRejectsSQLServerColumnRemoval(t *testing.T) {
	c := qt.New(t)

	plan := New()
	diff := &difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{{
			TableName:      "users",
			ColumnsRemoved: difftypes.ColumnChanges{{Name: "legacy_id"}},
		}},
	}

	_, err := plan.GenerateMigrationAST(diff)

	c.Assert(err, qt.ErrorMatches, `.*SQL Server planner does not support automatic DROP COLUMN for users; write an explicit migration that drops dependent constraints and indexes first.*`)
}
