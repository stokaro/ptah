package planner_test

import (
	"context"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
)

// On MySQL and MariaDB a PRIMARY KEY in MODIFY COLUMN asks for a key the table
// already has. Measured on master against MySQL 26.7 and MariaDB 12.3,
// `MODIFY COLUMN id BIGINT PRIMARY KEY` on the key column answers Error 1068,
// "Multiple primary key defined", so neither a type change nor an
// auto-increment change of a key column could be applied. The MODIFY says
// PRIMARY KEY only when the change makes the column the key, and restates the
// key column NOT NULL.
func TestGenerateSchemaDiffSQLStatements_MySQLFamilyModifyStatesPrimaryKeyOnlyWhenItAddsTheKey(t *testing.T) {
	tests := []struct {
		name    string
		desired schemamodel.Field
		changes map[string]string
		want    string
	}{
		{
			name:    "the key column gains AUTO_INCREMENT",
			desired: schemamodel.Field{Name: "id", Type: "INT", StructName: "Flag", Primary: true, AutoInc: true},
			changes: map[string]string{"auto_increment": "false -> true"},
			want:    "ALTER TABLE `flags` MODIFY COLUMN `id` INT NOT NULL AUTO_INCREMENT",
		},
		{
			name:    "the key column loses AUTO_INCREMENT",
			desired: schemamodel.Field{Name: "id", Type: "INT", StructName: "Flag", Primary: true},
			changes: map[string]string{"auto_increment": "true -> false"},
			want:    "ALTER TABLE `flags` MODIFY COLUMN `id` INT NOT NULL",
		},
		{
			name:    "the key column's type changes",
			desired: schemamodel.Field{Name: "id", Type: "BIGINT", StructName: "Flag", Primary: true},
			changes: map[string]string{"type": "int -> bigint"},
			want:    "ALTER TABLE `flags` MODIFY COLUMN `id` BIGINT NOT NULL",
		},
		{
			name:    "a column becomes the key",
			desired: schemamodel.Field{Name: "id", Type: "INT", StructName: "Flag", Primary: true},
			changes: map[string]string{"primary_key": "false -> true"},
			want:    "ALTER TABLE `flags` MODIFY COLUMN `id` INT PRIMARY KEY",
		},
	}
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)

				got, err := planner.GenerateSchemaDiffSQLStatements(
					context.Background(), must.Must(builtin.New()),
					oneModifiedColumn(test.desired, test.changes), dialect,
				)

				c.Assert(err, qt.IsNil)
				c.Assert(got, qt.HasLen, 1)
				c.Assert(got[0], qt.Matches, `(?s).*`+regexp.QuoteMeta(test.want))
			})
		}
	}
}

// SQL Server's ALTER COLUMN has no IDENTITY: SQL Server 2025 (17.0.5005.3)
// answers `ALTER TABLE t ALTER COLUMN id INT IDENTITY(1,1) NOT NULL` with
// Msg 156, "Incorrect syntax near the keyword 'IDENTITY'". A change of a
// column's identity is refused by name before any statement exists.
func TestGenerateSchemaDiffSQLStatements_SQLServerIdentityChange_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		desired schemamodel.Field
		change  string
	}{
		{name: "a column gains IDENTITY", desired: schemamodel.Field{Name: "id", Type: "INT", StructName: "Flag", Primary: true, AutoInc: true}, change: "false -> true"},
		{name: "a column loses IDENTITY", desired: schemamodel.Field{Name: "id", Type: "INT", StructName: "Flag", Primary: true}, change: "true -> false"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := planner.GenerateSchemaDiffSQLStatements(
				context.Background(), must.Must(builtin.New()),
				oneModifiedColumn(test.desired, map[string]string{"auto_increment": test.change}), platform.SQLServer,
			)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `(?s).*SQL Server cannot add IDENTITY to an existing column or remove it from one: `+
				`flags\.id changes identity `+regexp.QuoteMeta(test.change)+`; .*`)
			c.Assert(got, qt.IsNil)
		})
	}
}
