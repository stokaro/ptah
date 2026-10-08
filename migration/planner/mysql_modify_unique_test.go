package planner_test

import (
	"context"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
)

// On MySQL and MariaDB a UNIQUE in MODIFY COLUMN asks for a unique key, and the
// server builds one whatever keys the column already has. Measured on MySQL
// 8.4.11 and MariaDB 11.8.9 with `x int UNIQUE` changed to `x bigint UNIQUE`:
// `MODIFY COLUMN x bigint UNIQUE` left keys `x` and `x_2`, and the next
// comparison dropped `x_2`. Atlas CE v1.3.0 writes `MODIFY COLUMN x bigint
// NULL` and its next comparison is synced. So the MODIFY says UNIQUE only when
// the change gives the column its UNIQUE (stokaro/ptah#3857).
func TestGenerateSchemaDiffSQLStatements_MySQLFamilyModifyStatesUniqueOnlyWhenItAddsTheKey(t *testing.T) {
	tests := []struct {
		name    string
		desired schemamodel.Field
		changes map[string]string
		want    string
	}{
		{
			name:    "a UNIQUE column's type changes",
			desired: schemamodel.Field{Name: "x", Type: "BIGINT", StructName: "Flag", Nullable: true, Unique: true},
			changes: map[string]string{"type": "int -> bigint"},
			want:    "ALTER TABLE `flags` MODIFY COLUMN `x` BIGINT",
		},
		{
			name:    "a UNIQUE column becomes NOT NULL",
			desired: schemamodel.Field{Name: "x", Type: "INT", StructName: "Flag", Unique: true},
			changes: map[string]string{"nullable": "true -> false"},
			want:    "ALTER TABLE `flags` MODIFY COLUMN `x` INT NOT NULL",
		},
		{
			name:    "a UNIQUE column gains a default",
			desired: schemamodel.Field{Name: "x", Type: "INT", StructName: "Flag", Nullable: true, Unique: true, Default: "5"},
			changes: map[string]string{"default": " -> 5"},
			want:    "ALTER TABLE `flags` MODIFY COLUMN `x` INT DEFAULT 5",
		},
		{
			name:    "a column gains its UNIQUE",
			desired: schemamodel.Field{Name: "x", Type: "INT", StructName: "Flag", Nullable: true, Unique: true},
			changes: map[string]string{"unique": "false -> true"},
			want:    "ALTER TABLE `flags` MODIFY COLUMN `x` INT UNIQUE",
		},
		{
			name:    "a column gains its UNIQUE as its type changes",
			desired: schemamodel.Field{Name: "x", Type: "BIGINT", StructName: "Flag", Nullable: true, Unique: true},
			changes: map[string]string{"type": "int -> bigint", "unique": "false -> true"},
			want:    "ALTER TABLE `flags` MODIFY COLUMN `x` BIGINT UNIQUE",
		},
		{
			name:    "a column loses its UNIQUE",
			desired: schemamodel.Field{Name: "x", Type: "INT", StructName: "Flag", Nullable: true},
			changes: map[string]string{"unique": "true -> false"},
			want:    "ALTER TABLE `flags` MODIFY COLUMN `x` INT",
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
				c.Assert(got[0], qt.Matches, `(?s).*-- ALTER statements: --\n`+regexp.QuoteMeta(test.want))
			})
		}
	}
}
