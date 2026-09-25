package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/planner"
)

// OmitNullBackfill plans a column made NOT NULL without the fill from its
// declared default. The PostgreSQL plan then writes SET NOT NULL and
// SET DEFAULT as Atlas CE v1.3.0 does, where the default plan fills the NULL
// rows first. MySQL's MODIFY never fills and reads nothing from the option.
func TestGenerateSchemaDiffSQLStatementsWithOptions_OmitNullBackfill(t *testing.T) {
	const header = "-- Add/modify columns for table: flags --\n" +
		"-- Modify column flags.c: default_expr:  -> 9, nullable: true -> false --\n" +
		"-- ALTER statements: --\n"
	tests := []struct {
		name    string
		dialect string
		omit    bool
		want    []string
	}{
		{
			name:    "PostgreSQL, the default plan",
			dialect: platform.Postgres,
			want: []string{
				header + "DO $$\nBEGIN\n" +
					"    IF EXISTS (SELECT 1 FROM \"flags\" WHERE \"c\" IS NULL LIMIT 1) THEN\n" +
					"        UPDATE \"flags\" SET \"c\" = '9' WHERE \"c\" IS NULL;\n" +
					"    END IF;\nEND\n$$",
				`ALTER TABLE "flags" ALTER COLUMN "c" SET NOT NULL`,
				`ALTER TABLE "flags" ALTER COLUMN "c" SET DEFAULT 9`,
			},
		},
		{
			name:    "PostgreSQL, the fill omitted",
			dialect: platform.Postgres,
			omit:    true,
			want: []string{
				header + "-- POSTGRES: SET NOT NULL fails if any row of \"flags\" holds NULL in \"c\"; " +
					"this plan does not fill it with the column's default.\n" +
					`ALTER TABLE "flags" ALTER COLUMN "c" SET NOT NULL`,
				`ALTER TABLE "flags" ALTER COLUMN "c" SET DEFAULT 9`,
			},
		},
		{
			name:    "MySQL, the fill omitted",
			dialect: platform.MySQL,
			omit:    true,
			want: []string{"-- Modify table: flags --\n" +
				"-- Modify column flags.c: default_expr:  -> 9, nullable: true -> false --\n" +
				"-- ALTER statements: --\n" +
				"ALTER TABLE `flags` MODIFY COLUMN `c` INTEGER NOT NULL DEFAULT 9"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := oneModifiedColumn(
				schemamodel.Field{Name: "c", Type: "INTEGER", StructName: "Flag", Default: "9"},
				map[string]string{"nullable": "true -> false", "default_expr": " -> 9"},
			)

			got, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(diff, test.dialect, planner.Options{OmitNullBackfill: test.omit})

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}
