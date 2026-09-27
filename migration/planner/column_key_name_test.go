package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff/difftypes"
)

// uniqueX is c.x declared UNIQUE.
func uniqueX() schemamodel.Field {
	return schemamodel.Field{Name: "x", Type: "INTEGER", StructName: "C", Nullable: true, Unique: true}
}

// xGainsKey is a diff in which c.x gains its own UNIQUE and the constraints and
// indexes given are removed.
func xGainsKey(constraints []difftypes.ConstraintRemovalInfo, indexes []difftypes.IndexRef) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{{
			TableName: "c",
			ColumnsModified: []difftypes.ColumnDiff{
				{ColumnName: "x", Desired: uniqueX(), Changes: map[string]string{"unique": "false -> true"}},
			},
		}},
		ConstraintsRemoved: constraints,
		IndexesRemoved:     indexes,
	}
}

// xAddedWithKey is a diff that adds c.x with its own UNIQUE and removes the
// constraints given.
func xAddedWithKey(constraints []difftypes.ConstraintRemovalInfo) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		TablesModified:     []difftypes.TableDiff{{TableName: "c", ColumnsAdded: difftypes.ColumnChanges{uniqueX()}}},
		ConstraintsRemoved: constraints,
		DeclaredTables:     []schemamodel.Table{{Name: "c", StructName: "C"}},
	}
}

// removedKey is a removed constraint of kind named name on table.
func removedKey(table, name, kind string) []difftypes.ConstraintRemovalInfo {
	return []difftypes.ConstraintRemovalInfo{{Name: name, TableName: table, Type: kind}}
}

// TestGenerateSchemaDiffSQLStatements_KeyHoldingTheColumnKeysNameGoesFirst
// covers stokaro/ptah#3723. A removed key that holds the name a column's own
// UNIQUE takes is dropped before the column takes its key, and not again
// where the removals land. Measured with Atlas CE v1.3.0 on MySQL 8.4.11,
// MariaDB 11.8.9 and PostgreSQL 18.6: dropped after, the MySQL-family key is
// named `x_2` and the next comparison renames it, and PostgreSQL refuses ADD
// CONSTRAINT c_x_key with `relation "c_x_key" already exists`, where Atlas CE
// drops the holder first.
func TestGenerateSchemaDiffSQLStatements_KeyHoldingTheColumnKeysNameGoesFirst(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		diff    *difftypes.SchemaDiff
		want    []string
	}{
		{
			name:    "MySQL, a UNIQUE named after the column",
			dialect: platform.MySQL,
			diff:    xGainsKey(removedKey("c", "x", "UNIQUE"), nil),
			want: []string{
				"-- ALTER statements: --\nALTER TABLE `c` DROP INDEX `x`",
				"-- Modify table: c --\n-- Modify column c.x: unique: false -> true --\n-- ALTER statements: --\n" +
					"ALTER TABLE `c` MODIFY COLUMN `x` INTEGER UNIQUE",
			},
		},
		{
			name:    "MariaDB, a UNIQUE named after the column",
			dialect: platform.MariaDB,
			diff:    xGainsKey(removedKey("c", "x", "UNIQUE"), nil),
			want: []string{
				"-- ALTER statements: --\nALTER TABLE `c` DROP INDEX IF EXISTS `x`",
				"-- Modify table: c --\n-- Modify column c.x: unique: false -> true --\n-- ALTER statements: --\n" +
					"ALTER TABLE `c` MODIFY COLUMN `x` INTEGER UNIQUE",
			},
		},
		{
			name:    "MySQL, an index named after the column in another case",
			dialect: platform.MySQL,
			diff:    xGainsKey(nil, []difftypes.IndexRef{{Name: "X", TableName: "c"}}),
			want: []string{
				"DROP INDEX `X` ON `c`",
				"-- Modify table: c --\n-- Modify column c.x: unique: false -> true --\n-- ALTER statements: --\n" +
					"ALTER TABLE `c` MODIFY COLUMN `x` INTEGER UNIQUE",
			},
		},
		{
			name:    "MySQL, a column added with its key",
			dialect: platform.MySQL,
			diff:    xAddedWithKey(removedKey("c", "x", "UNIQUE")),
			want: []string{
				"-- ALTER statements: --\nALTER TABLE `c` DROP INDEX `x`",
				"-- Modify table: c --\n-- ALTER statements: --\nALTER TABLE `c` ADD COLUMN `x` INTEGER UNIQUE",
			},
		},
		{
			name:    "PostgreSQL, a UNIQUE under the table's name for the column",
			dialect: platform.Postgres,
			diff:    xGainsKey(removedKey("c", "c_x_key", "UNIQUE"), nil),
			want: []string{
				"-- ALTER statements: --\n" + `ALTER TABLE "c" DROP CONSTRAINT IF EXISTS "c_x_key"`,
				"-- Add/modify columns for table: c --\n-- Modify column c.x: unique: false -> true --\n-- ALTER statements: --\n" +
					`ALTER TABLE "c" ADD CONSTRAINT "c_x_key" UNIQUE ("x")`,
			},
		},
		{
			name:    "PostgreSQL, a CHECK under the table's name for the column",
			dialect: platform.Postgres,
			diff:    xGainsKey(removedKey("c", "c_x_key", "CHECK"), nil),
			want: []string{
				"-- ALTER statements: --\n" + `ALTER TABLE "c" DROP CONSTRAINT IF EXISTS "c_x_key"`,
				"-- Add/modify columns for table: c --\n-- Modify column c.x: unique: false -> true --\n-- ALTER statements: --\n" +
					`ALTER TABLE "c" ADD CONSTRAINT "c_x_key" UNIQUE ("x")`,
			},
		},
		{
			name:    "PostgreSQL, an index under the table's name for the column",
			dialect: platform.Postgres,
			diff:    xGainsKey(nil, []difftypes.IndexRef{{Name: "c_x_key", TableName: "c"}}),
			want: []string{
				`DROP INDEX IF EXISTS "c_x_key"`,
				"-- Add/modify columns for table: c --\n-- Modify column c.x: unique: false -> true --\n-- ALTER statements: --\n" +
					`ALTER TABLE "c" ADD CONSTRAINT "c_x_key" UNIQUE ("x")`,
			},
		},
		{
			name:    "PostgreSQL, a column added with its key",
			dialect: platform.Postgres,
			diff:    xAddedWithKey(removedKey("c", "c_x_key", "UNIQUE")),
			want: []string{
				"-- ALTER statements: --\n" + `ALTER TABLE "c" DROP CONSTRAINT IF EXISTS "c_x_key"`,
				"-- Add/modify columns for table: c --\n-- ALTER statements: --\n" + `ALTER TABLE "c" ADD COLUMN "x" INTEGER UNIQUE`,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := planner.GenerateSchemaDiffSQLStatements(test.diff, test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestGenerateSchemaDiffSQLStatements_KeyUnderAnotherNameStaysWithTheRemovals
// is the control for the rows above. A removed key that holds no name the
// column's key takes is dropped after the column takes its key, where every
// removal lands; so is one on an engine whose naming is not measured.
func TestGenerateSchemaDiffSQLStatements_KeyUnderAnotherNameStaysWithTheRemovals(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		diff    *difftypes.SchemaDiff
		want    []string
	}{
		{
			name:    "MySQL, a UNIQUE under another name",
			dialect: platform.MySQL,
			diff:    xGainsKey(removedKey("c", "c_x_uq", "UNIQUE"), nil),
			want: []string{
				"-- Modify table: c --\n-- Modify column c.x: unique: false -> true --\n-- ALTER statements: --\n" +
					"ALTER TABLE `c` MODIFY COLUMN `x` INTEGER UNIQUE",
				"-- ALTER statements: --\nALTER TABLE `c` DROP INDEX `c_x_uq`",
			},
		},
		{
			name:    "MySQL, a CHECK named after the column",
			dialect: platform.MySQL,
			diff:    xGainsKey(removedKey("c", "x", "CHECK"), nil),
			want: []string{
				"-- Modify table: c --\n-- Modify column c.x: unique: false -> true --\n-- ALTER statements: --\n" +
					"ALTER TABLE `c` MODIFY COLUMN `x` INTEGER UNIQUE",
				"-- ALTER statements: --\nALTER TABLE `c` DROP CONSTRAINT `x`",
			},
		},
		{
			name:    "MySQL, a UNIQUE named after the column on another table",
			dialect: platform.MySQL,
			diff:    xGainsKey(removedKey("d", "x", "UNIQUE"), nil),
			want: []string{
				"-- Modify table: c --\n-- Modify column c.x: unique: false -> true --\n-- ALTER statements: --\n" +
					"ALTER TABLE `c` MODIFY COLUMN `x` INTEGER UNIQUE",
				"-- ALTER statements: --\nALTER TABLE `d` DROP INDEX `x`",
			},
		},
		{
			name:    "PostgreSQL, a UNIQUE under MySQL's name",
			dialect: platform.Postgres,
			diff:    xGainsKey(removedKey("c", "x", "UNIQUE"), nil),
			want: []string{
				"-- Add/modify columns for table: c --\n-- Modify column c.x: unique: false -> true --\n-- ALTER statements: --\n" +
					`ALTER TABLE "c" ADD CONSTRAINT "c_x_key" UNIQUE ("x")`,
				"-- ALTER statements: --\n" + `ALTER TABLE "c" DROP CONSTRAINT IF EXISTS "x"`,
			},
		},
		{
			name:    "SQL Server, a column added beside a UNIQUE named after it",
			dialect: platform.SQLServer,
			diff:    xAddedWithKey(removedKey("c", "x", "UNIQUE")),
			want: []string{
				"-- Modify table: c\nALTER TABLE [c] ADD [x] INT UNIQUE",
				"ALTER TABLE [c] DROP CONSTRAINT IF EXISTS [x]",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := planner.GenerateSchemaDiffSQLStatements(test.diff, test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}
