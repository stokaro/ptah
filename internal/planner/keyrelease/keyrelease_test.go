package keyrelease_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/planner/keyrelease"
	"ptah.run/migration/schemadiff/difftypes"
)

// gains is a diff in which column x of table changes with the given changes,
// declared as field, and the constraints and indexes given are removed.
func gains(
	table string,
	field schemamodel.Field,
	changes map[string]string,
	constraints []difftypes.ConstraintRemovalInfo,
	indexes []difftypes.IndexRef,
) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{{
			TableName:       table,
			ColumnsModified: []difftypes.ColumnDiff{{ColumnName: field.Name, Desired: field, Changes: changes}},
		}},
		ConstraintsRemoved: constraints,
		IndexesRemoved:     indexes,
	}
}

// unique is column x declared UNIQUE.
var unique = schemamodel.Field{Name: "x", Type: "INTEGER", StructName: "C", Nullable: true, Unique: true}

// becomesUnique is the change of a column that gains its own UNIQUE.
var becomesUnique = map[string]string{"unique": "false -> true"}

// removed is a removed constraint of kind named name on table.
func removed(table, name, kind string) []difftypes.ConstraintRemovalInfo {
	return []difftypes.ConstraintRemovalInfo{{Name: name, TableName: table, Type: kind}}
}

// released lists what releases holds by kind and name.
func released(releases keyrelease.Releases) []string {
	var names []string
	for _, info := range releases.Constraints {
		names = append(names, "constraint "+info.TableName+"."+info.Name)
	}
	for _, ref := range releases.Indexes {
		names = append(names, "index "+ref.TableName+"."+ref.Name)
	}
	return names
}

// Each row is a removed key that holds the name the column's own UNIQUE takes.
func TestFind_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		diff    *difftypes.SchemaDiff
		want    []string
	}{
		{
			name:    "MySQL, a UNIQUE named after the column",
			dialect: platform.MySQL,
			diff:    gains("c", unique, becomesUnique, removed("c", "x", "UNIQUE"), nil),
			want:    []string{"constraint c.x"},
		},
		{
			name:    "MariaDB, an index named after the column in another case",
			dialect: platform.MariaDB,
			diff:    gains("c", unique, becomesUnique, nil, []difftypes.IndexRef{{Name: "X", TableName: "c"}}),
			want:    []string{"index c.X"},
		},
		{
			name:    "PostgreSQL, a table in another schema is named without it",
			dialect: platform.Postgres,
			diff:    gains("app.c", unique, becomesUnique, removed("app.c", "c_x_key", "UNIQUE"), nil),
			want:    []string{"constraint app.c.c_x_key"},
		},
		{
			name:    "PostgreSQL, a constraint and an index",
			dialect: platform.Postgres,
			diff: gains(
				"c", unique, becomesUnique,
				removed("c", "c_x_key", "CHECK"), []difftypes.IndexRef{{Name: "c_x_key", TableName: "c"}},
			),
			want: []string{"constraint c.c_x_key", "index c.c_x_key"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := keyrelease.Find(test.diff, test.dialect)

			c.Assert(released(got), qt.DeepEquals, test.want)
		})
	}
}

// Each row holds a removed key named after the column that the plan must drop
// where its removals land, or a column that takes no key.
func TestFind_ReleasesNothing(t *testing.T) {
	uniqueExpr := unique
	uniqueExpr.UniqueExpr = "lower(x)"
	notUnique := unique
	notUnique.Unique = false
	modified := gains("c", unique, becomesUnique, removed("c", "x", "UNIQUE"), nil)
	modified.ConstraintsAdded = []difftypes.ConstraintAdditionInfo{{Name: "x", TableName: "c", Type: "UNIQUE"}}

	tests := []struct {
		name    string
		dialect string
		diff    *difftypes.SchemaDiff
	}{
		{name: "no diff", dialect: platform.MySQL},
		{
			name:    "an engine whose naming is not measured",
			dialect: platform.SQLServer,
			diff:    gains("c", unique, becomesUnique, removed("c", "x", "UNIQUE"), nil),
		},
		{
			name:    "a key the plan adds again is a modification",
			dialect: platform.MySQL,
			diff:    modified,
		},
		{
			name:    "a column declaring unique_expr takes no key",
			dialect: platform.MySQL,
			diff:    gains("c", uniqueExpr, becomesUnique, removed("c", "x", "UNIQUE"), nil),
		},
		{
			name:    "a column that loses its UNIQUE takes no key",
			dialect: platform.MySQL,
			diff:    gains("c", notUnique, map[string]string{"unique": "true -> false"}, removed("c", "x", "UNIQUE"), nil),
		},
		{
			name:    "a column whose other property changes takes no new key",
			dialect: platform.MySQL,
			diff:    gains("c", unique, map[string]string{"nullable": "true -> false"}, removed("c", "x", "UNIQUE"), nil),
		},
		{
			name:    "MySQL, a CHECK named after the column",
			dialect: platform.MySQL,
			diff:    gains("c", unique, becomesUnique, removed("c", "x", "CHECK"), nil),
		},
		{
			name:    "a key on another table",
			dialect: platform.Postgres,
			diff:    gains("c", unique, becomesUnique, removed("d", "c_x_key", "UNIQUE"), nil),
		},
		{
			name:    "PostgreSQL, a name in another case",
			dialect: platform.Postgres,
			diff:    gains("c", unique, becomesUnique, removed("c", "C_X_KEY", "UNIQUE"), nil),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := keyrelease.Find(test.diff, test.dialect)

			c.Assert(got, qt.DeepEquals, keyrelease.Releases{})
		})
	}
}
