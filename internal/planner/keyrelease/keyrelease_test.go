package keyrelease_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/planner/keyrelease"
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

// TestFind_TheNameTheDiffCarries releases the key that holds the name the
// comparison gives the column's key, and not one that holds the name the
// server tries first when the column takes a later one (stokaro/ptah#3859).
func TestFind_TheNameTheDiffCarries(t *testing.T) {
	tests := []struct {
		name    string
		removed string
		want    []string
	}{
		{name: "a removed key holds the carried name", removed: "c_x_key1", want: []string{"constraint c.c_x_key1"}},
		{name: "a removed key holds the first name only", removed: "c_x_key", want: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := gains("c", unique, becomesUnique, removed("c", test.removed, "UNIQUE"), nil)
			diff.TablesModified[0].ColumnKeyNames = map[string]string{"x": "c_x_key1"}

			got := keyrelease.Find(diff, platform.Postgres)

			c.Assert(released(got), qt.DeepEquals, test.want)
		})
	}
}

// drops is a diff that drops column from table c and removes the constraint
// name of kind there.
func drops(column schemamodel.Field, name, kind string) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		TablesModified:     []difftypes.TableDiff{{TableName: "c", ColumnsRemoved: difftypes.ColumnChanges{column}}},
		ConstraintsRemoved: removed("c", name, kind),
	}
}

// droppedNames lists the names of the identities dropped holds.
func droppedNames(dropped map[difftypes.ConstraintIdentity]struct{}) []string {
	var names []string
	for identity := range dropped {
		names = append(names, identity.Name)
	}
	return names
}

// TestDroppedWithColumns_HappyPath finds the removed key of a dropped UNIQUE
// column under the name the server gives it. want is the name of the identity
// found, folded as the engine compares index names.
func TestDroppedWithColumns_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		key     string
		want    string
	}{
		{name: "MySQL names the key after the column", dialect: platform.MySQL, key: "x", want: "x"},
		{name: "MySQL compares the name without case", dialect: platform.MySQL, key: "X", want: "x"},
		{name: "MariaDB names the key after the column", dialect: platform.MariaDB, key: "x", want: "x"},
		{name: "PostgreSQL names the key after the table and the column", dialect: platform.Postgres, key: "c_x_key", want: "c_x_key"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := keyrelease.DroppedWithColumns(drops(unique, test.key, "UNIQUE"), test.dialect)

			c.Assert(droppedNames(got), qt.DeepEquals, []string{test.want})
		})
	}
}

// TestDroppedWithColumns_FindsNothing leaves every removal the column's drop
// does not take: a key under another name, a key of another kind, a column
// that is not UNIQUE, and an engine whose naming is not measured.
func TestDroppedWithColumns_FindsNothing(t *testing.T) {
	plain := schemamodel.Field{Name: "x", Type: "INTEGER", StructName: "C", Nullable: true}
	expression := unique
	expression.UniqueExpr = "lower(x)"
	tests := []struct {
		name    string
		dialect string
		column  schemamodel.Field
		key     string
		kind    string
	}{
		{name: "another name", dialect: platform.MySQL, column: unique, key: "c_x_uq", kind: "UNIQUE"},
		{name: "another kind", dialect: platform.Postgres, column: unique, key: "c_x_key", kind: "CHECK"},
		{name: "a column without UNIQUE", dialect: platform.MySQL, column: plain, key: "x", kind: "UNIQUE"},
		{name: "a column with a unique expression", dialect: platform.MySQL, column: expression, key: "x", kind: "UNIQUE"},
		{name: "an engine whose naming is not measured", dialect: platform.SQLite, column: unique, key: "x", kind: "UNIQUE"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := keyrelease.DroppedWithColumns(drops(test.column, test.key, test.kind), test.dialect)

			c.Assert(got, qt.HasLen, 0)
		})
	}
}

// addedKey is a foreign key the plan adds on child, referencing parent.
func addedKey(parent string) difftypes.ConstraintAdditionInfo {
	return difftypes.ConstraintAdditionInfo{Name: "child_fk", TableName: "child", Type: "FOREIGN KEY",
		Columns: []string{"p"}, ForeignTable: parent, ForeignColumns: []string{"id"}}
}

// TestUnderAddedForeignKeys_HappyPath pins what goes ahead of a foreign key the plan
// adds: every removed key and index of the table it references, whichever spelling
// names that table.
func TestUnderAddedForeignKeys_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
		want []string
	}{
		{
			name: "a UNIQUE replaced under another name",
			diff: &difftypes.SchemaDiff{
				ConstraintsAdded:   []difftypes.ConstraintAdditionInfo{addedKey("parent")},
				ConstraintsRemoved: removed("parent", "parent_code_old", "UNIQUE"),
			},
			want: []string{"constraint parent.parent_code_old"},
		},
		{
			name: "a PRIMARY KEY the plan does not replace",
			diff: &difftypes.SchemaDiff{
				ConstraintsAdded:   []difftypes.ConstraintAdditionInfo{addedKey("parent")},
				ConstraintsRemoved: removed("parent", "parent_pkey", "PRIMARY KEY"),
			},
			want: []string{"constraint parent.parent_pkey"},
		},
		{
			name: "an index",
			diff: &difftypes.SchemaDiff{
				ConstraintsAdded: []difftypes.ConstraintAdditionInfo{addedKey("parent")},
				IndexesRemoved:   []difftypes.IndexRef{{Name: "parent_code_ui", TableName: "parent"}},
			},
			want: []string{"index parent.parent_code_ui"},
		},
		{
			name: "the table qualified on one side",
			diff: &difftypes.SchemaDiff{
				ConstraintsAdded:   []difftypes.ConstraintAdditionInfo{addedKey("public.parent")},
				ConstraintsRemoved: removed("parent", "parent_code_old", "UNIQUE"),
			},
			want: []string{"constraint parent.parent_code_old"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(released(keyrelease.UnderAddedForeignKeys(test.diff, platform.Postgres)), qt.DeepEquals, test.want)
		})
	}
}

// TestUnderAddedForeignKeys_ReleasesNothing pins what stays where its own step puts
// it: a foreign key no removal sits under, a key another step drops ahead of its
// addition, and what is no key.
func TestUnderAddedForeignKeys_ReleasesNothing(t *testing.T) {
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
	}{
		{name: "no diff"},
		{
			name: "no foreign key added",
			diff: &difftypes.SchemaDiff{ConstraintsRemoved: removed("parent", "parent_code_old", "UNIQUE")},
		},
		{
			name: "another table's key",
			diff: &difftypes.SchemaDiff{
				ConstraintsAdded:   []difftypes.ConstraintAdditionInfo{addedKey("parent")},
				ConstraintsRemoved: removed("other", "other_code_old", "UNIQUE"),
			},
		},
		{
			name: "a primary key the plan replaces",
			diff: &difftypes.SchemaDiff{
				ConstraintsAdded: []difftypes.ConstraintAdditionInfo{
					addedKey("parent"),
					{Name: "parent_pkey2", TableName: "parent", Type: "PRIMARY KEY", Columns: []string{"code"}},
				},
				ConstraintsRemoved: removed("parent", "parent_pkey", "PRIMARY KEY"),
			},
		},
		{
			name: "a key the plan adds again under its name",
			diff: &difftypes.SchemaDiff{
				ConstraintsAdded: []difftypes.ConstraintAdditionInfo{
					addedKey("parent"),
					{Name: "parent_code_key", TableName: "parent", Type: "UNIQUE", Columns: []string{"code"}},
				},
				ConstraintsRemoved: removed("parent", "parent_code_key", "UNIQUE"),
			},
		},
		{
			name: "a CHECK",
			diff: &difftypes.SchemaDiff{
				ConstraintsAdded:   []difftypes.ConstraintAdditionInfo{addedKey("parent")},
				ConstraintsRemoved: removed("parent", "parent_code_positive", "CHECK"),
			},
		},
		{
			name: "the foreign key itself",
			diff: &difftypes.SchemaDiff{
				ConstraintsAdded:   []difftypes.ConstraintAdditionInfo{addedKey("parent")},
				ConstraintsRemoved: removed("child", "child_fk", "FOREIGN KEY"),
			},
		},
		{
			name: "an index the plan creates again",
			diff: &difftypes.SchemaDiff{
				ConstraintsAdded: []difftypes.ConstraintAdditionInfo{addedKey("parent")},
				IndexesRemoved:   []difftypes.IndexRef{{Name: "parent_code_ui", TableName: "parent"}},
				IndexesAdded:     difftypes.IndexChanges{{TableName: "parent", Index: schemamodel.Index{Name: "parent_code_ui"}}},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(released(keyrelease.UnderAddedForeignKeys(test.diff, platform.Postgres)), qt.HasLen, 0)
		})
	}
}

// TestReleasesWith_HoldsEachRemovalOnce merges the two reasons a removal may have
// to go first, keeping the order of the first.
func TestReleasesWith_HoldsEachRemovalOnce(t *testing.T) {
	c := qt.New(t)
	shared := difftypes.ConstraintRemovalInfo{Name: "k", TableName: "t", Type: "UNIQUE", Identity: difftypes.ConstraintIdentity{Table: "t", Name: "k"}}
	other := difftypes.ConstraintRemovalInfo{Name: "m", TableName: "t", Type: "UNIQUE", Identity: difftypes.ConstraintIdentity{Table: "t", Name: "m"}}
	index := difftypes.IndexRef{Name: "i", TableName: "t"}

	merged := keyrelease.Releases{Constraints: []difftypes.ConstraintRemovalInfo{shared}, Indexes: []difftypes.IndexRef{index}}.
		With(keyrelease.Releases{Constraints: []difftypes.ConstraintRemovalInfo{other, shared}, Indexes: []difftypes.IndexRef{index}})

	c.Assert(released(merged), qt.DeepEquals, []string{"constraint t.k", "constraint t.m", "index t.i"})
}

// losesNotNull is a change of column x that leaves the table's key and allows NULL.
var losesNotNull = difftypes.ColumnDiff{ColumnName: "x", Changes: map[string]string{
	"primary_key": "true -> false",
	"nullable":    "false -> true",
}}

// TestPrimaryKeysUnderColumnChanges_HappyPath pins the column changes a removed
// primary key has to leave ahead of, whether or not the plan adds another key.
func TestPrimaryKeysUnderColumnChanges_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		table difftypes.TableDiff
		added []difftypes.ConstraintAdditionInfo
	}{
		{
			name:  "a column of the key allows NULL",
			table: difftypes.TableDiff{TableName: "t", ColumnsModified: []difftypes.ColumnDiff{losesNotNull}},
		},
		{
			name:  "the plan adds another key as well",
			table: difftypes.TableDiff{TableName: "t", ColumnsModified: []difftypes.ColumnDiff{losesNotNull}},
			added: []difftypes.ConstraintAdditionInfo{{Name: "t_pkey", TableName: "t", Type: "PRIMARY KEY", Columns: []string{"y"}}},
		},
		{
			name:  "an added column declares the key",
			table: difftypes.TableDiff{TableName: "t", ColumnsAdded: difftypes.ColumnChanges{{Name: "y", Type: "BIGINT", Primary: true}}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{test.table},
				ConstraintsRemoved: removed("t", "t_pkey", "PRIMARY KEY"), ConstraintsAdded: test.added}
			c.Assert(released(keyrelease.PrimaryKeysUnderColumnChanges(diff, platform.Postgres)), qt.DeepEquals, []string{"constraint t.t_pkey"})
		})
	}
}

// TestPrimaryKeysUnderColumnChanges_ReleasesNothing pins what stays where its step
// puts it.
func TestPrimaryKeysUnderColumnChanges_ReleasesNothing(t *testing.T) {
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
	}{
		{name: "no diff"},
		{
			name: "the column stays NOT NULL",
			diff: &difftypes.SchemaDiff{
				TablesModified: []difftypes.TableDiff{{TableName: "t", ColumnsModified: []difftypes.ColumnDiff{
					{ColumnName: "x", Changes: map[string]string{"primary_key": "true -> false"}},
				}}},
				ConstraintsRemoved: removed("t", "t_pkey", "PRIMARY KEY"),
			},
		},
		{
			name: "the column also gives up AUTO_INCREMENT",
			diff: &difftypes.SchemaDiff{
				TablesModified: []difftypes.TableDiff{{TableName: "t", ColumnsModified: []difftypes.ColumnDiff{
					{ColumnName: "x", Changes: map[string]string{"primary_key": "true -> false", "nullable": "false -> true", "auto_increment": "true -> false"}},
				}}},
				ConstraintsRemoved: removed("t", "t_pkey", "PRIMARY KEY"),
			},
		},
		{
			name: "a column outside the key allows NULL",
			diff: &difftypes.SchemaDiff{
				TablesModified: []difftypes.TableDiff{{TableName: "t", ColumnsModified: []difftypes.ColumnDiff{
					{ColumnName: "x", Changes: map[string]string{"nullable": "false -> true"}},
				}}},
				ConstraintsRemoved: removed("t", "t_pkey", "PRIMARY KEY"),
			},
		},
		{
			name: "another table's key",
			diff: &difftypes.SchemaDiff{
				TablesModified:     []difftypes.TableDiff{{TableName: "t", ColumnsModified: []difftypes.ColumnDiff{losesNotNull}}},
				ConstraintsRemoved: removed("other", "other_pkey", "PRIMARY KEY"),
			},
		},
		{
			name: "a UNIQUE",
			diff: &difftypes.SchemaDiff{
				TablesModified:     []difftypes.TableDiff{{TableName: "t", ColumnsModified: []difftypes.ColumnDiff{losesNotNull}}},
				ConstraintsRemoved: removed("t", "t_x_key", "UNIQUE"),
			},
		},
		{
			name: "an added column that declares no key",
			diff: &difftypes.SchemaDiff{
				TablesModified:     []difftypes.TableDiff{{TableName: "t", ColumnsAdded: difftypes.ColumnChanges{{Name: "y", Type: "BIGINT"}}}},
				ConstraintsRemoved: removed("t", "t_pkey", "PRIMARY KEY"),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(released(keyrelease.PrimaryKeysUnderColumnChanges(test.diff, platform.Postgres)), qt.HasLen, 0)
		})
	}
}

// TestPrimaryKeysBehindAutoIncrement_HappyPath pins the removed key of a table whose
// column gives up AUTO_INCREMENT and the key at once.
func TestPrimaryKeysBehindAutoIncrement_HappyPath(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{{TableName: "t", ColumnsModified: []difftypes.ColumnDiff{
			{ColumnName: "x", Changes: map[string]string{"auto_increment": "true -> false", "primary_key": "true -> false"}},
		}}},
		ConstraintsRemoved: removed("t", "t_pkey", "PRIMARY KEY"),
	}

	c.Assert(released(keyrelease.PrimaryKeysBehindAutoIncrement(diff, platform.MySQL)), qt.DeepEquals, []string{"constraint t.t_pkey"})
}

// TestPrimaryKeysBehindAutoIncrement_FailurePath leaves a key alone where its column
// keeps either of the two.
func TestPrimaryKeysBehindAutoIncrement_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		changes map[string]string
	}{
		{name: "the column keeps AUTO_INCREMENT", changes: map[string]string{"primary_key": "true -> false"}},
		{name: "the column keeps the key", changes: map[string]string{"auto_increment": "true -> false"}},
		{name: "the column gains both", changes: map[string]string{"auto_increment": "false -> true", "primary_key": "false -> true"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{
				TablesModified:     []difftypes.TableDiff{{TableName: "t", ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "x", Changes: test.changes}}}},
				ConstraintsRemoved: removed("t", "t_pkey", "PRIMARY KEY"),
			}

			c.Assert(released(keyrelease.PrimaryKeysBehindAutoIncrement(diff, platform.MySQL)), qt.HasLen, 0)
		})
	}
}
