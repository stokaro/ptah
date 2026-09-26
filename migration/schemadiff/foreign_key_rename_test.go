package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/platform"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/planner/dialects/postgres"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// childDeclaringColumnKey is `c.p_id REFERENCES p(id) ON DELETE CASCADE`, the
// foreign key declared on the column under keyName. A SQL schema file read for
// PostgreSQL holds it under the name PostgreSQL gives it, `c_p_id_fkey`; an
// empty keyName leaves the comparison to give it Ptah's default name.
func childDeclaringColumnKey(keyName string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "P", Name: "p"},
			{StructName: "Q", Name: "q"},
			{StructName: "C", Name: "c"},
		},
		Fields: []schemamodel.Field{
			{StructName: "P", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Q", Name: "a", Type: "BIGINT", Nullable: true},
			{StructName: "Q", Name: "b", Type: "BIGINT", Nullable: true},
			{StructName: "C", Name: "id", Type: "BIGINT", Primary: true},
			{
				StructName:     "C",
				Name:           "p_id",
				Type:           "BIGINT",
				Foreign:        "p(id)",
				ForeignKeyName: keyName,
				OnDelete:       "CASCADE",
			},
			{StructName: "C", Name: "k", Type: "BIGINT", Nullable: true},
		},
	}
}

// liveKey is one foreign key of `c` as a catalog reports it.
type liveKey struct {
	name       string
	columns    []string
	table      string
	references []string
	onDelete   string
}

// childHolding is the catalog of the three tables, `c` holding the given
// foreign keys.
func childHolding(keys ...liveKey) *catalog.Database {
	database := &catalog.Database{
		Tables: []catalog.Table{
			{Name: "p", Columns: []catalog.Column{
				{Name: "id", DataType: "bigint", IsNullable: "NO", IsPrimaryKey: true},
			}},
			{Name: "q", Columns: []catalog.Column{
				{Name: "a", DataType: "bigint", IsNullable: "YES"},
				{Name: "b", DataType: "bigint", IsNullable: "YES"},
			}},
			{Name: "c", Columns: []catalog.Column{
				{Name: "id", DataType: "bigint", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "p_id", DataType: "bigint", IsNullable: "NO"},
				{Name: "k", DataType: "bigint", IsNullable: "YES"},
			}},
		},
	}
	for _, key := range keys {
		database.Constraints = append(database.Constraints, catalog.Constraint{
			Name:           key.name,
			TableName:      "c",
			Type:           "FOREIGN KEY",
			ColumnName:     key.columns[0],
			ColumnNames:    key.columns,
			ForeignTable:   new(key.table),
			ForeignColumn:  new(key.references[0]),
			ForeignColumns: key.references,
			DeleteRule:     new(key.onDelete),
			UpdateRule:     new("NO ACTION"),
		})
	}
	return database
}

// The live keys the rows below start from.
var (
	declaredKey = liveKey{
		name: "c_p_id_fkey", columns: []string{"p_id"}, table: "p", references: []string{"id"}, onDelete: "CASCADE",
	}
	renamedKey = liveKey{
		name: "c_p_fk", columns: []string{"p_id"}, table: "p", references: []string{"id"}, onDelete: "CASCADE",
	}
)

// constraintNames lists the names one side of a diff holds, nil when there are
// none, so an empty side compares equal to a row that expects nothing.
func constraintNames(names []string) []string {
	if len(names) == 0 {
		return nil
	}
	return names
}

// removedKinds lists each removal as its name and the kind a planner drops it
// by.
func removedKinds(removals difftypes.ConstraintRemovals) []string {
	kinds := make([]string, 0, len(removals))
	for _, removal := range removals {
		kinds = append(kinds, removal.Name+" "+removal.Type)
	}
	return kinds
}

func comparePostgres(desired *schemamodel.Database, current *catalog.Database) *difftypes.SchemaDiff {
	opts := config.DefaultCompareOptions()
	opts.Dialect = platform.Postgres
	return schemadiff.CompareWithOptions(desired, current, opts)
}

// TestCompare_AColumnForeignKeyIsPairedByItsName covers stokaro/ptah#3718.
//
// The key declared on the column is compared by the name it carries, so the
// comparison adds it when the database holds no key of that name. A database
// key excused because its column declares a key is never dropped, and the table
// ends with two foreign keys where the declaration has one. Atlas CE v1.3.0
// plans the same drop and add for each renamed row.
//
// The rows where the name matches are the controls. A key that is the declared
// one plans nothing, and a key whose ON DELETE changed is still replaced.
func TestCompare_AColumnForeignKeyIsPairedByItsName(t *testing.T) {
	tests := []struct {
		name        string
		live        []liveKey
		wantAdded   []string
		wantRemoved []string
	}{
		{
			name:        "the live key is under another name",
			live:        []liveKey{renamedKey},
			wantAdded:   []string{"c_p_id_fkey"},
			wantRemoved: []string{"c_p_fk"},
		},
		{
			name: "the live key is under another name and its ON DELETE differs",
			live: []liveKey{{
				name: "c_p_fk", columns: []string{"p_id"}, table: "p", references: []string{"id"}, onDelete: "RESTRICT",
			}},
			wantAdded:   []string{"c_p_id_fkey"},
			wantRemoved: []string{"c_p_fk"},
		},
		{
			name: "the live key is composite and led by the column",
			live: []liveKey{{
				name: "c_pk_fk", columns: []string{"p_id", "k"}, table: "q", references: []string{"a", "b"}, onDelete: "NO ACTION",
			}},
			wantAdded:   []string{"c_p_id_fkey"},
			wantRemoved: []string{"c_pk_fk"},
		},
		{
			name:        "a second live key sits beside the declared one",
			live:        []liveKey{declaredKey, renamedKey},
			wantAdded:   nil,
			wantRemoved: []string{"c_p_fk"},
		},
		{
			name:        "the live key is the declared one",
			live:        []liveKey{declaredKey},
			wantAdded:   nil,
			wantRemoved: nil,
		},
		{
			name: "the declared key's ON DELETE differs",
			live: []liveKey{{
				name: "c_p_id_fkey", columns: []string{"p_id"}, table: "p", references: []string{"id"}, onDelete: "RESTRICT",
			}},
			wantAdded:   []string{"c_p_id_fkey"},
			wantRemoved: []string{"c_p_id_fkey"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := comparePostgres(childDeclaringColumnKey("c_p_id_fkey"), childHolding(test.live...))

			c.Assert(constraintNames(diff.ConstraintsAdded.Names()), qt.DeepEquals, test.wantAdded)
			c.Assert(constraintNames(diff.ConstraintsRemoved.Names()), qt.DeepEquals, test.wantRemoved)
		})
	}
}

// TestCompare_AColumnForeignKeyUnderAnotherNameIsPairedByNameOnMySQL pins that
// the rule is not PostgreSQL's alone. MySQL names an unnamed key
// `<table>_ibfk_<n>`, and a column declaring the key under Ptah's default name
// has the live key dropped, with its kind, so the MySQL planner writes
// DROP FOREIGN KEY for it.
func TestCompare_AColumnForeignKeyUnderAnotherNameIsPairedByNameOnMySQL(t *testing.T) {
	c := qt.New(t)
	live := renamedKey
	live.name = "c_ibfk_1"
	opts := config.DefaultCompareOptions()
	opts.Dialect = platform.MySQL

	diff := schemadiff.CompareWithOptions(childDeclaringColumnKey(""), childHolding(live), opts)

	c.Assert(constraintNames(diff.ConstraintsAdded.Names()), qt.DeepEquals, []string{"fk_c_p_id"})
	c.Assert(removedKinds(diff.ConstraintsRemoved), qt.DeepEquals, []string{"c_ibfk_1 FOREIGN KEY"})
}

// TestCompare_AColumnForeignKeyUnderAnotherNameMigrationSQL pins the statements
// the PostgreSQL planner writes for the rename: the declared key is added and
// the live one dropped from its table, so the table ends with one key.
func TestCompare_AColumnForeignKeyUnderAnotherNameMigrationSQL(t *testing.T) {
	c := qt.New(t)
	diff := comparePostgres(childDeclaringColumnKey("c_p_id_fkey"), childHolding(renamedKey))

	nodes, err := postgres.New().GenerateMigrationAST(diff)
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL(platform.Postgres, nodes...)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)

	c.Assert(sql, qt.Contains,
		"ALTER TABLE c ADD CONSTRAINT c_p_id_fkey FOREIGN KEY (p_id) REFERENCES p(id) ON DELETE CASCADE;")
	c.Assert(sql, qt.Contains, "ALTER TABLE c DROP CONSTRAINT IF EXISTS c_p_fk;")
}
