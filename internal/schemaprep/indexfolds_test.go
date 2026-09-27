package schemaprep_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemaprep"
)

var foldTable = schemamodel.Table{StructName: "T", Name: "t"}

func foldUnique(name string, columns ...string) schemamodel.Constraint {
	return schemamodel.Constraint{StructName: "T", Name: name, Type: "UNIQUE", Columns: columns}
}

func foldExclude(method, elements, where string) schemamodel.Constraint {
	return schemamodel.Constraint{
		StructName: "T", Type: "EXCLUDE", UsingMethod: method, ExcludeElements: elements, WhereCondition: where,
	}
}

// TestFoldedIndexConstraints_Folds lists constraints PostgreSQL leaves out of
// a CREATE TABLE. Each row was run on PostgreSQL 18.6 as one CREATE TABLE, and
// pg_constraint held one index constraint for each fold.
func TestFoldedIndexConstraints_Folds(t *testing.T) {
	notDistinct, distinct := false, true
	tests := []struct {
		name        string
		table       schemamodel.Table
		fields      []schemamodel.Field
		constraints []schemamodel.Constraint
		wantFolds   []schemaprep.IndexConstraintFold
	}{
		{
			name:        "an EXCLUDE declared twice",
			table:       foldTable,
			constraints: []schemamodel.Constraint{foldExclude("btree", "r WITH =", ""), foldExclude("btree", "r WITH =", "")},
			wantFolds:   []schemaprep.IndexConstraintFold{{Folded: 1, Into: 0}},
		},
		{
			name:  "an EXCLUDE spelled in other case and spacing",
			table: foldTable,
			constraints: []schemamodel.Constraint{
				foldExclude("BTREE", "R  WITH =", "r > 0"),
				foldExclude("btree", "r WITH =", "R>0"),
			},
			wantFolds: []schemaprep.IndexConstraintFold{{Folded: 1, Into: 0}},
		},
		{
			name:        "a UNIQUE over the columns of the table's key",
			table:       schemamodel.Table{StructName: "T", Name: "t", PrimaryKey: []string{"a", "b"}},
			constraints: []schemamodel.Constraint{foldUnique("", "a", "b")},
			wantFolds:   []schemaprep.IndexConstraintFold{{Folded: 0, Into: schemaprep.PrimaryKeyOutsideList}},
		},
		{
			name:        "a UNIQUE over a column that declares itself the key",
			table:       foldTable,
			fields:      []schemamodel.Field{{StructName: "T", Name: "id", Primary: true}, {StructName: "O", Name: "a", Primary: true}},
			constraints: []schemamodel.Constraint{foldUnique("u", "id")},
			wantFolds:   []schemaprep.IndexConstraintFold{{Folded: 0, Into: schemaprep.PrimaryKeyOutsideList}},
		},
		{
			name:  "a UNIQUE written before the key it folds into",
			table: foldTable,
			constraints: []schemamodel.Constraint{
				foldUnique("u2", "id"),
				{StructName: "T", Type: "PRIMARY KEY", Columns: []string{"id"}},
			},
			wantFolds: []schemaprep.IndexConstraintFold{{Folded: 0, Into: 1}},
		},
		{
			name:  "NULLS DISTINCT written out and left out",
			table: foldTable,
			constraints: []schemamodel.Constraint{
				{StructName: "T", Type: "UNIQUE", Columns: []string{"a"}, NullsDistinct: &distinct},
				foldUnique("", "a"),
			},
			wantFolds: []schemaprep.IndexConstraintFold{{Folded: 1, Into: 0}},
		},
		{
			name:  "NULLS NOT DISTINCT twice",
			table: foldTable,
			constraints: []schemamodel.Constraint{
				{StructName: "T", Type: "UNIQUE", Columns: []string{"a"}, NullsDistinct: &notDistinct},
				{StructName: "T", Type: "UNIQUE", Columns: []string{"a"}, NullsDistinct: &notDistinct},
			},
			wantFolds: []schemaprep.IndexConstraintFold{{Folded: 1, Into: 0}},
		},
		{
			name:  "three of one key among others",
			table: foldTable,
			constraints: []schemamodel.Constraint{
				foldUnique("", "b"),
				foldUnique("", "a"),
				{StructName: "T", Type: "CHECK", CheckExpression: "a > 0"},
				foldUnique("n1", "a"),
				foldUnique("n2", "a"),
			},
			wantFolds: []schemaprep.IndexConstraintFold{{Folded: 3, Into: 1}, {Folded: 4, Into: 1}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := schemaprep.FoldedIndexConstraints(test.table, test.fields, test.constraints, platform.Postgres)

			c.Assert(got, qt.DeepEquals, test.wantFolds)
		})
	}
}

// TestFoldedIndexConstraints_Keeps is the control: constraints PostgreSQL
// 18.6 builds an index for each of in one CREATE TABLE, constraints of two
// tables, and constraints on a dialect the rule does not describe.
func TestFoldedIndexConstraints_Keeps(t *testing.T) {
	notDistinct := false
	tests := []struct {
		name        string
		dialect     string
		table       schemamodel.Table
		fields      []schemamodel.Field
		constraints []schemamodel.Constraint
	}{
		{
			name:        "columns in another order",
			dialect:     platform.Postgres,
			table:       foldTable,
			constraints: []schemamodel.Constraint{foldUnique("", "a", "b"), foldUnique("", "b", "a")},
		},
		{
			name:    "other INCLUDE columns",
			dialect: platform.Postgres,
			table:   foldTable,
			constraints: []schemamodel.Constraint{
				{StructName: "T", Type: "UNIQUE", Columns: []string{"a"}, IncludeColumns: []string{"b"}},
				foldUnique("", "a"),
			},
		},
		{
			name:    "NULLS NOT DISTINCT beside a plain UNIQUE",
			dialect: platform.Postgres,
			table:   foldTable,
			constraints: []schemamodel.Constraint{
				foldUnique("", "a"),
				{StructName: "T", Type: "UNIQUE", Columns: []string{"a"}, NullsDistinct: &notDistinct},
			},
		},
		{
			name:        "a UNIQUE over part of the table's key",
			dialect:     platform.Postgres,
			table:       schemamodel.Table{StructName: "T", Name: "t", PrimaryKey: []string{"a", "b"}},
			constraints: []schemamodel.Constraint{foldUnique("", "a")},
		},
		{
			name:    "a key with INCLUDE columns",
			dialect: platform.Postgres,
			table: schemamodel.Table{
				StructName: "T", Name: "t", PrimaryKey: []string{"id"}, PrimaryKeyInclude: []string{"b"},
			},
			constraints: []schemamodel.Constraint{foldUnique("", "id")},
		},
		{
			name:    "a PRIMARY KEY entry with INCLUDE columns",
			dialect: platform.Postgres,
			table:   foldTable,
			constraints: []schemamodel.Constraint{
				{StructName: "T", Type: "PRIMARY KEY", Columns: []string{"id"}, IncludeColumns: []string{"b"}},
				foldUnique("", "id"),
			},
		},
		{
			name:    "another table's PRIMARY KEY entry",
			dialect: platform.Postgres,
			table:   foldTable,
			constraints: []schemamodel.Constraint{
				{StructName: "O", Type: "PRIMARY KEY", Columns: []string{"id"}},
				foldUnique("", "id"),
			},
		},
		{
			name:        "another table's key",
			dialect:     platform.Postgres,
			table:       foldTable,
			fields:      []schemamodel.Field{{StructName: "O", Name: "id", Primary: true}},
			constraints: []schemamodel.Constraint{foldUnique("", "id")},
		},
		{
			name:    "another table's UNIQUE",
			dialect: platform.Postgres,
			table:   foldTable,
			constraints: []schemamodel.Constraint{
				{StructName: "O", Type: "UNIQUE", Columns: []string{"a"}},
				foldUnique("", "a"),
			},
		},
		{
			name:        "an EXCLUDE under another access method",
			dialect:     platform.Postgres,
			table:       foldTable,
			constraints: []schemamodel.Constraint{foldExclude("btree", "r WITH =", ""), foldExclude("hash", "r WITH =", "")},
		},
		{
			name:        "an EXCLUDE over another operator",
			dialect:     platform.Postgres,
			table:       foldTable,
			constraints: []schemamodel.Constraint{foldExclude("gist", "r WITH =", ""), foldExclude("gist", "r WITH &&", "")},
		},
		{
			name:        "an EXCLUDE with another predicate",
			dialect:     platform.Postgres,
			table:       foldTable,
			constraints: []schemamodel.Constraint{foldExclude("btree", "r WITH =", "r > 0"), foldExclude("btree", "r WITH =", "")},
		},
		{
			name:        "a UNIQUE and an EXCLUDE over one column",
			dialect:     platform.Postgres,
			table:       foldTable,
			constraints: []schemamodel.Constraint{foldUnique("", "r"), foldExclude("btree", "r WITH =", "")},
		},
		{
			name:        "a UNIQUE declared twice on MySQL",
			dialect:     platform.MySQL,
			table:       foldTable,
			constraints: []schemamodel.Constraint{foldUnique("", "a"), foldUnique("", "a")},
		},
		{
			name:        "an EXCLUDE declared twice on CockroachDB",
			dialect:     platform.CockroachDB,
			table:       foldTable,
			constraints: []schemamodel.Constraint{foldExclude("btree", "r WITH =", ""), foldExclude("btree", "r WITH =", "")},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := schemaprep.FoldedIndexConstraints(test.table, test.fields, test.constraints, test.dialect)

			c.Assert(got, qt.IsNil)
		})
	}
}
