package mysqlcomparison_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/mysqlcomparison"
)

// TestIndexes_OptionsEqual reads a declared index's access method, comment and
// key prefixes against what MySQL and MariaDB report. MariaDB records a USING
// clause on every engine and MySQL only on MEMORY, so a desired HASH against a
// reported BTREE is a difference on MariaDB alone.
func TestIndexes_OptionsEqual(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		declared schemamodel.Index
		reported catalog.Index
		want     bool
	}{
		{
			name:     "the same index",
			dialect:  "mysql",
			declared: schemamodel.Index{Fields: []string{"a"}},
			reported: catalog.Index{Columns: []string{"a"}},
			want:     true,
		},
		{
			name:     "MySQL accepts the method it reports",
			dialect:  "mysql",
			declared: schemamodel.Index{Type: "HASH", Fields: []string{"a"}},
			reported: catalog.Index{Method: "BTREE", Columns: []string{"a"}},
			want:     true,
		},
		{
			name:     "MariaDB records the method asked for",
			dialect:  "mariadb",
			declared: schemamodel.Index{Type: "HASH", Fields: []string{"a"}},
			reported: catalog.Index{Method: "BTREE", Columns: []string{"a"}},
			want:     false,
		},
		{
			name:     "a SPATIAL index against a BTREE one",
			dialect:  "mysql",
			declared: schemamodel.Index{Type: "SPATIAL", Fields: []string{"g"}},
			reported: catalog.Index{Method: "BTREE", Columns: []string{"g"}},
			want:     false,
		},
		{
			name:     "a changed comment",
			dialect:  "mysql",
			declared: schemamodel.Index{Fields: []string{"a"}, Comment: "lookup"},
			reported: catalog.Index{Columns: []string{"a"}},
			want:     false,
		},
		{
			name:     "a prefix in other spacing",
			dialect:  "mariadb",
			declared: schemamodel.Index{Parts: []schemamodel.IndexPart{{Name: "a", Prefix: " 10 "}}},
			reported: catalog.Index{Parts: []catalog.IndexPart{{Name: "a", Prefix: "10"}}},
			want:     true,
		},
		{
			name:     "a changed prefix",
			dialect:  "mysql",
			declared: schemamodel.Index{Parts: []schemamodel.IndexPart{{Name: "a", Prefix: "10"}}},
			reported: catalog.Index{Parts: []catalog.IndexPart{{Name: "a", Prefix: "20"}}},
			want:     false,
		},
		{
			name:     "a key read in part has no prefix to line up",
			dialect:  "mysql",
			declared: schemamodel.Index{Parts: []schemamodel.IndexPart{{Name: "a", Prefix: "10"}, {Expr: "b + 1"}}},
			reported: catalog.Index{Parts: []catalog.IndexPart{{Name: "a"}}, KeyPartsIncomplete: true},
			want:     true,
		},
		{
			name:     "another number of key parts is the shared comparison's",
			dialect:  "mysql",
			declared: schemamodel.Index{Fields: []string{"a", "b"}},
			reported: catalog.Index{Columns: []string{"a"}},
			want:     true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			indexes := mysqlcomparison.Rules(test.dialect).Indexes
			c.Assert(indexes.OptionsEqual(test.declared, test.reported), qt.Equals, test.want)
		})
	}
}

// TestIndexes_RenameCarries pins that a MySQL-family index carries no facet
// through a rename, so the comparison rebuilds an index with a facet.
func TestIndexes_RenameCarries(t *testing.T) {
	c := qt.New(t)
	c.Assert(mysqlcomparison.Rules("mysql").Indexes.RenameCarries(schemaext.Kind("example.org/facet")), qt.IsFalse)
}

// TestGeneratedExpressions_Spelling reads a generated column's expression the
// way the MySQL family prints it back: names out of their backticks, without
// spaces, in lower case, with `lcase(` read as `lower(`. A double-quoted token
// is a string, so its content keeps its case.
func TestGeneratedExpressions_Spelling(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		want       string
	}{
		{name: "a declaration", expression: "LOWER(email)", want: "lower(email)"},
		{name: "what the catalog prints", expression: "lcase(`email`)", want: "lower(email)"},
		{name: "a name that only ends in lcase", expression: "xlcase(a)", want: "xlcase(a)"},
		{name: "lcase inside a string is text", expression: "concat('lcase(', a)", want: "concat('lcase(',a)"},
		{name: "a double-quoted string keeps its case", expression: `concat("Ab C", a)`, want: `concat("Ab C",a)`},
		{name: "a parenthesized name", expression: "(`a`) + 1", want: "a+1"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(mysqlcomparison.Rules("mysql").GeneratedExpressions.Spelling(test.expression), qt.Equals, test.want)
		})
	}
}

// indexNames lists the names of indexes in order, nil for none.
func indexNames(indexes []catalog.Index) []string {
	var names []string
	for _, index := range indexes {
		names = append(names, index.Name)
	}
	return names
}

// foreignKey is the foreign key children(a) references parents(id) under name.
func foreignKey(name string) catalog.Constraint {
	return catalog.Constraint{Name: name, Type: "FOREIGN KEY", TableName: "children", ColumnName: "a"}
}

// TestForeignKeyIndexes_BackingIndexes finds the index the server built for a
// foreign key from two signals, the name the server could have given it and
// the leading columns, and finds none where another index already covers the
// key. A descending leading part covers a key on MariaDB and not on MySQL,
// measured on MySQL 8.4.11 and MariaDB 11.8.9. A name reads as the server's
// only where the dialect's descriptor states the naming
// (descriptor.ForeignKeyNamingTableNumbered).
func TestForeignKeyIndexes_BackingIndexes(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		key     catalog.Constraint
		indexes []catalog.Index
		want    []string
	}{
		{
			name:    "an index under the key's name over its column",
			dialect: "mysql",
			key:     foreignKey("fk_parent"),
			indexes: []catalog.Index{{Name: "fk_parent", TableName: "children", Columns: []string{"a"}}},
			want:    []string{"fk_parent"},
		},
		{
			name:    "an index under the key's name over another column",
			dialect: "mysql",
			key:     foreignKey("f"),
			indexes: []catalog.Index{{Name: "f", TableName: "children", Columns: []string{"b"}}},
			want:    nil,
		},
		{
			name:    "an unnamed key's index is named after its column",
			dialect: "mysql",
			key:     foreignKey("children_ibfk_1"),
			indexes: []catalog.Index{{Name: "a", TableName: "children", Columns: []string{"a"}}},
			want:    []string{"a"},
		},
		{
			name:    "an engine whose descriptor names no unnamed key reads the name as the author's",
			dialect: "sqlite",
			key:     foreignKey("children_ibfk_1"),
			indexes: []catalog.Index{{Name: "a", TableName: "children", Columns: []string{"a"}}},
			want:    nil,
		},
		{
			name:    "a key the author named builds no column-named index",
			dialect: "mysql",
			key:     foreignKey("fk_parent"),
			indexes: []catalog.Index{{Name: "a", TableName: "children", Columns: []string{"a"}}},
			want:    nil,
		},
		{
			name:    "MariaDB numbers an unnamed key",
			dialect: "mariadb",
			key:     foreignKey("1"),
			indexes: []catalog.Index{{Name: "a", TableName: "children", Columns: []string{"a"}}},
			want:    []string{"a"},
		},
		{
			name:    "another index covers the key",
			dialect: "mysql",
			key:     foreignKey("f"),
			indexes: []catalog.Index{
				{Name: "cover", TableName: "children", Columns: []string{"a"}},
				{Name: "f", TableName: "children", Columns: []string{"a"}},
			},
			want: nil,
		},
		{
			name:    "a descending cover does not cover on MySQL",
			dialect: "mysql",
			key:     foreignKey("f"),
			indexes: []catalog.Index{
				{Name: "cover", TableName: "children", Columns: []string{"a"}, Parts: []catalog.IndexPart{{Name: "a", Desc: true}}},
				{Name: "f", TableName: "children", Columns: []string{"a"}},
			},
			want: []string{"f"},
		},
		{
			name:    "a descending cover covers on MariaDB",
			dialect: "mariadb",
			key:     foreignKey("f"),
			indexes: []catalog.Index{
				{Name: "cover", TableName: "children", Columns: []string{"a"}, Parts: []catalog.IndexPart{{Name: "a", Desc: true}}},
				{Name: "f", TableName: "children", Columns: []string{"a"}},
			},
			want: nil,
		},
		{
			name:    "an index of another table",
			dialect: "mysql",
			key:     foreignKey("f"),
			indexes: []catalog.Index{{Name: "f", TableName: "others", Columns: []string{"a"}}},
			want:    nil,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			current := &catalog.Database{Indexes: test.indexes}
			backing := mysqlcomparison.Rules(test.dialect).ForeignKeyIndexes.BackingIndexes(current, test.key, identifier.ForDialect(test.dialect))
			c.Assert(indexNames(backing), qt.DeepEquals, test.want)
		})
	}
}

// TestRules pins the rules the MySQL family registers: its index options, its
// generated expressions and its foreign key indexes, and nothing it leaves to
// the shared comparison.
func TestRules(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			rules := mysqlcomparison.Rules(dialect)
			c.Assert(rules.Indexes, qt.IsNotNil)
			c.Assert(rules.GeneratedExpressions, qt.IsNotNil)
			c.Assert(rules.ForeignKeyIndexes, qt.IsNotNil)
			c.Assert(rules.IndexDefinitions, qt.IsNil)
			c.Assert(rules.IndexPredicates, qt.IsNil)
			c.Assert(rules.Columns, qt.IsNil)
			c.Assert(rules.Views, qt.IsNil)
			c.Assert(rules.Routines, qt.IsNil)
		})
	}
}
