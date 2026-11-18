package referencedkey_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/referencedkey"
)

// The policies a target's capabilities name, each with foreign keys on.
var (
	uniqueReference = capability.Of(map[capability.Capability]bool{
		capability.ForeignKeys: true, capability.ForeignKeysRequireUniqueReference: true,
	})
	indexedReference = capability.Of(map[capability.Capability]bool{
		capability.ForeignKeys: true, capability.ForeignKeysRequireIndexedReference: true,
	})
	backingIndex = capability.Of(map[capability.Capability]bool{
		capability.ForeignKeys: true, capability.ForeignKeysCreateBackingIndex: true,
	})
)

// parent is the referenced table of every case.
var parent = schemamodel.Table{StructName: "Parent", Name: "parent"}

// parentSchema declares parent with a composite primary key (a, b), a UNIQUE
// column u, a UNIQUE constraint (c, d), a unique index on i, a plain index on
// (x, y) and a column n no key or index covers.
func parentSchema() schemamodel.Database {
	return schemamodel.Database{
		Tables: []schemamodel.Table{parent},
		Fields: []schemamodel.Field{
			{StructName: "Parent", Name: "a", Type: "INTEGER", Primary: true},
			{StructName: "Parent", Name: "b", Type: "INTEGER", Primary: true},
			{StructName: "Parent", Name: "u", Type: "INTEGER", Unique: true},
			{StructName: "Parent", Name: "c", Type: "INTEGER"},
			{StructName: "Parent", Name: "d", Type: "INTEGER"},
			{StructName: "Parent", Name: "i", Type: "INTEGER"},
			{StructName: "Parent", Name: "x", Type: "INTEGER"},
			{StructName: "Parent", Name: "y", Type: "INTEGER"},
			{StructName: "Parent", Name: "n", Type: "INTEGER"},
		},
		Constraints: []schemamodel.Constraint{{Name: "parent_cd", Type: "UNIQUE", Table: "parent", Columns: []string{"c", "d"}}},
		Indexes: []schemamodel.Index{
			{StructName: "Parent", Name: "parent_i", Fields: []string{"i"}, Unique: true},
			{StructName: "Parent", Name: "parent_xy", Fields: []string{"x", "y"}},
		},
	}
}

// TestCheck_HappyPath lists referenced columns each policy accepts.
func TestCheck_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name    string
		dialect string
		caps    capability.Capabilities
		columns []string
	}{
		{name: "unique: the primary key", dialect: "postgres", caps: uniqueReference, columns: []string{"a", "b"}},
		{name: "unique: a UNIQUE column", dialect: "postgres", caps: uniqueReference, columns: []string{"u"}},
		{name: "unique: a UNIQUE constraint", dialect: "postgres", caps: uniqueReference, columns: []string{"c", "d"}},
		{name: "unique: a standalone unique index", dialect: "postgres", caps: uniqueReference, columns: []string{"i"}},
		{name: "unique: the key in another order where any order matches", dialect: "postgres", caps: uniqueReference, columns: []string{"b", "a"}},
		{name: "unique: a declared key where standalone indexes are ignored", dialect: "sqlite", caps: uniqueReference, columns: []string{"u"}},
		{name: "indexed: the leftmost prefix of an index", dialect: "mysql", caps: indexedReference, columns: []string{"x"}},
		{name: "indexed: the leftmost prefix of the primary key", dialect: "mysql", caps: indexedReference, columns: []string{"a"}},
		{name: "backing index: columns no key covers", dialect: "postgres", caps: backingIndex, columns: []string{"n"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := referencedkey.Check(parentSchema(), test.dialect, test.caps, parent, test.columns)

			c.Assert(err, qt.IsNil)
		})
	}
}

// TestCheck_FailurePath lists referenced columns each policy refuses, with the
// line the refusal writes.
func TestCheck_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name    string
		dialect string
		caps    capability.Capabilities
		columns []string
		want    string
	}{
		{
			name: "unique: a column no key covers", dialect: "postgres", caps: uniqueReference, columns: []string{"n"},
			want: `postgres requires referenced columns n on table "parent" to be declared unique`,
		},
		{
			name: "unique: a plain index", dialect: "postgres", caps: uniqueReference, columns: []string{"x", "y"},
			want: `postgres requires referenced columns x, y on table "parent" to be declared unique`,
		},
		{
			name: "unique: the key in another order where the order must match", dialect: "mysql", caps: uniqueReference, columns: []string{"b", "a"},
			want: `mysql requires referenced columns b, a on table "parent" to be declared unique`,
		},
		{
			name: "unique: a standalone unique index where it is ignored", dialect: "sqlite", caps: uniqueReference, columns: []string{"i"},
			want: `sqlite requires referenced columns i on table "parent" to be declared unique`,
		},
		{
			name: "indexed: a column no index leads", dialect: "mysql", caps: indexedReference, columns: []string{"y"},
			want: `mysql requires referenced columns y on table "parent" to be the full leftmost prefix of an index`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := referencedkey.Check(parentSchema(), test.dialect, test.caps, parent, test.columns)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

// TestProblems_ReportsEveryRefusedKey reports a column key and a constraint key that
// reference n, in that order, and leaves out a key whose referenced table no
// declaration defines.
func TestProblems_ReportsEveryRefusedKey(t *testing.T) {
	c := qt.New(t)
	database := parentSchema()
	database.Tables = append(database.Tables, schemamodel.Table{StructName: "Child", Name: "child"})
	database.Fields = append(database.Fields,
		schemamodel.Field{StructName: "Child", Name: "n", Type: "INTEGER", Foreign: "parent(n)"},
		schemamodel.Field{StructName: "Child", Name: "m", Type: "INTEGER", Foreign: "missing(id)"},
	)
	database.Constraints = append(database.Constraints, schemamodel.Constraint{
		Name: "child_n", Type: "FOREIGN KEY", Table: "child", Columns: []string{"n"}, ForeignTable: "parent", ForeignColumns: []string{"n"},
	})

	problems := referencedkey.Problems(database, "postgres", uniqueReference)

	c.Assert(problems, qt.HasLen, 2)
	c.Assert(problems[0], qt.ErrorMatches, `postgres requires referenced columns n on table "parent" to be declared unique`)
	c.Assert(problems[1], qt.ErrorMatches, `postgres requires referenced columns n on table "parent" to be declared unique`)
}
