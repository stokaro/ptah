package referencedkey_test

import (
	"regexp"
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
// column u, a UNIQUE constraint (c, d), a unique index on i, a descending
// unique index on s, a plain index on (x, y) and a column n no key or index
// covers. Five unique indexes cover a column in a way no foreign key can use:
// a partial index on p, an expression index over e, a COLLATE key on k, which
// the SQL frontend keeps as the element's text, an operator class on o, as
// PostgreSQL writes one, and a prefix length on f, as MySQL writes one.
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
			{StructName: "Parent", Name: "s", Type: "TEXT"},
			{StructName: "Parent", Name: "p", Type: "INTEGER"},
			{StructName: "Parent", Name: "e", Type: "TEXT"},
			{StructName: "Parent", Name: "k", Type: "TEXT"},
			{StructName: "Parent", Name: "o", Type: "TEXT"},
			{StructName: "Parent", Name: "f", Type: "TEXT"},
		},
		Constraints: []schemamodel.Constraint{{Name: "parent_cd", Type: "UNIQUE", Table: "parent", Columns: []string{"c", "d"}}},
		Indexes: []schemamodel.Index{
			{StructName: "Parent", Name: "parent_i", Fields: []string{"i"}, Unique: true},
			{StructName: "Parent", Name: "parent_xy", Fields: []string{"x", "y"}},
			{StructName: "Parent", Name: "parent_s", Fields: []string{"s"}, Parts: []schemamodel.IndexPart{{Name: "s", Desc: true}}, Unique: true},
			{StructName: "Parent", Name: "parent_p", Fields: []string{"p"}, Unique: true, Condition: "p > 0"},
			{StructName: "Parent", Name: "parent_e", Fields: []string{"lower(e)"}, Parts: []schemamodel.IndexPart{{Expr: "lower(e)"}}, Unique: true},
			{StructName: "Parent", Name: "parent_k", Fields: []string{"k COLLATE NOCASE"}, Unique: true},
			{StructName: "Parent", Name: "parent_o", Fields: []string{"o"}, Parts: []schemamodel.IndexPart{{Name: "o", Operator: "text_pattern_ops"}}, Unique: true},
			{StructName: "Parent", Name: "parent_f", Fields: []string{"f"}, Parts: []schemamodel.IndexPart{{Name: "f", Prefix: "10"}}, Unique: true},
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
		{name: "unique: a UNIQUE column on SQLite", dialect: "sqlite", caps: uniqueReference, columns: []string{"u"}},
		{name: "unique: a standalone unique index on SQLite", dialect: "sqlite", caps: uniqueReference, columns: []string{"i"}},
		{name: "unique: a descending unique index on SQLite", dialect: "sqlite", caps: uniqueReference, columns: []string{"s"}},
		{name: "unique: the primary key in another order on SQLite", dialect: "sqlite", caps: uniqueReference, columns: []string{"b", "a"}},
		{name: "unique: a UNIQUE constraint in another order on SQLite", dialect: "sqlite", caps: uniqueReference, columns: []string{"d", "c"}},
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

// The reasons the unique-key refusal gives, where the key may list the columns
// in any order and where it must list them in the order the foreign key does.
const (
	anyOrderReason = `a primary key, UNIQUE constraint or unique index must key exactly those columns`
	inOrderReason  = anyOrderReason + ` in that order`
)

// TestCheck_FailurePath lists referenced columns each policy refuses, with the
// line the refusal writes. A unique index the policy refuses over the columns
// is named with what it carries, so each dialect's line names only what that
// dialect's schema holds, and an index over another column is not named.
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
			want: `postgres requires referenced columns n on table "parent" to be declared unique: ` + anyOrderReason,
		},
		{
			name: "unique: a plain index", dialect: "postgres", caps: uniqueReference, columns: []string{"x", "y"},
			want: `postgres requires referenced columns x, y on table "parent" to be declared unique: ` + anyOrderReason,
		},
		{
			name: "unique: a column named twice where any order matches", dialect: "postgres", caps: uniqueReference, columns: []string{"a", "a"},
			want: `postgres requires referenced columns a, a on table "parent" to be declared unique: ` + anyOrderReason,
		},
		{
			name: "unique: the key in another order where the order must match", dialect: "mysql", caps: uniqueReference, columns: []string{"b", "a"},
			want: `mysql requires referenced columns b, a on table "parent" to be declared unique: ` + inOrderReason,
		},
		{
			name: "unique: a partial unique index", dialect: "postgres", caps: uniqueReference, columns: []string{"p"},
			want: `postgres requires referenced columns p on table "parent" to be declared unique: ` + anyOrderReason +
				`; unique index "parent_p" has a WHERE clause`,
		},
		{
			name: "unique: an expression unique index", dialect: "postgres", caps: uniqueReference, columns: []string{"e"},
			want: `postgres requires referenced columns e on table "parent" to be declared unique: ` + anyOrderReason +
				`; unique index "parent_e" keys lower(e) rather than a column`,
		},
		{
			name: "unique: a unique index with an operator class", dialect: "postgres", caps: uniqueReference, columns: []string{"o"},
			want: `postgres requires referenced columns o on table "parent" to be declared unique: ` + anyOrderReason +
				`; unique index "parent_o" keys o text_pattern_ops rather than a column`,
		},
		{
			name: "unique: a unique index with a prefix length", dialect: "mysql", caps: uniqueReference, columns: []string{"f"},
			want: `mysql requires referenced columns f on table "parent" to be declared unique: ` + inOrderReason +
				`; unique index "parent_f" keys f(10) rather than a column`,
		},
		{
			name: "unique: a plain index on SQLite", dialect: "sqlite", caps: uniqueReference, columns: []string{"x", "y"},
			want: `sqlite requires referenced columns x, y on table "parent" to be declared unique: ` + anyOrderReason,
		},
		{
			name: "unique: a partial unique index on SQLite", dialect: "sqlite", caps: uniqueReference, columns: []string{"p"},
			want: `sqlite requires referenced columns p on table "parent" to be declared unique: ` + anyOrderReason +
				`; unique index "parent_p" has a WHERE clause`,
		},
		{
			name: "unique: an expression unique index on SQLite", dialect: "sqlite", caps: uniqueReference, columns: []string{"e"},
			want: `sqlite requires referenced columns e on table "parent" to be declared unique: ` + anyOrderReason +
				`; unique index "parent_e" keys lower(e) rather than a column`,
		},
		{
			name: "unique: a unique index with a COLLATE key on SQLite", dialect: "sqlite", caps: uniqueReference, columns: []string{"k"},
			want: `sqlite requires referenced columns k on table "parent" to be declared unique: ` + anyOrderReason +
				`; unique index "parent_k" keys k COLLATE NOCASE rather than a column`,
		},
		{
			name: "unique: a column named twice on SQLite", dialect: "sqlite", caps: uniqueReference, columns: []string{"a", "a"},
			want: `sqlite requires referenced columns a, a on table "parent" to be declared unique: ` + anyOrderReason,
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
			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(test.want))
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
	c.Assert(problems[0], qt.ErrorMatches, `postgres requires referenced columns n on table "parent" to be declared unique: `+anyOrderReason)
	c.Assert(problems[1], qt.ErrorMatches, `postgres requires referenced columns n on table "parent" to be declared unique: `+anyOrderReason)
}
