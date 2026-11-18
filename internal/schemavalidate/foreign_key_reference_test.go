package schemavalidate_test

import (
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/schemavalidate"
)

// uniqueReference is the referenced-key policy of PostgreSQL: a foreign key
// references a declared unique key.
var uniqueReference = capability.Of(map[capability.Capability]bool{
	capability.ForeignKeys:                       true,
	capability.ForeignKeysRequireUniqueReference: true,
})

// indexedReference is the policy of MySQL 8.0: a foreign key references the
// leftmost prefix of an index.
var indexedReference = capability.Of(map[capability.Capability]bool{
	capability.ForeignKeys:                        true,
	capability.ForeignKeysRequireIndexedReference: true,
})

// referenceSchema declares parent (id, code, rank) with id as its primary key
// and an index on (code, rank), and child, whose foreign keys are fields.
func referenceSchema(fields ...schemamodel.Field) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Parent", Name: "parent"}, {StructName: "Child", Name: "child"}},
		Fields: append([]schemamodel.Field{
			{StructName: "Parent", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Parent", Name: "code", Type: "TEXT"},
			{StructName: "Parent", Name: "rank", Type: "INTEGER"},
			{StructName: "Child", Name: "id", Type: "INTEGER", Primary: true},
		}, fields...),
		Indexes: []schemamodel.Index{{StructName: "Parent", Name: "parent_code_rank", Fields: []string{"code", "rank"}}},
	}
}

// TestCollectTargetForeignKeyReferenceProblems_ReportsEveryRefusedKey reads a
// schema with three refused keys, two declared on columns and one as a
// constraint, and expects each in declaration order, with the line the
// built-in validation refuses it with.
func TestCollectTargetForeignKeyReferenceProblems_ReportsEveryRefusedKey(t *testing.T) {
	c := qt.New(t)
	database := referenceSchema(
		schemamodel.Field{StructName: "Child", Name: "code", Type: "TEXT", Foreign: "parent(code)"},
		schemamodel.Field{StructName: "Child", Name: "parent_id", Type: "INTEGER", Foreign: "parent(id)"},
		schemamodel.Field{StructName: "Child", Name: "rank", Type: "INTEGER", Foreign: "parent(rank)"},
	)
	database.Constraints = []schemamodel.Constraint{{
		Name: "child_pair", Type: "FOREIGN KEY", Table: "child", Columns: []string{"code", "rank"},
		ForeignTable: "parent", ForeignColumns: []string{"code", "rank"},
	}}

	problems, err := schemavalidate.CollectTargetForeignKeyReferenceProblems(must.Must(builtin.New()), database, "postgres", uniqueReference)

	c.Assert(err, qt.IsNil)
	c.Assert(problems, qt.HasLen, 3)
	c.Assert(problems[0], qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(problems[0], qt.ErrorMatches, `postgres requires referenced columns code on table "parent" to be declared unique: a primary key, UNIQUE constraint or unique index must key exactly those columns, with no WHERE clause and no expression, COLLATE, prefix length or operator class on a key`)
	c.Assert(problems[1], qt.ErrorMatches, `postgres requires referenced columns rank on table "parent" to be declared unique: a primary key, UNIQUE constraint or unique index must key exactly those columns, with no WHERE clause and no expression, COLLATE, prefix length or operator class on a key`)
	c.Assert(problems[2], qt.ErrorMatches, `postgres requires referenced columns code, rank on table "parent" to be declared unique: a primary key, UNIQUE constraint or unique index must key exactly those columns, with no WHERE clause and no expression, COLLATE, prefix length or operator class on a key`)
}

// TestCollectTargetForeignKeyReferenceProblems_ReadsThePolicyOfTheCapabilities
// holds one schema to each policy. The index on (code, rank) serves a key that
// references code where the policy asks for an index, and not where it asks
// for a unique key; where the server builds the index, any columns are a key.
func TestCollectTargetForeignKeyReferenceProblems_ReadsThePolicyOfTheCapabilities(t *testing.T) {
	database := referenceSchema(
		schemamodel.Field{StructName: "Child", Name: "code", Type: "TEXT", Foreign: "parent(code)"},
		schemamodel.Field{StructName: "Child", Name: "rank", Type: "INTEGER", Foreign: "parent(rank)"},
	)
	for _, test := range []struct {
		name    string
		dialect string
		caps    capability.Capabilities
		want    []string
	}{
		{
			name:    "unique key",
			dialect: "postgres",
			caps:    uniqueReference,
			want: []string{
				`postgres requires referenced columns code on table "parent" to be declared unique: a primary key, UNIQUE constraint or unique index must key exactly those columns, with no WHERE clause and no expression, COLLATE, prefix length or operator class on a key`,
				`postgres requires referenced columns rank on table "parent" to be declared unique: a primary key, UNIQUE constraint or unique index must key exactly those columns, with no WHERE clause and no expression, COLLATE, prefix length or operator class on a key`,
			},
		},
		{
			name:    "leftmost index prefix",
			dialect: "mysql",
			caps:    indexedReference,
			want:    []string{`mysql requires referenced columns rank on table "parent" to be the full leftmost prefix of an index`},
		},
		{
			name:    "index built by the server",
			dialect: "postgres",
			caps:    uniqueReference.With(capability.ForeignKeysRequireUniqueReference, false).With(capability.ForeignKeysCreateBackingIndex, true),
			want:    make([]string, 0),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			problems, err := schemavalidate.CollectTargetForeignKeyReferenceProblems(must.Must(builtin.New()), database, test.dialect, test.caps)

			c.Assert(err, qt.IsNil)
			c.Assert(errorLines(problems), qt.DeepEquals, test.want)
		})
	}
}

// TestCollectTargetForeignKeyReferenceProblems_ReadsNoOtherRule declares keys
// the built-in validation refuses for other reasons: SET NULL on a NOT NULL
// column, a key whose referenced table no declaration defines, and a key whose
// owner is undeclared. None is a referenced-key fault, so none is reported.
func TestCollectTargetForeignKeyReferenceProblems_ReadsNoOtherRule(t *testing.T) {
	c := qt.New(t)
	database := referenceSchema(
		schemamodel.Field{StructName: "Child", Name: "parent_id", Type: "INTEGER", Foreign: "parent(id)", OnDelete: "SET NULL"},
		schemamodel.Field{StructName: "Child", Name: "other_id", Type: "INTEGER", Foreign: "missing(id)"},
		schemamodel.Field{StructName: "Orphan", Name: "code", Type: "TEXT", Foreign: "parent(code)"},
	)

	problems, err := schemavalidate.CollectTargetForeignKeyReferenceProblems(must.Must(builtin.New()), database, "postgres", uniqueReference)

	c.Assert(err, qt.IsNil)
	c.Assert(problems, qt.HasLen, 0)
}

// TestCollectTargetForeignKeyReferenceProblems_ANilSchemaHasNoProblems passes
// no schema.
func TestCollectTargetForeignKeyReferenceProblems_ANilSchemaHasNoProblems(t *testing.T) {
	c := qt.New(t)

	problems, err := schemavalidate.CollectTargetForeignKeyReferenceProblems(must.Must(builtin.New()), nil, "postgres", uniqueReference)

	c.Assert(err, qt.IsNil)
	c.Assert(problems, qt.HasLen, 0)
}

// unresolvedTarget serves no target.
type unresolvedTarget struct{}

var errNoTarget = errors.New("no such target")

func (unresolvedTarget) ResolveTarget(string) (schemaext.TargetSelection, error) {
	return schemaext.TargetSelection{}, errNoTarget
}

// TestCollectTargetForeignKeyReferenceProblems_FailurePath returns the
// resolver's refusal, and no problems.
func TestCollectTargetForeignKeyReferenceProblems_FailurePath(t *testing.T) {
	c := qt.New(t)

	problems, err := schemavalidate.CollectTargetForeignKeyReferenceProblems(unresolvedTarget{}, referenceSchema(), "postgres", uniqueReference)

	c.Assert(err, qt.ErrorIs, errNoTarget)
	c.Assert(problems, qt.IsNil)
}

// errorLines reduces a problem list to its messages.
func errorLines(problems []error) []string {
	lines := make([]string, 0, len(problems))
	for _, problem := range problems {
		lines = append(lines, problem.Error())
	}
	return lines
}
