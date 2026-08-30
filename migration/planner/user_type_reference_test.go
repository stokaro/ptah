package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// TestGenerateSchemaDiffSQLQualifiesDeclaredUserTypes pins the second of the
// two entry points fromschema.QualifyDeclaredUserTypes is wired into.
//
// The planner is where a diff-driven CREATE TABLE is built, and it reads its
// columns out of one prepared value that every dialect planner shares. Wiring
// the pass into fromschema.FromDatabase alone is the plausible half-fix -- that
// is the obvious whole-schema conversion -- and it leaves every `schema diff`
// and every generated migration untouched, which is the path an inspected
// database actually travels.
//
// Measured on PostgreSQL 17.10 through `ptah-compat schema diff`, planning an
// inspected schema `wf1138s` into a fresh database:
//
//	before   CREATE TABLE "wf1138s"."t" (..., "enum_array" mood[], ...)
//	         psql -v ON_ERROR_STOP=1   ERROR: type "mood[]" does not exist   exit 3
//	after    CREATE TABLE "wf1138s"."t" (..., "enum_array" wf1138s.mood[], ...)
//	         psql -v ON_ERROR_STOP=1   exit 0
//
// See stokaro/ptah#1138.
func TestGenerateSchemaDiffSQLQualifiesDeclaredUserTypes(t *testing.T) {
	tests := []struct {
		name       string
		columnType string
		want       string
	}{
		{
			name:       "an enum array",
			columnType: "mood[]",
			want:       "\"c\" app.mood[]",
		},
		{
			name:       "a domain",
			columnType: "positive_int",
			want:       "\"c\" app.positive_int",
		},
		{
			name:       "a domain array",
			columnType: "positive_int[]",
			want:       "\"c\" app.positive_int[]",
		},
		{
			// The control. A built-in type has no declaration to name, and
			// prefixing it would produce a type no server has.
			name:       "a built-in array",
			columnType: "character varying(100)[]",
			want:       "\"c\" character varying(100)[]",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			schema := plannerUserTypeSchema(test.columnType)
			diff := &difftypes.SchemaDiff{
				TablesAdded: difftypes.TableCreationsFor(schema, "t"),
				// A hand-built diff carries the vocabulary itself; a comparison
				// fills it in (stokaro/ptah#2315).
				DeclaredUserTypes: difftypes.UserTypeVocabularyOf(schema),
			}

			sql, err := planner.GenerateSchemaDiffSQL(diff, platform.Postgres)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
		})
	}
}

// TestGenerateSchemaDiffSQLLeavesABuiltInTypeAlone is the same enumeration at
// the planner, and it is here rather than only in the fromschema package
// because this is the path an inspected database travels: `schema diff` builds
// its CREATE TABLE here, and a guard that holds in the pass but is bypassed on
// the way to this call site would still ship the retyped column.
//
// Measured on PostgreSQL 17.10 through `ptah-compat schema diff`, source seeded
// as `CREATE DOMAIN advm.money AS numeric(12,2)` beside an ordinary `money`
// column, each plan replayed into a fresh database at exit 0 and the catalog
// read back:
//
//	before this guard   "builtin_col" advm.money   ->  advm.money | advm       | d
//	with it             "builtin_col" money        ->  money      | pg_catalog | b
//
// The source column is `money | pg_catalog | b`.
func TestGenerateSchemaDiffSQLLeavesABuiltInTypeAlone(t *testing.T) {
	tests := []struct {
		name       string
		columnType string
		want       string
	}{
		{
			name:       "a domain shadowing money, scalar",
			columnType: "money",
			want:       "\"c\" money",
		},
		{
			name:       "a domain shadowing money, array",
			columnType: "money[]",
			want:       "\"c\" money[]",
		},
		{
			// The control, and the cell stokaro/ptah#1138 closed. Same
			// document, same call, a name pg_catalog does not answer to.
			name:       "a domain with a name of its own",
			columnType: "positive_int",
			want:       "\"c\" advm.positive_int",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			schema := &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "T", Name: "t", Schema: "advm"}},
				Fields: []schemamodel.Field{{StructName: "T", Name: "c", Type: test.columnType}},
				Domains: []schemamodel.Domain{
					{Name: "money", Schema: "advm", BaseType: "numeric(12,2)"},
					{Name: "positive_int", Schema: "advm", BaseType: "integer"},
				},
			}
			diff := &difftypes.SchemaDiff{
				TablesAdded: difftypes.TableCreationsFor(schema, "t"),
				// A hand-built diff carries the vocabulary itself; a comparison
				// fills it in (stokaro/ptah#2315).
				DeclaredUserTypes: difftypes.UserTypeVocabularyOf(schema),
			}

			sql, err := planner.GenerateSchemaDiffSQL(diff, platform.Postgres)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
		})
	}
}

func plannerUserTypeSchema(columnType string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t", Schema: "app"}},
		Fields: []schemamodel.Field{{StructName: "T", Name: "c", Type: columnType}},
		Enums: []schemamodel.Enum{{
			Name: "mood", Schema: "app", Values: []string{"sad", "ok"},
		}},
		Domains: []schemamodel.Domain{{
			Name: "positive_int", Schema: "app", BaseType: "integer",
		}},
	}
}

// TestCompare_ACreatedColumnIsTypedByTheComparisonsVocabulary is the half the
// hand-built rows above cannot reach.
//
// They supply the vocabulary themselves, so they pin that the planner USES it.
// This one drives the comparison, which is what fills it in for every real
// plan: a diff produced from a declaration carries the type vocabulary that
// declaration states, and a created column resolves through it.
//
// Without that, a column declared `positive_int` renders as the bare name and
// applies to whatever the connection's search path finds -- or to nothing.
func TestCompare_ACreatedColumnIsTypedByTheComparisonsVocabulary(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t", Schema: "app"}},
		Fields: []schemamodel.Field{{StructName: "T", Name: "c", Type: "positive_int"}},
		Domains: []schemamodel.Domain{{
			Name: "positive_int", Schema: "app", BaseType: "integer",
		}},
	}

	diff := schemadiff.CompareWithDialect(desired, &catalog.Database{}, platform.Postgres)

	c.Assert(diff.DeclaredUserTypes.Domains, qt.HasLen, 1,
		qt.Commentf("the comparison carries the declaration's type vocabulary"))

	sql, err := planner.GenerateSchemaDiffSQL(diff, platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `"c" app.positive_int`,
		qt.Commentf("the created column resolves through it\n%s", sql))
}

// TestCompare_AForeignKeyResolvesAgainstTheComparisonsTables is the reference
// half of the vocabulary assertion above.
//
// A foreign key names the table it points at, and the declaration says which
// schema that table lives in. A child in `app` referencing `parents` means
// `app.parents`, and resolving it needs the declared table list — which a
// creation cannot carry, because the referenced table is usually one the diff
// does not touch.
//
// Driving the comparison is the point: the hand-built rows elsewhere supply the
// list themselves, so they pin that the planner USES it, not that a real plan
// has it.
func TestCompare_AForeignKeyResolvesAgainstTheComparisonsTables(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Parent", Name: "parents", Schema: "app"},
			{StructName: "Child", Name: "children", Schema: "app"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Parent", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "Child", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "Child", Name: "parent_id", Type: "INTEGER", Foreign: "parents(id)"},
		},
	}

	diff := schemadiff.CompareWithDialect(desired, &catalog.Database{}, platform.Postgres)

	c.Assert(diff.DeclaredTables, qt.HasLen, 2,
		qt.Commentf("the comparison carries the declared tables a reference resolves against"))

	sql, err := planner.GenerateSchemaDiffSQL(diff, platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `REFERENCES "app"."parents"`,
		qt.Commentf("the reference resolves to the schema the declaration puts the parent in\n%s", sql))
}
