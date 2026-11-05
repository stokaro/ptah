package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/internal/sqlschema"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// columnKeyPlan plans on dialect the change from the SQL schema held, with the
// primary key a reader reports over liveKey, to the SQL schema desired.
func columnKeyPlan(c *qt.C, dialect, held, desired string, liveKey ...string) []string {
	c.Helper()
	runtime := builtintest.Runtime()
	read := func(sql string) schemamodel.Database {
		schema, _, err := sqlschema.ReadWith(c.Context(), []byte(sql), platform.Postgres, nil, sqlschema.Options{Owners: runtime.SQL()})
		c.Assert(err, qt.IsNil, qt.Commentf("%s", sql))
		return schema
	}
	heldSchema, desiredSchema := read(held), read(desired)
	current, err := goschematodb.ToDBSchema(c.Context(), &heldSchema, dialect, runtime)
	c.Assert(err, qt.IsNil)
	if len(liveKey) > 0 {
		current.Constraints = append(current.Constraints, catalog.Constraint{Name: "parent_pkey", TableName: "parent",
			Type: "PRIMARY KEY", ColumnNames: liveKey, ColumnName: liveKey[0]})
	}
	diff, err := schemadiff.CompareWithDialect(c.Context(), &desiredSchema, current, dialect, runtime)
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(c.Context(), must.Must(builtin.New()), diff, dialect,
		planext.Options{Capabilities: capability.ForDialect(dialect), Statements: planext.StatementStyleExact})
	c.Assert(err, qt.IsNil)
	return statements
}

// TestColumnPrimaryKey_ComparedAsTheTablesKey pins what a primary key a column
// declares plans on a table that holds another key or none. Compared only as a flag on
// the column, it planned nothing, so the table lost its key or never had one and the
// next run reported the schema synced.
func TestColumnPrimaryKey_ComparedAsTheTablesKey(t *testing.T) {
	tests := []struct {
		name          string
		held, desired string
		liveKey       []string
		want          []string
	}{
		{
			name:    "moved to another column",
			held:    `CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL);`,
			desired: `CREATE TABLE parent (id bigint NOT NULL, code bigint PRIMARY KEY);`,
			liveKey: []string{"id"},
			want:    []string{`ALTER TABLE "parent" DROP CONSTRAINT "parent_pkey"`, `ALTER TABLE "parent" ADD PRIMARY KEY ("code")`},
		},
		{
			name:    "a table with no key gains one",
			held:    `CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL);`,
			desired: `CREATE TABLE parent (id bigint NOT NULL, code bigint PRIMARY KEY);`,
			want:    []string{`ALTER TABLE "parent" ADD PRIMARY KEY ("code")`},
		},
		{
			name:    "a key over two columns narrowed to the first",
			held:    `CREATE TABLE parent (a bigint NOT NULL, b bigint NOT NULL, CONSTRAINT parent_pkey PRIMARY KEY (a, b));`,
			desired: `CREATE TABLE parent (a bigint PRIMARY KEY, b bigint NOT NULL);`,
			liveKey: []string{"a", "b"},
			want:    []string{`ALTER TABLE "parent" DROP CONSTRAINT "parent_pkey"`, `ALTER TABLE "parent" ADD PRIMARY KEY ("a")`},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(columnKeyPlan(c, platform.Postgres, test.held, test.desired, test.liveKey...), qt.DeepEquals, test.want)
		})
	}
}

// TestColumnPrimaryKey_StaysAFlag pins where the column's flag is enough: a key the
// table already holds, and a column the plan adds, which takes its key in its own
// ADD COLUMN.
func TestColumnPrimaryKey_StaysAFlag(t *testing.T) {
	tests := []struct {
		name          string
		held, desired string
		liveKey       []string
		want          []string
	}{
		{
			name:    "the key the table holds",
			held:    `CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL);`,
			desired: `CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL);`,
			liveKey: []string{"id"},
			want:    make([]string, 0),
		},
		{
			name:    "a column the plan adds",
			held:    `CREATE TABLE parent (id bigint NOT NULL);`,
			desired: `CREATE TABLE parent (id bigint NOT NULL, code bigint PRIMARY KEY);`,
			want:    []string{`ALTER TABLE "parent" ADD COLUMN "code" bigint PRIMARY KEY NOT NULL`},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(columnKeyPlan(c, platform.Postgres, test.held, test.desired, test.liveKey...), qt.DeepEquals, test.want)
		})
	}
}

// TestColumnPrimaryKey_AnotherEngineKeepsItsPlan holds the rule to the engine it was
// measured on.
func TestColumnPrimaryKey_AnotherEngineKeepsItsPlan(t *testing.T) {
	c := qt.New(t)

	got := columnKeyPlan(c, platform.CockroachDB,
		`CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL);`,
		`CREATE TABLE parent (id bigint NOT NULL, code bigint PRIMARY KEY);`)

	c.Assert(got, qt.Not(qt.Contains), `ALTER TABLE "parent" ADD PRIMARY KEY ("code")`)
}
