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

// The child table of every row below: one foreign key, which the declaration keeps
// as it is.
const (
	childOnID   = `CREATE TABLE child (id bigint PRIMARY KEY, p bigint, CONSTRAINT child_fk FOREIGN KEY (p) REFERENCES parent (id));`
	childOnCode = `CREATE TABLE child (id bigint PRIMARY KEY, p bigint, CONSTRAINT child_fk FOREIGN KEY (p) REFERENCES parent (code));`
	dropChildFK = `ALTER TABLE "child" DROP CONSTRAINT "child_fk"`
	addFKOnID   = `ALTER TABLE "child" ADD CONSTRAINT "child_fk" FOREIGN KEY ("p") REFERENCES "parent"("id")`
	addFKOnCode = `ALTER TABLE "child" ADD CONSTRAINT "child_fk" FOREIGN KEY ("p") REFERENCES "parent"("code")`
)

// keyPlan plans on dialect the change from the SQL schema held to the SQL schema
// desired. boundTo is what the server reports the held foreign key depends on, and
// empty for a catalog built from a description, which says nothing about it.
func keyPlan(c *qt.C, dialect, held, desired, boundTo string, also ...catalog.Constraint) []string {
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
	for i := range current.Constraints {
		current.Constraints[i].ReferencedIndex = boundTo
	}
	// A reader reports a primary key as a constraint of its table, and a catalog
	// built from a description leaves it on the column.
	current.Constraints = append(current.Constraints, catalog.Constraint{Name: "parent_pkey", TableName: "parent",
		Type: "PRIMARY KEY", ColumnNames: []string{"id"}, ColumnName: "id"})
	current.Constraints = append(current.Constraints, also...)
	diff, err := schemadiff.CompareWithDialect(c.Context(), &desiredSchema, current, dialect, runtime)
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(c.Context(), must.Must(builtin.New()), diff, dialect,
		planext.Options{Capabilities: capability.ForDialect(dialect), Statements: planext.StatementStyleExact})
	c.Assert(err, qt.IsNil)
	return statements
}

// TestForeignKeyOverRemovedKey_LeavesAndComesBack pins the order PostgreSQL requires.
// It refuses to drop the index a foreign key depends on, and it binds a new foreign
// key to the oldest index over its columns, so the removed key goes between the two
// halves of the foreign key.
func TestForeignKeyOverRemovedKey_LeavesAndComesBack(t *testing.T) {
	tests := []struct {
		name          string
		held, desired string
		boundTo       string
		want          []string
	}{
		{
			name:    "a primary key moved to another column",
			held:    `CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL);` + childOnID,
			desired: `CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL, CONSTRAINT parent_pkey PRIMARY KEY (code), CONSTRAINT parent_id_key UNIQUE (id));` + childOnID,
			boundTo: "parent_pkey",
			want: []string{
				dropChildFK,
				`ALTER TABLE "parent" DROP CONSTRAINT "parent_pkey"`,
				`ALTER TABLE "parent" ADD PRIMARY KEY ("code")`,
				`ALTER TABLE "parent" ADD CONSTRAINT "parent_id_key" UNIQUE ("id")`,
				addFKOnID,
			},
		},
		{
			name:    "a primary key dropped with a UNIQUE over its column declared",
			held:    `CREATE TABLE parent (id bigint PRIMARY KEY);` + childOnID,
			desired: `CREATE TABLE parent (id bigint NOT NULL, CONSTRAINT parent_id_key UNIQUE (id));` + childOnID,
			boundTo: "parent_pkey",
			want: []string{
				dropChildFK,
				`ALTER TABLE "parent" DROP CONSTRAINT "parent_pkey"`,
				`ALTER TABLE "parent" ADD CONSTRAINT "parent_id_key" UNIQUE ("id")`,
				addFKOnID,
			},
		},
		{
			name:    "a UNIQUE replaced under another name",
			held:    `CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL, CONSTRAINT parent_code_old UNIQUE (code));` + childOnCode,
			desired: `CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL, CONSTRAINT parent_code_new UNIQUE (code));` + childOnCode,
			boundTo: "parent_code_old",
			want: []string{
				dropChildFK,
				`ALTER TABLE "parent" DROP CONSTRAINT "parent_code_old"`,
				`ALTER TABLE "parent" ADD CONSTRAINT "parent_code_new" UNIQUE ("code")`,
				addFKOnCode,
			},
		},
		{
			name:    "a UNIQUE redefined under its name",
			held:    `CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL, x int, CONSTRAINT parent_code_key UNIQUE (code));` + childOnCode,
			desired: `CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL, x int, CONSTRAINT parent_code_key UNIQUE (code) INCLUDE (x));` + childOnCode,
			boundTo: "parent_code_key",
			want: []string{
				dropChildFK,
				`ALTER TABLE "parent" DROP CONSTRAINT "parent_code_key"`,
				`ALTER TABLE "parent" ADD CONSTRAINT "parent_code_key" UNIQUE ("code") INCLUDE ("x")`,
				addFKOnCode,
			},
		},
		{
			name:    "a unique index replaced by a UNIQUE constraint",
			held:    `CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL); CREATE UNIQUE INDEX parent_code_ui ON parent (code);` + childOnCode,
			desired: `CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL, CONSTRAINT parent_code_key UNIQUE (code));` + childOnCode,
			boundTo: "parent_code_ui",
			want: []string{
				dropChildFK,
				`DROP INDEX "parent_code_ui"`,
				`ALTER TABLE "parent" ADD CONSTRAINT "parent_code_key" UNIQUE ("code")`,
				addFKOnCode,
			},
		},
		{
			name:    "a read that names no index leaves the columns to decide",
			held:    `CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL, CONSTRAINT parent_code_old UNIQUE (code));` + childOnCode,
			desired: `CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL, CONSTRAINT parent_code_new UNIQUE (code));` + childOnCode,
			want: []string{
				dropChildFK,
				`ALTER TABLE "parent" DROP CONSTRAINT "parent_code_old"`,
				`ALTER TABLE "parent" ADD CONSTRAINT "parent_code_new" UNIQUE ("code")`,
				addFKOnCode,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(keyPlan(c, platform.Postgres, test.held, test.desired, test.boundTo), qt.DeepEquals, test.want)
		})
	}
}

// TestForeignKeyOverRemovedKey_LeavesAForeignKeyOnAKeyThatStays is the control for
// the read of the index: another key covers the same column, the server reports the
// foreign key on that one, so the plan does not touch the foreign key. Read by
// columns alone it would take a lock and validate every row for nothing.
func TestForeignKeyOverRemovedKey_LeavesAForeignKeyOnAKeyThatStays(t *testing.T) {
	c := qt.New(t)

	got := keyPlan(c, platform.Postgres,
		`CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL);`+childOnID,
		`CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL, CONSTRAINT parent_pkey PRIMARY KEY (code), CONSTRAINT parent_id_key UNIQUE (id));`+childOnID,
		"parent_id_key",
		catalog.Constraint{Name: "parent_id_key", TableName: "parent", Type: "UNIQUE", ColumnNames: []string{"id"}, ColumnName: "id"})

	c.Assert(got, qt.DeepEquals, []string{
		`ALTER TABLE "parent" DROP CONSTRAINT "parent_pkey"`,
		`ALTER TABLE "parent" ADD PRIMARY KEY ("code")`,
	})
}

// TestForeignKeyOverRemovedKey_AnotherEngineKeepsItsPlan holds the rule to the engine
// it was measured on: no other engine's plan gains a foreign key rebuild.
func TestForeignKeyOverRemovedKey_AnotherEngineKeepsItsPlan(t *testing.T) {
	c := qt.New(t)

	got := keyPlan(c, platform.CockroachDB,
		`CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL);`+childOnID,
		`CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL, CONSTRAINT parent_pkey PRIMARY KEY (code), CONSTRAINT parent_id_key UNIQUE (id));`+childOnID,
		"parent_pkey")

	c.Assert(got, qt.Not(qt.Contains), dropChildFK)
	c.Assert(got, qt.Not(qt.Contains), addFKOnID)
}
