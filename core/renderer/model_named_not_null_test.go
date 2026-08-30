package renderer_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/schemamodel"
)

// namedNotNullDatabase is one table whose second column carries a NOT NULL
// constraint name, declared on the model rather than built as an AST node.
//
// The distinction is the whole point. core/renderer/named_not_null_test.go
// already proves the PostgreSQL renderer keeps and refuses the name correctly;
// it hands the renderer an [go.5x5.cz/ptah/core/ast.ColumnNode] it built
// itself. The desired-state path goes through the model, and the field stopped
// there -- so the name was gone before any of those decisions could be reached
// (stokaro/ptah#2590).
func namedNotNullDatabase(constraintName string, nullable, primary bool) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Widget", Name: "widgets"}},
		Fields: []schemamodel.Field{
			{StructName: "Widget", Name: "id", Type: "INTEGER", Primary: true},
			{
				StructName:            "Widget",
				Name:                  "a",
				Type:                  "INTEGER",
				Nullable:              nullable,
				Primary:               primary,
				NotNullConstraintName: constraintName,
			},
		},
	}
}

// TestModelNamedNotNull_HappyPath is the measured case: PostgreSQL 18 records
// one row per NOT NULL in pg_constraint and answers to the name.
func TestModelNamedNotNull_HappyPath(t *testing.T) {
	c := qt.New(t)

	statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(
		namedNotNullDatabase("widgets_a_nn", false, false), "postgres", capability.Postgres18())

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Contains, `CONSTRAINT "widgets_a_nn" NOT NULL`)
}

// TestModelNamedNotNull_AnUnnamedConstraintIsUnaffected is the acceptance
// control. Without it, a conversion that wrote some fixed name onto every NOT
// NULL column would satisfy the happy path, and a renderer that refused every
// named column would satisfy the failure paths below.
func TestModelNamedNotNull_AnUnnamedConstraintIsUnaffected(t *testing.T) {
	c := qt.New(t)

	statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(
		namedNotNullDatabase("", false, false), "postgres", capability.Postgres18())

	c.Assert(err, qt.IsNil)
	sql := strings.Join(statements, "\n")
	c.Assert(sql, qt.Contains, `"a" INTEGER NOT NULL`)
	c.Assert(sql, qt.Not(qt.Contains), "CONSTRAINT")
}

// TestModelNamedNotNull_APrimaryKeyColumnDropsTheName is the one column where
// dropping is right, and it is why the model path cannot simply carry the name
// through to the renderer's refusal.
//
// PostgreSQL 18 names the NOT NULL on a primary-key column by itself. Measured
// on 18.6, `CREATE TABLE accounts (id BIGINT PRIMARY KEY, email TEXT NOT NULL)`
// leaves pg_constraint holding `accounts_id_not_null` with contype 'n' beside
// the key's own `accounts_pkey`. So every PG18 table with a primary key
// produces such a name on the way in, and refusing it here made `ptah db read`
// against any of them exit non-zero rather than describe the database.
//
// The refusal itself is right and still reachable; it belongs to an AST a
// caller built, which is
// TestNamedNotNull_ANameOnAPrimaryKeyColumnIsRefused in
// core/renderer/named_not_null_test.go.
func TestModelNamedNotNull_APrimaryKeyColumnDropsTheName(t *testing.T) {
	c := qt.New(t)

	statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(
		namedNotNullDatabase("widgets_a_nn", false, true), "postgres", capability.Postgres18())

	c.Assert(err, qt.IsNil)
	sql := strings.Join(statements, "\n")
	c.Assert(sql, qt.Not(qt.Contains), "widgets_a_nn")
	// The name is dropped for being on a key column, not because carrying it
	// stopped working: a non-key column in the same table still keeps one.
	nonKey, err := renderer.GetOrderedCreateStatementsWithCapabilities(
		namedNotNullDatabase("widgets_a_nn", false, false), "postgres", capability.Postgres18())
	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(nonKey, "\n"), qt.Contains, `CONSTRAINT "widgets_a_nn" NOT NULL`)
}

// TestModelNamedNotNull_FailurePath drives the two refusals the name makes
// reachable from a model. Each was already implemented and neither could fire
// from one, which is what the defect was: the branch that refuses rather than
// dropping is only a guarantee while something can reach it.
func TestModelNamedNotNull_FailurePath(t *testing.T) {
	tests := []struct {
		name        string
		database    *schemamodel.Database
		caps        capability.Capabilities
		wantErrLike string
	}{
		{
			// PostgreSQL 17 accepts `CONSTRAINT c NOT NULL` and stores nothing,
			// so emitting the name there produces DDL that applies, reads back
			// bare, and leaves every later comparison reporting a difference no
			// apply can settle.
			name:        "a target that records nothing",
			database:    namedNotNullDatabase("widgets_a_nn", false, false),
			caps:        capability.Postgres17(),
			wantErrLike: "does not keep a NOT NULL constraint name",
		},
		{
			// A name on a nullable column describes a constraint that is not
			// there. Dropping it would render a nullable column and report
			// success.
			name:        "a name on a nullable column",
			database:    namedNotNullDatabase("widgets_a_nn", true, false),
			caps:        capability.Postgres18(),
			wantErrLike: "names no constraint",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(
				test.database, "postgres", test.caps)

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, test.wantErrLike)
			c.Assert(statements, qt.IsNil)
		})
	}
}
