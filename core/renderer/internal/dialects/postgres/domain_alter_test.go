package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/renderer/internal/dialects/postgres"
)

// TestPostgreSQLRenderer_AlterDomain covers the three in-place operations a
// changed domain reaches for, which exist because the alternative -- drop and
// recreate -- PostgreSQL refuses for any domain a column uses
// (stokaro/ptah#1717).
func TestPostgreSQLRenderer_AlterDomain(t *testing.T) {
	tests := []struct {
		name      string
		operation ast.TypeOperation
		want      string
	}{
		{
			name:      "drop a constraint by its catalog name",
			operation: ast.NewDropDomainConstraintOperation("positive_check"),
			want:      `ALTER DOMAIN "positive" DROP CONSTRAINT "positive_check";`,
		},
		{
			name:      "add a constraint by its declared expression",
			operation: ast.NewAddDomainConstraintOperation("VALUE > 0"),
			want:      `ALTER DOMAIN "positive" ADD CHECK (VALUE > 0);`,
		},
		{
			name:      "add a named constraint",
			operation: &ast.DomainConstraintOperation{AddExpression: "VALUE > 0", AddName: "positive_check"},
			want:      `ALTER DOMAIN "positive" ADD CONSTRAINT "positive_check" CHECK (VALUE > 0);`,
		},
		{
			name:      "set a default",
			operation: ast.NewSetDomainDefaultOperation("now()"),
			want:      `ALTER DOMAIN "positive" SET DEFAULT now();`,
		},
		{
			name:      "drop a default",
			operation: ast.NewDropDomainDefaultOperation(),
			want:      `ALTER DOMAIN "positive" DROP DEFAULT;`,
		},
		{
			name:      "set NOT NULL",
			operation: ast.NewDomainNotNullOperation(true),
			want:      `ALTER DOMAIN "positive" SET NOT NULL;`,
		},
		{
			name:      "drop NOT NULL",
			operation: ast.NewDomainNotNullOperation(false),
			want:      `ALTER DOMAIN "positive" DROP NOT NULL;`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			renderer := postgres.New()

			sql, err := renderer.Render(ast.NewAlterType("positive").AddOperation(test.operation))

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
		})
	}
}

// TestPostgreSQLRenderer_AlterDomain_ReplacementEmitsBothHalves pins the pair.
//
// A replacement that emitted only its drop would leave the domain
// unconstrained, and one that emitted only its add would leave it constrained
// twice; the second of those fails on the next apply and the first fails
// silently, which is worse.
func TestPostgreSQLRenderer_AlterDomain_ReplacementEmitsBothHalves(t *testing.T) {
	c := qt.New(t)
	renderer := postgres.New()

	sql, err := renderer.Render(ast.NewAlterType("positive").AddOperation(
		&ast.DomainConstraintOperation{DropName: "positive_check", AddExpression: "VALUE > 0"},
	))

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `ALTER DOMAIN "positive" DROP CONSTRAINT "positive_check";`)
	c.Assert(sql, qt.Contains, `ALTER DOMAIN "positive" ADD CHECK (VALUE > 0);`)
}

// TestPostgreSQLRenderer_AlterDomain_NamesTheSkipOnATargetWithoutDomains keeps
// the in-place path behind the same key the CREATE is behind.
//
// CockroachDB refuses CREATE DOMAIN, and a target that never took the domain
// must not be handed an ALTER for it either -- the omission is named before
// SQL, the way every other object kind decides it (stokaro/ptah#1738).
func TestPostgreSQLRenderer_AlterDomain_NamesTheSkipOnATargetWithoutDomains(t *testing.T) {
	c := qt.New(t)
	renderer := postgres.NewWithCapabilities(capability.CockroachDB25(), platform.CockroachDB)

	sql, err := renderer.Render(ast.NewAlterType("positive").AddOperation(
		ast.NewDropDomainConstraintOperation("positive_check"),
	))

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Not(qt.Contains), "ALTER DOMAIN")
	c.Assert(sql, qt.Contains, "domain positive is not supported by this target; skipped.")
}
