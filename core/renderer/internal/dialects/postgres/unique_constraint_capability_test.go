package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer/internal/dialects/postgres"
)

// uniqueConstraintTable is the table-level spelling: CONSTRAINT x UNIQUE (col).
func uniqueConstraintTable() *ast.CreateTableNode {
	return ast.NewCreateTable("users").
		AddColumn(ast.NewColumn("id", "BIGINT").SetPrimary()).
		AddColumn(ast.NewColumn("email", "VARCHAR(255)").SetNotNull()).
		AddConstraint(ast.NewUniqueConstraint("uq_users_email", "email"))
}

// uniqueColumnTable is the column-level spelling: col ... UNIQUE. Spanner
// refuses it with the same sentence, measured, so both reach the same refusal.
func uniqueColumnTable() *ast.CreateTableNode {
	return ast.NewCreateTable("users").
		AddColumn(ast.NewColumn("id", "BIGINT").SetPrimary()).
		AddColumn(ast.NewColumn("email", "VARCHAR(255)").SetNotNull().SetUnique())
}

// TestUniqueConstraint_HappyPath is the control for the refusal below.
//
// This renderer serves four engines and only one of them lacks the capability,
// so a refusal that fired everywhere would be a much larger break wearing the
// same green. Every dialect here must still emit the constraint.
func TestUniqueConstraint_HappyPath(t *testing.T) {
	tests := []struct {
		name         string
		capabilities capability.Capabilities
		dialect      string
		want         string
	}{
		{name: "postgres table-level", capabilities: capability.Postgres16(), dialect: platform.Postgres, want: "UNIQUE"},
		{name: "cockroachdb table-level", capabilities: capability.CockroachDB23(), dialect: platform.CockroachDB, want: "UNIQUE"},
		{name: "yugabytedb table-level", capabilities: capability.YugabyteDB25(), dialect: platform.YugabyteDB, want: "UNIQUE"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			renderer := postgres.NewWithCapabilities(test.capabilities, test.dialect)

			sql, err := renderer.Render(uniqueConstraintTable())

			c.Assert(err, qt.IsNil)
			c.Assert(legacyPostgresSQL(sql), qt.Contains, test.want)
		})
	}
}

// TestUniqueConstraint_FailurePath is stokaro/ptah#2585.
//
// Measured on the Cloud Spanner emulator behind PGAdapter 0.55.2, both
// spellings: `<UNIQUE> constraint is not supported, create a unique index
// instead.` The same statement without the UNIQUE creates, and CREATE UNIQUE
// INDEX on the same column creates, so the refusal is the constraint spelling.
//
// Ptah rendered both and exited 0, which left the author to learn it from the
// server one step later than Ptah already knew.
func TestUniqueConstraint_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		table   *ast.CreateTableNode
		wantErr string
	}{
		{
			name:  "spanner refuses the table-level constraint",
			table: uniqueConstraintTable(),
			wantErr: `error rendering constraint: unsupported feature: spanner: UNIQUE constraint "uq_users_email" ` +
				`cannot be rendered: this target has no UNIQUE constraint and takes the same guarantee as a ` +
				`unique index — declare a unique index on those columns instead`,
		},
		{
			name:  "spanner refuses the column-level constraint",
			table: uniqueColumnTable(),
			wantErr: `error rendering column email: unsupported feature: spanner: the UNIQUE constraint on \(email\) ` +
				`cannot be rendered: this target has no UNIQUE constraint and takes the same guarantee as a ` +
				`unique index — declare a unique index on those columns instead`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			renderer := postgres.NewWithCapabilities(capability.SpannerPostgres(), platform.Spanner)

			sql, err := renderer.Render(test.table)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

// TestUniqueConstraint_CapabilityIsWhatDecides pins the refusal to the key
// rather than to the dialect name.
//
// A renderer told the target has the capability must emit, whatever it is
// called; otherwise the next engine measured to accept UNIQUE would need a
// second place changed, and the one measured to refuse it would keep rendering.
func TestUniqueConstraint_CapabilityIsWhatDecides(t *testing.T) {
	t.Run("spanner with the capability forced on emits", func(t *testing.T) {
		c := qt.New(t)
		caps := capability.SpannerPostgres().With(capability.UniqueConstraints, true)
		renderer := postgres.NewWithCapabilities(caps, platform.Spanner)

		sql, err := renderer.Render(uniqueConstraintTable())

		c.Assert(err, qt.IsNil)
		c.Assert(legacyPostgresSQL(sql), qt.Contains, "UNIQUE")
	})

	t.Run("postgres with the capability forced off refuses", func(t *testing.T) {
		c := qt.New(t)
		caps := capability.Postgres16().With(capability.UniqueConstraints, false)
		renderer := postgres.NewWithCapabilities(caps, platform.Postgres)

		_, err := renderer.Render(uniqueConstraintTable())

		c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	})
}
