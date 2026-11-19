package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/engine/builtin"
)

// generatedReadings is table readings whose column g is generated from n with
// kind.
func generatedReadings(kind string) ast.Node {
	table := ast.NewCreateTable("readings")
	table.AddColumn(ast.NewColumn("n", "int"))
	generated := ast.NewColumn("g", "int")
	generated.GeneratedExpression = "n + 1"
	generated.GeneratedKind = kind
	table.AddColumn(generated)
	return table
}

// TestPostgresRenderer_WritesTheKindOfAGeneratedColumn_HappyPath renders a
// generated column of each kind the target builds. A column that names no
// kind is STORED, and a VIRTUAL one is written VIRTUAL where the target has
// capability.VirtualGeneratedColumns: PostgreSQL 18 and CockroachDB.
func TestPostgresRenderer_WritesTheKindOfAGeneratedColumn_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		caps    capability.Capabilities
		kind    string
		want    string
	}{
		{name: "no kind on postgres 17", dialect: "postgres", caps: capability.Postgres17(), kind: "", want: `"g" int GENERATED ALWAYS AS (n + 1) STORED`},
		{name: "stored on postgres 17", dialect: "postgres", caps: capability.Postgres17(), kind: "STORED", want: `"g" int GENERATED ALWAYS AS (n + 1) STORED`},
		{name: "virtual on postgres 18", dialect: "postgres", caps: capability.Postgres18(), kind: "VIRTUAL", want: `"g" int GENERATED ALWAYS AS (n + 1) VIRTUAL`},
		{name: "virtual spelled in lower case on postgres 18", dialect: "postgres", caps: capability.Postgres18(), kind: "virtual", want: `"g" int GENERATED ALWAYS AS (n + 1) VIRTUAL`},
		{name: "virtual on cockroachdb", dialect: "cockroachdb", caps: capability.CockroachDB26(), kind: "VIRTUAL", want: `"g" int GENERATED ALWAYS AS (n + 1) VIRTUAL`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			r, err := builtin.NewRendererWithCapabilities(test.dialect, test.caps)
			c.Assert(err, qt.IsNil)

			sql, err := r.Render(generatedReadings(test.kind))

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
		})
	}
}

// TestPostgresRenderer_WritesTheKindOfAGeneratedColumn_FailurePath refuses a
// VIRTUAL column on a target without capability.VirtualGeneratedColumns, and
// a kind the PostgreSQL family has no clause for. Writing STORED instead would
// build another table than the one declared.
func TestPostgresRenderer_WritesTheKindOfAGeneratedColumn_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		caps    capability.Capabilities
		kind    string
		want    string
	}{
		{
			name: "virtual on postgres 17", dialect: "postgres", caps: capability.Postgres17(), kind: "VIRTUAL",
			want: `.*unsupported feature: postgres on this server line does not support VIRTUAL generated columns \(capability virtual_generated_columns\); column "g" declares one`,
		},
		{
			name: "virtual on yugabytedb", dialect: "yugabytedb", caps: capability.YugabyteDB25(), kind: "VIRTUAL",
			want: `.*unsupported feature: yugabytedb on this server line does not support VIRTUAL generated columns \(capability virtual_generated_columns\); column "g" declares one`,
		},
		{
			name: "persisted on postgres 18", dialect: "postgres", caps: capability.Postgres18(), kind: "PERSISTED",
			want: `.*postgres does not support PERSISTED generated columns; column "g" declares one`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			r, err := builtin.NewRendererWithCapabilities(test.dialect, test.caps)
			c.Assert(err, qt.IsNil)

			sql, err := r.Render(generatedReadings(test.kind))

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

// TestPostgresRenderer_RefusesAVirtualColumnAsUnsupported pins the sentinel a
// caller tells a capability refusal apart by.
func TestPostgresRenderer_RefusesAVirtualColumnAsUnsupported(t *testing.T) {
	c := qt.New(t)
	r, err := builtin.NewRendererWithCapabilities("postgres", capability.Postgres17())
	c.Assert(err, qt.IsNil)

	_, err = r.Render(generatedReadings("VIRTUAL"))

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
}
