package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/engine/builtin"
)

// TestSQLServerRenderer_WritesTheKindOfAComputedColumn_HappyPath renders a
// computed column of each kind SQL Server builds. STORED and PERSISTED are
// stored, and so is a column that names no kind, SQL Server's default; VIRTUAL
// is computed on read. Without PERSISTED a column declared STORED is computed
// on read, and the server reports it as is_persisted = 0.
func TestSQLServerRenderer_WritesTheKindOfAComputedColumn_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		kind string
		want string
	}{
		{name: "no kind", kind: "", want: "  [g] AS (n + 1) PERSISTED\n"},
		{name: "stored", kind: "STORED", want: "  [g] AS (n + 1) PERSISTED\n"},
		{name: "stored in lower case", kind: "stored", want: "  [g] AS (n + 1) PERSISTED\n"},
		{name: "persisted", kind: "PERSISTED", want: "  [g] AS (n + 1) PERSISTED\n"},
		{name: "virtual", kind: "VIRTUAL", want: "  [g] AS (n + 1)\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			r, err := builtin.NewRenderer("sqlserver")
			c.Assert(err, qt.IsNil)

			sql, err := r.Render(generatedReadings(test.kind))

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
		})
	}
}

// TestSQLServerRenderer_WritesTheKindOfAComputedColumn_FailurePath refuses a
// kind SQL Server has no clause for. Written either way, the column would be
// another column than the one declared.
func TestSQLServerRenderer_WritesTheKindOfAComputedColumn_FailurePath(t *testing.T) {
	c := qt.New(t)
	r, err := builtin.NewRenderer("sqlserver")
	c.Assert(err, qt.IsNil)

	sql, err := r.Render(generatedReadings("MATERIALIZED"))

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `(?s).*column g is generated as MATERIALIZED: SQL Server takes STORED or PERSISTED, `+
		`which it stores, and VIRTUAL, which it computes on read.*`)
	c.Assert(sql, qt.Equals, "")
}
