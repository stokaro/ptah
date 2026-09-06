package embedpg_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/embedgen"
	"ptah.run/internal/embedpg"
)

// scannable is a specification a source can be built for.
func scannable() embedgen.Spec {
	return embedgen.Spec{
		Source: embedgen.Source{
			Schema: "public", Table: "articles",
			KeyFields: []string{"id"}, InputFields: []string{"title"},
		},
		Model:  embedgen.Model{Provider: "fake", Identifier: "m", ReportedDimension: 4},
		Target: embedgen.Target{Table: "articles", Column: "embedding", Representation: "vector"},
	}
}

// TestNewSource_RefusesASpecificationItCannotScan pins what a keyset needs.
func TestNewSource_RefusesASpecificationItCannotScan(t *testing.T) {
	tests := []struct {
		name   string
		change func(*embedgen.Spec)
		want   string
	}{
		{
			name:   "no table",
			change: func(s *embedgen.Spec) { s.Source.Table = " " },
			want:   "the specification names no source table",
		},
		{
			name:   "no key fields",
			change: func(s *embedgen.Spec) { s.Source.KeyFields = nil },
			want:   "the specification names no key fields, so a scan has nothing to resume after",
		},
		{
			name:   "no input fields",
			change: func(s *embedgen.Spec) { s.Source.InputFields = nil },
			want:   "the specification names no input fields, so there is nothing to embed",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			spec := scannable()
			test.change(&spec)

			_, err := embedpg.NewSource(nil, spec)

			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

// TestNewSource_AcceptsAScannableSpecification is the control: a constructor
// that refused everything would satisfy every row above.
func TestNewSource_AcceptsAScannableSpecification(t *testing.T) {
	c := qt.New(t)

	source, err := embedpg.NewSource(nil, scannable())

	c.Assert(err, qt.IsNil)
	c.Assert(source, qt.IsNotNil)
}

// TestScan_RefusesAnUnboundedLimit keeps a scan from reading a table into
// memory.
//
// Zero is the value a caller gets from a zero BatchBounds, so it is the one
// that arrives by accident. The refusal happens before the query, which is why
// this can be asked without a database.
func TestScan_RefusesAnUnboundedLimit(t *testing.T) {
	tests := []struct {
		name  string
		limit int
	}{
		{name: "zero", limit: 0},
		{name: "negative", limit: -1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			source, err := embedpg.NewSource(nil, scannable())
			c.Assert(err, qt.IsNil)

			_, err = source.Scan(context.Background(), nil, test.limit)

			c.Assert(err, qt.ErrorMatches, `a scan limit of .* would read the whole table into memory`)
		})
	}
}

// TestScan_RefusesACursorOfTheWrongShape keeps a cursor from one specification
// resuming another.
//
// Two components against a one-part key would bind a placeholder the query does
// not have, and the error a driver gives for that names a placeholder rather
// than the cursor.
func TestScan_RefusesACursorOfTheWrongShape(t *testing.T) {
	c := qt.New(t)
	source, err := embedpg.NewSource(nil, scannable())
	c.Assert(err, qt.IsNil)

	_, err = source.Scan(context.Background(), []string{"a", "b"}, 10)

	c.Assert(err, qt.ErrorMatches, `the cursor has 2 components and the key has 1`)
}
