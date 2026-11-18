package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/lintext"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

// pgDomains are the domains of the state the analyzed version starts from:
// dd over text with the database default, dc over text with C, dchk with a
// CHECK and dnn with a NOT NULL.
var pgDomains = []lintext.BaselineObject{
	{Version: 2, Kind: lintext.BaselineType, Name: "dd", Domain: true, BaseType: "text"},
	{Version: 2, Kind: lintext.BaselineType, Name: "dc", Domain: true, BaseType: "text", Collation: "C"},
	{Version: 2, Kind: lintext.BaselineType, Name: "dchk", Domain: true, BaseType: "text", Constrained: true},
	{Version: 2, Kind: lintext.BaselineType, Name: "dnn", Domain: true, BaseType: "text", Constrained: true},
}

func analyzePGDomainState(c *qt.C, alter string, column lintext.BaselineColumn) lint.Analysis {
	c.Helper()
	analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fixture(pgFS(alter)), lint.Options{
		Dialect:         "postgres",
		DirFormat:       migrationfile.DirFormatAtlas,
		Selection:       lint.VersionSelection{Versions: []int64{2}, Restricted: true},
		Baseline:        []lintext.BaselineColumn{column},
		BaselineIndexes: []lintext.BaselineIndex{pgIndex("orders_body", "body")},
		BaselineObjects: pgDomains,
	})
	c.Assert(err, qt.IsNil)
	return analysis
}

// TestTypeRewriteRule_ReadsADomainThroughTheBaseline pins PG301 to the type
// changes into and out of a domain measured on PostgreSQL 17.11, an index on
// the column each time: the domain's collation decides whether the index is
// rebuilt, and a NOT NULL or a CHECK on the new domain rewrites the table.
func TestTypeRewriteRule_ReadsADomainThroughTheBaseline(t *testing.T) {
	tests := []struct {
		name   string
		alter  string
		column lintext.BaselineColumn
		want   []string
	}{
		{
			name:   "a C column to a domain with the database default",
			alter:  "ALTER TABLE orders ALTER COLUMN body TYPE dd;",
			column: pgOwnCollated("body", "text", "C"),
			want:   []string{`TYPE dd changes the collation from "C" to the database default`, "(orders_body)"},
		},
		{
			name:   "a default-collated column to a domain with C",
			alter:  "ALTER TABLE orders ALTER COLUMN body TYPE dc;",
			column: pgCollated("body", "text", ""),
			want:   []string{`TYPE dc changes the collation from the database default to "C"`, "(orders_body)"},
		},
		{
			name:   "a column of a domain with C to its base type",
			alter:  "ALTER TABLE orders ALTER COLUMN body TYPE text;",
			column: pgCollated("body", "dc", "C"),
			want:   []string{`TYPE text changes the collation from "C" to the database default`, "(orders_body)"},
		},
		{
			name:   "a column of one domain to another over the same type with another collation",
			alter:  "ALTER TABLE orders ALTER COLUMN body TYPE dc;",
			column: pgCollated("body", "dd", ""),
			want:   []string{`TYPE dc changes the collation from the database default to "C"`},
		},
		{
			name:   "a column of a domain with C that names POSIX, restated",
			alter:  "ALTER TABLE orders ALTER COLUMN body TYPE dc;",
			column: pgOwnCollated("body", "dc", "POSIX"),
			want:   []string{`TYPE dc changes the collation from "POSIX" to "C"`, "(orders_body)"},
		},
		{
			name:   "a column to a domain with a CHECK",
			alter:  "ALTER TABLE orders ALTER COLUMN body TYPE dchk;",
			column: pgCollated("body", "text", ""),
			want:   []string{"TYPE dchk changes the type from text to the domain dchk, whose NOT NULL or CHECK", "rewrites the whole table"},
		},
		{
			name:   "a column to a domain with a NOT NULL",
			alter:  "ALTER TABLE orders ALTER COLUMN body TYPE dnn;",
			column: pgCollated("body", "text", ""),
			want:   []string{"TYPE dnn changes the type from text to the domain dnn", "rewrites the whole table"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzePGDomainState(c, test.alter, test.column)
			c.Assert(pgCostCodes(rulesOf(analysis.Findings())), qt.DeepEquals, []string{"PG301"})
			message := messageOf(analysis.Findings(), "PG301")
			for _, want := range test.want {
				c.Assert(message, qt.Contains, want)
			}
		})
	}
}

// TestTypeRewriteRule_StaysQuietOverADomainOfTheSameCollation pins the
// measured catalog edits: a change between a domain and its base type, or to
// a domain restated, that keeps the collation keeps the table and the index,
// and PG301P names it as one.
func TestTypeRewriteRule_StaysQuietOverADomainOfTheSameCollation(t *testing.T) {
	tests := []struct {
		name   string
		alter  string
		column lintext.BaselineColumn
		want   string
	}{
		{
			name:   "a default-collated column to a domain with the database default",
			alter:  "ALTER TABLE orders ALTER COLUMN body TYPE dd;",
			column: pgCollated("body", "text", ""),
			want:   "TYPE dd changes the type from text to dd over the same base type; PostgreSQL applies the statement as a catalog edit",
		},
		{
			name:   "a column of a domain with the database default to its base type",
			alter:  "ALTER TABLE orders ALTER COLUMN body TYPE text;",
			column: pgCollated("body", "dd", ""),
			want:   "TYPE text changes the type from dd to text over the same base type; PostgreSQL applies the statement as a catalog edit",
		},
		{
			name:   "a column of a domain with C that names C, restated",
			alter:  "ALTER TABLE orders ALTER COLUMN body TYPE dc;",
			column: pgOwnCollated("body", "dc", "C"),
			want:   "ALTER TABLE orders restates its column types; PostgreSQL applies the statement as a catalog edit",
		},
		{
			name:   "a column of a domain with a CHECK restated",
			alter:  "ALTER TABLE orders ALTER COLUMN body TYPE dchk;",
			column: pgCollated("body", "dchk", ""),
			want:   "ALTER TABLE orders restates its column types; PostgreSQL applies the statement as a catalog edit",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzePGDomainState(c, test.alter, test.column)
			c.Assert(pgCostCodes(rulesOf(analysis.Findings())), qt.HasLen, 0)
			c.Assert(messageOf(analysis.Findings(), "PG301P"), qt.Contains, test.want)
		})
	}
}
